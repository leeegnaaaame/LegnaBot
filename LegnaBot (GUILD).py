#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# LegnaBot_MERGED.py — merge di ORIGINALE + fix backup/text history + UTC aware

import os, re, io, json, asyncio, shlex, pathlib
from datetime import time as dtime, datetime, timedelta, timezone
from typing import List, Optional, Tuple, Set

import discord
from discord.ext import commands, tasks
import aiohttp
import shutil
from dotenv import load_dotenv

# ================== ENV / TOKEN ==================
BASE_DIR = pathlib.Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env", override=True)

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
if not DISCORD_TOKEN:
    raise SystemExit("❌ DISCORD_TOKEN mancante. Mettilo nel file .env")

# ================== CONFIG (TUOI ID) ==================
GUILD_ID               = 1280153711200436375
VERIFY_CHANNEL_ID      = 1410142029765279794
STAFF_LOG_CHANNEL_ID   = 1410546754780270697

ROLE_VERIFIED_ID       = 1305145247461937193   # "Verificato"
ROLE_UNVERIFIED_ID     = 1410546466250035251   # "Non verificato"

AGE_MIN                = 16
VERIFY_TIMEOUT_MINUTES = 15

# ---- Freeze supervisor (debounce) ----
FREEZE_TICK_SECONDS        = 1
PAUSE_START_DELAY          = 5
ACCUMULATE_WINDOW_SECONDS  = 30
QUIET_GAP_SECONDS          = 5
MAX_BATCH_REMOVE           = 10

# ---- AI Filter ----
SLOWDOWN_MINUTES       = 10
AI_FILTER_ENABLED      = True
AI_FILTER_DELETE_MSG   = True  # cancella il messaggio tossico

# ---- Snapshot ruoli personali ----
USER_ROLE_PRESETS_FILE = "user_role_presets.json"

# ---- Reminders ----
REMINDERS_FILE         = "reminders.json"

# ---- Ticket evoluto ----
TICKET_CONFIG_FILE     = "ticket_config.json"     # salva hub/notify/staff/transcript

# ---- Notifier (API via env) ----
TWITCH_CLIENT_ID       = os.getenv("TWITCH_CLIENT_ID", "")
TWITCH_CLIENT_SECRET   = os.getenv("TWITCH_CLIENT_SECRET", "")
YOUTUBE_API_KEY        = os.getenv("YOUTUBE_API_KEY", "")

PLATFORM_ICONS = {
    "twitch":  "https://static.twitchcdn.net/assets/favicon-256-e29e246c157142c94346.png",
    "youtube": "https://www.youtube.com/s/desktop/fe7f1b0a/img/favicon_144x144.png",
    "kick":    "https://kick.com/favicon.ico",
    "tiktok":  "https://www.tiktok.com/favicon.ico",
}
NOTIFY_FILE            = "notifier_config.json"

# ================== INTENTS / BOT ==================
intents = discord.Intents.default()
intents.message_content = True     # per AI filter
intents.members         = True     # per ruoli/timeout/kick
intents.guilds          = True
ALLOWED = discord.AllowedMentions(everyone=False, roles=False, users=True)
bot = commands.Bot(command_prefix="!", intents=intents, allowed_mentions=ALLOWED)

# ================== BACKUP SERVER ==================

# ================== BACKUP (completo: pannello, lista bottoni, scheduler) ==================
BACKUPS_DIR = "backups"
BACKUP_MAX_ATTACH_MB = 200
BACKUP_CONCURRENCY = 4

# Config per-guild salvata su file
BACKUP_CFG_FILE = "backup_config.json"
if os.path.exists(BACKUP_CFG_FILE):
    with open(BACKUP_CFG_FILE, "r", encoding="utf-8") as _f:
        BACKUP_CFG = json.load(_f)
else:
    BACKUP_CFG = {}  # {guild_id: {enabled, hour, minute, days, include_attachments, keep, last_run_date}}

def _bc_node(gid: int) -> dict:
    key = str(gid)
    node = BACKUP_CFG.get(key)
    if not node:
        node = {
            "enabled": True,
            "hour": 3,
            "minute": 0,
            "days": 0,  # 0=tutta la storia
            "include_attachments": True,
            "keep": 7,
            "last_run_date": ""  # ISO date "YYYY-MM-DD" dell’ultima esecuzione
        }
        BACKUP_CFG[key] = node
        _bc_save()
    # backward-compat
    node.setdefault("enabled", True)
    node.setdefault("hour", 3)
    node.setdefault("minute", 0)
    node.setdefault("days", 0)
    node.setdefault("include_attachments", True)
    node.setdefault("keep", 7)
    node.setdefault("last_run_date", "")
    return node

def _bc_save():
    with open(BACKUP_CFG_FILE, "w", encoding="utf-8") as f:
        json.dump(BACKUP_CFG, f, ensure_ascii=False, indent=2)

# Helpers
os.makedirs(BACKUPS_DIR, exist_ok=True)

def _ts() -> str:
    return discord.utils.utcnow().strftime("%Y%m%d_%H%M%S")

def _safe_name(name: str) -> str:
    return re.sub(r'[^0-9A-Za-z _.-]+', '_', name).strip()[:80] or "unnamed"

def _serialize_role(r: discord.Role) -> dict:
    return {
        "id": r.id, "name": r.name, "color": r.color.value, "hoist": r.hoist,
        "mentionable": r.mentionable, "managed": r.managed, "position": r.position,
        "permissions": r.permissions.value
    }

def _serialize_overwrites(ch: discord.abc.GuildChannel) -> list:
    out = []
    for target, ow in ch.overwrites.items():
        allow, deny = ow.pair()
        out.append({
            "type": "role" if isinstance(target, discord.Role) else "member",
            "id": target.id,
            "allow": allow.value,
            "deny": deny.value
        })
    return out

def _serialize_channel(ch: discord.abc.GuildChannel) -> dict:
    base = {
        "id": ch.id, "name": ch.name, "position": getattr(ch, "position", 0),
        "parent_id": ch.category_id, "overwrites": _serialize_overwrites(ch)
    }
    if isinstance(ch, discord.TextChannel):
        base.update({
            "type": "text",
            "topic": ch.topic, "nsfw": ch.nsfw,
            "slowmode": ch.slowmode_delay
        })
    elif isinstance(ch, discord.VoiceChannel):
        base.update({
            "type": "voice",
            "bitrate": ch.bitrate, "user_limit": ch.user_limit
        })
    elif isinstance(ch, discord.CategoryChannel):
        base.update({"type": "category"})
    elif isinstance(ch, discord.ForumChannel):
        base.update({"type": "forum"})
    elif isinstance(ch, discord.StageChannel):
        base.update({"type": "stage"})
    else:
        base.update({"type": str(ch.type)})
    return base

async def _export_guild_structure(guild: discord.Guild) -> dict:
    roles = sorted(guild.roles, key=lambda r: r.position)
    roles_data = [_serialize_role(r) for r in roles]
    channels = sorted(guild.channels, key=lambda c: (c.category.position if c.category else -1, getattr(c, "position", 0)))
    channels_data = [_serialize_channel(c) for c in channels]
    info = {
        "id": guild.id, "name": guild.name, "owner_id": guild.owner_id,
        "description": guild.description, "icon": str(guild.icon.url) if guild.icon else None,
        "member_count": guild.member_count
    }
    return {"guild": info, "roles": roles_data, "channels": channels_data}

# ---- FIX aiofiles_open: async context manager corretto (nessun warning) ----
try:
    import aiofiles
    def aiofiles_open(path, mode="r", encoding=None):
        return aiofiles.open(path, mode, encoding=encoding)
except Exception:
    # fallback sync in executor
    class _AsyncFile:
        def __init__(self, path, mode, encoding):
            self._path = path; self._mode = mode; self._encoding = encoding
            self._f = None
        async def __aenter__(self):
            loop = asyncio.get_running_loop()
            def _open():
                return open(self._path, self._mode, encoding=self._encoding)
            self._f = await loop.run_in_executor(None, _open)
            return self
        async def __aexit__(self, *a):
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._f.close)
        async def write(self, s: str):
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._f.write, s)
    def aiofiles_open(path, mode="r", encoding=None):
        return _AsyncFile(path, mode, encoding)

# ---- Iterator canali messaggiabili + thread (attivi + archiviati) ----
async def _iter_text_messageables(guild: discord.Guild):
    # 1) tutti i canali testuali
    for tc in guild.text_channels:
        yield tc
        # thread attivi
        for th in list(tc.threads):
            yield th
        # thread archiviati pubblici
        try:
            async for th in tc.archived_threads(limit=None):
                yield th
        except Exception:
            pass

    # 2) forum → thread attivi + archiviati (pubblici e privati)
    for fc in [c for c in guild.channels if isinstance(c, discord.ForumChannel)]:
        for th in list(fc.threads):
            yield th
        # archiviati pubblici
        try:
            async for th in fc.archived_threads(limit=None, private=False):
                yield th
        except Exception:
            pass
        # archiviati privati
        try:
            async for th in fc.archived_threads(limit=None, private=True):
                yield th
        except Exception:
            pass

async def _backup_textlike_target(ch: discord.abc.Messageable, root: pathlib.Path,
                                  since: Optional[datetime], include_attachments: bool) -> tuple[int, int]:
    msgs_dir = root / "channels"; msgs_dir.mkdir(parents=True, exist_ok=True)
    att_root = root / "attachments" / str(ch.id)
    saved_msgs = 0; saved_files = 0
    jsonl = msgs_dir / f"{ch.id}.jsonl"

    async with aiofiles_open(jsonl, "w", encoding="utf-8") as f:
        async for m in ch.history(limit=None, oldest_first=True, after=since):
            saved_msgs += 1
            entry = {
                "id": m.id,
                "created_at": m.created_at.isoformat(),
                "edited_at": m.edited_at.isoformat() if m.edited_at else None,
                "author": {"id": m.author.id, "name": getattr(m.author, "display_name", m.author.name)},
                "content": m.content,
                "pinned": m.pinned,
                "references": m.reference.resolved.id if m.reference and m.reference.resolved else None,
                "embeds": [e.to_dict() for e in m.embeds] if m.embeds else [],
                "stickers": [s.name for s in getattr(m, "stickers", [])] if hasattr(m, "stickers") else [],
                "attachments": []
            }
            if include_attachments and m.attachments:
                att_root.mkdir(parents=True, exist_ok=True)
                for a in m.attachments:
                    if a.size and a.size > BACKUP_MAX_ATTACH_MB * 1024 * 1024:
                        entry["attachments"].append({"filename": a.filename, "url": a.url, "saved": None, "too_big": True})
                        continue
                    dest = att_root / f"{m.id}_{_safe_name(a.filename)}"
                    try:
                        await a.save(dest)
                        saved_files += 1
                        entry["attachments"].append({"filename": a.filename, "url": a.url, "saved": str(dest.relative_to(root))})
                    except Exception:
                        entry["attachments"].append({"filename": a.filename, "url": a.url, "saved": None, "error": True})
                        continue
            await f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    return saved_msgs, saved_files

async def _run_backup(guild: discord.Guild, *, days: int, include_attachments: bool) -> tuple[str, pathlib.Path]:
    ts = _ts()
    root = pathlib.Path(BACKUPS_DIR) / f"{guild.id}_{_safe_name(guild.name)}_{ts}"
    root.mkdir(parents=True, exist_ok=True)

    # 1) struttura
    data = await _export_guild_structure(guild)
    with open(root / "guild.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    # 2) messaggi
    since = None
    if days and days > 0:
        since = discord.utils.utcnow() - timedelta(days=days)

    saved_msgs = 0
    saved_files = 0

    async for target in _iter_text_messageables(guild):
        try:
            m, a = await _backup_textlike_target(target, root, since, include_attachments)
            saved_msgs += m; saved_files += a
        except discord.Forbidden:
            await staff_log(guild, "Backup", f"Permessi mancanti per leggere {getattr(target,'mention',target.id)}.")
        except AttributeError:
            # es. se un ForumChannel venisse passato per errore
            pass
        except Exception as e:
            await staff_log(guild, "Backup", f"Errore su {getattr(target,'mention',target.id)}: {e.__class__.__name__}")

    # 3) zip + report
    zip_path = pathlib.Path(shutil.make_archive(str(root), "zip", root))
    report = (f"📦 Backup completato:\n"
              f"• Cartella: `{root.name}`\n"
              f"• Messaggi salvati: **{saved_msgs}**\n"
              f"• Allegati salvati: **{saved_files}**\n"
              f"• Archivio: `{zip_path.name}` ({zip_path.stat().st_size // (1024*1024)} MB)")
    return report, zip_path

def _prune_backups(guild_id: int, keep_latest: int):
    all_zips = sorted(
        [p for p in pathlib.Path(BACKUPS_DIR).glob(f"{guild_id}_*.zip")],
        key=lambda p: p.stat().st_mtime,
        reverse=True
    )
    for p in all_zips[keep_latest:]:
        try:
            p.unlink()
        except Exception:
            pass

# ---------- Pannello di setup ----------
class BackupSettingsModal(discord.ui.Modal, title="Impostazioni Backup"):
    enabled = discord.ui.TextInput(label="Automatico (true/false)", default="true", max_length=5)
    time_utc = discord.ui.TextInput(label="Ora (UTC) HH:MM", default="03:00", max_length=5)
    days = discord.ui.TextInput(label="Giorni da salvare (0=tutta la storia)", default="0", max_length=6)
    include_att = discord.ui.TextInput(label="Includi allegati (true/false)", default="true", max_length=5)
    keep = discord.ui.TextInput(label="Rotazione: tieni ultimi N archivi", default="7", max_length=4)

    def __init__(self, guild: discord.Guild):
        super().__init__(timeout=180)
        self.guild = guild
        node = _bc_node(guild.id)
        self.enabled.default = "true" if node["enabled"] else "false"
        self.time_utc.default = f'{node["hour"]:02d}:{node["minute"]:02d}'
        self.days.default = str(node["days"])
        self.include_att.default = "true" if node["include_attachments"] else "false"
        self.keep.default = str(node["keep"])

    async def on_submit(self, inter: discord.Interaction):
        node = _bc_node(inter.guild.id)
        node["enabled"] = str(self.enabled.value).strip().lower() == "true"
        try:
            hh, mm = map(int, str(self.time_utc.value).strip().split(":"))
            node["hour"] = max(0, min(23, hh))
            node["minute"] = max(0, min(59, mm))
        except Exception:
            node["hour"], node["minute"] = 3, 0
        try:
            node["days"] = max(0, int(str(self.days.value).strip()))
        except Exception:
            node["days"] = 0
        node["include_attachments"] = str(self.include_att.value).strip().lower() == "true"
        try:
            node["keep"] = max(1, int(str(self.keep.value).strip()))
        except Exception:
            node["keep"] = 7
        _bc_save()
        await inter.response.send_message(
            f"✅ Config salvata:\n"
            f"• Auto: **{node['enabled']}** • Ora UTC: **{node['hour']:02d}:{node['minute']:02d}**\n"
            f"• Giorni: **{node['days']}** • Allegati: **{node['include_attachments']}** • Rotazione: **{node['keep']}**",
            ephemeral=True
        )

class BackupPanelView(discord.ui.View):
    def __init__(self, guild: discord.Guild):
        super().__init__(timeout=120)
        self.guild = guild

    @discord.ui.button(label="✏️ Modifica impostazioni", style=discord.ButtonStyle.primary)
    async def edit_btn(self, inter: discord.Interaction, b: discord.ui.Button):
        if not inter.user.guild_permissions.manage_guild:
            return await inter.response.send_message("Permesso negato.", ephemeral=True)
        await inter.response.send_modal(BackupSettingsModal(inter.guild))

    @discord.ui.button(label="▶️ Esegui adesso", style=discord.ButtonStyle.success)
    async def run_now(self, inter: discord.Interaction, b: discord.ui.Button):
        if not inter.user.guild_permissions.manage_guild:
            return await inter.response.send_message("Permesso negato.", ephemeral=True)
        await inter.response.defer(ephemeral=True, thinking=True)
        node = _bc_node(inter.guild.id)
        report, zip_file = await _run_backup(
            inter.guild,
            days=node["days"],
            include_attachments=node["include_attachments"]
        )
        _prune_backups(inter.guild.id, node["keep"])
        # prova ad allegare se < 20MB
        try:
            if zip_file.stat().st_size <= 20*1024*1024:
                await inter.followup.send(report, file=discord.File(str(zip_file)), ephemeral=True)
            else:
                await inter.followup.send(report + "\n\nIl file è grande: recuperalo dal filesystem del bot.", ephemeral=True)
        except Exception:
            await inter.followup.send(report, ephemeral=True)
        await staff_log(inter.guild, "Backup terminato", report)

@bot.tree.command(name="backup_panel", description="(Admin) Pannello impostazioni backup", guild=discord.Object(id=GUILD_ID))
async def backup_panel_cmd(inter: discord.Interaction):
    if not inter.user.guild_permissions.manage_guild:
        return await inter.response.send_message("Permesso negato.", ephemeral=True)
    node = _bc_node(inter.guild.id)
    txt = (f"**Impostazioni correnti**\n"
           f"• Auto: **{node['enabled']}**\n"
           f"• Ora (UTC): **{node['hour']:02d}:{node['minute']:02d}**\n"
           f"• Giorni da salvare: **{node['days']}** (0=tutto)\n"
           f"• Includi allegati: **{node['include_attachments']}**\n"
           f"• Rotazione: tieni ultimi **{node['keep']}** archivi")
    await inter.response.send_message(txt, view=BackupPanelView(inter.guild), ephemeral=True)

# ---------- Scheduler automatico (ogni 60s) ----------
@tasks.loop(seconds=60)
async def backup_scheduler_loop():
    await bot.wait_until_ready()
    now = discord.utils.utcnow()
    for g in bot.guilds:
        node = _bc_node(g.id)
        if not node["enabled"]:
            continue
        # esegui una sola volta al giorno alla HH:MM configurata
        already = node.get("last_run_date") == now.date().isoformat()
        if already:
            continue
        if now.hour == node["hour"] and now.minute == node["minute"]:
            try:
                report, zip_file = await _run_backup(
                    g,
                    days=node["days"],
                    include_attachments=node["include_attachments"]
                )
                node["last_run_date"] = now.date().isoformat()
                _bc_save()
                _prune_backups(g.id, node["keep"])
                await staff_log(g, "Backup giornaliero", report)
            except Exception as e:
                await staff_log(g, "Backup scheduler", f"Errore: {e.__class__.__name__}")

@backup_scheduler_loop.before_loop
async def _bkwait():
    await bot.wait_until_ready()

def _ts() -> str:
    return discord.utils.utcnow().strftime("%Y%m%d_%H%M%S")

def _safe_name(name: str) -> str:
    return re.sub(r'[^0-9A-Za-z _.-]+', '_', name).strip()[:80] or "unnamed"

def _serialize_role(r: discord.Role) -> dict:
    return {
        "id": r.id, "name": r.name, "color": r.color.value, "hoist": r.hoist,
        "mentionable": r.mentionable, "managed": r.managed, "position": r.position,
        "permissions": r.permissions.value
    }

def _serialize_overwrites(ch: discord.abc.GuildChannel) -> list:
    out = []
    for target, ow in ch.overwrites.items():
        allow, deny = ow.pair()
        out.append({
            "type": "role" if isinstance(target, discord.Role) else "member",
            "id": target.id,
            "allow": allow.value,
            "deny": deny.value
        })
    return out

def _serialize_channel(ch: discord.abc.GuildChannel) -> dict:
    base = {
        "id": ch.id, "name": ch.name, "position": getattr(ch, "position", 0),
        "parent_id": ch.category_id, "overwrites": _serialize_overwrites(ch)
    }
    if isinstance(ch, discord.TextChannel):
        base.update({
            "type": "text",
            "topic": ch.topic, "nsfw": ch.nsfw,
            "slowmode": ch.slowmode_delay
        })
    elif isinstance(ch, discord.VoiceChannel):
        base.update({
            "type": "voice",
            "bitrate": ch.bitrate, "user_limit": ch.user_limit
        })
    elif isinstance(ch, discord.CategoryChannel):
        base.update({"type": "category"})
    elif isinstance(ch, discord.ForumChannel):
        base.update({"type": "forum"})
    elif isinstance(ch, discord.StageChannel):
        base.update({"type": "stage"})
    else:
        base.update({"type": str(ch.type)})
    return base

async def _export_guild_structure(guild: discord.Guild) -> dict:
    roles = sorted(guild.roles, key=lambda r: r.position)
    roles_data = [_serialize_role(r) for r in roles]
    channels = sorted(guild.channels, key=lambda c: (c.category.position if c.category else -1, getattr(c, "position", 0)))
    channels_data = [_serialize_channel(c) for c in channels]
    info = {
        "id": guild.id, "name": guild.name, "owner_id": guild.owner_id,
        "description": guild.description, "icon": str(guild.icon.url) if guild.icon else None,
        "member_count": guild.member_count
    }
    return {"guild": info, "roles": roles_data, "channels": channels_data}

# ---- FIX aiofiles_open: async context manager corretto (nessun warning) ----
try:
    import aiofiles
    def aiofiles_open(path, mode="r", encoding=None):
        return aiofiles.open(path, mode, encoding=encoding)
except Exception:
    # fallback sync in executor
    class _AsyncFile:
        def __init__(self, path, mode, encoding):
            self._path = path; self._mode = mode; self._encoding = encoding
            self._f = None
        async def __aenter__(self):
            loop = asyncio.get_running_loop()
            def _open():
                return open(self._path, self._mode, encoding=self._encoding)
            self._f = await loop.run_in_executor(None, _open)
            return self
        async def __aexit__(self, *a):
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._f.close)
        async def write(self, s: str):
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._f.write, s)
    def aiofiles_open(path, mode="r", encoding=None):
        return _AsyncFile(path, mode, encoding)

# ---- Iterator canali messaggiabili + thread (attivi + archiviati) ----
async def _iter_text_messageables(guild: discord.Guild):
    # 1) tutti i canali testuali
    for tc in guild.text_channels:
        yield tc
        # thread attivi
        for th in list(tc.threads):
            yield th
        # thread archiviati pubblici
        try:
            async for th in tc.archived_threads(limit=None):
                yield th
        except Exception:
            pass

    # 2) forum → thread attivi + archiviati (pubblici e privati)
    for fc in [c for c in guild.channels if isinstance(c, discord.ForumChannel)]:
        for th in list(fc.threads):
            yield th
        # archiviati pubblici
        try:
            async for th in fc.archived_threads(limit=None, private=False):
                yield th
        except Exception:
            pass
        # archiviati privati
        try:
            async for th in fc.archived_threads(limit=None, private=True):
                yield th
        except Exception:
            pass

async def _backup_textlike_target(ch: discord.abc.Messageable, root: pathlib.Path,
                                  since: Optional[datetime], include_attachments: bool) -> tuple[int, int]:
    msgs_dir = root / "channels"; msgs_dir.mkdir(parents=True, exist_ok=True)
    att_root = root / "attachments" / str(ch.id)
    saved_msgs = 0; saved_files = 0
    jsonl = msgs_dir / f"{ch.id}.jsonl"

    async with aiofiles_open(jsonl, "w", encoding="utf-8") as f:
        async for m in ch.history(limit=None, oldest_first=True, after=since):
            saved_msgs += 1
            entry = {
                "id": m.id,
                "created_at": m.created_at.isoformat(),
                "edited_at": m.edited_at.isoformat() if m.edited_at else None,
                "author": {"id": m.author.id, "name": getattr(m.author, "display_name", m.author.name)},
                "content": m.content,
                "pinned": m.pinned,
                "references": m.reference.resolved.id if m.reference and m.reference.resolved else None,
                "embeds": [e.to_dict() for e in m.embeds] if m.embeds else [],
                "stickers": [s.name for s in getattr(m, "stickers", [])] if hasattr(m, "stickers") else [],
                "attachments": []
            }
            if include_attachments and m.attachments:
                att_root.mkdir(parents=True, exist_ok=True)
                for a in m.attachments:
                    if a.size and a.size > BACKUP_MAX_ATTACH_MB * 1024 * 1024:
                        entry["attachments"].append({"filename": a.filename, "url": a.url, "saved": None, "too_big": True})
                        continue
                    dest = att_root / f"{m.id}_{_safe_name(a.filename)}"
                    try:
                        await a.save(dest)
                        saved_files += 1
                        entry["attachments"].append({"filename": a.filename, "url": a.url, "saved": str(dest.relative_to(root))})
                    except Exception:
                        entry["attachments"].append({"filename": a.filename, "url": a.url, "saved": None, "error": True})
                        continue
            await f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    return saved_msgs, saved_files

# ---------- Pannello di setup ----------
class BackupSettingsModal(discord.ui.Modal, title="Impostazioni Backup"):
    enabled = discord.ui.TextInput(label="Automatico (true/false)", default="true", max_length=5)
    time_utc = discord.ui.TextInput(label="Ora (UTC) HH:MM", default="03:00", max_length=5)
    days = discord.ui.TextInput(label="Giorni da salvare (0=tutta la storia)", default="0", max_length=6)
    include_att = discord.ui.TextInput(label="Includi allegati (true/false)", default="true", max_length=5)
    keep = discord.ui.TextInput(label="Rotazione: tieni ultimi N archivi", default="7", max_length=4)

    def __init__(self, guild: discord.Guild):
        super().__init__(timeout=180)
        self.guild = guild
        node = _bc_node(guild.id)
        self.enabled.default = "true" if node["enabled"] else "false"
        self.time_utc.default = f'{node["hour"]:02d}:{node["minute"]:02d}'
        self.days.default = str(node["days"])
        self.include_att.default = "true" if node["include_attachments"] else "false"
        self.keep.default = str(node["keep"])

    async def on_submit(self, inter: discord.Interaction):
        node = _bc_node(inter.guild.id)
        node["enabled"] = str(self.enabled.value).strip().lower() == "true"
        try:
            hh, mm = map(int, str(self.time_utc.value).strip().split(":"))
            node["hour"] = max(0, min(23, hh))
            node["minute"] = max(0, min(59, mm))
        except Exception:
            node["hour"], node["minute"] = 3, 0
        try:
            node["days"] = max(0, int(str(self.days.value).strip()))
        except Exception:
            node["days"] = 0
        node["include_attachments"] = str(self.include_att.value).strip().lower() == "true"
        try:
            node["keep"] = max(1, int(str(self.keep.value).strip()))
        except Exception:
            node["keep"] = 7
        _bc_save()
        await inter.response.send_message(
            f"✅ Config salvata:\n"
            f"• Auto: **{node['enabled']}** • Ora UTC: **{node['hour']:02d}:{node['minute']:02d}**\n"
            f"• Giorni: **{node['days']}** • Allegati: **{node['include_attachments']}** • Rotazione: **{node['keep']}**",
            ephemeral=True
        )

class BackupPanelView(discord.ui.View):
    def __init__(self, guild: discord.Guild):
        super().__init__(timeout=120)
        self.guild = guild

    @discord.ui.button(label="✏️ Modifica impostazioni", style=discord.ButtonStyle.primary)
    async def edit_btn(self, inter: discord.Interaction, b: discord.ui.Button):
        if not inter.user.guild_permissions.manage_guild:
            return await inter.response.send_message("Permesso negato.", ephemeral=True)
        await inter.response.send_modal(BackupSettingsModal(inter.guild))

    @discord.ui.button(label="▶️ Esegui adesso", style=discord.ButtonStyle.success)
    async def run_now(self, inter: discord.Interaction, b: discord.ui.Button):
        if not inter.user.guild_permissions.manage_guild:
            return await inter.response.send_message("Permesso negato.", ephemeral=True)
        await inter.response.defer(ephemeral=True, thinking=True)
        node = _bc_node(inter.guild.id)
        report, zip_file = await _run_backup(
            inter.guild,
            days=node["days"],
            include_attachments=node["include_attachments"]
        )
        _prune_backups(inter.guild.id, node["keep"])
        # prova ad allegare se < 20MB
        try:
            if zip_file.stat().st_size <= 20*1024*1024:
                await inter.followup.send(report, file=discord.File(str(zip_file)), ephemeral=True)
            else:
                await inter.followup.send(report + "\n\nIl file è grande: recuperalo dal filesystem del bot.", ephemeral=True)
        except Exception:
            await inter.followup.send(report, ephemeral=True)
        await staff_log(inter.guild, "Backup terminato", report)

# ========= BACKUP: LISTA INTERATTIVA (VERSIONE ROBUSTA) =========
import uuid
import traceback

class BackupListView(discord.ui.View):
    def __init__(self, author_id: int):
        super().__init__(timeout=180)  # NON persistente
        self.author_id = author_id
        self.files = sorted(
            [p for p in pathlib.Path(BACKUPS_DIR).glob("*.zip")],
            key=lambda x: x.stat().st_mtime, reverse=True
        )
        self.selected_index: Optional[int] = None

        # Select dinamico (niente custom_id perché NON persistente)
        options = [
                      discord.SelectOption(
                          label=p.name[:100],
                          value=str(i),
                          description=f"{p.stat().st_size // (1024*1024)} MB"
                      )
                      for i, p in enumerate(self.files[:25])
                  ] or [discord.SelectOption(label="(nessun backup)", value="-1", default=True)]

        self.select = discord.ui.Select(
            placeholder="Scegli uno ZIP...",
            min_values=1, max_values=1,
            options=options
        )
        self.select.callback = self._on_select
        self.add_item(self.select)

        # Bottoni
        self.send_btn = discord.ui.Button(label="📤 Invia", style=discord.ButtonStyle.primary)
        self.del_btn  = discord.ui.Button(label="🗑️ Elimina", style=discord.ButtonStyle.danger)
        self.refresh_btn = discord.ui.Button(label="🔄 Aggiorna", style=discord.ButtonStyle.secondary)
        self.send_btn.callback = self._send
        self.del_btn.callback  = self._delete
        self.refresh_btn.callback = self._refresh

        self.add_item(self.send_btn)
        self.add_item(self.del_btn)
        self.add_item(self.refresh_btn)

    async def _guard(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("Questa view non è tua.", ephemeral=True)
            return False
        return True

    async def _on_select(self, interaction: discord.Interaction):
        if not await self._guard(interaction): return
        try:
            self.selected_index = int(self.select.values[0])
        except Exception:
            self.selected_index = None
        await interaction.response.defer()  # ack rapido

    async def _send(self, interaction: discord.Interaction):
        if not await self._guard(interaction): return
        if self.selected_index is None or self.selected_index < 0 or self.selected_index >= len(self.files):
            return await interaction.response.send_message("Seleziona prima un file.", ephemeral=True)

        path = self.files[self.selected_index]
        # se vuoi inviare nel canale pubblico:
        await interaction.response.defer()  # ack entro 3s
        if path.stat().st_size <= 20 * 1024 * 1024:
            try:
                await interaction.channel.send(file=discord.File(str(path)))
            except Exception:
                await interaction.followup.send("Impossibile inviare il file qui.", ephemeral=True)
        else:
            await interaction.followup.send("File troppo grande per essere inviato su Discord (>20MB).", ephemeral=True)

    async def _delete(self, interaction: discord.Interaction):
        if not await self._guard(interaction): return
        if self.selected_index is None or self.selected_index < 0 or self.selected_index >= len(self.files):
            return await interaction.response.send_message("Seleziona prima un file.", ephemeral=True)
        path = self.files[self.selected_index]
        try:
            path.unlink()
            await interaction.response.send_message(f"🗑️ Eliminato `{path.name}`.", ephemeral=True)
        except Exception:
            await interaction.response.send_message("Non sono riuscito a eliminare il file.", ephemeral=True)

    async def _refresh(self, interaction: discord.Interaction):
        if not await self._guard(interaction): return
        # rilegge i file e ricrea le options
        self.files = sorted(
            [p for p in pathlib.Path(BACKUPS_DIR).glob("*.zip")],
            key=lambda x: x.stat().st_mtime, reverse=True
        )
        options = [
                      discord.SelectOption(
                          label=p.name[:100],
                          value=str(i),
                          description=f"{p.stat().st_size // (1024*1024)} MB"
                      )
                      for i, p in enumerate(self.files[:25])
                  ] or [discord.SelectOption(label="(nessun backup)", value="-1", default=True)]
        self.select.options = options
        self.selected_index = None
        await interaction.response.edit_message(view=self)

@bot.tree.command(name="backup_list_bottoni", description="(Admin) Lista ZIP con bottoni", guild=discord.Object(id=GUILD_ID))
async def backup_list_bottoni(inter: discord.Interaction):
    if not inter.user.guild_permissions.manage_guild:
        return await inter.response.send_message("❌ Permesso negato.", ephemeral=True)

    view = BackupListView(inter.user.id)
    # ⚠️ NON fare bot.add_view(view) qui!
    await inter.response.send_message("Scegli un archivio:", view=view, ephemeral=True)

    # Usa message hooks per il router (discord.py invoca i callback degli items;
    # qui intercettiamo i custom_id dei bottoni creati manualmente)
    bot.add_view(view)  # mantiene viva la view finché timeout
    await inter.response.send_message("Scegli un archivio:", view=view, ephemeral=True)

# ---------- Scheduler automatico (ogni 60s) ----------
@tasks.loop(seconds=60)
async def backup_scheduler_loop():
    await bot.wait_until_ready()
    now = discord.utils.utcnow()
    for g in bot.guilds:
        node = _bc_node(g.id)
        if not node["enabled"]:
            continue
        # esegui una sola volta al giorno alla HH:MM configurata
        already = node.get("last_run_date") == now.date().isoformat()
        if already:
            continue
        if now.hour == node["hour"] and now.minute == node["minute"]:
            try:
                report, zip_file = await _run_backup(
                    g,
                    days=node["days"],
                    include_attachments=node["include_attachments"]
                )
                node["last_run_date"] = now.date().isoformat()
                _bc_save()
                _prune_backups(g.id, node["keep"])
                await staff_log(g, "Backup giornaliero", report)
            except Exception as e:
                await staff_log(g, "Backup scheduler", f"Errore: {e.__class__.__name__}")

@backup_scheduler_loop.before_loop
async def _bkwait():
    await bot.wait_until_ready()


os.makedirs(BACKUPS_DIR, exist_ok=True)

async def _iter_message_sources(guild: discord.Guild):
    """Yield di tutti i target *messaggiabili*:
       - TextChannel
       - thread (attivi + archiviati) dei TextChannel
       - thread (attivi + archiviati) dei Forum
    """
    # TextChannel + threads
    for ch in guild.text_channels:
        yield ch
        for t in ch.threads:
            yield t
        if hasattr(ch, "archived_threads"):
            try:
                async for t in ch.archived_threads(limit=None):
                    yield t
            except TypeError:
                try:
                    async for t in ch.archived_threads(private=True, limit=None):
                        yield t
                except Exception:
                    pass

    # Forum -> solo threads
    forums = [c for c in guild.channels if isinstance(c, discord.ForumChannel)]
    for fch in forums:
        for t in fch.threads:
            yield t
        if hasattr(fch, "archived_threads"):
            try:
                async for t in fch.archived_threads(limit=None):
                    yield t
            except TypeError:
                try:
                    async for t in fch.archived_threads(private=True, limit=None):
                        yield t
                except Exception:
                    pass


SNAPSHOT_FILE = "role_freeze_snapshots.json"
if os.path.exists(SNAPSHOT_FILE):
    with open(SNAPSHOT_FILE, "r", encoding="utf-8") as f:
        ROLE_SNAPSHOTS = json.load(f)
else:
    ROLE_SNAPSHOTS = {}  # {user_id: [role_ids]}

def has_snapshot(uid: int) -> bool:
    return str(uid) in ROLE_SNAPSHOTS

def get_snapshot(uid: int) -> List[int]:
    return ROLE_SNAPSHOTS.get(str(uid), [])

def set_snapshot(uid: int, ids: List[int]):
    ROLE_SNAPSHOTS[str(uid)] = ids
    with open(SNAPSHOT_FILE, "w", encoding="utf-8") as f:
        json.dump(ROLE_SNAPSHOTS, f, ensure_ascii=False)

def clear_snapshot(uid: int):
    ROLE_SNAPSHOTS.pop(str(uid), None)
    with open(SNAPSHOT_FILE, "w", encoding="utf-8") as f:
        json.dump(ROLE_SNAPSHOTS, f, ensure_ascii=False)

async def post_verified(member: discord.Member, reason: str = "Age gate: marked verified"):
    guild = member.guild

    # ripristina snapshot
    snap = get_snapshot(member.id)
    to_add = []
    for rid in snap:
        r = guild.get_role(rid)
        if r and r not in member.roles:
            to_add.append(r)
    if to_add:
        await add_roles_safely(member, to_add, reason="Age gate: restore after verify")

    r_ver = guild.get_role(ROLE_VERIFIED_ID)
    r_unv = guild.get_role(ROLE_UNVERIFIED_ID)

    if r_ver and r_ver not in member.roles:
        try: await member.add_roles(r_ver, reason=reason)
        except Exception: pass

    if r_unv and r_unv in member.roles:
        try: await member.remove_roles(r_unv, reason=reason)
        except Exception: pass

    clear_snapshot(member.id)
    grant_bypass(member.id, 120)  # tempo per completare modifiche senza freeze
    await staff_log(guild, "Verifica completata/forzata", f"{member.mention} → Verificato ({reason}).")

async def staff_log(guild: discord.Guild, title: str, body: str):
    ch = guild.get_channel(STAFF_LOG_CHANNEL_ID)
    if not ch:
        return
    emb = discord.Embed(title=title, description=body, color=discord.Color.red(), timestamp=discord.utils.utcnow())
    try:
        await ch.send(embed=emb, allowed_mentions=ALLOWED)
    except Exception:
        pass

async def remove_roles_safely(member: discord.Member, roles: List[discord.Role], reason: str = ""):
    for r in roles:
        try:
            await member.remove_roles(r, reason=reason)
        except discord.Forbidden:
            await staff_log(member.guild, "Rimozione negata", f"Non posso togliere **{r.name}** a {member.mention}.")
        except discord.HTTPException:
            await asyncio.sleep(1.0)
            try:
                await member.remove_roles(r, reason=reason)
            except Exception:
                pass
        await asyncio.sleep(0.3)

async def add_roles_safely(member: discord.Member, roles: List[discord.Role], reason: str = ""):
    for r in roles:
        try:
            await member.add_roles(r, reason=reason)
        except discord.Forbidden:
            await staff_log(member.guild, "Assegnazione negata", f"Non posso assegnare **{r.name}** a {member.mention}.")
        except discord.HTTPException:
            await asyncio.sleep(1.0)
            try:
                await member.add_roles(r, reason=reason)
            except Exception:
                pass
        await asyncio.sleep(0.3)

FREEZE_SKIP: Set[int] = set()   # utenti che hanno cliccato 16+
KICKED_IDS: Set[int]  = set()   # marcati come espulsi

# === AgeGate helpers / bypass ===
ADMIN_BYPASS: dict[int, float] = {}  # user_id -> unix expiry

def is_verified(member: discord.Member) -> bool:
    r_ver = member.guild.get_role(ROLE_VERIFIED_ID)
    return bool(r_ver and r_ver in member.roles)

def in_bypass(uid: int) -> bool:
    exp = ADMIN_BYPASS.get(uid, 0.0)
    return exp and exp > asyncio.get_event_loop().time()

def grant_bypass(uid: int, seconds: int = 90):
    ADMIN_BYPASS[uid] = asyncio.get_event_loop().time() + max(1, seconds)

async def maybe_admin_bypass(after: discord.Member) -> bool:
    """
    Se uno staffer ha appena cambiato i ruoli (Audit Log), non freezare.
    Richiede il permesso 'View Audit Log' al bot.
    """
    try:
        async for e in after.guild.audit_logs(limit=3, action=discord.AuditLogAction.member_role_update):
            if getattr(e.target, "id", None) != after.id:
                continue
            # entry recente (5s)
            if (discord.utils.utcnow() - e.created_at).total_seconds() > 5:
                continue
            actor = e.user
            if actor and actor.id != bot.user.id:
                # se è (o era) staff → bypass 90s
                if not isinstance(actor, discord.Member) or actor.guild_permissions.manage_roles or actor.guild_permissions.administrator:
                    grant_bypass(after.id, 90)
                    return True
        return False
    except Exception:
        # se non riesco a leggere gli audit log non faccio bypass
        return False

# ================== VIEWS (definite PRIMA di setup_hook!) ==================
class AgeVerifyView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="🟢 Ho 16+ anni", style=discord.ButtonStyle.success, custom_id="verify_16_plus")
    async def plus16(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        member: discord.Member = interaction.user

        # sblocca il supervisor
        FREEZE_SKIP.add(member.id)

        # ripristina gli eventuali ruoli "in pausa"
        snap = get_snapshot(member.id)
        to_add = []
        for rid in snap:
            r = guild.get_role(rid)
            if r and r not in member.roles:
                to_add.append(r)
        if to_add:
            await add_roles_safely(member, to_add, reason="Age gate: restore after verify")

        # ruolo Verificato
        r_ver = guild.get_role(ROLE_VERIFIED_ID)
        r_unv = guild.get_role(ROLE_UNVERIFIED_ID)

        if r_ver and r_ver not in member.roles:
            try:
                await member.add_roles(r_ver, reason="Age gate: confermato 16+")
            except discord.Forbidden:
                await staff_log(guild, "Permessi mancanti",
                                f"Non riesco ad aggiungere **Verificato** a {member.mention}. "
                                "Sposta il ruolo del bot sopra 'Verificato' e abilita Gestisci Ruoli.")

        # rimuovi il Non verificato
        if r_unv and r_unv in member.roles:
            try:
                await member.remove_roles(r_unv, reason="Age gate: confermato 16+")
            except Exception:
                await staff_log(guild, "Errore rimozione",
                                f"Non sono riuscito a togliere **Non verificato** a {member.mention}.")

        clear_snapshot(member.id)

        grant_bypass(member.id, 120)

        # feedback + log
        await interaction.response.send_message("✅ Verifica completata. Benvenuto!", ephemeral=True)
        await staff_log(guild, "Verifica completata", f"{member.mention} ha confermato 16+ → **Verificato** assegnato e ruoli ripristinati.")

    @discord.ui.button(label="🔴 Ho meno di 16", style=discord.ButtonStyle.danger, custom_id="verify_under_16")
    async def under16(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        member: discord.Member = interaction.user
        try:
            try:
                await member.send(f"❌ Sei minorenne (<{AGE_MIN}). Verrai espulso.")
            except Exception:
                pass
            KICKED_IDS.add(member.id)
            await interaction.response.send_message("⏳ Espulsione in corso…", ephemeral=True)
            await asyncio.sleep(0.8)
            await member.kick(reason=f"Age gate: minorenne (<{AGE_MIN})")
            clear_snapshot(member.id)
            FREEZE_SKIP.discard(member.id)
            await staff_log(guild, "Kick minorenne", f"{member.mention} espulso via bottone <16.")
        except discord.Forbidden:
            await interaction.followup.send("⚠️ Non ho i permessi per espellerti. Lo staff verrà avvisato.", ephemeral=True)
            await staff_log(guild, "Kick fallito", f"Permessi insufficienti per kick {member.mention}.")

class TicketPanelViewBasic(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="🎫 Apri Ticket", style=discord.ButtonStyle.success, custom_id="tk_open_basic")
    async def open_ticket(self, inter: discord.Interaction, b: discord.ui.Button):
        node = _tk_node(inter.guild.id)
        hub = inter.guild.get_channel(node["hub_channel_id"])
        staff_role = inter.guild.get_role(node["staff_role_id"])
        if not hub or not staff_role:
            return await inter.response.send_message("Ticket hub/ruolo non configurato. Usa /ticket_setup_panel", ephemeral=True)

        thread = await hub.create_thread(
            name=f"🎫 {inter.user.display_name}",
            type=discord.ChannelType.private_thread,
            invitable=False
        )
        await thread.add_user(inter.user)

        view = TicketControlsView(thread.id, staff_role.id)
        emb = discord.Embed(
            title="Nuovo Ticket",
            description=f"**Autore:** {inter.user.mention}\n*(ticket base)*",
            color=discord.Color.red(), timestamp=discord.utils.utcnow()
        )
        await thread.send(content=staff_role.mention, embed=emb, view=view)
        await inter.response.send_message(f"✅ Ticket creato: {thread.mention}", ephemeral=True)

        notif = discord.Embed(
            title="🟢 Ticket aperto",
            description=f"**Utente:** {inter.user.mention}\n**Thread:** {thread.mention}",
            color=discord.Color.green(), timestamp=discord.utils.utcnow()
        )
        await _ticket_notify(inter.guild, notif)

# ================== AGE GATE: JOIN + SUPERVISOR ==================
async def instant_freeze(member: discord.Member):
    if in_bypass(member.id) or is_verified(member):
        return

    guild = member.guild
    r_unv = guild.get_role(ROLE_UNVERIFIED_ID)

    if r_unv and r_unv not in member.roles:
        try:
            await member.add_roles(r_unv, reason="Age gate: enforce Unverified (instant)")
        except Exception:
            pass

    exclude = {ROLE_UNVERIFIED_ID, ROLE_VERIFIED_ID}
    active = [r for r in member.roles if not r.is_default() and r.id not in exclude]
    if not active:
        return

    # salva snapshot e rimuove solo i ruoli 'extra'
    already = set(get_snapshot(member.id)) if has_snapshot(member.id) else set()
    newset = already | {r.id for r in active}
    if newset != already:
        set_snapshot(member.id, list(newset))

    await remove_roles_safely(member, active[:MAX_BATCH_REMOVE], reason="Age gate: instant freeze")

@bot.event
async def on_member_update(before: discord.Member, after: discord.Member):
    if after.guild.id != GUILD_ID or after.bot:
        return

    # Se uno staffer ha appena cambiato i ruoli → bypass e non toccare
    if await maybe_admin_bypass(after):
        return

    before_ver = any(r.id == ROLE_VERIFIED_ID for r in before.roles)
    after_ver  = any(r.id == ROLE_VERIFIED_ID for r in after.roles)

    # Se è appena diventato Verificato (es. staff assegna il ruolo) completa il flusso
    if not before_ver and after_ver:
        await post_verified(after, reason="staff/manual")
        return

    # Se è già verificato o in bypass → mai freeze
    if after_ver or in_bypass(after.id):
        return

    # Solo utenti NON verificati possono essere freezati
    if {r.id for r in before.roles} != {r.id for r in after.roles}:
        await instant_freeze(after)

@bot.event
async def on_member_join(member: discord.Member):
    if member.guild.id != GUILD_ID:
        return

    r_unv = member.guild.get_role(ROLE_UNVERIFIED_ID)
    r_ver = member.guild.get_role(ROLE_VERIFIED_ID)

    if r_ver and r_ver in member.roles:
        try:
            await member.remove_roles(r_ver, reason="Age gate: override onboarding Verified")
            await staff_log(member.guild, "Override Verified all'ingresso", f"Rimosso 'Verificato' da {member.mention}.")
        except Exception:
            pass

    if r_unv and r_unv not in member.roles:
        try:
            await member.add_roles(r_unv, reason="Age gate: join -> Unverified")
        except Exception:
            await staff_log(member.guild, "Permessi mancanti", f"Non posso assegnare **Non verificato** a {member.mention}.")

    ch = member.guild.get_channel(VERIFY_CHANNEL_ID)
    if ch:
        try:
            await ch.send(
                f"Benvenuto {member.mention}! Clicca un pulsante qui sotto per verificare l'età.",
                view=AgeVerifyView(),
                delete_after=300  # aumenta/TOGLI questo parametro se vuoi che resti per sempre
            )
        except Exception:
            pass

    bot.loop.create_task(freeze_supervisor(member.id))
    bot.loop.create_task(verify_timeout_task(member.id))

async def freeze_supervisor(user_id: int):
    await asyncio.sleep(PAUSE_START_DELAY)
    guild = bot.get_guild(GUILD_ID)
    if not guild:
        return

    await staff_log(guild, "FREEZE START", f"Avvio supervisor per ID {user_id} (debounced).")

    first_log = False
    start_ts = discord.utils.utcnow()
    last_change_ts = None
    accumulating = True
    last_seen_active_ids: Set[int] = set()

    while True:
        if user_id in FREEZE_SKIP or in_bypass(user_id):
            break

        try:
            member = guild.get_member(user_id) or await guild.fetch_member(user_id)
        except Exception:
            member = None
        if member is None:
            break

        # stop se è verificato
        if is_verified(member):
            break

        r_unv = guild.get_role(ROLE_UNVERIFIED_ID)
        if r_unv and r_unv not in member.roles:
            try:
                await member.add_roles(r_unv, reason="Age gate: enforce Unverified")
            except Exception:
                await staff_log(guild, "Permessi mancanti", f"Non posso assegnare 'Non verificato' a {member.mention}.")

        exclude = {ROLE_UNVERIFIED_ID, ROLE_VERIFIED_ID}
        active_roles = [r for r in member.roles if (not r.is_default()) and (r.id not in exclude)]
        active_ids = {r.id for r in active_roles}

        if active_ids != last_seen_active_ids:
            last_seen_active_ids = active_ids
            last_change_ts = discord.utils.utcnow()
            if active_roles:
                names = ", ".join(f"{r.name}({r.id})" for r in active_roles)
                await staff_log(guild, "FREEZE SEEN", f"{member.mention}: ruoli da mettere in pausa: {names}")

        now = discord.utils.utcnow()

        if accumulating:
            if (now - start_ts).total_seconds() >= ACCUMULATE_WINDOW_SECONDS:
                accumulating = False
                await staff_log(guild, "FREEZE ACCUMULO FINITO", f"{member.mention}: passo a quiet-gap dopo {ACCUMULATE_WINDOW_SECONDS}s.")
            await asyncio.sleep(FREEZE_TICK_SECONDS)
            continue

        if last_change_ts and (now - last_change_ts).total_seconds() < QUIET_GAP_SECONDS:
            await asyncio.sleep(FREEZE_TICK_SECONDS)
            continue

        if active_roles:
            already = set(get_snapshot(member.id)) if has_snapshot(member.id) else set()
            newset = already | {r.id for r in active_roles}
            if newset != already:
                set_snapshot(member.id, list(newset))
                if not first_log:
                    await staff_log(guild, "Snapshot creato", f"Catturati {len(newset)} ruoli per {member.mention}.")
                    first_log = True

            batch = active_roles[:MAX_BATCH_REMOVE]
            await staff_log(guild, "FREEZE REMOVE", f"{member.mention}: rimuovo {len(batch)} ruoli in batch.")
            await remove_roles_safely(member, batch, reason="Age gate: FREEZE (debounced)")
            last_seen_active_ids = set()
            last_change_ts = discord.utils.utcnow()
            await asyncio.sleep(FREEZE_TICK_SECONDS)
            continue

        await asyncio.sleep(FREEZE_TICK_SECONDS)

    member = guild.get_member(user_id)
    if member and is_verified(member):
        msg = f"{member.mention} risulta **Verificato**. Supervisor terminato."
    elif user_id in FREEZE_SKIP:
        msg = f"{member.mention if member else user_id} ha completato la verifica (16+)."
    elif member is None:
        msg = f"ID {user_id} ha lasciato il server."
    elif user_id in KICKED_IDS:
        msg = f"{member.mention if member else user_id} espulso per età < {AGE_MIN}."
    elif in_bypass(user_id):
        msg = f"{member.mention if member else user_id}: bypass admin attivo, termino."
    else:
        msg = f"{member.mention if member else user_id}: supervisor terminato."
    await staff_log(guild, "Freeze terminato", msg)
    KICKED_IDS.discard(user_id)
    FREEZE_SKIP.discard(user_id)

async def verify_timeout_task(user_id: int):
    await asyncio.sleep(VERIFY_TIMEOUT_MINUTES * 60)
    guild = bot.get_guild(GUILD_ID)
    member = guild.get_member(user_id) if guild else None
    if not member:
        return
    r_ver = guild.get_role(ROLE_VERIFIED_ID)
    if r_ver and r_ver in member.roles:
        return
    if not has_snapshot(member.id):
        current = [r.id for r in member.roles if not r.is_default()
                   and r.id not in {ROLE_UNVERIFIED_ID, ROLE_VERIFIED_ID}]
        set_snapshot(member.id, current)
    await staff_log(guild, "Timeout verifica",
                    f"{member.mention} non ha risposto entro {VERIFY_TIMEOUT_MINUTES} min. Ruoli in pausa (snapshot salvato).")

# ================== AI FILTER (IT/EN/ES) ==================
TOXIC_PATTERNS = [
    r"\b(coglione|stronzo|vaffanculo|merda|cretino|idiota|pezzo di m)\b",
    r"\b(fuck|shit|asshole|bitch|dumbass|retard|stupid)\b",
    r"\b(pendejo|puto|mierda|jodete|imb[eé]cil|idiota|perra)\b",
]
SEVERE_PATTERNS = [ r"\b(ammazzo|ti uccido|muori|ti vengo a prendere|die)\b" ]
tox_re = [re.compile(p, re.IGNORECASE) for p in TOXIC_PATTERNS]
sev_re = [re.compile(p, re.IGNORECASE) for p in SEVERE_PATTERNS]

def toxicity_score(text: str) -> int:
    s = 0
    for r in tox_re:
        if r.search(text): s += 1
    for r in sev_re:
        if r.search(text): s += 2
    if len(text) >= 12 and sum(1 for c in text if c.isupper()) > len(text)*0.6: s += 1
    return s

TIMED_RECENT: Set[int] = set()

@bot.event
async def on_message(message: discord.Message):
    if not AI_FILTER_ENABLED or message.guild is None or message.author.bot:
        return
    if message.guild.id != GUILD_ID:
        return
    if isinstance(message.author, discord.Member) and message.author.guild_permissions.manage_messages:
        return
    txt = message.content or ""
    if toxicity_score(txt) >= 2:
        if AI_FILTER_DELETE_MSG:
            try:
                await message.delete()
            except Exception:
                pass
        member: discord.Member = message.author
        if member.id not in TIMED_RECENT:
            TIMED_RECENT.add(member.id)
            try:
                until = discord.utils.utcnow() + timedelta(minutes=SLOWDOWN_MINUTES)
                await member.timeout(until=until, reason="Flame rilevato (AI filter)")
            except discord.Forbidden:
                await staff_log(message.guild, "AI Filter: permesso mancante", f"Non posso timeout {member.mention}. Serve Moderate Members.")
            try:
                await member.send(f"⚠️ Sei in **slowdown {SLOWDOWN_MINUTES} min** per flame. Evita insulti/attacchi.")
            except Exception:
                pass
            prev = (txt[:170] + "…") if len(txt) > 180 else txt
            await staff_log(message.guild, "AI Filter: timeout applicato", f"{member.mention} → {SLOWDOWN_MINUTES} min.\nMsg: {prev}")
            async def _cool():
                await asyncio.sleep(120); TIMED_RECENT.discard(member.id)
            bot.loop.create_task(_cool())
    await bot.process_commands(message)

# ================== SNAPSHOT RUOLI PERSONALI (comandi /roles_*) ==================
if os.path.exists(USER_ROLE_PRESETS_FILE):
    with open(USER_ROLE_PRESETS_FILE, "r", encoding="utf-8") as f:
        USER_PRESETS = json.load(f)
else:
    USER_PRESETS = {}  # {guild: {user: {name: [ids]}}}

def _presets_node(gid: int, uid: int, create=True):
    g = USER_PRESETS.get(str(gid))
    if g is None and create:
        USER_PRESETS[str(gid)] = {}; g = USER_PRESETS[str(gid)]
    u = g.get(str(uid)) if g else None
    if u is None and create:
        g[str(uid)] = {}; u = g[str(uid)]
    return u

def _save_presets():
    with open(USER_ROLE_PRESETS_FILE, "w", encoding="utf-8") as f:
        json.dump(USER_PRESETS, f, ensure_ascii=False)

EXCLUDE_ROLE_IDS = {ROLE_UNVERIFIED_ID, ROLE_VERIFIED_ID}

@bot.tree.command(name="roles_save", description="Salva i tuoi ruoli come preset", guild=discord.Object(id=GUILD_ID))
async def roles_save(inter: discord.Interaction, name: str):
    m: discord.Member = inter.user
    roles = [r.id for r in m.roles if not r.is_default() and r.id not in EXCLUDE_ROLE_IDS and not r.managed]
    node = _presets_node(inter.guild.id, m.id, create=True)
    node[name] = roles
    _save_presets()
    await inter.response.send_message(f"✅ Salvato preset **{name}** con {len(roles)} ruoli.", ephemeral=True)

@bot.tree.command(name="roles_list", description="Elenca i tuoi preset ruoli", guild=discord.Object(id=GUILD_ID))
async def roles_list(inter: discord.Interaction):
    node = _presets_node(inter.guild.id, inter.user.id, create=False)
    if not node:
        return await inter.response.send_message("Non hai preset salvati.", ephemeral=True)
    lines = [f"- **{k}** ({len(v)} ruoli)" for k, v in node.items()]
    await inter.response.send_message("I tuoi preset:\n" + "\n".join(lines), ephemeral=True)

@bot.tree.command(name="roles_load", description="Applica un tuo preset ruoli", guild=discord.Object(id=GUILD_ID))
async def roles_load(inter: discord.Interaction, name: str):
    m: discord.Member = inter.user
    node = _presets_node(inter.guild.id, m.id, create=False)
    if not node or name not in node:
        return await inter.response.send_message("Preset inesistente.", ephemeral=True)
    ids = node[name]
    to_add = []
    for rid in ids:
        r = inter.guild.get_role(rid)
        if r and r not in m.roles:
            to_add.append(r)
    if to_add:
        await add_roles_safely(m, to_add, reason=f"User preset apply: {name}")
    await inter.response.send_message(f"✅ Applicato **{name}**. Aggiunti {len(to_add)} ruoli.", ephemeral=True)

@bot.tree.command(name="roles_delete", description="Elimina un tuo preset ruoli", guild=discord.Object(id=GUILD_ID))
async def roles_delete(inter: discord.Interaction, name: str):
    node = _presets_node(inter.guild.id, inter.user.id, create=False)
    if not node or name not in node:
        return await inter.response.send_message("Preset inesistente.", ephemeral=True)
    del node[name]; _save_presets()
    await inter.response.send_message(f"🗑️ Eliminato preset **{name}**.", ephemeral=True)

# ================== ADMIN: pannello & diagnostica ==================
@bot.tree.command(name="age_panel", description="(Admin) Pubblica il pannello con i bottoni di verifica", guild=discord.Object(id=GUILD_ID))
async def age_panel(inter: discord.Interaction, channel: Optional[discord.TextChannel] = None):
    if not inter.user.guild_permissions.manage_guild:
        return await inter.response.send_message("Permesso negato.", ephemeral=True)
    ch = channel or inter.guild.get_channel(VERIFY_CHANNEL_ID)
    if not ch:
        return await inter.response.send_message("Canale verifica non trovato.", ephemeral=True)
    await ch.send("**Verifica età** — Clicca un pulsante:", view=AgeVerifyView())
    await inter.response.send_message("Pannello pubblicato ✅", ephemeral=True)


@bot.tree.command(name="age_diag", description="(Admin) Diagnostica snapshot e ruoli utente", guild=discord.Object(id=GUILD_ID))
async def age_diag(inter: discord.Interaction, user: discord.Member):
    g = inter.guild
    r_unv = g.get_role(ROLE_UNVERIFIED_ID)
    r_ver = g.get_role(ROLE_VERIFIED_ID)
    snap = get_snapshot(user.id)
    roles = [f"{r.name}({r.id})" for r in user.roles if not r.is_default()]
    msg = (f"Utente: {user.mention}\n"
           f"Ruoli: {', '.join(roles) if roles else '(nessuno)'}\n"
           f"Non verificato: {'✅' if r_unv in user.roles else '❌'} | "
           f"Verificato: {'✅' if r_ver in user.roles else '❌'}\n"
           f"Snapshot: {len(snap)} ruoli: {', '.join(map(str, snap)) if snap else '(vuoto)'}")
    await inter.response.send_message(msg, ephemeral=True)

@bot.tree.command(name="restore_roles", description="(Admin) Ripristina i ruoli salvati di un utente", guild=discord.Object(id=GUILD_ID))
async def restore_roles(inter: discord.Interaction, user: discord.Member):
    if not inter.user.guild_permissions.manage_roles:
        return await inter.response.send_message("Permesso negato.", ephemeral=True)
    if not has_snapshot(user.id):
        return await inter.response.send_message("Nessuno snapshot trovato per questo utente.", ephemeral=True)

    await post_verified(user, reason="Restore roles (snapshot)")
    await inter.response.send_message("✅ Snapshot ripristinato, **Verificato** assegnato e **Non verificato** rimosso.", ephemeral=True)

# ================== CREAZIONE/DELETE RUOLI IN MASSA ==================
def parse_names(blob: str) -> List[str]:
    normalized = blob.replace(",", " ").replace("\n", " ")
    try:
        tokens = shlex.split(normalized)
    except ValueError:
        tokens = normalized.split()
    seen, out = set(), []
    for t in tokens:
        k = t.strip()
        if not k:
            continue
        l = k.lower()
        if l in seen:
            continue
        seen.add(l)
        out.append(k)
    return out

async def safe_create_role(guild: discord.Guild, **kwargs):
    delay = 1.0
    for _ in range(5):
        try:
            return await guild.create_role(**kwargs)
        except discord.HTTPException:
            await asyncio.sleep(delay); delay = min(delay*2, 8)
    raise RuntimeError("create_role failed after retries")

async def safe_delete_role(role: discord.Role, reason: Optional[str] = None):
    delay = 1.0
    for _ in range(5):
        try:
            return await role.delete(reason=reason)
        except discord.HTTPException:
            await asyncio.sleep(delay); delay = min(delay*2, 8)
    raise RuntimeError("delete_role failed after retries")

@bot.tree.command(name="createroles", description="Crea ruoli (spazi/newline/virgolette; virgole ok)", guild=discord.Object(id=GUILD_ID))
async def create_roles_cmd(inter: discord.Interaction, roles_text: str):
    await inter.response.defer(ephemeral=True)
    names = parse_names(roles_text)
    if not names:
        return await inter.followup.send("❌ Nessun nome ruolo valido.", ephemeral=True)
    g = inter.guild
    existing_lower = {r.name.lower() for r in g.roles}
    created, skipped, failed = [], [], []
    for n in names:
        if n.lower() in existing_lower:
            skipped.append(n); continue
        try:
            await safe_create_role(g, name=n, reason=f"Create in bulk da {inter.user}")
            created.append(n)
            await asyncio.sleep(0.35)
        except Exception as e:
            failed.append(f"{n} ({e.__class__.__name__})")
    msg = []
    if created: msg.append(f"✅ Creati ({len(created)}): " + ", ".join(created))
    if skipped: msg.append(f"↩️ Già esistenti ({len(skipped)}): " + ", ".join(skipped))
    if failed:  msg.append(f"❌ Falliti ({len(failed)}): "  + ", ".join(failed))
    out = "\n".join(msg) or "Nessuna operazione."
    await inter.followup.send(out, ephemeral=True)
    await staff_log(g, "Bulk Create Roles", out)

# ---- Tendina paginata per eliminare ruoli ----
class RolesPageView(discord.ui.View):
    def __init__(self, guild: discord.Guild, roles: List[discord.Role], author_id: int, page_size: int = 25):
        super().__init__(timeout=180)
        self.guild = guild
        self.roles_all = roles
        self.author_id = author_id
        self.page_size = page_size
        self.page = 0
        self.selected_ids: List[int] = []
        self.update_children()

    def page_roles(self) -> List[discord.Role]:
        s = self.page * self.page_size; e = s + self.page_size
        return self.roles_all[s:e]

    def update_children(self):
        self.clear_items()
        opts = [discord.SelectOption(label=r.name[:100], value=str(r.id)) for r in self.page_roles()]
        if not opts:
            opts = [discord.SelectOption(label="(Nessun ruolo in questa pagina)", value="none", default=True)]
        select = discord.ui.Select(placeholder="Seleziona ruoli da eliminare… (multi)",
                                   min_values=0, max_values=min(25, len(opts)), options=opts)

        async def on_select(interaction: discord.Interaction):
            if interaction.user.id != self.author_id:
                return await interaction.response.send_message("Questa selezione non è tua.", ephemeral=True)
            chosen = [int(v) for v in select.values if v != "none"]
            page_ids = {r.id for r in self.page_roles()}
            self.selected_ids = [rid for rid in self.selected_ids if rid not in page_ids]
            self.selected_ids.extend(chosen)
            await interaction.response.defer()

        select.callback = on_select
        self.add_item(select)

        prev_btn = discord.ui.Button(label="⬅️ Prev", style=discord.ButtonStyle.secondary)
        next_btn = discord.ui.Button(label="Next ➡️", style=discord.ButtonStyle.secondary)
        confirm_btn = discord.ui.Button(label="🗑️ Elimina selezionati", style=discord.ButtonStyle.danger)
        cancel_btn = discord.ui.Button(label="Annulla", style=discord.ButtonStyle.secondary)

        async def do_prev(interaction: discord.Interaction):
            if interaction.user.id != self.author_id:
                return await interaction.response.send_message("Non puoi usare questo controllo.", ephemeral=True)
            if self.page > 0:
                self.page -= 1; self.update_children()
                await interaction.response.edit_message(view=self)

        async def do_next(interaction: discord.Interaction):
            if interaction.user.id != self.author_id:
                return await interaction.response.send_message("Non puoi usare questo controllo.", ephemeral=True)
            max_page = (len(self.roles_all) - 1) // self.page_size
            if self.page < max_page:
                self.page += 1; self.update_children()
                await interaction.response.edit_message(view=self)

        async def do_cancel(interaction: discord.Interaction):
            if interaction.user.id != self.author_id:
                return await interaction.response.send_message("Non puoi usare questo controllo.", ephemeral=True)
            await interaction.response.edit_message(content="Operazione annullata.", view=None)

        async def do_confirm(interaction: discord.Interaction):
            if interaction.user.id != self.author_id:
                return await interaction.response.send_message("Non puoi usare questo controllo.", ephemeral=True)
            me = self.guild.me or await self.guild.fetch_member(bot.user.id)
            top_pos = me.top_role.position if me else 0
            targets = [self.guild.get_role(rid) for rid in self.selected_ids]
            targets = [r for r in targets if r]

            deleted, skipped, failed = [], [], []
            for role in targets:
                if role.is_default():
                    skipped.append(f"{role.name} (@everyone)"); continue
                if role.position >= top_pos:
                    skipped.append(f"{role.name} (sopra/pari al bot)"); continue
                try:
                    await safe_delete_role(role, reason=f"Eliminato via /deleteroles_menu")
                    deleted.append(role.name); await asyncio.sleep(0.35)
                except Exception as e:
                    failed.append(f"{role.name} ({e.__class__.__name__})")

            txt_parts = []
            if deleted: txt_parts.append(f"🗑️ Eliminati ({len(deleted)}): " + ", ".join(deleted))
            if skipped: txt_parts.append(f"↩️ Skippati ({len(skipped)}): " + ", ".join(skipped))
            if failed:  txt_parts.append(f"❌ Falliti ({len(failed)}): "  + ", ".join(failed))
            txt = "\n".join(txt_parts) or "Nessuna operazione."
            await interaction.response.edit_message(content=txt, view=None)
            await staff_log(self.guild, "Bulk Delete Roles (menu)", txt)

        prev_btn.callback = do_prev
        next_btn.callback = do_next
        confirm_btn.callback = do_confirm
        cancel_btn.callback = do_cancel

        self.add_item(prev_btn); self.add_item(next_btn)
        self.add_item(confirm_btn); self.add_item(cancel_btn)

@bot.tree.command(name="deleteroles_menu", description="Elimina ruoli tramite tendina (paginata)", guild=discord.Object(id=GUILD_ID))
async def deleteroles_menu(inter: discord.Interaction):
    await inter.response.defer(ephemeral=True)
    g = inter.guild
    me = g.me or await g.fetch_member(bot.user.id)
    top_pos = me.top_role.position if me else 0
    roles = [r for r in g.roles if not r.is_default() and r.position < top_pos]
    roles.sort(key=lambda r: r.position)
    if not roles:
        return await inter.followup.send("Nessun ruolo eliminabile trovato.", ephemeral=True)
    view = RolesPageView(g, roles, inter.user.id, page_size=25)
    await inter.followup.send("Seleziona i ruoli da **eliminare** e premi **🗑️ Elimina selezionati**.",
                              view=view, ephemeral=True)

# ================== REMINDERS ==================
if os.path.exists(REMINDERS_FILE):
    with open(REMINDERS_FILE, "r", encoding="utf-8") as f:
        REMINDERS = json.load(f)
else:
    REMINDERS = []

def _save_reminders():
    with open(REMINDERS_FILE, "w", encoding="utf-8") as f:
        json.dump(REMINDERS, f, ensure_ascii=False)

def parse_when(s: str) -> Optional[datetime]:
    s = s.strip()
    now = discord.utils.utcnow()
    m = re.fullmatch(r"(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?", s)
    if m and any(m.groups()):
        d = int(m.group(1) or 0); h = int(m.group(2) or 0); mi = int(m.group(3) or 0)
        return now + timedelta(days=d, hours=h, minutes=mi)
    m2 = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2})", s)
    if m2:
        y, mo, d, hh, mm = map(int, m2.groups())
        return datetime(y, mo, d, hh, mm, tzinfo=timezone.utc)
    return None

@bot.tree.command(name="remind", description="Crea un reminder (10m, 2h, 1d, 2025-12-31 18:30)", guild=discord.Object(id=GUILD_ID))
async def remind(inter: discord.Interaction, when: str, message: str, channel: Optional[discord.TextChannel] = None):
    dt = parse_when(when)
    if not dt:
        return await inter.response.send_message("Formato **when** non valido. Esempi: `10m`, `2h`, `1d`, `2025-12-31 18:30` (UTC).", ephemeral=True)
    target_ch = channel or inter.channel
    rid = (REMINDERS[-1]["id"] + 1) if REMINDERS else 1
    REMINDERS.append({
        "id": rid, "guild_id": inter.guild.id, "channel_id": target_ch.id,
        "author_id": inter.user.id, "when": dt.isoformat(), "message": message
    })
    _save_reminders()
    await inter.response.send_message(f"⏰ Reminder #{rid} creato per {discord.utils.format_dt(dt, style='R')} in {target_ch.mention}.", ephemeral=True)

@bot.tree.command(name="reminders_list", description="Vedi i tuoi reminder", guild=discord.Object(id=GUILD_ID))
async def reminders_list(inter: discord.Interaction):
    my = [r for r in REMINDERS if r["guild_id"] == inter.guild.id and r["author_id"] == inter.user.id]
    if not my:
        return await inter.response.send_message("Non hai reminder.", ephemeral=True)
    lines = [f"#{r['id']} → {r['message']} @ {r['when']}" for r in my[:20]]
    await inter.response.send_message("I tuoi reminder:\n" + "\n".join(lines), ephemeral=True)

@bot.tree.command(name="reminders_cancel", description="Cancella un reminder per ID", guild=discord.Object(id=GUILD_ID))
async def reminders_cancel(inter: discord.Interaction, reminder_id: int):
    idx = next((i for i, r in enumerate(REMINDERS) if r["id"] == reminder_id and r["guild_id"] == inter.guild.id and r["author_id"] == inter.user.id), None)
    if idx is None:
        return await inter.response.send_message("Reminder non trovato (o non è tuo).", ephemeral=True)
    REMINDERS.pop(idx); _save_reminders()
    await inter.response.send_message(f"🗑️ Reminder #{reminder_id} cancellato.", ephemeral=True)

@tasks.loop(seconds=30)
async def reminder_loop():
    if not REMINDERS:
        return
    now = discord.utils.utcnow()
    due = [r for r in REMINDERS if now >= datetime.fromisoformat(r["when"])]
    for r in due:
        guild = bot.get_guild(r["guild_id"])
        ch = guild.get_channel(r["channel_id"]) if guild else None
        if ch:
            try:
                await ch.send(f"⏰ <@{r['author_id']}> **Reminder:** {r['message']}")
            except Exception:
                pass
        try:
            REMINDERS.remove(r)
        except ValueError:
            pass
    if due:
        _save_reminders()

@reminder_loop.before_loop
async def _reminders_wait_ready():
    await bot.wait_until_ready()

# ================== TICKET EVOLUTO ==================
if os.path.exists(TICKET_CONFIG_FILE):
    with open(TICKET_CONFIG_FILE, "r", encoding="utf-8") as f:
        TICKET_CFG = json.load(f)
else:
    TICKET_CFG = {}  # {guild_id: {...}}

def _save_ticket_cfg():
    with open(TICKET_CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(TICKET_CFG, f, ensure_ascii=False)

def _tk_node(gid: int):
    key = str(gid)
    if key not in TICKET_CFG:
        TICKET_CFG[key] = {
            "hub_channel_id": 0,
            "staff_role_id": 0,
            "notify_channel_id": 0,
            "transcript_channel_id": 0,
            # 👇 NUOVO
            "panel_message": "**Supporto** → apri un ticket con il pulsante qui sotto:",
            "ticket_mode": "advanced"  # "base" oppure "advanced"
        }
    else:
        TICKET_CFG[key].setdefault("panel_message", "**Supporto** → apri un ticket con il pulsante qui sotto:")
        TICKET_CFG[key].setdefault("ticket_mode", "advanced")
    return TICKET_CFG[key]

async def _ticket_notify(guild: discord.Guild, embed: discord.Embed):
    node = _tk_node(guild.id)
    ch = guild.get_channel(node.get("notify_channel_id", 0))
    if ch and isinstance(ch, discord.TextChannel):
        try:
            await ch.send(embed=embed)
        except Exception:
            pass

class TicketSetupView(discord.ui.View):
    def __init__(self, guild: discord.Guild):
        super().__init__(timeout=None)

        node = _tk_node(guild.id)
        saved_mode = node.get("ticket_mode", "advanced")
        saved_msg  = node.get("panel_message", "**Supporto** → apri un ticket con il pulsante qui sotto:")

        self.hub_sel = discord.ui.ChannelSelect(
            placeholder="Seleziona HUB (testo) per i thread Ticket",
            channel_types=[discord.ChannelType.text],
            min_values=1, max_values=1
        )
        self.notify_sel = discord.ui.ChannelSelect(
            placeholder="Seleziona CANALE NOTIFICHE Staff",
            channel_types=[discord.ChannelType.text],
            min_values=1, max_values=1
        )
        self.trans_sel = discord.ui.ChannelSelect(
            placeholder="Seleziona CANALE Transcript",
            channel_types=[discord.ChannelType.text],
            min_values=1, max_values=1
        )
        self.role_sel = discord.ui.RoleSelect(
            placeholder="Seleziona RUOLO Staff Ticket",
            min_values=1, max_values=1
        )

        self.mode_sel = discord.ui.Select(
            placeholder="Tipo di ticket: Base o Avanzato",
            min_values=1, max_values=1,
            options=[
                discord.SelectOption(label="Base", value="base", default=(saved_mode == "base")),
                discord.SelectOption(label="Avanzato", value="advanced", default=(saved_mode != "base")),
            ],
        )

        async def _ack(i: discord.Interaction):
            await i.response.defer()

        self.hub_sel.callback    = _ack
        self.notify_sel.callback = _ack
        self.trans_sel.callback  = _ack
        self.role_sel.callback   = _ack
        self.mode_sel.callback   = _ack

        self.add_item(self.hub_sel)
        self.add_item(self.notify_sel)
        self.add_item(self.trans_sel)
        self.add_item(self.role_sel)
        self.add_item(self.mode_sel)

        self.msg_btn = discord.ui.Button(label="✏️ Testo sopra il pulsante", style=discord.ButtonStyle.primary)
        async def _open_msg_modal(i: discord.Interaction):
            await i.response.send_modal(_PanelMsgModal(current_text=saved_msg))
        self.msg_btn.callback = _open_msg_modal
        self.add_item(self.msg_btn)

        self.save_btn = discord.ui.Button(label="💾 Salva configurazione", style=discord.ButtonStyle.success)
        self.save_btn.callback = self._save
        self.add_item(self.save_btn)

    async def _save(self, interaction: discord.Interaction):
        try:
            hub    = self.hub_sel.values[0]
            notify = self.notify_sel.values[0]
            trans  = self.trans_sel.values[0]
            role   = self.role_sel.values[0]
            mode   = self.mode_sel.values[0] if self.mode_sel.values else "advanced"
        except IndexError:
            return await interaction.response.send_message(
                "⚠️ Seleziona **tutti** i campi (Hub, Notifiche, Transcript, Ruolo) prima di salvare.",
                ephemeral=True
            )

        gid = str(interaction.guild.id)
        TICKET_CFG[gid] = {
            "hub_channel_id": hub.id,
            "notify_channel_id": notify.id,
            "staff_role_id": role.id,
            "transcript_channel_id": trans.id,
            "panel_message": _tk_node(interaction.guild.id)["panel_message"],
            "ticket_mode": mode
        }
        _save_ticket_cfg()

        await interaction.response.edit_message(
            content=(f"✅ Config salvata:\n"
                     f"• Hub: {hub.mention}\n"
                     f"• Notifiche: {notify.mention}\n"
                     f"• Staff: {role.mention}\n"
                     f"• Transcript: {trans.mention}\n"
                     f"• **Tipo ticket:** {mode.upper()}"),
            view=None
        )

class _PanelMsgModal(discord.ui.Modal, title="Testo del pannello Ticket"):
    def __init__(self, current_text: str):
        super().__init__(timeout=None)
        self.msg = discord.ui.TextInput(
            label="Messaggio sopra il pulsante",
            style=discord.TextStyle.paragraph,
            default=current_text,
            max_length=1000
        )
        self.add_item(self.msg)

    async def on_submit(self, interaction: discord.Interaction):
        node = _tk_node(interaction.guild.id)
        node["panel_message"] = str(self.msg.value)
        _save_ticket_cfg()
        await interaction.response.send_message(
            "✅ Testo aggiornato. Usa di nuovo **/ticket_panel** per pubblicarlo.",
            ephemeral=True
        )

@bot.tree.command(name="ticket_setup_panel", description="(Admin) Pannello di setup ticket", guild=discord.Object(id=GUILD_ID))
async def ticket_setup_panel(inter: discord.Interaction):
    if not inter.user.guild_permissions.manage_guild:
        return await inter.response.send_message("Permesso negato.", ephemeral=True)
    # Rispondi subito per evitare timeout
    await inter.response.send_message("Configura Hub / Notifiche / Transcript / Staff per i ticket:", ephemeral=True, view=TicketSetupView(inter.guild))

class NewTicketModal(discord.ui.Modal, title="Apri Ticket"):
    categoria   = discord.ui.TextInput(label="Categoria", placeholder="Segnalazione / Aiuto / Reclamo", max_length=50)
    priorita    = discord.ui.TextInput(label="Priorità (Low/Normal/High)", default="Normal", max_length=10)
    descrizione = discord.ui.TextInput(label="Descrizione", style=discord.TextStyle.paragraph, max_length=1000)

    def __init__(self, opener: discord.Interaction):
        super().__init__()
        self.opener = opener

    async def on_submit(self, inter: discord.Interaction):
        node = _tk_node(inter.guild.id)
        hub = inter.guild.get_channel(node["hub_channel_id"])
        staff_role = inter.guild.get_role(node["staff_role_id"])
        if not hub or not staff_role:
            return await inter.response.send_message("Ticket hub/ruolo non configurato. Usa /ticket_setup_panel", ephemeral=True)

        thread = await hub.create_thread(name=f"🎫 {inter.user.display_name} • {self.categoria.value[:20]}",
                                         type=discord.ChannelType.private_thread, invitable=False)
        await thread.add_user(inter.user)

        view = TicketControlsView(thread.id, staff_role.id)
        emb = discord.Embed(
            title="Nuovo Ticket",
            description=f"**Autore:** {inter.user.mention}\n**Categoria:** {self.categoria.value}\n**Priorità:** {self.priorita.value}\n\n**Descrizione:**\n{self.descrizione.value}",
            color=discord.Color.red(), timestamp=discord.utils.utcnow()
        )
        await thread.send(content=staff_role.mention, embed=emb, view=view)
        await inter.response.send_message(f"✅ Ticket creato: {thread.mention}", ephemeral=True)

        notif = discord.Embed(
            title="🟢 Ticket aperto",
            description=f"**Utente:** {inter.user.mention}\n**Categoria:** {self.categoria.value} • **Priorità:** {self.priorita.value}\n**Thread:** {thread.mention}",
            color=discord.Color.green(), timestamp=discord.utils.utcnow()
        )
        await _ticket_notify(inter.guild, notif)

        async def sla_nudge():
            await asyncio.sleep(300)
            hist = [m async for m in thread.history(limit=20)]
            staff_replied = any((isinstance(m.author, discord.Member) and staff_role in m.author.roles) for m in hist)
            if not staff_replied:
                try:
                    await thread.send(f"⏳ Nessuna risposta staff. {staff_role.mention} potete passare?")
                except Exception:
                    pass
        bot.loop.create_task(sla_nudge())

class TicketControlsView(discord.ui.View):
    def __init__(self, thread_id: int, staff_role_id: int):
        super().__init__(timeout=None)
        self.thread_id = thread_id
        self.staff_role_id = staff_role_id

    @discord.ui.button(label="Chiudi", style=discord.ButtonStyle.danger, custom_id="tk_close")
    async def close(self, inter: discord.Interaction, b: discord.ui.Button):
        thread = inter.guild.get_thread(self.thread_id)
        if not thread:
            return await inter.response.send_message("Thread non trovato.", ephemeral=True)
        staff_role = inter.guild.get_role(self.staff_role_id)
        if not (inter.user == thread.owner or (isinstance(inter.user, discord.Member) and staff_role in inter.user.roles) or inter.user.guild_permissions.manage_threads):
            return await inter.response.send_message("Non puoi chiudere questo ticket.", ephemeral=True)

        buf = io.StringIO()
        async for m in thread.history(limit=None, oldest_first=True):
            who = f"{m.author.display_name}"
            buf.write(f"[{m.created_at.isoformat()}] {who}: {m.content}\n")
            for a in m.attachments:
                buf.write(f"   [attachment] {a.url}\n")
        data = buf.getvalue().encode("utf-8")
        file = discord.File(io.BytesIO(data), filename=f"ticket_{thread.id}_transcript.txt")

        notif = discord.Embed(title="🔴 Ticket chiuso", description=f"**Chiuso da:** {inter.user.mention}\n**Thread:** <#{thread.id}>",
                              color=discord.Color.orange(), timestamp=discord.utils.utcnow())
        await _ticket_notify(inter.guild, notif)
        await staff_log(inter.guild, "Ticket chiuso", f"Thread {thread.mention} chiuso da {inter.user.mention}.")
        try:
            await inter.response.send_message("✅ Ticket chiuso.", ephemeral=True)
            await thread.send("🗂️ Transcript generato. Il ticket verrà archiviato.")
        except Exception:
            pass

        node = _tk_node(inter.guild.id)
        log_ch = inter.guild.get_channel(node.get("transcript_channel_id") or 0) \
                 or inter.guild.get_channel(STAFF_LOG_CHANNEL_ID)
        if log_ch:
            try:
                await log_ch.send(file=file)
            except Exception:
                pass

        try:
            await thread.edit(archived=True, locked=True)
        except Exception:
            pass

class TicketPanelViewReal(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="🎫 Apri Ticket", style=discord.ButtonStyle.success, custom_id="tk_open_real")
    async def open_ticket(self, inter: discord.Interaction, b: discord.ui.Button):
        await inter.response.send_modal(NewTicketModal(opener=inter))

@bot.tree.command(name="ticket_panel", description="(Admin) Pubblica il pulsante per i ticket", guild=discord.Object(id=GUILD_ID))
async def ticket_panel(inter: discord.Interaction, channel: Optional[discord.TextChannel] = None):
    if not inter.user.guild_permissions.manage_guild:
        return await inter.response.send_message("Permesso negato.", ephemeral=True)

    node = _tk_node(inter.guild.id)
    ch = channel or inter.channel
    msg  = node.get("panel_message") or "**Supporto** → apri un ticket con il pulsante qui sotto:"
    mode = node.get("ticket_mode", "advanced")

    view = TicketPanelViewBasic() if mode == "base" else TicketPanelViewReal()
    await ch.send(msg, view=view)
    await inter.response.send_message(f"Pannello pubblicato ✅ (modalità: **{mode}**)", ephemeral=True)

# ================== NOTIFIER (Twitch/Kick/YouTube/TikTok) ==================
if os.path.exists(NOTIFY_FILE):
    with open(NOTIFY_FILE, "r", encoding="utf-8") as f:
        NOTIFY_CFG = json.load(f)
else:
    NOTIFY_CFG = {}

def _n_g(gid: int):
    key = str(gid)
    if key not in NOTIFY_CFG:
        NOTIFY_CFG[key] = {
            "channel_id": 0,
            "message_templates": {
                "twitch_live":  "🔴 {name} è in LIVE su {platform}: {title} → {url}",
                "kick_live":    "🟢 {name} è in LIVE su {platform}: {title} → {url}",
                "yt_live":      "🔴 {name} LIVE su {platform}: {title} → {url}",
                "yt_upload":    "📺 Nuovo video di {name}: {title} → {url}",
                "tiktok_live":  "🔥 {name} LIVE su {platform}: {url}",
                "tiktok_upload":"🎵 Nuovo video TikTok di {name}: {url}",
            },
            "sources": []
        }
    return NOTIFY_CFG[key]

def _n_save():
    with open(NOTIFY_FILE, "w", encoding="utf-8") as f:
        json.dump(NOTIFY_CFG, f, ensure_ascii=False)

def _tmpl(guild: discord.Guild, key: str) -> str:
    return _n_g(guild.id)["message_templates"].get(key, "{name}: {title} → {url}")

async def _send_notify(guild: discord.Guild, platform: str, title: str, desc: str, url: str, thumbnail: Optional[str] = None):
    node = _n_g(guild.id)
    ch = guild.get_channel(node["channel_id"])
    if not ch or not isinstance(ch, discord.TextChannel):
        await staff_log(guild, "Notifier", "Canale notifiche non configurato. Usa /notify_setup.")
        return
    icon = PLATFORM_ICONS.get(platform)
    emb = discord.Embed(title=title, description=desc, color=discord.Color.red(), timestamp=discord.utils.utcnow())
    if icon:
        emb.set_author(name=platform.upper(), icon_url=icon)
    if thumbnail:
        emb.set_thumbnail(url=thumbnail)
    emb.url = url
    try:
        await ch.send(embed=emb)
    except Exception:
        pass

_tw_access = {"token":"", "exp": discord.utils.utcnow()}

async def _twitch_token(session: aiohttp.ClientSession) -> str:
    if not TWITCH_CLIENT_ID or not TWITCH_CLIENT_SECRET:
        return ""
    if _tw_access["token"] and discord.utils.utcnow() < _tw_access["exp"]:
        return _tw_access["token"]
    url = "https://id.twitch.tv/oauth2/token"
    params = {"client_id": TWITCH_CLIENT_ID, "client_secret": TWITCH_CLIENT_SECRET, "grant_type": "client_credentials"}
    async with session.post(url, params=params) as r:
        js = await r.json()
        _tw_access["token"] = js.get("access_token","")
        _tw_access["exp"]   = discord.utils.utcnow() + timedelta(hours=1)
        return _tw_access["token"]

async def check_twitch_live(session: aiohttp.ClientSession, login: str) -> Tuple[bool, str, str, str]:
    tok = await _twitch_token(session)
    if not tok:
        return (False, "", "", f"https://twitch.tv/{login}")
    headers = {"Client-ID": TWITCH_CLIENT_ID, "Authorization": f"Bearer {tok}"}
    url = f"https://api.twitch.tv/helix/streams?user_login={login}"
    async with session.get(url, headers=headers) as r:
        js = await r.json()
        data = js.get("data", [])
        if data:
            d = data[0]
            return (True, d.get("title",""), d.get("game_name",""), f"https://twitch.tv/{login}")
        return (False, "", f"https://twitch.tv/{login}")

async def check_kick_live(session: aiohttp.ClientSession, login: str) -> Tuple[bool, str, str]:
    url = f"https://kick.com/api/v2/channels/{login}"
    try:
        async with session.get(url, headers={"Accept":"application/json"}) as r:
            if r.status != 200:
                return (False, "", f"https://kick.com/{login}")
            js = await r.json()
            live = js.get("livestream")
            if live:
                title = live.get("session_title") or "Live"
                return (True, title, f"https://kick.com/{login}")
            return (False, "", f"https://kick.com/{login}")
    except Exception:
        return (False, "", f"https://kick.com/{login}")

async def check_youtube_live(session: aiohttp.ClientSession, channel_id: str) -> Tuple[bool, str, str]:
    if not YOUTUBE_API_KEY:
        return (False, "", f"https://www.youtube.com/channel/{channel_id}")
    api = "https://www.googleapis.com/youtube/v3/search"
    params = {"part":"snippet","channelId":channel_id,"eventType":"live","type":"video","maxResults":1,"key":YOUTUBE_API_KEY}
    async with session.get(api, params=params) as r:
        js = await r.json()
        items = js.get("items", [])
        if items:
            it = items[0]
            vid = it["id"]["videoId"]
            title = it["snippet"]["title"]
            return (True, title, f"https://www.youtube.com/watch?v={vid}")
        return (False, "", f"https://www.youtube.com/channel/{channel_id}")

async def check_youtube_upload(session: aiohttp.ClientSession, channel_id: str, last_video_id: str) -> Tuple[str, str]:
    rss = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
    try:
        async with session.get(rss) as r:
            if r.status != 200:
                return ("", "")
            txt = await r.text()
            m_id = re.search(r"<yt:videoId>([^<]+)</yt:videoId>", txt)
            if not m_id:
                return ("", "")
            vid = m_id.group(1)
            if vid == last_video_id:
                return ("", "")
            titles = re.findall(r"<title>([^<]+)</title>", txt)
            title = titles[1] if len(titles) > 1 else "Nuovo video"
            return (vid, title)
    except Exception:
        return ("", "")

async def check_tiktok_live(session: aiohttp.ClientSession, username: str) -> Tuple[bool, str]:
    url = f"https://www.tiktok.com/@{username}/live"
    try:
        async with session.get(url, headers={"User-Agent":"Mozilla/5.0"}) as r:
            txt = await r.text()
            if r.status == 200 and ("LIVE" in txt or "liveRoom" in txt):
                return (True, url)
            return (False, url)
    except Exception:
        return (False, url)

async def check_tiktok_upload(session: aiohttp.ClientSession, username: str, last_video_id: str) -> Tuple[str, str]:
    url = f"https://www.tiktok.com/@{username}"
    try:
        async with session.get(url, headers={"User-Agent":"Mozilla/5.0"}) as r:
            txt = await r.text()
            m = re.search(r'"videoId":"(\d+)"', txt)
            if not m:
                return ("", "")
            vid = m.group(1)
            if vid == last_video_id:
                return ("", "")
            title = "Nuovo TikTok"
            return (vid, title)
    except Exception:
        return ("", "")

@bot.tree.command(name="notify_setup", description="Imposta il canale notifiche", guild=discord.Object(id=GUILD_ID))
async def notify_setup(inter: discord.Interaction, channel: discord.TextChannel):
    if not inter.user.guild_permissions.manage_guild:
        return await inter.response.send_message("Permesso negato.", ephemeral=True)
    node = _n_g(inter.guild.id)
    node["channel_id"] = channel.id
    _n_save()
    await inter.response.send_message(f"✅ Canale notifiche impostato: {channel.mention}", ephemeral=True)

@bot.tree.command(name="notify_add", description="Aggiungi sorgente (twitch|youtube|kick|tiktok)", guild=discord.Object(id=GUILD_ID))
async def notify_add(inter: discord.Interaction, platform: str, identifier: str, display_name: Optional[str] = None):
    if not inter.user.guild_permissions.manage_guild:
        return await inter.response.send_message("Permesso negato.", ephemeral=True)
    platform = platform.lower().strip()
    if platform not in {"twitch","youtube","kick","tiktok"}:
        return await inter.response.send_message("Piattaforma non valida.", ephemeral=True)
    node = _n_g(inter.guild.id)
    sid = f"{platform}:{identifier}"
    if any(s["id"] == sid for s in node["sources"]):
        return await inter.response.send_message("Sorgente già presente.", ephemeral=True)
    node["sources"].append({"id":sid,"platform":platform,"name":identifier,"display":display_name or identifier,"last_live":False,"last_video_id":""})
    _n_save()
    await inter.response.send_message(f"✅ Aggiunto **{sid}**", ephemeral=True)

@bot.tree.command(name="notify_remove", description="Rimuovi sorgente per ID (es. twitch:shroud)", guild=discord.Object(id=GUILD_ID))
async def notify_remove(inter: discord.Interaction, source_id: str):
    if not inter.user.guild_permissions.manage_guild:
        return await inter.response.send_message("Permesso negato.", ephemeral=True)
    node = _n_g(inter.guild.id)
    before = len(node["sources"])
    node["sources"] = [s for s in node["sources"] if s["id"] != source_id]
    _n_save()
    after = len(node["sources"])
    await inter.response.send_message(f"🗑️ Rimosse {before - after} sorgenti.", ephemeral=True)

@bot.tree.command(name="notify_list", description="Elenca le sorgenti configurate", guild=discord.Object(id=GUILD_ID))
async def notify_list(inter: discord.Interaction):
    node = _n_g(inter.guild.id)
    if not node["sources"]:
        return await inter.response.send_message("Nessuna sorgente.", ephemeral=True)
    lines = [f"- `{s['id']}` → **{s['display']}**" for s in node["sources"]]
    await inter.response.send_message("Sorgenti:\n" + "\n".join(lines[:60]), ephemeral=True)

@bot.tree.command(name="notify_message", description="Personalizza template messaggio", guild=discord.Object(id=GUILD_ID))
async def notify_message_cmd(inter: discord.Interaction, event_key: str, template: str):
    if not inter.user.guild_permissions.manage_guild:
        return await inter.response.send_message("Permesso negato.", ephemeral=True)
    node = _n_g(inter.guild.id)
    if event_key not in node["message_templates"]:
        return await inter.response.send_message("Chiave evento non valida.", ephemeral=True)
    node["message_templates"][event_key] = template
    _n_save()
    await inter.response.send_message(f"✅ Template aggiornato per **{event_key}**", ephemeral=True)

@bot.tree.command(name="notify_message_panel", description="Pannello per modificare i messaggi", guild=discord.Object(id=GUILD_ID))
async def notify_message_panel(inter: discord.Interaction):
    if not inter.user.guild_permissions.manage_guild:
        return await inter.response.send_message("Permesso negato.", ephemeral=True)

    class NotifyTemplateModal(discord.ui.Modal, title="Modifica template messaggio"):
        def __init__(self, event_key: str, current_template: str):
            super().__init__(timeout=None)
            self.event_key = event_key
            self.template = discord.ui.TextInput(
                label=f"Template per {event_key}",
                style=discord.TextStyle.paragraph,
                default=current_template,
                placeholder="{platform} {name} {title} {url}",
                max_length=1000
            )
            self.add_item(self.template)

        async def on_submit(self, interaction: discord.Interaction):
            node = _n_g(interaction.guild.id)
            node["message_templates"][self.event_key] = str(self.template.value)
            _n_save()
            await interaction.response.send_message(
                f"✅ Template aggiornato per **{self.event_key}**.\n"
                "Segnaposto validi: `{platform}` `{name}` `{title}` `{url}`",
                ephemeral=True
            )

    class NotifyTemplateView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=60)
            keys = ["twitch_live","kick_live","yt_live","yt_upload","tiktok_live","tiktok_upload"]
            self.select = discord.ui.Select(
                placeholder="Scegli l'evento da modificare",
                min_values=1, max_values=1,
                options=[discord.SelectOption(label=k, value=k) for k in keys]
            )
            self.add_item(self.select)

        @discord.ui.select()
        async def on_select(self, interaction: discord.Interaction, select: discord.ui.Select):
            k = select.values[0]
            node = _n_g(interaction.guild.id)
            current = node["message_templates"].get(k, "{name}: {title} → {url}")
            await interaction.response.send_modal(NotifyTemplateModal(k, current))

    await inter.response.send_message(
        "Scegli l'evento e inserisci il template (placeholders: `{platform}` `{name}` `{title}` `{url}`).",
        view=NotifyTemplateView(),
        ephemeral=True
    )

@bot.tree.command(name="notify_test", description="Forza una notifica di test", guild=discord.Object(id=GUILD_ID))
async def notify_test(inter: discord.Interaction, platform: str, name_or_id: str, title: Optional[str] = "Test title", url: Optional[str] = "https://example.com"):
    platform = platform.lower()
    key = 'yt_live' if platform == 'youtube' else f'{platform}_live'
    txt = _tmpl(inter.guild, key).format(platform=platform.capitalize(), name=name_or_id, title=title, url=url)
    await _send_notify(inter.guild, platform, f"{platform.capitalize()} • {name_or_id}", txt, url)
    await inter.response.send_message("📣 Test inviato.", ephemeral=True)

@bot.tree.command(name="backup", description="(Admin) Esegui backup completo del server", guild=discord.Object(id=GUILD_ID))
async def backup_cmd(inter: discord.Interaction,
                     days: Optional[int] = None,
                     include_attachments: Optional[bool] = None):
    if not inter.user.guild_permissions.manage_guild:
        return await inter.response.send_message("❌ Permesso negato.", ephemeral=True)

    node = _bc_node(inter.guild.id)
    # se non passati, usa i valori da pannello
    days = node["days"] if days is None else max(0, int(days))
    include_attachments = node["include_attachments"] if include_attachments is None else bool(include_attachments)

    await inter.response.defer(ephemeral=True, thinking=True)
    report, zip_file = await _run_backup(inter.guild, days=days, include_attachments=include_attachments)
    _prune_backups(inter.guild.id, node["keep"])

    try:
        if zip_file.stat().st_size <= 20 * 1024 * 1024:
            await inter.followup.send(report, file=discord.File(str(zip_file)), ephemeral=True)
        else:
            await inter.followup.send(report + "\n\nIl file è grande: recuperalo dal filesystem del bot.", ephemeral=True)
    except Exception:
        await inter.followup.send(report, ephemeral=True)

    await staff_log(inter.guild, "Backup terminato", report)

@tasks.loop(seconds=60)
async def notifier_loop():
    if not NOTIFY_CFG:
        return
    async with aiohttp.ClientSession() as session:
        for gkey, node in NOTIFY_CFG.items():
            guild = bot.get_guild(int(gkey))
            if not guild or not node.get("sources"):
                continue
            for s in list(node["sources"]):
                plat = s["platform"]; name = s["name"]; disp = s.get("display", name)
                if plat == "twitch":
                    live, title, game, url = await check_twitch_live(session, name)
                    if live and not s.get("last_live"):
                        txt = _tmpl(guild, "twitch_live").format(platform="Twitch", name=disp, title=title or game or "Live", url=url)
                        await _send_notify(guild, "twitch", f"{disp} è in LIVE", txt, url)
                        s["last_live"] = True; _n_save()
                    if not live and s.get("last_live"):
                        s["last_live"] = False; _n_save()
                elif plat == "kick":
                    live, title, url = await check_kick_live(session, name)
                    if live and not s.get("last_live"):
                        txt = _tmpl(guild, "kick_live").format(platform="Kick", name=disp, title=title or "Live", url=url)
                        await _send_notify(guild, "kick", f"{disp} è in LIVE", txt, url)
                        s["last_live"] = True; _n_save()
                    if not live and s.get("last_live"):
                        s["last_live"] = False; _n_save()
                elif plat == "youtube":
                    live, title, url = await check_youtube_live(session, name)
                    if live and not s.get("last_live"):
                        txt = _tmpl(guild, "yt_live").format(platform="YouTube", name=disp, title=title or "Live", url=url)
                        await _send_notify(guild, "youtube", f"{disp} LIVE", txt, url)
                        s["last_live"] = True; _n_save()
                    if not live and s.get("last_live"):
                        s["last_live"] = False; _n_save()
                    new_vid, vtitle = await check_youtube_upload(session, name, s.get("last_video_id",""))
                    if new_vid:
                        vurl = f"https://www.youtube.com/watch?v={new_vid}"
                        txt = _tmpl(guild, "yt_upload").format(platform="YouTube", name=disp, title=vtitle, url=vurl)
                        await _send_notify(guild, "youtube", f"Nuovo video • {disp}", txt, vurl)
                        s["last_video_id"] = new_vid; _n_save()
                elif plat == "tiktok":
                    live, url = await check_tiktok_live(session, name)
                    if live and not s.get("last_live"):
                        txt = _tmpl(guild, "tiktok_live").format(platform="TikTok", name=disp, title="", url=url)
                        await _send_notify(guild, "tiktok", f"{disp} è in LIVE", txt, url)
                        s["last_live"] = True; _n_save()
                    if not live and s.get("last_live"):
                        s["last_live"] = False; _n_save()
                    new_vid, _ = await check_tiktok_upload(session, name, s.get("last_video_id",""))
                    if new_vid:
                        vurl = f"https://www.tiktok.com/@{name}/video/{new_vid}"
                        txt = _tmpl(guild, "tiktok_upload").format(platform="TikTok", name=disp, title="", url=vurl)
                        await _send_notify(guild, "tiktok", f"Nuovo TikTok • {disp}", txt, vurl)
                        s["last_video_id"] = new_vid; _n_save()

@notifier_loop.before_loop
async def _notify_wait_ready():
    await bot.wait_until_ready()

# ================== SETUP_HOOK & ON_READY ==================
async def _setup_hook():
    bot.add_view(TicketPanelViewReal())   # advanced
    bot.add_view(TicketPanelViewBasic())  # base
    # bot.add_view(AgeVerifyView())  # se vuoi persistenza dei bottoni dopo riavvio

    try:
        await bot.tree.sync(guild=discord.Object(id=GUILD_ID))
        print("✅ Slash commands sincronizzati sulla guild.")
    except Exception as e:
        print(f"⚠️ Sync slash fallita: {e!r}")

bot.setup_hook = _setup_hook

@bot.event
async def on_ready():
    print(f"✅ Bot online come {bot.user}")
    if not reminder_loop.is_running():
        reminder_loop.start()
    if not notifier_loop.is_running():
        notifier_loop.start()
    if not backup_scheduler_loop.is_running():
        backup_scheduler_loop.start()

# ================== AVVIO BOT ==================
if __name__ == "__main__":
    print(f"🔐 Token caricato (len={len(DISCORD_TOKEN)}) — avvio bot…")
    bot.run(DISCORD_TOKEN)