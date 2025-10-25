#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""LegnaBot — Discord bot con integrazione dashboard.

Questo modulo fornisce una versione completamente rivisitata del bot pensata per
lavorare in coppia con una dashboard online sul modello di Wickbot.  La logica è
stata organizzata in componenti ben separate:

* ConfigManager — gestisce la configurazione locale e il merge con quella
  remota fornita dalla dashboard.
* DashboardBridge — piccolo client HTTP per sincronizzare eventi/stato con il
  backend della dashboard.
* Cogs (Verification, Reminders, Notifier, DashboardSync) — implementano le
  funzionalità principali del bot e notificano la dashboard quando succede
  qualcosa di rilevante.

L'obiettivo è offrire una base pronta per collegarsi a un pannello remoto
(consentendo di leggere/scrivere configurazioni) mantenendo comunque un fallback
locale su file JSON.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiohttp
import discord
from discord.ext import commands, tasks
from dotenv import load_dotenv

# ========================= Setup logging & env ==========================
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env", override=True)

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s:%(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("legnabot")

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
if not DISCORD_TOKEN:
    raise SystemExit("❌ DISCORD_TOKEN mancante. Inseriscilo nel file .env")

DASHBOARD_BASE_URL = os.getenv("DASHBOARD_BASE_URL", "http://localhost:8000")
DASHBOARD_API_KEY = os.getenv("DASHBOARD_API_KEY", "")

CONFIG_FILE = BASE_DIR / "config.json"
REMINDERS_FILE = BASE_DIR / "reminders.json"
UTC = timezone.utc

# ========================= Data structures ==============================


@dataclass
class FreezeSettings:
    enabled: bool = True
    debounce_seconds: int = 1
    pause_start_delay: int = 5
    accumulate_window_seconds: int = 30
    quiet_gap_seconds: int = 5
    max_batch_remove: int = 10


@dataclass
class VerificationSettings:
    guild_id: int = 0
    verify_channel_id: int = 0
    staff_log_channel_id: int = 0
    verified_role_id: int = 0
    unverified_role_id: int = 0
    min_age: int = 16
    timeout_minutes: int = 15
    welcome_message: str = (
        "Benvenuto! Completa la verifica seguendo le istruzioni del modulo "
        "sulla dashboard."
    )

    def merge(self, payload: Dict[str, Any]) -> None:
        for key in (
            "verify_channel_id",
            "staff_log_channel_id",
            "verified_role_id",
            "unverified_role_id",
            "min_age",
            "timeout_minutes",
            "welcome_message",
        ):
            if key in payload and payload[key] is not None:
                setattr(self, key, payload[key])


@dataclass
class Reminder:
    author_id: int
    channel_id: int
    message: str
    trigger_at: datetime

    def to_dict(self) -> Dict[str, Any]:
        return {
            "author_id": self.author_id,
            "channel_id": self.channel_id,
            "message": self.message,
            "trigger_at": self.trigger_at.astimezone(UTC).isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Reminder":
        trigger = datetime.fromisoformat(data["trigger_at"])
        return cls(
            author_id=data["author_id"],
            channel_id=data["channel_id"],
            message=data["message"],
            trigger_at=trigger,
        )


@dataclass
class NotifierTarget:
    platform: str
    url: str
    role_id: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return {"platform": self.platform, "url": self.url, "role_id": self.role_id}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "NotifierTarget":
        return cls(platform=data["platform"], url=data["url"], role_id=data.get("role_id"))


@dataclass
class NotifierSettings:
    enabled: bool = False
    notify_channel_id: int = 0
    targets: List[NotifierTarget] = field(default_factory=list)

    def merge(self, data: Dict[str, Any]) -> None:
        if "enabled" in data:
            self.enabled = bool(data["enabled"])
        if "notify_channel_id" in data:
            self.notify_channel_id = int(data["notify_channel_id"] or 0)
        if "targets" in data and isinstance(data["targets"], list):
            self.targets = [NotifierTarget.from_dict(t) for t in data["targets"]]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "enabled": self.enabled,
            "notify_channel_id": self.notify_channel_id,
            "targets": [t.to_dict() for t in self.targets],
        }


@dataclass
class BotConfig:
    guild_id: int
    prefix: str = "!"
    dashboard_sync_interval: int = 60
    freeze: FreezeSettings = field(default_factory=FreezeSettings)
    verification: VerificationSettings = field(default_factory=VerificationSettings)
    notifier: NotifierSettings = field(default_factory=NotifierSettings)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BotConfig":
        freeze = FreezeSettings(**data.get("freeze", {}))
        verification = VerificationSettings(**data.get("verification", {}))
        notifier = NotifierSettings()
        notifier.merge(data.get("notifier", {}))
        return cls(
            guild_id=int(data.get("guild_id", 0)),
            prefix=data.get("prefix", "!"),
            dashboard_sync_interval=int(data.get("dashboard_sync_interval", 60)),
            freeze=freeze,
            verification=verification,
            notifier=notifier,
        )

    def merge(self, data: Dict[str, Any]) -> None:
        if "prefix" in data and data["prefix"]:
            self.prefix = str(data["prefix"])
        if "dashboard_sync_interval" in data and data["dashboard_sync_interval"]:
            self.dashboard_sync_interval = int(data["dashboard_sync_interval"])
        if "freeze" in data:
            for key, value in data["freeze"].items():
                if hasattr(self.freeze, key) and value is not None:
                    setattr(self.freeze, key, value)
        if "verification" in data:
            self.verification.merge(data["verification"])
        if "notifier" in data:
            self.notifier.merge(data["notifier"])


# ========================= Config manager ===============================


class ConfigManager:
    """Gestisce il caricamento/salvataggio della configurazione."""

    def __init__(self, file_path: Path):
        self.file_path = file_path
        self._config = BotConfig(guild_id=int(os.getenv("GUILD_ID", "0") or 0))
        self.load()
        self._config.verification.guild_id = self._config.guild_id

    @property
    def config(self) -> BotConfig:
        return self._config

    def load(self) -> None:
        if not self.file_path.exists():
            log.warning("Config file %s non trovato, uso configurazione di default", self.file_path)
            self.save()
            return
        try:
            with self.file_path.open("r", encoding="utf-8") as fp:
                data = json.load(fp)
        except Exception as exc:  # pragma: no cover - log di emergenza
            log.error("Impossibile caricare la configurazione: %s", exc)
            return
        self._config = BotConfig.from_dict(data)
        self._config.verification.guild_id = self._config.guild_id
        log.info("Configurazione caricata (prefix=%s)", self._config.prefix)

    def save(self) -> None:
        data = {
            "guild_id": self._config.guild_id,
            "prefix": self._config.prefix,
            "dashboard_sync_interval": self._config.dashboard_sync_interval,
            "freeze": asdict(self._config.freeze),
            "verification": asdict(self._config.verification),
            "notifier": self._config.notifier.to_dict(),
        }
        with self.file_path.open("w", encoding="utf-8") as fp:
            json.dump(data, fp, ensure_ascii=False, indent=2)
        log.debug("Configurazione salvata su %s", self.file_path)

    def update_from_dashboard(self, payload: Dict[str, Any]) -> BotConfig:
        log.info("Merge configurazione da dashboard")
        self._config.merge(payload)
        self._config.verification.guild_id = self._config.guild_id
        self.save()
        return self._config


# ========================= Dashboard bridge ============================


class DashboardBridge:
    """Gestisce la comunicazione con il backend della dashboard."""

    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.session: Optional[aiohttp.ClientSession] = None
        self._default_headers = {
            "User-Agent": "LegnaBot/2",
            "Content-Type": "application/json",
        }
        if self.api_key:
            self._default_headers["Authorization"] = f"Bearer {self.api_key}"

    async def ensure_session(self) -> aiohttp.ClientSession:
        if self.session and not self.session.closed:
            return self.session
        timeout = aiohttp.ClientTimeout(total=15)
        self.session = aiohttp.ClientSession(timeout=timeout)
        return self.session

    async def close(self) -> None:
        if self.session and not self.session.closed:
            await self.session.close()

    async def fetch_config(self, guild_id: int) -> Optional[Dict[str, Any]]:
        session = await self.ensure_session()
        url = f"{self.base_url}/api/bots/{guild_id}/config"
        try:
            async with session.get(url, headers=self._default_headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    log.info("Configurazione remota ricevuta dalla dashboard")
                    return data
                log.warning("Dashboard config GET %s -> %s", url, resp.status)
        except Exception as exc:
            log.error("Errore nel recupero configurazione dashboard: %s", exc)
        return None

    async def push_event(self, guild_id: int, event: str, payload: Dict[str, Any]) -> None:
        session = await self.ensure_session()
        url = f"{self.base_url}/api/bots/{guild_id}/events"
        body = {"event": event, "payload": payload, "timestamp": datetime.utcnow().isoformat()}
        try:
            async with session.post(url, json=body, headers=self._default_headers) as resp:
                if resp.status >= 400:
                    log.warning("Dashboard event POST %s -> %s", event, resp.status)
        except Exception as exc:
            log.error("Errore nell'invio evento alla dashboard: %s", exc)

    async def push_metrics(self, guild_id: int, metrics: Dict[str, Any]) -> None:
        session = await self.ensure_session()
        url = f"{self.base_url}/api/bots/{guild_id}/metrics"
        payload = {"metrics": metrics, "timestamp": datetime.utcnow().isoformat()}
        try:
            async with session.post(url, json=payload, headers=self._default_headers) as resp:
                if resp.status >= 400:
                    log.warning("Dashboard metrics POST -> %s", resp.status)
        except Exception as exc:
            log.error("Errore nell'invio metriche: %s", exc)


# ========================= Utility =====================================


def human_delta(target: datetime) -> str:
    now = datetime.now(UTC)
    delta = target - now
    minutes = max(0, int(delta.total_seconds() // 60))
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)
    parts = []
    if days:
        parts.append(f"{days}g")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    return " ".join(parts) or "0m"


# ========================= Cogs ========================================


class DashboardSyncCog(commands.Cog):
    """Sincronizzazione periodica con la dashboard."""

    def __init__(self, bot: commands.Bot, config_manager: ConfigManager, bridge: DashboardBridge):
        self.bot = bot
        self.config_manager = config_manager
        self.bridge = bridge
        self.metrics_updater.start()

    def cog_unload(self) -> None:
        self.metrics_updater.cancel()

    async def sync_config(self, reason: str) -> None:
        cfg = self.config_manager.config
        if not cfg.guild_id:
            log.warning("Guild ID non configurato, impossibile contattare la dashboard")
            return
        payload = await self.bridge.fetch_config(cfg.guild_id)
        if payload:
            self.config_manager.update_from_dashboard(payload)
            await self.bridge.push_event(
                cfg.guild_id,
                "config_synced",
                {"reason": reason, "prefix": cfg.prefix},
            )

    @tasks.loop(seconds=60)
    async def metrics_updater(self) -> None:
        await self.bot.wait_until_ready()
        cfg = self.config_manager.config
        if not cfg.guild_id:
            return
        guild = self.bot.get_guild(cfg.guild_id)
        if not guild:
            return
        metrics = {
            "member_count": guild.member_count,
            "online_members": sum(1 for m in guild.members if m.status != discord.Status.offline),
            "pending_verifications": len(getattr(self.bot, "pending_verifications", {})),
            "reminders": len(getattr(self.bot, "reminders", [])),
        }
        await self.bridge.push_metrics(cfg.guild_id, metrics)
        interval = max(30, cfg.dashboard_sync_interval)
        self.metrics_updater.change_interval(seconds=interval)

    @metrics_updater.before_loop
    async def before_metrics(self) -> None:
        await self.bot.wait_until_ready()
        # Sync iniziale config
        await self.sync_config("startup")

    @commands.hybrid_command(name="syncdashboard", description="Forza la sincronizzazione con la dashboard")
    @commands.has_permissions(administrator=True)
    async def syncdashboard(self, ctx: commands.Context) -> None:
        if ctx.interaction:
            await ctx.interaction.response.defer(thinking=True)
        await self.sync_config("manual")
        await ctx.reply("Configurazione aggiornata dalla dashboard ✅")


class VerificationCog(commands.Cog):
    """Gestione flusso di verifica utenti."""

    def __init__(self, bot: commands.Bot, config_manager: ConfigManager, bridge: DashboardBridge):
        self.bot = bot
        self.config_manager = config_manager
        self.bridge = bridge
        self.pending: Dict[int, datetime] = {}
        bot.pending_verifications = self.pending
        self.cleanup_task.start()

    def cog_unload(self) -> None:
        self.cleanup_task.cancel()

    async def assign_role(self, member: discord.Member, role_id: int) -> None:
        if not role_id:
            return
        role = member.guild.get_role(role_id)
        if role:
            with contextlib.suppress(discord.HTTPException):
                await member.add_roles(role, reason="Auto-verifica LegnaBot")

    async def remove_role(self, member: discord.Member, role_id: int) -> None:
        if not role_id:
            return
        role = member.guild.get_role(role_id)
        if role:
            with contextlib.suppress(discord.HTTPException):
                await member.remove_roles(role, reason="Auto-verifica LegnaBot")

    async def log_staff(self, guild: discord.Guild, message: str) -> None:
        cfg = self.config_manager.config.verification
        channel = guild.get_channel(cfg.staff_log_channel_id)
        if channel and isinstance(channel, discord.TextChannel):
            with contextlib.suppress(discord.HTTPException):
                await channel.send(message)

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member) -> None:
        cfg = self.config_manager.config.verification
        if member.guild.id != self.config_manager.config.guild_id:
            return
        await self.assign_role(member, cfg.unverified_role_id)
        self.pending[member.id] = datetime.now(UTC)
        channel = member.guild.get_channel(cfg.verify_channel_id)
        if channel and isinstance(channel, discord.TextChannel):
            welcome = cfg.welcome_message.replace("{member}", member.mention)
            with contextlib.suppress(discord.HTTPException):
                await channel.send(welcome)
        await self.bridge.push_event(
            member.guild.id,
            "member_join",
            {"member_id": member.id, "name": str(member), "pending": True},
        )

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member) -> None:
        if member.guild.id != self.config_manager.config.guild_id:
            return
        self.pending.pop(member.id, None)
        await self.bridge.push_event(
            member.guild.id,
            "member_leave",
            {"member_id": member.id, "name": str(member)},
        )

    @commands.hybrid_command(name="verify", description="Approva un utente")
    @commands.has_permissions(manage_roles=True)
    async def verify(self, ctx: commands.Context, member: discord.Member) -> None:
        cfg = self.config_manager.config.verification
        await self.remove_role(member, cfg.unverified_role_id)
        await self.assign_role(member, cfg.verified_role_id)
        self.pending.pop(member.id, None)
        await self.log_staff(ctx.guild, f"✅ {member.mention} verificato da {ctx.author.mention}")
        await ctx.reply(f"{member.mention} ora è verificato!", mention_author=False)
        await self.bridge.push_event(
            ctx.guild.id,
            "member_verified",
            {"member_id": member.id, "by": ctx.author.id},
        )

    @tasks.loop(minutes=1)
    async def cleanup_task(self) -> None:
        await self.bot.wait_until_ready()
        cfg = self.config_manager.config.verification
        timeout = timedelta(minutes=cfg.timeout_minutes)
        now = datetime.now(UTC)
        expired = [mid for mid, ts in self.pending.items() if now - ts > timeout]
        if not expired:
            return
        guild = self.bot.get_guild(cfg.guild_id)
        if not guild:
            return
        for member_id in expired:
            member = guild.get_member(member_id)
            if not member:
                continue
            with contextlib.suppress(discord.HTTPException):
                await member.kick(reason="Timeout verifica")
            await self.log_staff(guild, f"⏱️ {member.mention} rimosso per timeout verifica")
            self.pending.pop(member_id, None)
            await self.bridge.push_event(
                guild.id,
                "member_timeout",
                {"member_id": member_id},
            )


class ReminderCog(commands.Cog):
    """Gestione promemoria personali."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.reminders: List[Reminder] = []
        bot.reminders = self.reminders
        self._load()
        self.dispatcher.start()

    def cog_unload(self) -> None:
        self.dispatcher.cancel()
        self._save()

    def _load(self) -> None:
        if not REMINDERS_FILE.exists():
            return
        try:
            with REMINDERS_FILE.open("r", encoding="utf-8") as fp:
                data = json.load(fp)
        except Exception as exc:  # pragma: no cover
            log.error("Errore caricando reminders: %s", exc)
            return
        for entry in data:
            try:
                self.reminders.append(Reminder.from_dict(entry))
            except Exception as exc:
                log.warning("Reminder non valido scartato: %s", exc)

    def _save(self) -> None:
        data = [r.to_dict() for r in self.reminders]
        with REMINDERS_FILE.open("w", encoding="utf-8") as fp:
            json.dump(data, fp, ensure_ascii=False, indent=2)

    @tasks.loop(seconds=30)
    async def dispatcher(self) -> None:
        await self.bot.wait_until_ready()
        now = datetime.now(UTC)
        due = [r for r in self.reminders if r.trigger_at <= now]
        if not due:
            return
        for reminder in due:
            channel = self.bot.get_channel(reminder.channel_id)
            if channel and isinstance(channel, discord.TextChannel):
                with contextlib.suppress(discord.HTTPException):
                    await channel.send(f"<@{reminder.author_id}> ⏰ {reminder.message}")
        remaining = [r for r in self.reminders if r.trigger_at > now]
        self.reminders.clear()
        self.reminders.extend(remaining)
        self._save()

    @dispatcher.before_loop
    async def before_dispatcher(self) -> None:
        await self.bot.wait_until_ready()

    @commands.hybrid_command(name="remind", description="Crea un promemoria")
    async def remind(self, ctx: commands.Context, minutes: int, *, message: str) -> None:
        trigger = datetime.now(UTC) + timedelta(minutes=max(1, minutes))
        reminder = Reminder(
            author_id=ctx.author.id,
            channel_id=ctx.channel.id,
            message=message,
            trigger_at=trigger,
        )
        self.reminders.append(reminder)
        self._save()
        await ctx.reply(f"Promemoria impostato per {human_delta(trigger)}", mention_author=False)


class NotifierCog(commands.Cog):
    """Invia notifiche quando la dashboard segnala nuovi eventi streaming."""

    def __init__(self, bot: commands.Bot, config_manager: ConfigManager):
        self.bot = bot
        self.config_manager = config_manager
        self.poll_task.start()

    def cog_unload(self) -> None:
        self.poll_task.cancel()

    async def fetch_targets(self) -> List[NotifierTarget]:
        cfg = self.config_manager.config.notifier
        return cfg.targets if cfg.enabled else []

    @tasks.loop(minutes=10)
    async def poll_task(self) -> None:
        await self.bot.wait_until_ready()
        cfg = self.config_manager.config.notifier
        if not cfg.enabled or not cfg.notify_channel_id:
            return
        channel = self.bot.get_channel(cfg.notify_channel_id)
        if not isinstance(channel, discord.TextChannel):
            return
        for target in await self.fetch_targets():
            text = f"🔔 Nuova attività su {target.platform.title()}: {target.url}"
            if target.role_id:
                text = f"<@&{target.role_id}> {text}"
            with contextlib.suppress(discord.HTTPException):
                await channel.send(text)

    @poll_task.before_loop
    async def before_poll(self) -> None:
        await self.bot.wait_until_ready()


# ========================= Bot factory =================================


def build_bot(config_manager: ConfigManager, bridge: DashboardBridge) -> commands.Bot:
    intents = discord.Intents.default()
    intents.members = True
    intents.message_content = True
    intents.guilds = True

    bot = commands.Bot(command_prefix=lambda bot, msg: config_manager.config.prefix, intents=intents)

    @bot.event
    async def on_ready() -> None:
        log.info("LegnaBot collegato come %s", bot.user)
        await bot.change_presence(activity=discord.Game(name="Gestisco la dashboard"))

    @bot.event
    async def on_command_error(ctx: commands.Context, error: commands.CommandError) -> None:
        if isinstance(error, commands.MissingPermissions):
            await ctx.reply("Permessi insufficienti", mention_author=False)
            return
        if isinstance(error, commands.CommandNotFound):
            return
        log.exception("Errore comando: %s", error)
        await ctx.reply("Si è verificato un errore imprevisto", mention_author=False)

    bot.add_cog(DashboardSyncCog(bot, config_manager, bridge))
    bot.add_cog(VerificationCog(bot, config_manager, bridge))
    bot.add_cog(ReminderCog(bot))
    bot.add_cog(NotifierCog(bot, config_manager))

    @bot.hybrid_command(name="prefix", description="Visualizza o imposta il prefix del bot")
    @commands.has_permissions(manage_guild=True)
    async def prefix_cmd(ctx: commands.Context, new_prefix: Optional[str] = None) -> None:
        if not new_prefix:
            await ctx.reply(f"Il prefix attuale è `{config_manager.config.prefix}`", mention_author=False)
            return
        config_manager.config.prefix = new_prefix
        config_manager.save()
        await ctx.reply(f"Prefix aggiornato a `{new_prefix}`", mention_author=False)

    return bot


# ========================= Entrypoint ==================================


def main() -> None:
    config_manager = ConfigManager(CONFIG_FILE)
    if not config_manager.config.guild_id:
        log.warning("GUILD_ID non impostato: aggiorna il file config.json o le variabili d'ambiente")
    bridge = DashboardBridge(DASHBOARD_BASE_URL, DASHBOARD_API_KEY)

    bot = build_bot(config_manager, bridge)

    try:
        bot.run(DISCORD_TOKEN)
    finally:
        if bridge.session and not bridge.session.closed:
            asyncio.run(bridge.close())


if __name__ == "__main__":
    main()
