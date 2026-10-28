import os
import re
import json
import time
import asyncio
import datetime as dt
from pathlib import Path

import aiohttp
import discord
from discord import app_commands
from discord.ext import tasks

# ===== НАСТРОЙКИ (из .env) =====
DISCORD_TOKEN = os.environ["DISCORD_TOKEN"]
BM_TOKEN = os.environ.get("BM_TOKEN", "").strip()          # необязательно: без токена API работает с меньшим лимитом
CHANNEL_ID = int(os.environ["CHANNEL_ID"])                  # канал для ежедневного отчёта
MIN_HOURS = float(os.environ.get("MIN_HOURS", "8"))         # норма часов в Rust
WINDOW_HOURS = float(os.environ.get("WINDOW_HOURS", "24"))  # за какой период считать (24 = сутки, 168 = неделя)
REPORT_TIME_UTC = os.environ.get("REPORT_TIME_UTC", "09:00")  # во сколько слать отчёт, по UTC (09:00 UTC = 12:00 МСК)
SERVER_IDS = {s.strip() for s in os.environ.get("SERVER_IDS", "").split(",") if s.strip()}  # пусто = все сервера
VOICE_FLAG_HOURS = 1.0                                      # сколько часов в гс считать «сидел в гс»
# ================================

API = "https://api.battlemetrics.com"
BM_HEADERS = {"Authorization": f"Bearer {BM_TOKEN}"} if BM_TOKEN else {}
DATA_FILE = Path(__file__).with_name("data.json")
KEEP_DAYS = 14

_h, _m = (int(x) for x in REPORT_TIME_UTC.split(":"))
REPORT_TIME = dt.time(hour=_h, minute=_m, tzinfo=dt.timezone.utc)


# ---------- хранилище ----------
class Store:
    """links: discord_id -> battlemetrics_id; voice: список [discord_id, start_ts, end_ts]."""

    def __init__(self):
        self.links: dict[str, int] = {}
        self.voice: list[list] = []
        self.open_voice: dict[str, float] = {}  # кто сейчас в гс -> время входа
        if DATA_FILE.exists():
            d = json.loads(DATA_FILE.read_text(encoding="utf-8"))
            self.links = d.get("links", {})
            self.voice = d.get("voice", [])

    def save(self):
        cutoff = time.time() - KEEP_DAYS * 86400
        self.voice = [v for v in self.voice if v[2] >= cutoff]
        tmp = DATA_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps({"links": self.links, "voice": self.voice}, ensure_ascii=False), encoding="utf-8")
        tmp.replace(DATA_FILE)

    def voice_start(self, uid: str):
        self.open_voice.setdefault(uid, time.time())

    def voice_stop(self, uid: str):
        start = self.open_voice.pop(uid, None)
        if start is not None:
            self.voice.append([uid, start, time.time()])
            self.save()

    def voice_hours(self, uid: str, since: float, now: float) -> float:
        total = 0.0
        spans = [(s, e) for u, s, e in self.voice if u == uid]
        if uid in self.open_voice:
            spans.append((self.open_voice[uid], now))
        for s, e in spans:
            overlap = min(e, now) - max(s, since)
            if overlap > 0:
                total += overlap
        return total / 3600


store = Store()


# ---------- BattleMetrics ----------
def parse_ts(s: str) -> dt.datetime:
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))


async def bm_get(session: aiohttp.ClientSession, url: str, params=None) -> dict:
    """GET с паузой и повторами при лимите (429)."""
    for attempt in range(6):
        async with session.get(url, params=params, headers=BM_HEADERS) as r:
            if r.status == 429:
                wait = float(r.headers.get("Retry-After", 5 * (attempt + 1)))
                await asyncio.sleep(min(wait, 60))
                continue
            r.raise_for_status()
            await asyncio.sleep(1.2)  # не долбим API (без токена лимит низкий)
            return await r.json()
    raise RuntimeError("BattleMetrics: слишком много запросов (429)")


async def rust_hours(session, bm_id: int, since: dt.datetime, now: dt.datetime) -> float:
    total = 0.0
    url = f"{API}/players/{bm_id}/relationships/sessions"
    params = {"page[size]": 100}
    while url:
        data = await bm_get(session, url, params)
        stop_paging = False
        for s in data.get("data", []):
            a = s["attributes"]
            start = parse_ts(a["start"])
            stop = parse_ts(a["stop"]) if a.get("stop") else now
            if stop < since:
                stop_paging = True  # сессии идут от новых к старым
                continue
            if SERVER_IDS:
                srv = (s.get("relationships", {}).get("server", {}).get("data") or {}).get("id")
                if srv not in SERVER_IDS:
                    continue
            overlap = (min(stop, now) - max(start, since)).total_seconds()
            if overlap > 0:
                total += overlap / 3600
        url = None if stop_paging else data.get("links", {}).get("next")
        params = None
    return total


async def bm_player_name(session, bm_id: int) -> str | None:
    try:
        data = await bm_get(session, f"{API}/players/{bm_id}")
        return data["data"]["attributes"]["name"]
    except Exception:
        return None


# ---------- отчёт ----------
async def build_report() -> list[str]:
    if not store.links:
        return ["Нет привязанных игроков. Админ может выложить кнопки командой `/panel`."]

    now_dt = dt.datetime.now(dt.timezone.utc)
    since_dt = now_dt - dt.timedelta(hours=WINDOW_HOURS)
    now, since = now_dt.timestamp(), since_dt.timestamp()

    lazy, ok, failed = [], 0, []
    async with aiohttp.ClientSession() as session:
        for uid, bm_id in store.links.items():
            voice = store.voice_hours(uid, since, now)
            try:
                rust = await rust_hours(session, bm_id, since_dt, now_dt)
            except Exception as e:
                print(f"BM ошибка для {uid}/{bm_id}: {e}")
                failed.append(uid)
                continue
            if rust >= MIN_HOURS:
                ok += 1
            else:
                lazy.append((uid, rust, voice))

    period = f"{WINDOW_HOURS:g} ч"
    lines = []
    if lazy:
        lazy.sort(key=lambda x: x[1])
        lines.append(f"⚠️ Не набрали {MIN_HOURS:g} ч в Rust за последние {period}:")
        for uid, rust, voice in lazy:
            note = ""
            if voice >= VOICE_FLAG_HOURS:
                note = " — **был в гс, но не играл**"
            lines.append(f"• <@{uid}> — Rust {rust:.1f} ч, гс {voice:.1f} ч{note}")
    else:
        lines.append(f"✅ За последние {period} все проверенные выполнили норму ({MIN_HOURS:g} ч).")
    if ok:
        lines.append(f"Выполнили норму: {ok}")
    if failed:
        lines.append("Не удалось проверить: " + ", ".join(f"<@{u}>" for u in failed))

    # режем под лимит Discord (2000 символов)
    chunks, cur = [], ""
    for line in lines:
        if len(cur) + len(line) + 1 > 1900:
            chunks.append(cur)
            cur = ""
        cur += line + "\n"
    chunks.append(cur)
    return chunks


# ---------- кнопки ----------
class LinkModal(discord.ui.Modal, title="Привязать профиль BattleMetrics"):
    url = discord.ui.TextInput(
        label="Ссылка на твой профиль",
        placeholder="https://www.battlemetrics.com/players/123456789",
        max_length=200,
    )

    async def on_submit(self, interaction: discord.Interaction):
        raw = self.url.value
        m = re.search(r"players/(\d+)", raw) or re.fullmatch(r"\s*(\d+)\s*", raw)
        if not m:
            await interaction.response.send_message(
                "Не нашёл ID в ссылке. Нужна ссылка вида battlemetrics.com/players/123456789", ephemeral=True)
            return
        bm_id = int(m.group(1))
        await interaction.response.defer(ephemeral=True)
        async with aiohttp.ClientSession() as session:
            name = await bm_player_name(session, bm_id)
        if name is None:
            await interaction.followup.send(
                "Профиль не открылся (неверная ссылка или BattleMetrics недоступен). Проверь ссылку и попробуй ещё раз.",
                ephemeral=True)
            return
        store.links[str(interaction.user.id)] = bm_id
        store.save()
        await interaction.followup.send(f"Привязал: игровой ник **{name}** (ID {bm_id}).", ephemeral=True)


class PanelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)  # постоянные кнопки, работают после перезапуска

    @discord.ui.button(label="Привязать профиль", style=discord.ButtonStyle.primary, custom_id="panel:link")
    async def link(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(LinkModal())


# ---------- бот ----------
class Bot(discord.Client):
    def __init__(self):
        intents = discord.Intents.default()  # voice_states входит в default
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)

    async def setup_hook(self):
        self.add_view(PanelView())
        await self.tree.sync()
        self.daily_report.start()

    async def on_ready(self):
        # подхватить тех, кто уже сидит в гс на момент запуска
        for guild in self.guilds:
            for vc in guild.voice_channels:
                for m in vc.members:
                    if not m.bot:
                        store.voice_start(str(m.id))
        print(f"Бот запущен как {self.user}")

    async def on_voice_state_update(self, member, before, after):
        if member.bot:
            return
        uid = str(member.id)
        if before.channel is None and after.channel is not None:
            store.voice_start(uid)
        elif before.channel is not None and after.channel is None:
            store.voice_stop(uid)

    @tasks.loop(time=REPORT_TIME)
    async def daily_report(self):
        channel = self.get_channel(CHANNEL_ID) or await self.fetch_channel(CHANNEL_ID)
        for chunk in await build_report():
            await channel.send(chunk, allowed_mentions=discord.AllowedMentions.none())


bot = Bot()


@bot.tree.command(name="panel", description="Выложить сообщение с кнопками (для админов)")
@app_commands.default_permissions(manage_guild=True)
async def panel(interaction: discord.Interaction):
    await interaction.channel.send(
        "**Клановая панель**\nНажми «Привязать профиль» и вставь ссылку на свой профиль BattleMetrics. "
        "Без этого бот не видит твой онлайн в Rust.",
        view=PanelView(),
    )
    await interaction.response.send_message("Панель выложена.", ephemeral=True)


@bot.tree.command(name="report", description="Запустить проверку прямо сейчас (для админов)")
@app_commands.default_permissions(manage_guild=True)
async def report(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    chunks = await build_report()
    await interaction.followup.send(chunks[0], allowed_mentions=discord.AllowedMentions.none())
    for chunk in chunks[1:]:
        await interaction.followup.send(chunk, allowed_mentions=discord.AllowedMentions.none())


bot.run(DISCORD_TOKEN)
