import os
import re
import json
import time
import asyncio
import datetime as dt
from pathlib import Path

import aiohttp
import discord
from cryptography.fernet import Fernet
from discord import app_commands
from discord.ext import tasks

# ===== НАСТРОЙКИ (из .env) =====
DISCORD_TOKEN = os.environ["DISCORD_TOKEN"]
BM_TOKEN = os.environ.get("BM_TOKEN", "").strip()          # необязательно: без токена API работает с меньшим лимитом
CHANNEL_ID = int(os.environ["CHANNEL_ID"])                  # канал для ежедневной сводки
MIN_HOURS = float(os.environ.get("MIN_HOURS", "8"))         # норма часов (и в гс, и в Rust)
WINDOW_HOURS = float(os.environ.get("WINDOW_HOURS", "24"))  # за какой период считать (24 = сутки, 168 = неделя)
REPORT_TIME_UTC = os.environ.get("REPORT_TIME_UTC", "09:00")  # во сколько слать сводку, UTC (09:00 UTC = 12:00 МСК)
SERVER_IDS = {s.strip() for s in os.environ.get("SERVER_IDS", "").split(",") if s.strip()}  # пусто = все сервера
# ================================

API = "https://api.battlemetrics.com"
BM_HEADERS = {"Authorization": f"Bearer {BM_TOKEN}"} if BM_TOKEN else {}
BASE = Path(__file__).parent
DATA_FILE = BASE / "data.json"
KEY_FILE = BASE / "secret.key"
KEEP_DAYS = 14

_h, _m = (int(x) for x in REPORT_TIME_UTC.split(":"))
REPORT_TIME = dt.time(hour=_h, minute=_m, tzinfo=dt.timezone.utc)


# ---------- шифрование карт ----------
def load_fernet() -> Fernet:
    if not KEY_FILE.exists():
        KEY_FILE.write_bytes(Fernet.generate_key())
        os.chmod(KEY_FILE, 0o600)
    return Fernet(KEY_FILE.read_bytes())


fernet = load_fernet()


def luhn_ok(num: str) -> bool:
    total = 0
    for i, ch in enumerate(reversed(num)):
        d = int(ch)
        if i % 2:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return total % 10 == 0


# ---------- хранилище ----------
class Store:
    """profiles: discord_id -> {steam, bm, region, card_enc}; voice: список [discord_id, start_ts, end_ts]."""

    def __init__(self):
        self.profiles: dict[str, dict] = {}
        self.voice: list[list] = []
        self.open_voice: dict[str, float] = {}  # кто сейчас в гс -> время входа
        if DATA_FILE.exists():
            d = json.loads(DATA_FILE.read_text(encoding="utf-8"))
            self.profiles = d.get("profiles", {})
            self.voice = d.get("voice", [])
            for uid, bm_id in d.get("links", {}).items():  # данные прошлой версии
                self.profiles.setdefault(uid, {"steam": "", "bm": bm_id, "region": "", "card_enc": ""})

    def save(self):
        cutoff = time.time() - KEEP_DAYS * 86400
        self.voice = [v for v in self.voice if v[2] >= cutoff]
        tmp = DATA_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps({"profiles": self.profiles, "voice": self.voice}, ensure_ascii=False),
                       encoding="utf-8")
        os.chmod(tmp, 0o600)
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


# ---------- сводка ----------
async def build_report() -> list[str]:
    if not store.profiles:
        return ["Нет заполненных профилей. Админ может выложить кнопку командой `/panel`."]

    now_dt = dt.datetime.now(dt.timezone.utc)
    since_dt = now_dt - dt.timedelta(hours=WINDOW_HOURS)
    now, since = now_dt.timestamp(), since_dt.timestamp()

    lazy, ok, failed = [], 0, []
    async with aiohttp.ClientSession() as session:
        for uid, p in store.profiles.items():
            # шаг 1: гс. Набрал норму — BattleMetrics не трогаем
            voice = store.voice_hours(uid, since, now)
            if voice >= MIN_HOURS:
                ok += 1
                continue
            # шаг 2: в гс мало — смотрим Rust в BattleMetrics
            try:
                rust = await rust_hours(session, p["bm"], since_dt, now_dt)
            except Exception as e:
                print(f"BM ошибка для {uid}/{p.get('bm')}: {e}")
                failed.append(uid)
                continue
            if rust >= MIN_HOURS:
                ok += 1
            else:
                lazy.append((uid, p, voice, rust))

    period = f"{WINDOW_HOURS:g} ч"
    lines = []
    if lazy:
        lazy.sort(key=lambda x: (x[3], x[2]))
        lines.append(f"⚠️ Не набрали {MIN_HOURS:g} ч ни в гс, ни в Rust за последние {period}:")
        for uid, p, voice, rust in lazy:
            extra = []
            if p.get("region"):
                extra.append(p["region"])
            if p.get("steam"):
                extra.append(f"[Steam](<{p['steam']}>)")
            extra.append(f"[BM](<https://www.battlemetrics.com/players/{p['bm']}>)")
            lines.append(f"• <@{uid}> — гс {voice:.1f} ч, Rust {rust:.1f} ч · " + " · ".join(extra))
    else:
        lines.append(f"✅ За последние {period} все проверенные выполнили норму ({MIN_HOURS:g} ч).")
    if ok:
        lines.append(f"Выполнили норму: {ok}")
    if failed:
        lines.append("Не удалось проверить BattleMetrics: " + ", ".join(f"<@{u}>" for u in failed))

    chunks, cur = [], ""
    for line in lines:  # режем под лимит Discord (2000 символов)
        if len(cur) + len(line) + 1 > 1900:
            chunks.append(cur)
            cur = ""
        cur += line + "\n"
    chunks.append(cur)
    return chunks


# ---------- профиль ----------
class ProfileModal(discord.ui.Modal, title="Мой профиль"):
    def __init__(self, existing: dict | None = None):
        super().__init__()
        e = existing or {}
        self.steam = discord.ui.TextInput(
            label="Ваш Steam ID", placeholder="76561198000000000 или ссылка на профиль Steam",
            default=e.get("steam") or None, max_length=200)
        self.bm = discord.ui.TextInput(
            label="Ваш BattleMetrics", placeholder="https://www.battlemetrics.com/players/123456789",
            default=f"https://www.battlemetrics.com/players/{e['bm']}" if e.get("bm") else None, max_length=200)
        self.region = discord.ui.TextInput(
            label="Регион", placeholder="Например: Москва", default=e.get("region") or None, max_length=60)
        self.card = discord.ui.TextInput(
            label="Карта для выплат (только номер)", placeholder="Без срока и CVV. Пусто = оставить прежнюю",
            required=False, max_length=30)
        for item in (self.steam, self.bm, self.region, self.card):
            self.add_item(item)

    async def on_submit(self, interaction: discord.Interaction):
        uid = str(interaction.user.id)
        old = store.profiles.get(uid, {})

        raw_steam = self.steam.value.strip()
        m = re.search(r"7656119\d{10}", raw_steam)
        if m:
            steam = f"https://steamcommunity.com/profiles/{m.group(0)}"
        elif "steamcommunity.com/" in raw_steam:
            steam = raw_steam if raw_steam.startswith("http") else "https://" + raw_steam
        else:
            await interaction.response.send_message(
                "Steam ID не распознан. Нужно 17 цифр (начинается с 7656119) или ссылка на профиль Steam.",
                ephemeral=True)
            return

        m = re.search(r"players/(\d+)", self.bm.value) or re.fullmatch(r"\s*(\d+)\s*", self.bm.value)
        if not m:
            await interaction.response.send_message(
                "Не нашёл ID в ссылке BattleMetrics. Нужна ссылка вида battlemetrics.com/players/123456789",
                ephemeral=True)
            return
        bm_id = int(m.group(1))

        card_digits = re.sub(r"\D", "", self.card.value)
        if self.card.value.strip():
            if not (13 <= len(card_digits) <= 19) or not luhn_ok(card_digits):
                await interaction.response.send_message(
                    "Номер карты не прошёл проверку. Введи только номер (13-19 цифр), без срока и CVV.",
                    ephemeral=True)
                return
            card_enc = fernet.encrypt(card_digits.encode()).decode()
        else:
            card_enc = old.get("card_enc", "")

        await interaction.response.defer(ephemeral=True)
        async with aiohttp.ClientSession() as session:
            name = await bm_player_name(session, bm_id)
        if name is None:
            await interaction.followup.send(
                "Профиль BattleMetrics не открылся (неверная ссылка или сайт недоступен). Проверь ссылку и попробуй ещё раз.",
                ephemeral=True)
            return

        store.profiles[uid] = {"steam": steam, "bm": bm_id, "region": self.region.value.strip(), "card_enc": card_enc}
        store.save()
        card_note = f"карта •••• {card_digits[-4:]}" if self.card.value.strip() else (
            "карта сохранена ранее" if card_enc else "карта не указана")
        await interaction.followup.send(
            f"Профиль сохранён. Ник в Rust: **{name}**, регион: {self.region.value.strip()}, {card_note}.",
            ephemeral=True)


class PanelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)  # постоянные кнопки, работают после перезапуска

    @discord.ui.button(label="Мой профиль", style=discord.ButtonStyle.primary, custom_id="panel:profile")
    async def profile(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(ProfileModal(store.profiles.get(str(interaction.user.id))))


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
        for guild in self.guilds:  # подхватить тех, кто уже сидит в гс на момент запуска
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


def is_admin(interaction: discord.Interaction) -> bool:
    perms = getattr(interaction.user, "guild_permissions", None)
    return bool(perms and perms.manage_guild)


@bot.tree.command(name="panel", description="Выложить сообщение с кнопкой профиля (для админов)")
@app_commands.default_permissions(manage_guild=True)
async def panel(interaction: discord.Interaction):
    if not is_admin(interaction):
        await interaction.response.send_message("Только для админов.", ephemeral=True)
        return
    await interaction.channel.send(
        "**Клановая панель**\nНажми «Мой профиль» и заполни: Steam ID, ссылку на BattleMetrics, регион и карту для выплат. "
        "Без этого бот не видит твой онлайн. Карта хранится в зашифрованном виде, видна только админам.",
        view=PanelView(),
    )
    await interaction.response.send_message("Панель выложена.", ephemeral=True)


@bot.tree.command(name="report", description="Запустить проверку прямо сейчас (для админов)")
@app_commands.default_permissions(manage_guild=True)
async def report(interaction: discord.Interaction):
    if not is_admin(interaction):
        await interaction.response.send_message("Только для админов.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    for chunk in await build_report():
        await interaction.followup.send(chunk, allowed_mentions=discord.AllowedMentions.none())


@bot.tree.command(name="card", description="Показать карту игрока для выплаты (для админов)")
@app_commands.describe(member="Чью карту показать")
@app_commands.default_permissions(manage_guild=True)
async def card(interaction: discord.Interaction, member: discord.Member):
    if not is_admin(interaction):
        await interaction.response.send_message("Только для админов.", ephemeral=True)
        return
    p = store.profiles.get(str(member.id))
    if not p or not p.get("card_enc"):
        await interaction.response.send_message("У этого игрока карта не указана.", ephemeral=True)
        return
    num = fernet.decrypt(p["card_enc"].encode()).decode()
    grouped = " ".join(num[i:i + 4] for i in range(0, len(num), 4))
    await interaction.response.send_message(
        f"{member.mention}: `{grouped}`\nРегион: {p.get('region') or '—'}. Видно только тебе.",
        ephemeral=True, allowed_mentions=discord.AllowedMentions.none())


bot.run(DISCORD_TOKEN)
