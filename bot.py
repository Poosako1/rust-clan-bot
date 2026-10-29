import os
import re
import json
import time
import asyncio
import difflib
import datetime as dt
from pathlib import Path

import aiohttp
import discord
from discord import app_commands
from discord.ext import tasks

# ===== НАСТРОЙКИ (из .env) =====
DISCORD_TOKEN = os.environ["DISCORD_TOKEN"]
BM_TOKEN = os.environ.get("BM_TOKEN", "").strip()          # необязательно: без токена API работает с меньшим лимитом
CHANNEL_ID = int(os.environ["CHANNEL_ID"])                  # канал для ежедневной сводки
SCRIPT_URL = os.environ["SCRIPT_URL"]                       # адрес веб-приложения Google Apps Script из таблицы
SCRIPT_SECRET = os.environ["SCRIPT_SECRET"]                 # секрет, такой же как в скрипте таблицы
MIN_HOURS = float(os.environ.get("MIN_HOURS", "8"))         # норма часов (и в гс, и в Rust)
WINDOW_HOURS = float(os.environ.get("WINDOW_HOURS", "24"))  # период ежедневной проверки (24 = сутки)
REPORT_TIME_UTC = os.environ.get("REPORT_TIME_UTC", "09:00")  # во сколько слать сводку, UTC (09:00 UTC = 12:00 МСК)
SERVER_IDS = {s.strip() for s in os.environ.get("SERVER_IDS", "").split(",") if s.strip()}  # пусто = все сервера
TZ_OFFSET_HOURS = float(os.environ.get("TZ_OFFSET_HOURS", "3"))  # часовой пояс для дат вайпа (3 = МСК)
WIPE_GRACE_MIN = float(os.environ.get("WIPE_GRACE_MIN", "5"))    # сколько минут после старта вайпа ещё не опоздание
# ================================

API = "https://api.battlemetrics.com"
BM_HEADERS = {"Authorization": f"Bearer {BM_TOKEN}"} if BM_TOKEN else {}
BASE = Path(__file__).parent
DATA_FILE = BASE / "data.json"
KEEP_DAYS = 45
TZ = dt.timezone(dt.timedelta(hours=TZ_OFFSET_HOURS))
NO_PINGS = discord.AllowedMentions.none()

_h, _m = (int(x) for x in REPORT_TIME_UTC.split(":"))
REPORT_TIME = dt.time(hour=_h, minute=_m, tzinfo=dt.timezone.utc)


# ---------- даты ----------
def parse_dt(s: str) -> float:
    s = s.strip().replace("/", ".")
    for fmt in ("%d.%m.%Y %H:%M", "%d.%m.%y %H:%M", "%d.%m.%Y %H:%M:%S"):
        try:
            return dt.datetime.strptime(s, fmt).replace(tzinfo=TZ).timestamp()
        except ValueError:
            pass
    raise ValueError(s)


def fmt_dt(ts: float) -> str:
    return dt.datetime.fromtimestamp(ts, TZ).strftime("%d.%m.%Y %H:%M")


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


# ---------- локальное хранилище: учёт гс и вайпы ----------
class Store:
    """voice: [[discord_id, start, end]]; open_voice: кто сейчас в гс; wipes: имя -> состояние вайпа."""

    def __init__(self):
        self.voice: list[list] = []
        self.open_voice: dict[str, float] = {}
        self.heartbeat: float = time.time()
        self.wipes: dict[str, dict] = {}
        if DATA_FILE.exists():
            d = json.loads(DATA_FILE.read_text(encoding="utf-8"))
            self.voice = d.get("voice", [])
            self.open_voice = d.get("open_voice", {})
            self.heartbeat = d.get("heartbeat", time.time())
            self.wipes = d.get("wipes", {})

    def save(self):
        self.heartbeat = time.time()
        cutoff = time.time() - KEEP_DAYS * 86400
        self.voice = [v for v in self.voice if v[2] >= cutoff]
        tmp = DATA_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps({"voice": self.voice, "open_voice": self.open_voice,
                                   "heartbeat": self.heartbeat, "wipes": self.wipes}), encoding="utf-8")
        tmp.replace(DATA_FILE)

    def reconcile(self, in_voice_now: set[str]):
        """После перезапуска: кто остался в гс — продолжаем сессию, кто ушёл — закрываем по последнему heartbeat."""
        for uid, start in list(self.open_voice.items()):
            if uid not in in_voice_now:
                self.voice.append([uid, start, max(start, self.heartbeat)])
                del self.open_voice[uid]
        for uid in in_voice_now:
            self.open_voice.setdefault(uid, time.time())
        self.save()

    def voice_start(self, uid: str):
        self.open_voice.setdefault(uid, time.time())
        self.save()

    def voice_stop(self, uid: str):
        start = self.open_voice.pop(uid, None)
        if start is not None:
            self.voice.append([uid, start, time.time()])
        self.save()

    def spans(self, uid: str, now: float) -> list[tuple[float, float]]:
        out = [(s, e) for u, s, e in self.voice if u == uid]
        if uid in self.open_voice:
            out.append((self.open_voice[uid], now))
        return out

    def voice_hours(self, uid: str, since: float, until: float) -> float:
        total = 0.0
        for s, e in self.spans(uid, until):
            overlap = min(e, until) - max(s, since)
            if overlap > 0:
                total += overlap
        return total / 3600

    def arrival(self, uid: str, start: float, end: float, now: float) -> float | None:
        """Когда игрок пришёл в гс на вайп: start, если уже сидел в гс на старте; иначе первый вход после старта."""
        spans = self.spans(uid, now)
        if any(s <= start < e for s, e in spans):
            return start
        later = [s for s, e in spans if start < s < end]
        return min(later) if later else None


store = Store()


# ---------- таблица через Google Apps Script ----------
async def sheet_call(payload: dict) -> dict:
    payload = {**payload, "secret": SCRIPT_SECRET}
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(SCRIPT_URL, json=payload) as r:  # Google отвечает редиректом, aiohttp идёт за ним
            r.raise_for_status()
            data = await r.json(content_type=None)
    if not data.get("ok"):
        raise RuntimeError(f"таблица: {data.get('error')}")
    return data


SHEET_PLAYERS, SHEET_WIPES, SHEET_RESULTS = "общее", "вайп", "итоги вайпа"
PLAYER_COLS = 9  # Никнейм | роль | зарплата | банк | батлметрикс | Discord ID | Steam | Регион | Discord


async def sheet_get(name: str) -> list[list[str]]:
    """Весь лист вместе с заголовком."""
    return (await sheet_call({"ops": [{"op": "get", "sheet": name}]}))["results"][0]


def padded(row: list, n: int) -> list:
    return list(row) + [""] * (n - len(row))


def role_rank(role: str) -> int:
    r = role.strip().lower()
    return ROLES.index(r) if r in ROLES else 99


async def sheet_players() -> dict[str, dict]:
    """Игроки с привязанным Discord ID (столбец F листа «общее»)."""
    out = {}
    for r in (await sheet_get(SHEET_PLAYERS))[1:]:
        r = padded(r, PLAYER_COLS)
        if not re.fullmatch(r"\d{15,25}", r[5].strip()):
            continue
        m = re.search(r"players/(\d+)", r[4])
        out[r[5].strip()] = {"nick": r[0].strip(), "role": r[1].strip(), "steam": r[6].strip(),
                             "bm": int(m.group(1)) if m else None, "region": r[7].strip()}
    return out


async def upsert_player(p: dict) -> dict:
    """Найти строку игрока (Discord ID, потом ссылка BM, потом ник) или вставить новую в блок его роли."""
    rows = await sheet_get(SHEET_PLAYERS)
    body = [padded(r, PLAYER_COLS) for r in rows[1:]]
    idx, how = None, "new"
    for i, r in enumerate(body):
        if r[5].strip() == p["discord"]:
            idx, how = i, "discord"
            break
    if idx is None:
        for i, r in enumerate(body):
            m = re.search(r"players/(\d+)", r[4])
            if m and int(m.group(1)) == p["bm_id"]:
                idx, how = i, "bm"
                break
    if idx is None and p.get("nick"):
        for i, r in enumerate(body):
            if r[0].strip().lower() == p["nick"].strip().lower():
                idx, how = i, "nick"
                break

    ops = []
    if idx is not None:
        row_no, existing = idx + 2, body[idx]
    else:
        mine = role_rank(p.get("role", ""))
        pos = next((i for i, r in enumerate(body) if role_rank(r[1]) > mine), len(body))
        row_no, existing = pos + 2, [""] * PLAYER_COLS
        if pos < len(body):
            ops.append({"op": "insert_before", "sheet": SHEET_PLAYERS, "row": row_no})
    cells = dict(p.get("cells", {}))
    for col, value in p.get("if_empty", {}).items():
        if not existing[int(col) - 1].strip():
            cells[col] = value
    for col, value in cells.items():
        ops.append({"op": "set", "sheet": SHEET_PLAYERS, "row": row_no, "col": int(col),
                    "values": [[str(value)]], "text": True})
    await sheet_call({"ops": ops})
    return {"how": how, "row": row_no}


async def sheet_wipe_status(w: dict, status: str):
    w["status"] = status
    store.save()
    rows = await sheet_get(SHEET_WIPES)
    row_no = next((i + 1 for i, r in enumerate(rows) if i > 0 and r and r[0] == w["name"]), max(len(rows), 1) + 1)
    await sheet_call({"ops": [{"op": "set", "sheet": SHEET_WIPES, "row": row_no, "col": 1, "text": True,
                               "values": [[w["name"], fmt_dt(w["start"]), fmt_dt(w["end"]), status]]}]})


async def sheet_results_write(wipe: str, results: list[list]):
    """Заменить строки этого вайпа на листе «итоги вайпа»."""
    rows = await sheet_get(SHEET_RESULTS)
    drop = [i + 1 for i, r in enumerate(rows) if i > 0 and r and r[0] == wipe]
    start = max(len(rows) - len(drop), 1) + 1
    ops = []
    if drop:
        ops.append({"op": "delete_rows", "sheet": SHEET_RESULTS, "rows": drop})
    if results:
        ops.append({"op": "set", "sheet": SHEET_RESULTS, "row": start, "col": 1, "text": True,
                    "values": [r[:4] for r in results]})
        ops.append({"op": "set", "sheet": SHEET_RESULTS, "row": start, "col": 5, "values": [r[4:7] for r in results]})
    if ops:
        await sheet_call({"ops": ops})


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


def short_error(e: Exception) -> str:
    """Короткая причина сбоя для показа в чате."""
    if isinstance(e, aiohttp.ClientResponseError):
        return f"BattleMetrics ответил {e.status} {e.message}"[:120]
    return f"{type(e).__name__}: {e}"[:120]


def chunk_lines(lines: list[str], limit: int = 1900) -> list[str]:
    chunks, cur = [], ""
    for line in lines:
        if len(cur) + len(line) + 1 > limit:
            chunks.append(cur)
            cur = ""
        cur += line + "\n"
    if cur:
        chunks.append(cur)
    return chunks


# ---------- ежедневная сводка ----------
async def build_report() -> list[str]:
    profiles = await sheet_players()
    if not profiles:
        return ["В таблице нет игроков с привязанным Discord. Админ может выложить кнопку командой `/panel`."]

    now_dt = dt.datetime.now(dt.timezone.utc)
    since_dt = now_dt - dt.timedelta(hours=WINDOW_HOURS)
    now, since = now_dt.timestamp(), since_dt.timestamp()

    lazy, ok, failed = [], 0, []
    async with aiohttp.ClientSession() as session:
        for uid, p in profiles.items():
            # шаг 1: гс. Набрал норму — BattleMetrics не трогаем
            voice = store.voice_hours(uid, since, now)
            if voice >= MIN_HOURS:
                ok += 1
                continue
            # шаг 2: в гс мало — смотрим Rust в BattleMetrics
            if p["bm"] is None:
                failed.append((uid, "нет ссылки BattleMetrics в таблице"))
                continue
            try:
                rust = await rust_hours(session, p["bm"], since_dt, now_dt)
            except Exception as e:
                print(f"BM ошибка для {uid}/{p['bm']}: {short_error(e)}", flush=True)
                failed.append((uid, short_error(e)))
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
            extra = [p["role"]] if p["role"] else []
            if p["region"]:
                extra.append(p["region"])
            extra.append(f"[BM](<https://www.battlemetrics.com/players/{p['bm']}>)")
            lines.append(f"• <@{uid}> ({p['nick']}) — гс {voice:.1f} ч, Rust {rust:.1f} ч · " + " · ".join(extra))
    elif ok:
        lines.append(f"✅ За последние {period} все проверенные выполнили норму ({MIN_HOURS:g} ч): {ok}.")
    else:
        lines.append(f"Проверить не удалось никого, нормы за последние {period} неизвестны.")
    if ok and lazy:
        lines.append(f"Выполнили норму: {ok}")
    if failed:
        lines.append("Не удалось проверить:")
        for uid, reason in failed:
            nick = profiles[uid]["nick"]
            lines.append(f"• <@{uid}> ({nick}): {reason}")
    return chunk_lines(lines)


# ---------- вайп ----------
def attendance_label(arrival: float | None, start: float, final: bool) -> tuple[str, int | str]:
    """-> (явка, опоздание в минутах)."""
    if arrival is None:
        return ("не явился" if final else "нет в гс (пока)"), ""
    late = max(0, round((arrival - start) / 60))
    return ("вовремя" if late <= WIPE_GRACE_MIN else "опоздал"), late


async def attendance_rows(w: dict, profiles: dict, final: bool, now: float, with_rust=None) -> list[list]:
    rows = []
    for uid, p in profiles.items():
        arr = store.arrival(uid, w["start"], w["end"], now)
        label, late = attendance_label(arr, w["start"], final)
        voice = round(store.voice_hours(uid, w["start"], min(now, w["end"])), 1)
        rust = with_rust.get(uid, "") if with_rust is not None else ""
        rows.append([w["name"], p["nick"], uid, label, late, voice, rust])
    return rows


async def start_wipe(bot, w: dict):
    profiles = await sheet_players()
    now = time.time()
    await sheet_wipe_status(w, "идёт")
    rows = await attendance_rows(w, profiles, final=False, now=now)
    await sheet_results_write(w["name"], rows)
    present = [r for r in rows if r[3] == "вовремя"]
    absent = [r for r in rows if r[3] != "вовремя"]
    lines = [f"🟢 **{w['name']}** начался ({fmt_dt(w['start'])}). В гс: {len(present)} из {len(rows)}."]
    if absent:
        lines.append("Нет в гс: " + ", ".join(f"<@{r[2]}>" for r in absent))
        lines.append(f"Кто зайдёт позже, будет отмечен как опоздавший (допуск {WIPE_GRACE_MIN:g} мин).")
    channel = bot.get_channel(w["channel"]) or await bot.fetch_channel(w["channel"])
    for chunk in chunk_lines(lines):
        await channel.send(chunk, allowed_mentions=NO_PINGS)


async def finish_wipe(bot, w: dict):
    w["status"] = "подсчёт"  # чтобы тик не запустил подсчёт повторно
    store.save()
    channel = bot.get_channel(w["channel"]) or await bot.fetch_channel(w["channel"])
    try:
        await _finish_wipe(channel, w)
    except Exception as e:
        print(f"Ошибка подсчёта итогов «{w['name']}»: {e}", flush=True)
        w["status"] = "ошибка итогов"
        store.save()
        await channel.send(f"Не удалось посчитать итоги **{w['name']}**: {e}. Смотри логи бота.")


async def _finish_wipe(channel, w: dict):
    await channel.send(f"🔴 **{w['name']}** завершён. Считаю итоги, это займёт несколько минут...")
    profiles = await sheet_players()
    now = time.time()
    since_dt = dt.datetime.fromtimestamp(w["start"], dt.timezone.utc)
    end_dt = dt.datetime.fromtimestamp(w["end"], dt.timezone.utc)

    rust: dict[str, float | str] = {}
    async with aiohttp.ClientSession() as session:
        for uid, p in profiles.items():
            if p["bm"] is None:
                rust[uid] = ""
                continue
            try:
                rust[uid] = round(await rust_hours(session, p["bm"], since_dt, end_dt), 1)
            except Exception as e:
                print(f"BM ошибка для {uid}/{p['bm']}: {short_error(e)}", flush=True)
                rust[uid] = ""

    rows = await attendance_rows(w, profiles, final=True, now=now, with_rust=rust)
    rows.sort(key=lambda r: (r[6] if isinstance(r[6], (int, float)) else -1), reverse=True)
    await sheet_results_write(w["name"], rows)
    await sheet_wipe_status(w, "завершён")

    lines = [f"📊 **Итоги {w['name']}** ({fmt_dt(w['start'])} — {fmt_dt(w['end'])})"]
    for _, nick, uid, label, late, voice, r in rows:
        rust_txt = f"{r:.1f} ч" if isinstance(r, (int, float)) else "нет данных"
        late_txt = f" {late} мин" if label == "опоздал" else ""
        lines.append(f"• <@{uid}> ({nick}) — Rust {rust_txt}, гс {voice:.1f} ч, {label}{late_txt}")
    late_n = sum(1 for r in rows if r[3] == "опоздал")
    miss_n = sum(1 for r in rows if r[3] == "не явился")
    lines.append(f"Опоздали: {late_n}, не явились: {miss_n}, всего игроков: {len(rows)}.")
    for chunk in chunk_lines(lines):
        await channel.send(chunk, allowed_mentions=NO_PINGS)


async def import_wipes_from_sheet():
    """Если data.json потерян: подхватить из листа «вайп» ожидающие и идущие вайпы."""
    for r in (await sheet_get(SHEET_WIPES))[1:]:
        r = padded(r, 4)
        name, status = r[0].strip(), r[3].strip()
        if not name or name in store.wipes or status not in ("ожидает", "идёт"):
            continue
        try:
            store.wipes[name] = {"name": name, "start": parse_dt(r[1]), "end": parse_dt(r[2]),
                                 "status": status, "channel": CHANNEL_ID}
        except ValueError:
            print(f"вайп «{name}»: не разобрал даты {r[1]!r}, {r[2]!r}", flush=True)
    store.save()


# ---------- кнопки и формы ----------
def is_admin(interaction: discord.Interaction) -> bool:
    perms = getattr(interaction.user, "guild_permissions", None)
    return bool(perms and perms.manage_guild)


ROLES = ["коллер", "комбат", "билдер", "фермер", "электрик"]


def normalize_role(text: str) -> str | None:
    t = text.strip().lower().replace("ё", "е")
    if len(t) < 3:
        return None
    for r in ROLES:
        if r.startswith(t) or t.startswith(r[:4]):
            return r
    close = difflib.get_close_matches(t, ROLES, n=1, cutoff=0.7)  # опечатки вроде «колер», «билдр»
    return close[0] if close else None


def profile_payload(uid: str, discord_name: str, bm_id: int, bm_name: str,
                    steam: str, region: str, role: str, payout: str) -> dict:
    """Что записать в таблицу. Ключи cells — номера столбцов листа «общее»."""
    return {"discord": uid, "bm_id": bm_id, "nick": bm_name, "role": role,
            "cells": {"2": role, "4": payout, "5": f"https://www.battlemetrics.com/players/{bm_id}",
                      "6": uid, "7": steam, "8": region, "9": discord_name},
            "if_empty": {"1": bm_name}}


class ProfileModal(discord.ui.Modal, title="Мой профиль"):
    steam = discord.ui.TextInput(
        label="Ваш Steam ID", placeholder="76561198000000000 или ссылка на профиль Steam", max_length=200)
    bm = discord.ui.TextInput(
        label="Ваш BattleMetrics", placeholder="https://www.battlemetrics.com/players/123456789", max_length=200)
    region = discord.ui.TextInput(label="Регион", placeholder="Например: UA, EU, Москва", max_length=60)
    role = discord.ui.TextInput(
        label="Твоя роль в клане", placeholder="коллер, комбат, билдер, фермер или электрик", max_length=30)
    payout = discord.ui.TextInput(
        label="Карта или номер для выплат", placeholder="Номер карты или телефона и банк", max_length=100)

    async def on_submit(self, interaction: discord.Interaction):
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

        role = normalize_role(self.role.value)
        if role is None:
            await interaction.response.send_message(
                "Роль не распознана. Напиши одну из: " + ", ".join(ROLES) + ".", ephemeral=True)
            return

        payout = self.payout.value.strip()
        digits = re.sub(r"\D", "", payout)
        if len(digits) < 10:
            await interaction.response.send_message(
                "Нужен номер карты или телефона (минимум 10 цифр) и банк.", ephemeral=True)
            return
        if 13 <= len(digits) <= 19 and not luhn_ok(digits):
            await interaction.response.send_message(
                "Номер карты не прошёл проверку, возможно опечатка. Проверь цифры и попробуй ещё раз. "
                "Срок и CVV не нужны.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)
        async with aiohttp.ClientSession() as session:
            name = await bm_player_name(session, bm_id)
        if name is None:
            await interaction.followup.send(
                "Профиль BattleMetrics не открылся (неверная ссылка или сайт недоступен). Проверь ссылку и попробуй ещё раз.",
                ephemeral=True)
            return

        try:
            res = await upsert_player(profile_payload(
                str(interaction.user.id), interaction.user.name, bm_id, name,
                steam, self.region.value.strip(), role, payout))
        except Exception as e:
            print(f"Ошибка записи в таблицу: {e}", flush=True)
            await interaction.followup.send("Не удалось записать в таблицу, скажи админу.", ephemeral=True)
            return
        how = {"discord": "обновил твою строку", "bm": "нашёл твою строку по ссылке BattleMetrics",
               "nick": "нашёл твою строку по нику", "new": "добавил новую строку"}.get(res.get("how"), "записал")
        await interaction.followup.send(
            f"Готово, {how}. Ник в Rust: **{name}**, роль: {role}, регион: {self.region.value.strip()}, "
            f"выплаты: ••••{digits[-4:]}.", ephemeral=True)


class WipeModal(discord.ui.Modal, title="Новый вайп"):
    def __init__(self):
        super().__init__()
        n = len(store.wipes) + 1
        now = dt.datetime.now(TZ)
        self.name = discord.ui.TextInput(label="Название", default=f"Вайп {n}", max_length=40)
        self.start = discord.ui.TextInput(
            label=f"Начало (ДД.ММ.ГГГГ ЧЧ:ММ, пояс UTC{TZ_OFFSET_HOURS:+g})",
            default=now.strftime("%d.%m.%Y %H:%M"), max_length=20)
        self.end = discord.ui.TextInput(
            label="Конец (ДД.ММ.ГГГГ ЧЧ:ММ)",
            default=(now + dt.timedelta(days=14)).strftime("%d.%m.%Y %H:%M"), max_length=20)
        for item in (self.name, self.start, self.end):
            self.add_item(item)

    async def on_submit(self, interaction: discord.Interaction):
        name = self.name.value.strip()
        try:
            start, end = parse_dt(self.start.value), parse_dt(self.end.value)
        except ValueError:
            await interaction.response.send_message(
                "Не разобрал дату. Формат: 29.10.2026 20:00", ephemeral=True)
            return
        if end <= start or end <= time.time():
            await interaction.response.send_message("Конец должен быть позже начала и в будущем.", ephemeral=True)
            return
        if name in store.wipes:
            await interaction.response.send_message(f"Вайп с названием «{name}» уже есть.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        w = {"name": name, "start": start, "end": end, "status": "ожидает", "channel": interaction.channel_id}
        store.wipes[name] = w
        try:
            await sheet_wipe_status(w, "ожидает")
        except Exception as e:
            del store.wipes[name]
            store.save()
            print(f"Ошибка записи вайпа в таблицу: {e}", flush=True)
            await interaction.followup.send("Не удалось записать вайп в таблицу, скажи админу.", ephemeral=True)
            return
        await interaction.followup.send("Вайп создан.", ephemeral=True)
        await interaction.channel.send(
            f"📅 **{name}** запланирован: {fmt_dt(start)} — {fmt_dt(end)}. "
            f"В момент старта бот отметит, кто в голосовом канале.")


class PanelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)  # постоянные кнопки, работают после перезапуска

    @discord.ui.button(label="Мой профиль", style=discord.ButtonStyle.primary, custom_id="panel:profile")
    async def profile(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(ProfileModal())


class WipeView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Вайп", style=discord.ButtonStyle.danger, custom_id="panel:wipe")
    async def wipe(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_admin(interaction):
            await interaction.response.send_message("Создавать вайп могут только админы.", ephemeral=True)
            return
        await interaction.response.send_modal(WipeModal())


# ---------- бот ----------
class Bot(discord.Client):
    def __init__(self):
        intents = discord.Intents.default()  # voice_states входит в default
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)

    async def setup_hook(self):
        self.add_view(PanelView())
        self.add_view(WipeView())
        await self.tree.sync()
        self.daily_report.start()
        self.wipe_tick.start()

    async def on_ready(self):
        in_voice = {str(m.id) for g in self.guilds for vc in g.voice_channels for m in vc.members if not m.bot}
        store.reconcile(in_voice)
        try:
            await import_wipes_from_sheet()
        except Exception as e:
            print(f"Не удалось прочитать вайпы из таблицы: {e}", flush=True)
        print(f"Бот запущен как {self.user}", flush=True)

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
            await channel.send(chunk, allowed_mentions=NO_PINGS)

    @tasks.loop(seconds=20)
    async def wipe_tick(self):
        store.save()  # heartbeat: отметка «бот жив», нужна для учёта гс при перезапуске
        now = time.time()
        for w in list(store.wipes.values()):
            try:
                if w["status"] == "ожидает" and now >= w["start"]:
                    await start_wipe(self, w)
                if w["status"] == "идёт" and now >= w["end"]:
                    await finish_wipe(self, w)
            except Exception as e:
                print(f"Ошибка вайпа «{w['name']}»: {e}", flush=True)

    @wipe_tick.before_loop
    async def _wait(self):
        await self.wait_until_ready()


bot = Bot()


async def admin_only(interaction: discord.Interaction) -> bool:
    if not is_admin(interaction):
        await interaction.response.send_message("Только для админов.", ephemeral=True)
        return False
    return True


@bot.tree.command(name="panel", description="Выложить кнопку «Мой профиль» (для админов)")
@app_commands.default_permissions(manage_guild=True)
async def panel(interaction: discord.Interaction):
    if not await admin_only(interaction):
        return
    await interaction.channel.send(
        "Без заполнения в клан попасть нельзя.",
        view=PanelView(),
    )
    await interaction.response.send_message("Панель выложена.", ephemeral=True)


@bot.tree.command(name="wipe_panel", description="Выложить кнопку «Вайп» (для админов)")
@app_commands.default_permissions(manage_guild=True)
async def wipe_panel(interaction: discord.Interaction):
    if not await admin_only(interaction):
        return
    await interaction.channel.send(
        "**Вайп**\nАдмин нажимает «Вайп» и указывает название, дату и время начала и конца. "
        "В момент старта бот отметит, кто пришёл в голосовой канал и кто опоздал, "
        "а по окончании выложит статистику: кто сколько наиграл.",
        view=WipeView(),
    )
    await interaction.response.send_message("Кнопка выложена.", ephemeral=True)


@bot.tree.command(name="wipe_status", description="Состояние вайпов и явка прямо сейчас (для админов)")
@app_commands.default_permissions(manage_guild=True)
async def wipe_status(interaction: discord.Interaction):
    if not await admin_only(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    if not store.wipes:
        await interaction.followup.send("Вайпов пока нет.")
        return
    profiles = await sheet_players()
    now = time.time()
    lines = []
    for w in store.wipes.values():
        lines.append(f"**{w['name']}**: {w['status']}, {fmt_dt(w['start'])} — {fmt_dt(w['end'])}")
        if w["status"] == "идёт":
            rows = await attendance_rows(w, profiles, final=False, now=now)
            for _, nick, uid, label, late, voice, _r in rows:
                late_txt = f" {late} мин" if label == "опоздал" else ""
                lines.append(f"• {nick} — {label}{late_txt}, гс {voice:.1f} ч")
    for chunk in chunk_lines(lines):
        await interaction.followup.send(chunk, allowed_mentions=NO_PINGS)


@bot.tree.command(name="report", description="Запустить проверку нормы прямо сейчас (для админов)")
@app_commands.default_permissions(manage_guild=True)
async def report(interaction: discord.Interaction):
    if not await admin_only(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    for chunk in await build_report():
        await interaction.followup.send(chunk, allowed_mentions=NO_PINGS)


bot.run(DISCORD_TOKEN)
