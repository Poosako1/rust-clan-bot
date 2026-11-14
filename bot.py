Warning: truncated output (original token count: 71256)
Total output lines: 5878

import os
import re
import secrets
import io
import json
import base64
import hashlib
import time
import asyncio
import difflib
import traceback
import datetime as dt
from pathlib import Path

import aiohttp
import discord
from discord import app_commands
from discord.ext import tasks

# ===== НАСТРОЙКИ (из .env) =====
DISCORD_TOKEN = os.environ["DISCORD_TOKEN"]
BM_TOKEN = os.environ.get("BM_TOKEN", "").strip()          # необязательно: без токена API работает с меньшим лимитом
CHANNEL_ID = int(os.environ["CHANNEL_ID"])                  # канал для сводки по команде
SCRIPT_URL = os.environ["SCRIPT_URL"]                       # адрес веб-приложения Google Apps Script из таблицы
SCRIPT_SECRET = os.environ["SCRIPT_SECRET"]                 # секрет, такой же как в скрипте таблицы
MIN_HOURS = float(os.environ.get("MIN_HOURS", "8"))         # норма часов (и в гс, и в Rust)
WINDOW_HOURS = float(os.environ.get("WINDOW_HOURS", "24"))  # период проверки (24 = сутки)
SERVER_IDS = {s.strip() for s in os.environ.get("SERVER_IDS", "").split(",") if s.strip()}  # пусто = все сервера
TZ_OFFSET_HOURS = float(os.environ.get("TZ_OFFSET_HOURS", "3"))  # часовой пояс для дат вайпа (3 = МСК)
WIPE_GRACE_MIN = float(os.environ.get("WIPE_GRACE_MIN", "5"))    # сколько минут после старта вайпа ещё не опоздание
OPENROUTER_KEY = os.environ.get("OPENROUTER_KEY", "").strip()          # ключ OpenRouter (редактор текста, необязательно)
EDITOR_CHANNEL_ID = int(os.environ.get("EDITOR_CHANNEL_ID", "0") or 0)  # канал-редактор
EDITOR_ENABLED = os.environ.get("EDITOR_ENABLED", "0") == "1"   # редактор текста выключен, пока не включён явно
INVITE_LOG_CHANNEL_ID = int(os.environ.get("INVITE_LOG_CHANNEL_ID", "1516857380221227030") or 1516857380221227030)  # канал модераторского лога удалённых инвайтов
OPENROUTER_MODEL = os.environ.get("OPENROUTER_MODEL", "anthropic/claude-sonnet-4.5")
VISION_MODEL = os.environ.get("VISION_MODEL", "google/gemini-2.5-flash")
OPENROUTER_URL = os.environ.get("OPENROUTER_URL", "https://openrouter.ai/api/v1/chat/completions")
# ================================

API = "https://api.battlemetrics.com"
BM_HEADERS = {"Authorization": f"Bearer {BM_TOKEN}"} if BM_TOKEN else {}
USE_BM = bool(BM_TOKEN)  # API BattleMetrics требует платную подписку; без токена проверка идёт только по гс
BASE = Path(__file__).parent
DATA_FILE = BASE / "data.json"
KEEP_DAYS = 45
TZ = dt.timezone(dt.timedelta(hours=TZ_OFFSET_HOURS))
NO_PINGS = discord.AllowedMentions.none()
MAIN_VOICE_TZ = dt.timezone(dt.timedelta(hours=3))
MAIN_VOICE_REPORT_HOUR = 14
MAIN_VOICE_NORM_HOURS = 8.0
STAFF_ROLE_ID = int(os.environ.get("STAFF_ROLE_ID", "0") or 0)  # роль стаффа, её упоминают в тикете (необязательно)

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
        self.cfg: dict = {}                 # настройки, созданные /setup_channels (роли и каналы)
        self.drafts: dict[str, dict] = {}   # id черновика -> {channel_id, text, author_id, ts}
        self.statuses: dict[str, dict] = {}  # discord id -> {kind, reason, until, since, nick}
        self.checks: dict[str, dict] = {}  # id сообщения сводки -> {rid, uid, nick, line, decision, by_id}
        self.ticket_counters: dict[str, int] = {}  # guild_id:тип -> последний номер тикета
        self.private_invites: dict[str, dict] = {}  # discord id -> ожидаемая приватка и роль после принятия
        self.wipe_events: dict[str, dict] = {}  # id -> сбор с голосами ✅/❌/❓
        self.main_voice_window_start: float = 0.0
        self.main_voice_seconds: dict[str, float] = {}
        self.main_voice_active: dict[str, float] = {}
        self.main_voice_pending: dict = {}
        self.moderator_rosters: dict[str, dict] = {}  # guild id -> 10 блоков модераторов
        self.vis_configs: dict[str, dict] = {}
        self.vis_profiles: dict[str, dict] = {}
        self.vis_farm_totals: dict[str, dict] = {}
        self.vis_seen_images: dict[str, dict] = {}
        self.vis_voice: list[list] = []  # [guild_id, user_id, start, end]
        self.vis_voice_open: dict[str, float] = {}  # guild_id:user_id -> start
        if DATA_FILE.exists():
            d = json.loads(DATA_FILE.read_text(encoding="utf-8"))
            self.voice = d.get("voice", [])
            self.open_voice = d.get("open_voice", {})
            self.heartbeat = d.get("heartbeat", time.time())
            self.wipes = d.get("wipes", {})
            self.checks = d.get("checks", {})
            self.statuses = d.get("statuses", {})
            self.drafts = d.get("drafts", {})
            self.cfg = d.get("cfg", {})
            self.ticket_counters = d.get("ticket_counters", {})
            self.private_invites = d.get("private_invites", {})
            self.wipe_events = d.get("wipe_events", {})
            self.main_voice_window_start = float(d.get("main_voice_window_start", 0) or 0)
            self.main_voice_seconds = {
                str(k): float(v) for k, v in d.get("main_voice_seconds", {}).items()
            }
            self.main_voice_active = {
                str(k): float(v) for k, v in d.get("main_voice_active", {}).items()
            }
            self.main_voice_pending = d.get("main_voice_pending", {}) or {}
            self.moderator_rosters = d.get("moderator_rosters", {}) or {}
            self.vis_configs = d.get("vis_configs", {}) or {}
            self.vis_profiles = d.get("vis_profiles", {}) or {}
            self.vis_farm_totals = d.get("vis_farm_totals", {}) or {}
            self.vis_seen_images = d.get("vis_seen_images", {}) or {}
            self.vis_voice = d.get("vis_voice", []) or []
            self.vis_voice_open = {
                str(k): float(v) for k, v in (d.get("vis_voice_open", {}) or {}).items()
            }

    def save(self):
        self.heartbeat = time.time()
        cutoff = time.time() - KEEP_DAYS * 86400
        self.voice = [v for v in self.voice if v[2] >= cutoff]
        self.checks = {k: v for k, v in self.checks.items() if v["rid"] >= cutoff}
        self.statuses = {k: v for k, v in self.statuses.items() if v["until"] >= time.time() - 86400}
        self.drafts = {k: v for k, v in self.drafts.items() if v["ts"] >= time.time() - 3 * 86400}
        invite_cutoff = time.time() - 14 * 86400
        self.private_invites = {
            k: v for k, v in self.private_invites.items()
            if float(v.get("created", 0)) >= invite_cutoff
        }
        voice_cutoff = time.time() - 14 * 86400
        self.vis_voice = [row for row in self.vis_voice if len(row) >= 4 and float(row[3]) >= voice_cutoff]
        image_cutoff = time.time() - 180 * 86400
        self.vis_seen_images = {
            guild_id: {
                image_hash: ts for image_hash, ts in hashes.items()
                if float(ts) >= image_cutoff
            }
            for guild_id, hashes in self.vis_seen_images.items()
        }
        tmp = DATA_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps({"voice": self.voice, "open_voice": self.open_voice,
                                   "heartbeat": self.heartbeat, "wipes": self.wipes,
                                   "checks": self.checks, "statuses": self.statuses,
                                   "drafts": self.drafts, "cfg": self.cfg,
                                   "ticket_counters": self.ticket_counters,
                                   "private_invites": self.private_invites,
                                   "wipe_events": self.wipe_events,
                                   "main_voice_window_start": self.main_voice_window_start,
                                   "main_voice_seconds": self.main_voice_seconds,
                                   "main_voice_active": self.main_voice_active,
                                   "main_voice_pending": self.main_voice_pending,
                                   "moderator_rosters": self.moderator_rosters,
                                   "vis_configs": self.vis_configs,
                                   "vis_profiles": self.vis_profiles,
                                   "vis_farm_totals": self.vis_farm_totals,
                                   "vis_seen_images": self.vis_seen_images,
                                   "vis_voice": self.vis_voice,
                                   "vis_voice_open": self.vis_voice_open}), encoding="utf-8")
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

    def main_voice_start(self, uid: str, now: float | None = None):
        current = now or time.time()
        if self.main_voice_window_start and current >= self.main_voice_window_start:
            self.main_voice_active.setdefault(uid, current)
            self.save()

    def main_voice_stop(self, uid: str, now: float | None = None):
        current = now or time.time()
        started = self.main_voice_active.pop(uid, None)
        if started is not None:
            self.main_voice_seconds[uid] = self.main_voice_seconds.get(uid, 0.0) + max(0.0, current - started)
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


def cfg_id(key: str, env: int = 0) -> int:
    return int(store.cfg.get(key) or env or 0)


def report_channel_id() -> int:
    return cfg_id("report", CHANNEL_ID)


def editor_channel_id() -> int:
    return cfg_id("editor", EDITOR_CHANNEL_ID)


def staff_role_id() -> int:
    return cfg_id("staff", STAFF_ROLE_ID)


def ticket_access_role_ids(kind: str | None = None) -> list[int]:
    """Роли доступа отдельно для Мейн, Секонд и поддержки.

    Пока новые настройки не сохранены, используются старые общие роли,
    чтобы обновление само по себе не закрыло доступ к уже работающим тикетам.
    """
    keys = {
        "main": "ticket_access_main_role",
        "second": "ticket_access_second_role",
        "support": "ticket_access_support_role",
    }
    separated = any(cfg_id(key) for key in keys.values())
    if separated:
        if kind in keys:
            role_id = cfg_id(keys[kind])
            return [role_id] if role_id else []
        return list(dict.fromkeys(
            role_id for role_id in (cfg_id(key) for key in keys.values()) if role_id
        ))
    return list(dict.fromkeys(role_id for role_id in (
        cfg_id("ticket_access_role"),
        cfg_id("ticket_access_role_2"),
    ) if role_id))


def ticket_kind_from_topic(topic: str) -> str | None:
    match = re.match(r"^ticket:(main|second|support)(?:\s|$)", topic or "")
    return match.group(1) if match else None


def use_editor() -> bool:
    return bool(EDITOR_ENABLED and OPENROUTER_KEY and editor_channel_id())


def is_staff_member(member) -> bool:
    """Админ (право «Управлять сервером») или участник с ролью «Лидер»."""
    perms = getattr(member, "guild_permissions", None)
    if perms and perms.manage_guild:
        return True
    rid = staff_role_id()
    return bool(rid and any(r.id == rid for r in getattr(member, "roles", [])))


def active_status(uid: str, now: float | None = None) -> dict | None:
    st = store.statuses.get(uid)
    return st if st and st["until"] > (now or time.time()) else None


# ---------- какие привилегированные intents включены в портале разработчика ----------
def members_intent_enabled() -> bool:
    """Server Members Intent включён в портале? Проверяем заранее, иначе бот не смог бы войти."""
    import urllib.request
    try:
        req = urllib.request.Request("https://discord.com/api/v10/applications/@me",
                                     headers={"Authorization": f"Bot {DISCORD_TOKEN}", "User-Agent": "rust-clan-bot"})
        with urllib.request.urlopen(req, timeout=5) as r:
            flags = json.loads(r.read().decode()).get("flags", 0)
        return bool(flags & ((1 << 14) | (1 << 15)))  # GATEWAY_GUILD_MEMBERS / _LIMITED
    except Exception as e:
        print(f"Не удалось проверить intents: {e}", flush=True)
        return False


MEMBERS_OK = members_intent_enabled()


def message_content_intent_enabled() -> bool:
    """Message Content Intent включён в портале разработчика?"""
    import urllib.request
    try:
        req = urllib.request.Request("https://discord.com/api/v10/applications/@me",
                                     headers={"Authorization": f"Bot {DISCORD_TOKEN}", "User-Agent": "rust-clan-bot"})
        with urllib.request.urlopen(req, timeout=5) as r:
            flags = json.loads(r.read().decode()).get("flags", 0)
        return bool(flags & ((1 << 18) | (1 << 19)))  # GATEWAY_MESSAGE_CONTENT / _LIMITED
    except Exception as e:
        print(f"Не удалось проверить Message Content Intent: {e}", flush=True)
        return False


MESSAGE_CONTENT_OK = message_content_intent_enabled()


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


SHEET_PLAYERS, SHEET_WIPES, SHEET_RESULTS, SHEET_CHECKS = "общее", "вайп", "итоги вайпа", "проверки"
SHEET_STATUS = "статусы"
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


async def registration_report() -> list[str]:
    """Кто заполнил анкету (в таблице есть Discord ID), а кого ещё нет."""
    done, todo = [], []
    for r in (await sheet_get(SHEET_PLAYERS))[1:]:
        r = padded(r, PLAYER_COLS)
        if not r[0].strip():
            continue
        (done if re.fullmatch(r"\d{15,25}", r[5].strip()) else todo).append(r[0].strip())
    lines = [f"**Заполнили анкету: {len(done)} из {len(done) + len(todo)}**"]
    if todo:
        lines += [f"**Не заполнили: {len(todo)}**"] + [f"• {n}" for n in todo]
    return lines


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


async def sheet_checks_add(label: str, entries: list[tuple[str, str, float]]):
    """Записать в лист «проверки» строки отчёта (решение пока пустое)."""
    rows = await sheet_get(SHEET_CHECKS)
    start = max(len(rows), 1) + 1
    values = [[label, nick, uid, f"{voice:.1f}", "", "", ""] for uid, nick, voice in entries]
    await sheet_call({"ops": [{"op": "set", "sheet": SHEET_CHECKS, "row": start, "col": 1, "text": True,
                               "values": values}]})


async def sheet_status_log(nick: str, uid: str, kind: str, reason: str, until: float):
    """Дописать строку в лист «статусы» (журнал инактива и АФК). Ошибки не критичны."""
    try:
        rows = await sheet_get(SHEET_STATUS)
        await sheet_call({"ops": [{"op": "set", "sheet": SHEET_STATUS, "row": max(len(rows), 1) + 1, "col": 1,
                                   "text": True, "values": [[fmt_dt(time.time()), nick, uid, kind, reason,
                                                             fmt_dt(until) if until else ""]]}]})
    except Exception as e:
        print(f"Не удалось записать статус в таблицу: {short_error(e)}", flush=True)


async def sheet_check_set(label: str, uid: str, nick: str, decision: str, by: str):
    rows = await sheet_get(SHEET_CHECKS)
    row_no = next((i + 1 for i, r in enumerate(rows) if i > 0 and len(r) > 2 and r[0] == label and r[2] == uid), None)
    ops = []
    if row_no is None:
        row_no = max(len(rows), 1) + 1
        ops.append({"op": "set", "sheet": SHEET_CHECKS, "row": row_no, "col": 1, "text": True,
                    "values": [[label, nick, uid, ""]]})
    ops.append({"op": "set", "sheet": SHEET_CHECKS, "row": row_no, "col": 5, "text": True,
                "values": [[decision, by, fmt_dt(time.time())]]})
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
        if e.status == 403:
            return "BattleMetrics требует платную подписку на API (403)"
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


# ---------- сводка по команде ----------
async def compute_report() -> dict:
    """Кто не набрал норму. -> {"empty": str|None, "lazy": [...], "ok": int, "failed": [...], "period": str}"""
    profiles = await sheet_players()
    if not profiles:
        return {"empty": "В таблице нет игроков с привязанным Discord. Админ может выложить кнопку командой `/panel`."}

    now_dt = dt.datetime.now(dt.timezone.utc)
    since_dt = now_dt - dt.timedelta(hours=WINDOW_HOURS)
    now, since = now_dt.timestamp(), since_dt.timestamp()

    lazy, ok, failed, inactive = [], 0, [], 0
    async with aiohttp.ClientSession() as session:
        for uid, p in profiles.items():
            st = active_status(uid, now)
            if st and st["kind"] == "инактив":  # в инактиве: не проверяем
                inactive += 1
                continue
            # шаг 1: гс. Набрал норму, всё в порядке
            voice = store.voice_hours(uid, since, now)
            if voice >= MIN_HOURS:
                ok += 1
                continue
            rust = None
            if USE_BM:  # шаг 2 (только с токеном BattleMetrics): в гс мало, смотрим Rust
                if p["bm"] is None:
                    failed.append((uid, p["nick"], "нет ссылки BattleMetrics в таблице"))
                    continue
                try:
                    rust = await rust_hours(session, p["bm"], since_dt, now_dt)
                except Exception as e:
                    print(f"BM ошибка для {uid}/{p['bm']}: {short_error(e)}", flush=True)
                    failed.append((uid, p["nick"], short_error(e)))
                    continue
                if rust >= MIN_HOURS:
                    ok += 1
                    continue
            lazy.append({"uid": uid, "nick": p["nick"], "bm": p["bm"], "voice": voice, "rust": rust})
    lazy.sort(key=lambda x: (x["rust"] if x["rust"] is not None else 0, x["voice"]))
    return {"empty": None, "lazy": lazy, "ok": ok, "failed": failed, "inactive": inactive, "period": f"{WINDOW_HOURS:g} ч"}


def lazy_line(x: dict) -> str:
    link = f"<https://www.battlemetrics.com/players/{x['bm']}>" if x["bm"] else "ссылки BattleMetrics нет"
    hours = f"гс {x['voice']:.1f} ч" + (f", Rust {x['rust']:.1f} ч" if x["rust"] is not None else "")
    return f"• {x['nick']} — {hours} — {link}"


def report_header(data: dict) -> str:
    if data["lazy"]:
        if USE_BM:
            return f"⚠️ Не набрали {MIN_HOURS:g} ч ни в гс, ни в Rust за последние {data['period']}:"
        return f"⚠️ Меньше {MIN_HOURS:g} ч в гс за последние {data['period']}:"
    if data["ok"]:
        return f"✅ За последние {data['period']} все проверенные выполнили норму ({MIN_HOURS:g} ч): {data['ok']}."
    return f"Проверить не удалось никого, нормы за последние {data['period']} неизвестны."


def report_tail(data: dict) -> list[str]:
    lines = []
    if data["ok"] and data["lazy"]:
        lines.append(f"Выполнили норму: {data['ok']}")
    if data.get("inactive"):
        lines.append(f"В инактиве (не проверялись): {data['inactive']}")
    if data["failed"]:
        lines.append("Не удалось проверить:")
        lines += [f"• {nick}: {reason}" for _, nick, reason in data["failed"]]
    return lines


async def build_report() -> list[str]:
    """Сводка просто текстом (без кнопок)."""
    data = await compute_report()
    if data["empty"]:
        return [data["empty"]]
    return chunk_lines([report_header(data)] + [lazy_line(x) for x in data["lazy"]] + report_tail(data))


# ---------- отметки ✅ / ❌ под игроками сводки ----------
DECISIONS = {"✅": "играл 8ч", "❌": "не играл 8ч"}


async def post_report(channel: discord.abc.Messageable):
    """Выложить сводку: каждый игрок отдельным сообщением (ник, часы в гс, ссылка BM), под ним ✅ и ❌."""
    data = await compute_report()
    if data["empty"]:
        await channel.send(data["empty"], allowed_mentions=NO_PINGS)
        return
    lazy = data["lazy"]
    await channel.send(report_header(data), allowed_mentions=NO_PINGS)
    if lazy:
        rid = int(time.time())
        try:
            await sheet_checks_add(fmt_dt(rid), [(x["uid"], x["nick"], x["voice"]) for x in lazy])
        except Exception as e:
            print(f"Ошибка записи в лист проверок: {short_error(e)}", flush=True)
            await channel.send("⚠️ Не удалось записать в лист «проверки», отметки могут не сохраниться.")
        for x in lazy:
            line = lazy_line(x)
            msg = await channel.send(line, allowed_mentions=NO_PINGS)
            store.checks[str(msg.id)] = {"rid": rid, "uid": x["uid"], "nick": x["nick"], "line": line,
                                         "decision": "", "by_id": 0, "ticket": 0,
                                         "day": (dt.datetime.now(TZ) - dt.timedelta(hours=WINDOW_HOURS)).strftime("%d.%m")}
            store.save()
            try:
                for emoji in DECISIONS:
                    await msg.add_reaction(emoji)
            except Exception as e:  # чаще всего у бота нет права «Добавлять реакции»
                print(f"Не удалось поставить реакцию: {short_error(e)}", flush=True)
                await channel.send("⚠️ Не могу ставить реакции: дай боту право «Добавлять реакции» "
                                   "и «Читать историю сообщений» в этом канале.")
                break
            await asyncio.sleep(0.3)
    tail = report_tail(data)
    for chunk in chunk_lines(tail):
        await channel.send(chunk, allowed_mentions=NO_PINGS)


async def open_ticket(channel, info: dict, admin) -> None:
    """❌ «не играл»: приватный канал с игроком и стаффом, бот задаёт один вопрос."""
    if info.get("ticket"):
        return
    guild = getattr(channel, "guild", None)
    member = guild.get_member(int(info["uid"])) if guild else None
    if member is None and guild is not None:
        try:
            member = await guild.fetch_member(int(info["uid"]))
        except Exception:
            member = None
    day = info.get("day") or dt.datetime.fromtimestamp(info["rid"] - WINDOW_HOURS * 3600, TZ).strftime("%d.%m")
    try:
        see = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True,
                                          attach_files=True)
        ow = {guild.default_role: discord.PermissionOverwrite(view_channel=False), guild.me: see}
        for u in (member, admin):
            if u is not None:
                ow[u] = see
        if staff_role_id():
            role = guild.get_role(staff_role_id())
            if role is not None:
                ow[role] = see
        slug = re.sub(r"[^\w\-]+", "-", f"не-играл-{info['nick']}-{day}".lower()).strip("-")[:90]
        ticket = await guild.create_text_channel(slug or "тикет", category=channel.category, overwrites=ow,
                                                 reason=f"не играл {day}: {info['nick']}")
        info["ticket"] = ticket.id
        store.save()
        staff = f"<@&{staff_role_id()}> " if staff_role_id() else ""
        who = member.mention if member is not None else f"**{info['nick']}**"
        await ticket.send(f"{who} {staff}\nПочему {day} не играл?",
                          allowed_mentions=discord.AllowedMentions(users=True, roles=True))
        if member is None:
            await ticket.send("⚠️ Игрока нет на сервере, добавьте его в канал вручную.")
    except Exception as e:
        print(f"Не удалось открыть тикет: {short_error(e)}", flush=True)
        await channel.send("⚠️ Не смог открыть тикет: дай боту права «Управлять каналами» и «Управлять ролями».")


async def apply_check_reaction(bot, payload, added: bool):
    """Админ поставил ✅ или ❌ под игроком: записать решение в таблицу и показать его в сообщении."""
    info = store.checks.get(str(payload.message_id))
    emoji = str(payload.emoji)
    if info is None or emoji not in DECISIONS or payload.user_id == bot.user.id:
        return
    channel = bot.get_channel(payload.channel_id)
    partial = channel.get_partial_message(payload.message_id) if channel else None

    if added:
        member = payload.member
        if member is None or not is_staff_member(member):
            try:  # реакцию не админа убираем (нужно право «Управлять сообщениями», иначе просто игнорируем)
                if partial is not None and member is not None:
                    await partial.remove_reaction(emoji, member)
            except Exception:
                pass
            return
        decision, who = DECISIONS[emoji], member.name
        info["decision"], info["by_id"] = emoji, payload.user_id
    else:  # админ снял свою отметку: решение очищаем
        if info["decision"] != emoji or info["by_id"] != payload.user_id:
            return
        decision, who = "", ""
        info["decision"], info["by_id"] = "", 0
    store.save()
    try:
        await sheet_check_set(fmt_dt(info["rid"]), info["uid"], info["nick"], decision, who)
    except Exception as e:
        print(f"Ошибка записи решения: {short_error(e)}", flush=True)
        return
    if partial is not None:
        text = info["line"] + (f" → {emoji} {decision} ({who})" if decision else "")
        try:
            await partial.edit(content=text, allowed_mentions=NO_PINGS)
        except Exception as e:
            print(f"Не удалось обновить сообщение: {short_error(e)}", flush=True)
    if decision and emoji == "❌" and channel is not None:
        await open_ticket(channel, info, payload.member)
        store.save()
    # все игроки этой сводки отмечены -> подтверждение
    if decision and channel is not None:
        same = [v for v in store.checks.values() if v["rid"] == info["rid"]]
        if all(v["decision"] for v in same):
            try:
                await channel.send(f"✅ Записано успешно: отмечены все ({len(same)}).")
            except Exception as e:
                print(f"Не удалось отправить подтверждение: {short_error(e)}", flush=True)


# ---------- редактор текста (OpenRouter) ----------
EDITOR_SYSTEM = (
    "Ты редактор текстов для Discord-сервера игрового клана (Rust). Тебе дают текст, оформи его красиво "
    "в разметке Discord: короткий жирный заголовок при необходимости, абзацы, списки через «•» или «-», "
    "уместные эмодзи (умеренно), **жирный** для главного. Сохрани смысл, факты, цифры, ники, упоминания "
    "(<@..>, <@&..>, @everyone) и ссылки без изменений, ничего не выдумывай. Исправь опечатки. Пиши на языке оригинала. "
    "Не используй заголовки через #, блоки кода и таблицы. Длина результата строго не больше 1700 символов. "
    "Верни только готовый текст, без пояснений и без кавычек вокруг."
)
DRAFT_LIMIT = 1800


async def llm_edit(text: str, instruction: str | None = None) -> str:
    if instruction is None:
        user = f"Оформи этот текст:\n\n{text}"
    else:
        user = (f"Вот текущий вариант текста:\n\n{text}\n\nВнеси правку: {instruction}\n\n"
                "Верни полный обновлённый текст.")
    body = {"model": OPENROUTER_MODEL, "temperature": 0.4,
            "messages": [{"role": "system", "content": EDITOR_SYSTEM}, {"role": "user", "content": user}]}
    headers = {"Authorization": f"Bearer {OPENROUTER_KEY}", "X-Title": "rust-clan-bot"}
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=90)) as session:
        async with session.post(OPENROUTER_URL, json=body, headers=headers) as r:
            data = await r.json(content_type=None)
            if r.status >= 400:
                err = (data.get("error") or {}) if isinstance(data, dict) else {}
                raise RuntimeError(f"OpenRouter {r.status}: {err.get('message') or data}")
    out = (data["choices"][0]["message"]["content"] or "").strip()
    if not out:
        raise RuntimeError("модель вернула пустой ответ")
    return out


def parse_editor_message(message) -> tuple[int | None, str]:
    """«#канал текст» -> (id канала, текст). Подходит и выбранное из подсказки #канал, и набранное имя."""
    content = message.content.strip()
    m = re.match(r"<#(\d+)>\s*(.*)", content, re.S)
    if m:
        return int(m.group(1)), m.group(2).strip()
    m = re.match(r"#([^\s]+)\s*(.*)", content, re.S)
    if m and message.guild is not None:
        name = m.group(1).lower().strip("#")
        for ch in message.guild.text_channels:
            if ch.name.lower() == name:
                return ch.id, m.group(2).strip()
    return None, content


def draft_text(channel_id: int, text: str) -> str:
    return f"**Черновик → <#{channel_id}>** (✅ опубликовать, ❌ отменить, или ответь на это сообщение, что изменить)\n\n{text}"


async def handle_editor_message(bot, message: discord.Message):
    if message.reference and str(message.reference.message_id) in store.drafts:  # правка черновика
        key = str(message.reference.message_id)
        d = store.drafts[key]
        async with message.channel.typing():
            try:
                new = await llm_edit(d["text"], message.content.strip())
            except Exception as e:
                await message.reply(f"⚠️ Не получилось: {short_error(e)}", mention_author=False)
                return
        if len(new) > DRAFT_LIMIT:
            await message.reply("⚠️ Вышло слишком длинно для одного сообщения, попроси сократить.", mention_author=False)
            return
        d["text"] = new
        store.save()
        try:
            draft_msg = await message.channel.fetch_message(int(key))
            await draft_msg.edit(content=draft_text(d["channel_id"], new))
        except Exception as e:
            await message.reply(f"⚠️ Не смог обновить черновик: {short_error(e)}", mention_author=False)
        return

    channel_id, text = parse_editor_message(message)
    if channel_id is None:
        await message.reply("Начни сообщение с канала: `#канал текст`. Выбери канал из подсказки после «#».",
                            mention_author=False)
        return
    if not text:
        await message.reply("После канала нужен текст: `#канал текст`.", mention_author=False)
        return
    async with message.channel.typing():
        try:
            new = await llm_edit(text)
        except Exception as e:
            await message.reply(f"⚠️ Не получилось оформить: {short_error(e)}", mention_author=False)
            return
    if len(new) > DRAFT_LIMIT:
        await message.reply("⚠️ Текст слишком длинный для одного сообщения Discord. Сократи или разбей на части.",
                            mention_author=False)
        return
    draft = await message.reply(draft_text(channel_id, new), mention_author=False,
                                allowed_mentions=NO_PINGS)
    store.drafts[str(draft.id)] = {"channel_id": channel_id, "text": new, "author_id": message.author.id,
                                   "ts": time.time()}
    store.save()
    try:
        for emoji in DECISIONS:
            await draft.add_reaction(emoji)
    except Exception as e:
        print(f"Не удалось поставить реакцию: {short_error(e)}", flush=True)
        await message.channel.send("⚠️ Не могу ставить реакции: дай боту право «Добавлять реакции».")


async def apply_draft_reaction(bot, payload):
    d = store.drafts.get(str(payload.message_id))
    emoji = str(payload.emoji)
    if d is None or emoji not in DECISIONS or payload.user_id == bot.user.id:
        return
    member = payload.member
    is_adm = bool(member and is_staff_member(member))
    if payload.user_id != d["author_id"] and not is_adm:
        return
    channel = bot.get_channel(payload.channel_id)
    if channel is None:
        return
    try:
        draft_msg = await channel.fetch_message(payload.message_id)
    except Exception:
        return
    if emoji == "❌":
        del store.drafts[str(payload.message_id)]
        store.save()
        await draft_msg.edit(content="❌ Отменено.\n\n" + d["text"], allowed_mentions=NO_PINGS)
        return
    target = bot.get_channel(d["channel_id"])
    if target is None:
        try:
            target = await bot.fetch_channel(d["channel_id"])
        except Exception:
            target = None
    if target is None:
        await channel.send("⚠️ Не нашёл канал для публикации.")
        return
    del store.drafts[str(payload.message_id)]
    store.save()
    mentions = discord.AllowedMentions.all() if is_adm else NO_PINGS  # пинги только если подтвердил админ
    try:
        await target.send(d["text"], allowed_mentions=mentions)
    except Exception as e:
        await channel.send(f"⚠️ Не смог опубликовать в <#{d['channel_id']}>: {short_error(e)}. "
                           "Проверь права бота в этом канале.")
        return
    await draft_msg.edit(content=f"✅ Опубликовано в <#{d['channel_id']}>.\n\n" + d["text"], allowed_mentions=NO_PINGS)


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
    for chunk in chunk_lines(lines):  # без разрешения на упоминания Discord показывает «Неизвестный пользователь»
        await channel.send(chunk, allowed_mentions=discord.AllowedMentions(users=True))


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
    await channel.send(f"🔴 **{w['name']}** завершён. Считаю итоги...")
    profiles = await sheet_players()
    now = time.time()
    since_dt = dt.datetime.fromtimestamp(w["start"], dt.timezone.utc)
    end_dt = dt.datetime.fromtimestamp(w["end"], dt.timezone.utc)

    rust: dict[str, float | str] = {}
    if USE_BM:  # часы в Rust только если есть токен BattleMetrics
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
    rows.sort(key=lambda r: ((r[6] if isinstance(r[6], (int, float)) else -1), r[5]), reverse=True)
    await sheet_results_write(w["name"], rows)
    await sheet_wipe_status(w, "завершён")

    lines = [f"📊 **Итоги {w['name']}** ({fmt_dt(w['start'])} — {fmt_dt(w['end'])})"]
    for _, nick, uid, label, late, voice, r in rows:
        rust_txt = (f"Rust {r:.1f} ч, " if isinstance(r, (int, float)) else ("Rust нет данных, " if USE_BM else ""))
        late_txt = f" {late} мин" if label == "опоздал" else ""
        lines.append(f"• {nick} — {rust_txt}гс {voice:.1f} ч, {label}{late_txt}")
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
                                 "status": status, "channel": report_channel_id()}
        except ValueError:
            print(f"вайп «{name}»: не разобрал даты {r[1]!r}, {r[2]!r}", flush=True)
    store.save()


# ---------- кнопки и формы ----------
def is_admin(interaction: discord.Interaction) -> bool:
    return is_staff_member(interaction.user)


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


class SafeModal(discord.ui.Modal):
    """Любая ошибка в форме пишется в лог (с текстом ошибки) и игрок получает понятный ответ."""

    async def on_error(self, interaction: discord.Interaction, error: Exception) -> None:
        print(f"Ошибка в форме «{self.title}» у {interaction.user} ({interaction.user.id}):", flush=True)
        traceback.print_exception(type(error), error, error.__traceback__)
        msg = "Не получилось записать. Нажми кнопку и заполни форму ещё раз, если снова не выйдет, напиши админу."
        try:
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except Exception:
            pass


class ProfileModal(SafeModal, title="Мой профиль"):
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
            name = await bm_player_name(session, bm_id)  # необязательно: API BattleMetrics может требовать подписку
        if name is None:
            name = interaction.user.display_name

        try:
            res = await upsert_player(profile_payload(
                str(interaction.user.id), interaction.user.name, bm_id, name,
                steam, self.region.value.strip(), role, payout))
        except Exception as e:
            print(f"Ошибка записи в таблицу: {e}", flush=True)
            await interaction.followup.send("Не удалось записать в таблицу, скажи админу.", ephemeral=True)
            return
        print(f"Анкета: {interaction.user} ({interaction.user.id}) -> {res.get('how')}", flush=True)
        how = {"discord": "обновил твою строку", "bm": "нашёл твою строку по ссылке BattleMetrics",
               "nick": "нашёл твою строку по нику", "new": "добавил новую строку"}.get(res.get("how"), "записал")
        await interaction.followup.send(
            f"Готово, {how}. Ник: **{name}**, роль: {role}, регион: {self.region.value.strip()}, "
            f"выплаты: ••••{digits[-4:]}.", ephemeral=True)


class WipeModal(SafeModal, title="Новый вайп"):
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


WIPE_RSVP_LABELS = {"yes": "Иду", "no": "Отсутствую", "late": "Опоздаю"}
WIPE_RSVP_EMOJIS = {"yes": "✅", "no": "❌", "late": "❓"}


def wipe_event_status_ids(event: dict, status: str) -> list[int]:
    return [int(user_id) for user_id, vote in event.get("votes", {}).items()
            if vote.get("status") == status]


def wipe_event_mention_chunks(user_ids: list[int], limit: int = 1000) -> list[str]:
    if not user_ids:
        return ["—"]
    chunks = []
    current = []
    size = 0
    for user_id in user_ids:
        mention = f"<@{user_id}>"
        extra = len(mention) + (1 if current else 0)
        if current and size + extra > limit:
            chunks.append(" ".join(current))
            current = []
            size = 0
        current.append(mention)
        size += len(mention) + (1 if size else 0)
    if current:
        chunks.append(" ".join(current))
    return chunks


def wipe_event_reason_chunks(event: dict, status: str, limit: int = 1000) -> list[str]:
    entries = []
    for user_id, vote in event.get("votes", {}).items():
        if vote.get("status") != status:
            continue
        reason = discord.utils.escape_markdown(vote.get("reason") or "Причина не указана")
        entries.append(f"<@{int(user_id)}>\nПричина: {reason}")
    if not entries:
        return ["—"]
    chunks = []
    current = []
    size = 0
    for entry in entries:
        extra = len(entry) + (2 if current else 0)
        if current and size + extra > limit:
            chunks.append("\n\n".join(current))
            current = []
            size = 0
        current.append(entry)
        size += len(entry) + (2 if size else 0)
    if current:
        chunks.append("\n\n".join(current))
    return chunks


def wipe_event_datetime(date_text: str, time_text: str) -> float:
    combined = f"{date_text.strip()} {time_text.strip()}".replace("/", ".")
    for fmt in ("%d.%m.%Y %H:%M", "%d.%m.%y %H:%M", "%d.%m %H:%M"):
        try:
            value = dt.datetime.strptime(combined, fmt)
        except ValueError:
            continue
        if fmt == "%d.%m %H:%M":
            value = value.replace(year=dt.datetime.now(TZ).year)
        return value.replace(tzinfo=TZ).timestamp()
    raise ValueError(combined)


def wipe_event_embed(event: dict) -> discord.Embed:
    started = event.get("status") == "started" or event["start"] <= time.time()
    embed = discord.Embed(
        title=event["name"],
        description=event.get("description") or "Сбор на вайп",
        color=0xE67E22 if not started else 0x2F3136,
        timestamp=dt.datetime.fromtimestamp(event["start"], dt.timezone.utc),
    )
    embed.add_field(
        name="Дата и время сбора",
        value=f"<t:{int(event['start'])}:F>\n<t:{int(event['start'])}:R>",
        inline=False,
    )
    groups = (
        ("✅ Иду", wipe_event_mention_chunks(wipe_event_status_ids(event, "yes"))),
        ("❌ Отсутствую", wipe_event_reason_chunks(event, "no")),
        ("❓ Опоздаю", wipe_event_reason_chunks(event, "late")),
        ("⚠️ Не ответили", wipe_event_mention_chunks(wipe_event_pending_ids(event))),
    )
    for title, values in groups:
        for index, value in enumerate(values):
            field_name = title if index == 0 else f"{title} · продолжение"
            embed.add_field(name=field_name, value=value, inline=False)
    embed.set_footer(text="Сбор уже начался" if started else "Ответ можно изменить до начала сбора")
    return embed


async def wipe_event_card_exists(client: discord.Client, event: dict) -> bool:
    """Проверить, существует ли сохранённая карточка сбора в Discord."""
    channel = client.get_channel(int(event.get("channel_id", 0)))
    if channel is None:
        try:
            channel = await client.fetch_channel(int(event.get("channel_id", 0)))
        except discord.NotFound:
            return False
        except Exception:
            return True
    try:
        await channel.fetch_message(int(event.get("message_id", 0)))
    except discord.NotFound:
        return False
    except Exception:
        return True
    return True


async def refresh_wipe_event_message(client: discord.Client, event: dict) -> None:
    channel = client.get_channel(int(event["channel_id"]))
    if channel is None:
        try:
            channel = await client.fetch_channel(int(event["channel_id"]))
        except Exception:
            return
    try:
        message = await channel.fetch_message(int(event["message_id"]))
        disabled = event.get("status") != "open" or event["start"] <= time.time()
        await message.edit(embed=wipe_event_embed(event),
                           view=WipeRsvpView(event["id"], disabled=disabled))
    except Exception as e:
        print(f"Не удалось обновить карточку сбора {event.get('id')}: {short_error(e)}", flush=True)


async def record_wipe_event_vote(interaction: discord.Interaction, event_id: str,
                                 status: str, reason: str = "") -> None:
    event = store.wipe_events.get(event_id)
    if event is None or int(event.get("guild_id", 0)) != interaction.guild_id:
        await interaction.response.send_message("Этот сбор больше не найден.", ephemeral=True)
        return
    if event.get("status") != "open" or event["start"] <= time.time():
        await interaction.response.send_message("Голосование уже закрыто.", ephemeral=True)
        return
    if not isinstance(interaction.user, discord.Member) or interaction.user.bot:
        await interaction.response.send_message(
            "Голосовать могут только участники сервера.", ephemeral=True
        )
        return
    participant_ids = {str(uid) for uid in event.get("participants", [])}
    if str(interaction.user.id) not in participant_ids:
        event.setdefault("participants", []).append(interaction.user.id)
    event.setdefault("votes", {})[str(interaction.user.id)] = {
        "status": status,
        "reason": reason.strip(),
        "updated": time.time(),
    }
    store.save()
    await interaction.response.send_message(
        f"Ответ сохранён: {WIPE_RSVP_EMOJIS[status]} **{WIPE_RSVP_LABELS[status]}**.",
        ephemeral=True,
    )
    await refresh_wipe_event_message(interaction.client, event)


class WipeRsvpReasonModal(SafeModal):
    def __init__(self, event_id: str, status: str):
        title = "Причина отсутствия" if status == "no" else "Причина опоздания"
        super().__init__(title=title)
        self.event_id = event_id
        self.status = status
        self.reason = discord.ui.TextInput(
            label=title,
            placeholder="Напиши причину подробно",
            style=discord.TextStyle.paragraph,
            min_length=3,
            max_length=500,
        )
        self.add_item(self.reason)

    async def on_submit(self, interaction: discord.Interaction):
        await record_wipe_event_vote(interaction, self.event_id, self.status, self.reason.value)


class WipeRsvpView(discord.ui.View):
    def __init__(self, event_id: str, disabled: bool = False):
        super().__init__(timeout=None)
        self.event_id = event_id
        choices = (
            ("Иду", "yes", discord.ButtonStyle.success, "✅"),
            ("Отсутствую", "no", discord.ButtonStyle.danger, "❌"),
            ("Опоздаю", "late", discord.ButtonStyle.secondary, "❓"),
        )
        for label, status, style, emoji in choices:
            button = discord.ui.Button(
                label=label,
                style=style,
                emoji=emoji,
                disabled=disabled,
                custom_id=f"wipe:rsvp:{event_id}:{status}",
            )
            button.callback = self._callback(status)
            self.add_item(button)

    def _callback(self, status: str):
        async def callback(interaction: discord.Interaction):
            event = store.wipe_events.get(self.event_id)
            if event is None or event.get("status") != "open" or event["start"] <= time.time():
                await interaction.response.send_message("Голосование уже закрыто.", ephemeral=True)
                return
            if str(interaction.user.id) not in {str(uid) for uid in event.get("participants", [])}:
                await interaction.response.send_message("Ты не входишь в список участников этого сбора.",
                                                        ephemeral=True)
                return
            if status in ("no", "late"):
                await interaction.response.send_modal(WipeRsvpReasonModal(self.event_id, status))
                return
            await record_wipe_event_vote(interaction, self.event_id, status)
        return callback


async def send_wipe_event_dm(member: discord.Member, event: dict, jump_url: str,
                             reminder: bool = False) -> bool:
    prefix = "Ты ещё не ответил на сбор." if reminder else "Создан новый сбор на вайп."
    try:
        await member.send(
            f"**{prefix}**\n\n"
            f"**{event['name']}**\n{event.get('description') or 'Сбор на вайп'}\n"
            f"Дата и время: <t:{int(event['start'])}:F>\n"
            f"Проголосовать: {jump_url}",
            allowed_mentions=NO_PINGS,
        )
        return True
    except Exception:
        return False


def wipe_event_pending_ids(event: dict) -> list[int]:
    voted = {str(user_id) for user_id in event.get("votes", {})}
    return [int(user_id) for user_id in event.get("participants", []) if str(user_id) not in voted]


async def send_wipe_event_final(client: discord.Client, event: dict) -> None:
    guild = client.get_guild(int(event["guild_id"]))
    channel = client.get_channel(int(event["channel_id"]))
    if guild is None or channel is None:
        return
    pending_ids = wipe_event_pending_ids(event)
    lines = [
        f"**Итоги подтверждения · {discord.utils.escape_markdown(event['name'])}**",
    ]
    groups = (
        ("✅ Идут", wipe_event_status_ids(event, "yes")),
        ("❌ Отсутствуют", wipe_event_status_ids(event, "no")),
        ("❓ Опоздают", wipe_event_status_ids(event, "late")),
        ("⚠️ Не проголосовали", pending_ids),
    )
    for title, user_ids in groups:
        lines.extend(("", f"**{title}:**"))
        if user_ids:
            for user_id in user_ids:
                lines.append(f"• <@{user_id}> — `{user_id}`")
                if title.startswith(("❌", "❓")):
                    vote = event.get("votes", {}).get(str(user_id), {})
                    reason = discord.utils.escape_markdown(vote.get("reason") or "Причина не указана")
                    lines.append(f"  Причина: {reason}")
        else:
            lines.append("• Никого")
    for chunk in chunk_lines(lines):
        await channel.send(chunk, allowed_mentions=NO_PINGS)


async def notify_wipe_event_attendees(client: discord.Client, event: dict) -> None:
    """В момент сбора написать в личку каждому, кто выбрал ✅ «Иду»."""
    guild = client.get_guild(int(event["guild_id"]))
    if guild is None:
        return
    notified = {str(user_id) for user_id in event.get("start_notified", [])}
    for user_id in wipe_event_status_ids(event, "yes"):
        if str(user_id) in notified:
            continue
        member = guild.get_member(user_id)
        if member is not None:
            try:
                await member.send(
                    f"<@{user_id}>, **{event['name']}** — заходи в войс, время пришло!",
                    allowed_mentions=discord.AllowedMentions(users=True),
                )
            except Exception:
                pass
        notified.add(str(user_id))
        event["start_notified"] = sorted(notified)
        store.save()
        await asyncio.sleep(0.2)
    event["start_notifications_done"] = True
    store.save()


async def process_wipe_event_reminders(client: discord.Client) -> None:
    now = dt.datetime.now(TZ)
    today = now.strftime("%Y-%m-%d")
    changed = False
    for event in store.wipe_events.values():
        if event.get("status") not in ("open", "started"):
            continue
        if event["start"] <= now.timestamp():
            if not event.get("start_notifications_done"):
                await notify_wipe_event_attendees(client, event)
            if event.get("status") != "started":
                event["status"] = "started"
                changed = True
                await refresh_wipe_event_message(client, event)
            if not event.get("final_sent"):
                try:
                    await send_wipe_event_final(client, event)
                except Exception as e:
                    print(f"Не удалось отправить итоги сбора {event.get('id')}: {short_error(e)}", flush=True)
                else:
                    event["final_sent"] = True
                    changed = True
            continue
        if event.get("status") != "open":
            continue
        if now.hour < 12 or event.get("last_reminder_date") == today:
            continue
        guild = client.get_guild(int(event["guild_id"]))
        channel = client.get_channel(int(event["channel_id"]))
        if guild is None or channel is None:
            continue
        jump_url = f"https://discord.com/channels/{event['guild_id']}/{event['channel_id']}/{event['message_id']}"
        voted = {str(uid) for uid in event.get("votes", {})}
        for uid in event.get("participants", []):
            if str(uid) in voted:
                continue
            member = guild.get_member(int(uid))
            if member is None:
                continue
            await send_wipe_event_dm(member, event, jump_url, reminder=True)
            await asyncio.sleep(0.2)
        event["last_reminder_date"] = today
        changed = True
    if changed:
        store.save()


def parse_day_end(text: str) -> float:
    """ДД.ММ.ГГГГ (или ДД.ММ.ГГ, ДД.ММ) -> конец этого дня по МСК."""
    t = text.strip().replace("/", ".")
    for fmt in ("%d.%m.%Y", "%d.%m.%y", "%d.%m"):
        try:
            d = dt.datetime.strptime(t, fmt)
        except ValueError:
            continue
        if fmt == "%d.%m":
            d = d.replace(year=dt.datetime.now(TZ).year)
        return d.replace(hour=23, minute=59, tzinfo=TZ).timestamp()
    raise ValueError(text)


async def set_status(interaction: discord.Interaction, kind: str, reason: str, until: float):
    uid, nick = str(interaction.user.id), interaction.user.display_name
    store.statuses[uid] = {"kind": kind, "reason": reason, "until": until, "since": time.time(), "nick": nick}
    store.save()
    await sheet_status_log(nick, uid, kind, reason, until)


class InactiveModal(SafeModal, title="Инактив"):
    reason = discord.ui.TextInput(label="Причина", placeholder="Например: уехал в школу",
                                  style=discord.TextStyle.paragraph, max_length=200)
    until = discord.ui.TextInput(label="До какой даты (ДД.ММ.ГГГГ)", placeholder="05.11.2026", max_length=10)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            until = parse_day_end(self.until.value)
        except ValueError:
            await interaction.response.send_message("Дата не распознана. Напиши в формате ДД.ММ.ГГГГ, например 05.11.2026.",
                                                    ephemeral=True)
            return
        if until <= time.time():
            await interaction.response.send_message("Эта дата уже прошла.", ephemeral=True)
            return
        await set_status(interaction, "инактив", self.reason.value.strip(), until)
        await interaction.response.send_message(
            f"🛌 Инактив записан до {fmt_dt(until)[:10]}. Проверок нормы в это время не будет. "
            "Вернуться раньше можно кнопкой «Я вернулся».", ephemeral=True)


class AfkModal(SafeModal, title="АФК"):
    reason = discord.ui.TextInput(label="Причина", placeholder="Например: отошёл поесть", max_length=200)
    hours = discord.ui.TextInput(label="Часы", placeholder="0", max_length=2, default="0", required=False)
    minutes = discord.ui.TextInput(label="Минуты", placeholder="5", max_length=3, default="5", required=False)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            h = int(self.hours.value.strip() or 0)
            m = int(self.minutes.value.strip() or 0)
            assert h >= 0 and m >= 0 and 1 <= h * 60 + m <= 24 * 60
        except (ValueError, AssertionError):
            await interaction.response.send_message(
                "Часы и минуты напиши числами, всего от 1 минуты до 24 часов.", ephemeral=True)
            return
        total = h * 60 + m
        until = time.time() + total * 60
        await set_status(interaction, "афк", self.reason.value.strip(), until)
        span = " ".join(x for x in (f"{h} ч" if h else "", f"{m} мин" if m else "") if x)
        await interaction.response.send_message(
            f"⏳ АФК на {span} (до {dt.datetime.fromtimestamp(until, TZ):%H:%M}). "
            "Раньше можно вернуться кнопкой «Я вернулся».", ephemeral=True)


class StatusView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Инактив", style=discord.ButtonStyle.secondary, custom_id="status:inactive")
    async def inactive(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(InactiveModal())

    @discord.ui.button(label="АФК", style=discord.ButtonStyle.secondary, custom_id="status:afk")
    async def afk(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(AfkModal())

    @discord.ui.button(label="Я вернулся", style=discord.ButtonStyle.success, custom_id="status:back")
    async def back(self, interaction: discord.Interaction, button: discord.ui.Button):
        uid = str(interaction.user.id)
        st = store.statuses.pop(uid, None)
        store.save()
        if st and st["until"] > time.time():
            await sheet_status_log(interaction.user.display_name, uid, "вернулся", "", 0)
            await interaction.response.send_message("С возвращением! Статус снят.", ephemeral=True)
        else:
            await interaction.response.send_message("У тебя нет активного статуса.", ephemeral=True)


async def send_status_list(interaction: discord.Interaction, kind: str):
    """Список тех, кто сейчас в АФК или в инактиве."""
    now = time.time()
    rows = sorted((v for v in store.statuses.values() if v["kind"] == kind and v["until"] > now),
                  key=lambda v: v["until"])
    title = "В АФК" if kind == "афк" else "В инактиве"
    if not rows:
        await interaction.response.send_message(f"{title}: никого.")
        return
    lines = [f"**{title}: {len(rows)}**"]
    for v in rows:
        until = (f"{dt.datetime.fromtimestamp(v['until'], TZ):%H:%M}" if kind == "афк"
                 else fmt_dt(v["until"])[:10])
        lines.append(f"• {v['nick']} до {until}, {v['reason']}")
    await interaction.response.send_message("\n".join(lines)[:1990], allowed_mentions=NO_PINGS)


class AdminView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    async def _guard(self, interaction: discord.Interaction) -> bool:
        if not is_admin(interaction):
            await interaction.response.send_message("Только для лидеров и админов.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Кто играет", style=discord.ButtonStyle.primary, custom_id="admin:who")
    async def who(self, interaction: discord.Interaction, button: discord.ui.Button):
        if await self._guard(interaction):
            await send_who_plays(interaction)

    @discord.ui.button(label="Кто в АФК", style=discord.ButtonStyle.secondary, custom_id="admin:afk")
    async def afk(self, interaction: discord.Interaction, button: discord.ui.Button):
        if await self._guard(interaction):
            await send_status_list(interaction, "афк")

    @discord.ui.button(label="Кто в инактиве", style=discord.ButtonStyle.secondary, custom_id="admin:inactive")
    async def inactive(self, interaction: discord.Interaction, button: discord.ui.Button):
        if await self._guard(interaction):
            await send_status_list(interaction, "инактив")

    @discord.ui.button(label="Сводка сейчас", style=discord.ButtonStyle.danger, custom_id="admin:report")
    async def report_now(self, interaction: discord.Interaction, button: discord.ui.Button):
        if await self._guard(interaction):
            await interaction.response.defer(ephemeral=True)
            await post_report(interaction.channel if interaction.channel_id == report_channel_id()
                              else (interaction.client.get_channel(report_channel_id()) or interaction.channel))
            await interaction.followup.send("Сводка выложена.", ephemeral=True)


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


# ---------- тикеты ----------
APPLICATION_FORM = (
    "**Заполни анкету:**\n\n"
    "1 - Ник и имя\n"
    "2 - Возраст\n"
    "3 - Ссылка на Стим\n"
    "4 - Часов в игре\n"
    "5 - Ваши комплектующие ПК?\n"
    "6 - В каких последних кланах играли и почему вышли?\n"
    "7 - Что ищете в нашем клане?"
)


TICKET_TYPES = {
    "main": {
        "prefix": "main",
        "button": "Открыть анкету в Мейн",
        "emoji": "👑",
        "color": 0xF1C40F,
        "panel_title": "МЕЙН СОСТАВ",
        "menu": "Мейн",
        "title": "Набор в Мейн состав",
        "description": ("Хочешь попасть в основной состав? Создай тикет и расскажи о себе.\n\n"
                        "Набираем игроков только с x3d.\n"
                        "Если у тебя нет x3d - ты можешь подать тикет во вторую тимку."),
        "welcome": APPLICATION_FORM,
        "style": discord.ButtonStyle.primary,
    },
    "second": {
        "prefix": "second",
        "button": "Открыть анкету во второй состав",
        "emoji": "⚔️",
        "color": 0x5865F2,
        "panel_title": "ВТОРОЙ СОСТАВ",
        "menu": "Второй состав",
        "title": "Набор во второй состав",
        "description": ("Хочешь присоединиться ко второму составу? Создай тикет и расскажи о себе.\n\n"
                        "Ищем игроков с онлайном от 8+ часов в день."),
        "welcome": APPLICATION_FORM,
        "style": discord.ButtonStyle.success,
    },
    "support": {
        "prefix": "support",
        "button": "Связаться с поддержкой",
        "emoji": "🛟",
        "color": 0x57F287,
        "panel_title": "ПОДДЕРЖКА",
        "title": "Поддержка сервера",
        "description": "Возник вопрос или нужна помощь? Создай тикет и подробно опиши ситуацию. Тебе помогут.",
        "welcome": "Опиши всю ситуацию и ожидай ответа.",
        "style": discord.ButtonStyle.secondary,
    },
}
_ticket_locks: dict[str, asyncio.Lock] = {}
TICKET_ROLES = ["коллер", "комбат", "fpv-пилот", "пилот", "крафтер", "электрик", "фермер", "билдер"]
TICKET_ROLE_SLUGS = {
    "коллер": "coller",
    "комбат": "combat",
    "fpv-пилот": "fpv",
    "пилот": "pilot",
    "крафтер": "crafter",
    "электрик": "electric",
    "фермер": "farmer",
    "билдер": "builder",
}
TICKET_SECOND_REQUIREMENTS = {
    "коллер": ("8к", 16),
    "комбат": ("3к", 15),
    "fpv-пилот": ("2.5к", 15),
    "пилот": ("3к", 15),
    "крафтер": ("3к", 15),
    "электрик": ("3к", 15),
    "фермер": ("2к", 15),
    "билдер": ("3к", 15),
}
TICKET_CLOSE_REASONS = {
    "low_hours": "Мало часов",
    "blacklist": "Блэклист",
    "inappropriate": "Неадекватный",
    "age": "Не проходит по возрасту",
    "obsolete": "Тикет неактуален",
    "accepted": "Принят",
}
TICKET_ROLE_CONFIG_KEYS = {slug: f"ticket_role_{slug}" for slug in TICKET_ROLE_SLUGS.values()}
TICKET_ROLE_LABELS = {
    "coller": "Коллер",
    "combat": "Комбат",
    "fpv": "FPV-пилот",
    "pilot": "Пилот",
    "crafter": "Крафтер",
    "electric": "Электрик",
    "farmer": "Фермер",
    "builder": "Билдер",
}
TICKET_SYNC_TEAM_LABELS = {"main": "Мейн", "second": "Второй состав"}
_ticket_role_sync_lock = asyncio.Lock()


def ticket_role_label(role: str) -> str:
    return "FPV-пилот" if role == "fpv-пилот" else role.capitalize()


def ticket_role_option(role: str, kind: str) -> discord.SelectOption:
    description = None
    if kind == "main":
        hours = "8к" if role == "коллер" else "4к"
        description = f"От {hours} часов · only x3d"
    elif kind == "second":
        hours, age = TICKET_SECOND_REQUIREMENTS[role]
        description = f"От {hours} часов · от {age} лет"
    return discord.SelectOption(label=ticket_role_label(role), value=role, description=description)


def ticket_overwrites(guild: discord.Guild, opener: discord.Member, kind: str) -> dict:
    """Доступ: автор, бот, роль нужного типа тикета и административные роли."""
    P = discord.PermissionOverwrite
    access = P(view_channel=True, send_messages=True, read_message_history=True, attach_files=True,
               embed_links=True)
    overwrites = {guild.default_role: P(view_channel=False), opener: access}
    if guild.me is not None:
        overwrites[guild.me] = P(view_channel=True, send_messages=True, read_message_history=True,
                                 attach_files=True, embed_links=True, manage_channels=True,
                                 manage_messages=True)
    for role_id in ticket_access_role_ids(kind):
        ticket_access = guild.get_role(role_id)
        if ticket_access is not None:
            overwrites[ticket_access] = access
    for role in guild.roles:
        if role.is_default():
            continue
        if role.permissions.administrator or role.permissions.manage_guild:
            overwrites[role] = access
    return overwrites


async def apply_ticket_access_roles(
    guild: discord.Guild,
    old_role_ids: list[int],
    roles_by_kind: dict[str, list[discord.Role]],
) -> int:
    """Разделить доступ ролей во всех уже открытых тикетах по их типу."""
    access = discord.PermissionOverwrite(
        view_channel=True, send_messages=True, read_message_history=True,
        attach_files=True, embed_links=True,
    )
    all_role_ids = set(old_role_ids)
    for roles in roles_by_kind.values():
        all_role_ids.update(role.id for role in roles)

    updated = 0
    for channel in guild.text_channels:
        kind = ticket_kind_from_topic(channel.topic or "")
        if kind is None:
            continue
        desired_roles = roles_by_kind.get(kind, [])
        desired_ids = {role.id for role in desired_roles}
        for role_id in all_role_ids:
            role = guild.get_role(role_id)
            if role is None:
                continue
            overwrite = access if role_id in desired_ids else None
            await channel.set_permissions(
                role, overwrite=overwrite, reason="Разделены роли доступа к тикетам"
            )
        updated += 1
    return updated


def next_ticket_number(guild: discord.Guild, kind: str, selected_role: str | None = None) -> tuple[int, str]:
    """Следующий номер с учётом сохранённого счётчика и уже существующих каналов."""
    prefix = TICKET_TYPES[kind]["prefix"]
    role_slug = TICKET_ROLE_SLUGS.get(selected_role or "")
    channel_prefix = f"{prefix}-{role_slug}" if role_slug else prefix
    pattern = re.compile(rf"^{re.escape(channel_prefix)}-(\d+)$")
    existing = [int(m.group(1)) for ch in guild.text_channels if (m := pattern.fullmatch(ch.name))]
    key = f"{guild.id}:{kind}:{role_slug or 'general'}"
    number = max([int(store.ticket_counters.get(key, 0)), *existing], default=0) + 1
    return number, channel_prefix


def find_open_ticket(guild: discord.Guild, owner_id: int) -> discord.TextChannel | None:
    """Найти любой ещё открытый тикет участника на сервере."""
    owner_pattern = re.compile(rf"^ticket:(?:main|second|support) owner:{owner_id}(?: |$)")
    return next((channel for channel in guild.text_channels
                 if owner_pattern.match(channel.topic or "")), None)


async def create_ticket(interaction: discord.Interaction, kind: str, selected_role: str | None = None) -> None:
    if interaction.guild is None or not isinstance(interaction.user, discord.Member):
        await interaction.response.send_message("Тикеты можно создавать только на сервере.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    guild = interaction.guild
    settings = TICKET_TYPES[kind]
    lock_key = f"{guild.id}:create"
    lock = _ticket_locks.setdefault(lock_key, asyncio.Lock())
    try:
        async with lock:
            existing_ticket = find_open_ticket(guild, interaction.user.id)
            if existing_ticket is not None:
                await interaction.followup.send(
                    f"У тебя уже есть открытый тикет: {existing_ticket.mention}. "
                    "Сначала дождись его закрытия.",
                    ephemeral=True,
                )
                return
            number, channel_prefix = next_ticket_number(guild, kind, selected_role)
            name = f"{channel_prefix}-{number}"
            channel = await guild.create_text_channel(
                name,
                category=getattr(interaction.channel, "category", None),
                overwrites=ticket_overwrites(guild, interaction.user, kind),
                topic=(f"ticket:{kind} owner:{interaction.user.id} "
                       f"role:{TICKET_ROLE_SLUGS.get(selected_role or '', 'general')}"),
                reason=f"{settings['title']}: {interaction.user}",
            )
            counter_key = f"{guild.id}:{kind}:{TICKET_ROLE_SLUGS.get(selected_role or '') or 'general'}"
            store.ticket_counters[counter_key] = number
            store.save()
        notify_roles = []
        for role_id in ticket_access_role_ids(kind):
            role = guild.get_role(role_id)
            if role is not None and role.id not in {item.id for item in notify_roles}:
                notify_roles.append(role)
        staff_mention = "".join(f" {role.mention}" for role in notify_roles)
        embed = discord.Embed(
            title=f"{settings['emoji']} {settings['title']} — #{number}",
            description=settings["welcome"] or None,
            colour=settings["color"],
        )
        embed.add_field(name="Автор тикета", value=interaction.user.mention, inline=True)
        if selected_role:
            embed.add_field(name="Направление", value=ticket_role_label(selected_role), inline=True)
        await channel.send(
            f"@everyone {interaction.user.mention}{staff_mention}",
            embed=embed,
            view=(TicketApplicationCloseView() if kind in ("main", "second") else TicketCloseView()),
            allowed_mentions=discord.AllowedMentions(everyone=True, users=True, roles=True),
        )
        await interaction.followup.send(f"Тикет создан: {channel.mention}", ephemeral=True)
    except discord.Forbidden:
        await interaction.followup.send(
            "Не получилось создать тикет. Дай боту право «Управлять каналами».", ephemeral=True)
    except Exception as e:
        print(f"Не удалось создать тикет {kind}: {short_error(e)}", flush=True)
        await interaction.followup.send(f"Не получилось создать тикет: {short_error(e)}", ephemeral=True)


class TicketPanelView(discord.ui.View):
    def __init__(self, kind: str):
        super().__init__(timeout=None)
        self.kind = kind
        settings = TICKET_TYPES[kind]
        if kind in ("main", "second"):
            self.role_select = discord.ui.Select(
                placeholder=f"Выбрать направление · {settings['menu']}",
                min_values=1,
                max_values=1,
                custom_id=f"ticket:role:{kind}",
                options=[ticket_role_option(role, kind) for role in TICKET_ROLES],
            )
            self.role_select.callback = self.select_role
            self.add_item(self.role_select)
        else:
            button = discord.ui.Button(label=settings["button"], style=settings["style"],
                                       emoji=settings["emoji"], custom_id=f"ticket:create:{kind}")
            button.callback = self.open_ticket
            self.add_item(button)

    async def open_ticket(self, interaction: discord.Interaction):
        await create_ticket(interaction, self.kind)

    async def select_role(self, interaction: discord.Interaction):
        await create_ticket(interaction, self.kind, self.role_select.values[0])


def can_close_ticket(interaction: discord.Interaction) -> bool:
    channel = interaction.channel
    if not isinstance(channel, discord.TextChannel):
        return False
    topic = channel.topic or ""
    if not topic.startswith("ticket:"):
        return False
    perms = getattr(interaction.user, "guild_permissions", None)
    if perms and perms.administrator:
        return True
    kind = ticket_kind_from_topic(topic)
    access_role_ids = set(ticket_access_role_ids(kind))
    return any(role.id in access_role_ids for role in getattr(interaction.user, "roles", []))


def ticket_log_overwrites(guild: discord.Guild) -> dict:
    P = discord.PermissionOverwrite
    access = P(view_channel=True, send_messages=True, read_message_history=True, attach_files=True,
               embed_links=True)
    overwrites = {guild.default_role: P(view_channel=False)}
    if guild.me is not None:
        overwrites[guild.me] = access
    staff = guild.get_role(staff_role_id())
    if staff is not None:
        overwrites[staff] = access
    for role in guild.roles:
        if not role.is_default() and (role.permissions.administrator or role.permissions.manage_guild):
            overwrites[role] = access
    return overwrites


async def ensure_ticket_log_channels(guild: discord.Guild, category=None) -> list[discord.TextChannel]:
    channels = []
    for key, name in (("ticket_support_logs", "логи-поддержки"),
                      ("ticket_application_logs", "логи-анкет")):
        channel = guild.get_channel(cfg_id(key)) or discord.utils.get(guild.text_channels, name=name)
        if channel is None:
            channel = await guild.create_text_channel(
                name, category=category, overwrites=ticket_log_overwrites(guild),
                reason="Логи тикетов CL Team",
            )
        else:
            for target, overwrite in ticket_log_overwrites(guild).items():
                await channel.set_permissions(target, overwrite=overwrite,
                                              reason="Настройка доступа к логам тикетов")
        store.cfg[key] = channel.id
        channels.append(channel)
    store.save()
    return channels


async def save_ticket_transcript(channel: discord.TextChannel, close_reason: str, closer) -> tuple[bool, str]:
    if not MESSAGE_CONTENT_OK:
        return False, ("В Discord Developer Portal включи Bot → Privileged Gateway Intents → "
                       "Message Content Intent и перезапусти бота.")
    topic = channel.topic or ""
    match = re.search(r"^ticket:(main|second|support) owner:(\d+)(?: role:([\w-]+))?", topic)
    if not match:
        return False, "Не удалось определить тип и автора тикета."
    kind = match.group(1)
    log_key = "ticket_support_logs" if kind == "support" else "ticket_application_logs"
    log_name = "логи-поддержки" if kind == "support" else "логи-анкет"
    log_channel = channel.guild.get_channel(cfg_id(log_key)) or discord.utils.get(
        channel.guild.text_channels, name=log_name)
    if not isinstance(log_channel, discord.TextChannel):
        try:
            created = await ensure_ticket_log_channels(channel.guild, channel.category)
            log_channel = created[0] if kind == "support" else created[1]
        except Exception as e:
            return False, f"Не удалось создать каналы логов: {short_error(e)}"

    messages = [m async for m in channel.history(limit=None, oldest_first=True)]
    lines = [f"Тикет: #{channel.name}", f"ID канала: {channel.id}",
             f"Причина закрытия: {close_reason}", f"Закрыл: {closer} ({closer.id})", ""]
    for message in messages:
        stamp = message.created_at.astimezone(TZ).strftime("%d.%m.%Y %H:%M:%S")
        author = f"{message.author} ({message.author.id})"
        content = message.content.strip()
        if content:
            lines.append(f"[{stamp}] {author}: {content}")
        else:
            lines.append(f"[{stamp}] {author}: [сообщение без текста]")
        for attachment in message.attachments:
            lines.append(f"  Вложение: {attachment.filename} — {attachment.url}")
        for embed in message.embeds:
            embed_text = " — ".join(x for x in (embed.title, embed.description) if x)
            if embed_text:
                lines.append(f"  Embed: {embed_text}")
            for field in embed.fields:
                lines.append(f"  {field.name}: {field.value}")

    transcript = "\n".join(lines).encode("utf-8")
    try:
        await log_channel.send(
            file=discord.File(io.BytesIO(transcript), filename=f"transcript-{channel.name}.txt"),
        )
    except Exception as e:
        return False, f"Не удалось сохранить лог: {short_error(e)}"
    return True, f"Лог сохранён в {log_channel.mention}."


async def send_accepted_private_invite(channel: discord.TextChannel,
                                       client: discord.Client) -> tuple[bool, str]:
    """При принятии только отправить одноразовое приглашение, без выдачи ролей."""
    match = re.search(r"^ticket:(main|second) owner:(\d+) role:([\w-]+)", channel.topic or "")
    if not match:
        return False, "Не удалось определить состав или автора тикета."
    kind, owner_id = match.group(1), int(match.group(2))
    member = channel.guild.get_member(owner_id)
    if member is None:
        try:
            member = await channel.guild.fetch_member(owner_id)
        except Exception:
            return False, "Автор тикета больше не находится на сервере."

    private_guild_id = cfg_id(ticket_sync_source_guild_key(kind))
    private_guild = client.get_guild(private_guild_id) if private_guild_id else None
    if private_guild is None:
        return False, (
            f"Не настроена приватка «{TICKET_SYNC_TEAM_LABELS[kind]}». "
            "Запусти в ней `/ticket_roles` и выбери состав."
        )
    invite_channel = private_guild.get_channel(cfg_id(ticket_private_invite_channel_key(kind)))
    if not isinstance(invite_channel, discord.TextChannel):
        return False, (
            f"В приватке «{private_guild.name}» не настроен канал приглашения. "
            "Укажи `invite_channel` в `/ticket_roles`."
        )
    private_me = private_guild.me
    if private_me is None or not invite_channel.permissions_for(private_me).create_instant_invite:
        return False, (
            f"У бота нет права «Создавать приглашения» в {invite_channel.mention} "
            f"приватки «{private_guild.name}»."
        )

    invite = None
    try:
        invite = await invite_channel.create_invite(
            max_age=7 * 86400,
            max_uses=1,
            unique=True,
            reason=f"Принят через тикет #{channel.name}",
        )
        await member.send(
            f"Твоя анкета в **{channel.guild.name}** принята.\n"
            f"Приглашение в **{private_guild.name}**: {invite.url}\n\n"
            "Ссылка одноразовая и действует 7 дней.",
            allowed_mentions=NO_PINGS,
        )
    except discord.Forbidden:
        detail = (
            "Бот не может создать приглашение или написать участнику в личку. "
            "Проверь право «Создавать приглашения» и попроси участника открыть личные сообщения."
        )
    except Exception as e:
        detail = f"Не удалось пригласить в приватку: {short_error(e)}"
    else:
        store.private_invites.pop(str(owner_id), None)
        store.save()
        return True, f"Приглашение в «{private_guild.name}» отправлено участнику в личку."

    if invite is not None:
        try:
            await invite.delete(reason="Не удалось доставить приглашение")
        except Exception:
            pass
    return False, detail


async def grant_inappropriate_ticket_role(channel: discord.TextChannel) -> tuple[bool, str]:
    """Выдать автору только роль «Неадекватный» в паблике."""
    match = re.search(r"^ticket:(main|second) owner:(\d+)", channel.topic or "")
    if not match:
        return False, "Не удалось определить автора тикета."
    owner_id = int(match.group(2))
    member = channel.guild.get_member(owner_id)
    if member is None:
        try:
            member = await channel.guild.fetch_member(owner_id)
        except Exception:
            return False, "Автор тикета больше не находится на сервере."

    role = channel.guild.get_role(cfg_id("ticket_inappropriate_role"))
    if role is None:
        role = discord.utils.find(
            lambda item: item.name.casefold() == "неадекватный", channel.guild.roles
        )
    if role is None:
        return False, (
            "Не найдена роль «Неадекватный». Создай её или настрой через "
            "`/ticket_roles inappropriate:@роль`."
        )
    me = channel.guild.me
    if me is None or role.managed or role >= me.top_role:
        return False, "Подними роль бота выше роли «Неадекватный»."
    try:
        if role not in member.roles:
            await member.add_roles(role, reason=f"Закрытие тикета #{channel.name}: Неадекватный")
    except discord.Forbidden:
        return False, "Бот не может выдать роль «Неадекватный»."
    except Exception as e:
        return False, f"Не удалось выдать роль «Неадекватный»: {short_error(e)}"
    return True, f"Роль {role.mention} выдана."


class TicketCloseConfirmView(discord.ui.View):
    def __init__(self, user_id: int, reason: str = "Закрыт", reason_key: str | None = None):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.reason = reason
        self.reason_key = reason_key

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id == self.user_id:
            return True
        await interaction.response.send_message("Подтвердить закрытие может только тот, кто нажал кнопку.",
                                                ephemeral=True)
        return False

    @discord.ui.button(label="Закрыть", style=discord.ButtonStyle.danger, custom_id="ticket:close:confirm")
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        channel = interaction.channel
        if not can_close_ticket(interaction) or not isinstance(channel, discord.TextChannel):
            await interaction.response.edit_message(content="У тебя нет права закрыть этот тикет.", view=None)
            return
        await interaction.response.defer(ephemeral=True)
        role_detail = ""
        if self.reason_key == "accepted":
            granted, role_detail = await send_accepted_private_invite(channel, interaction.client)
            if not granted:
                await interaction.edit_original_response(
                    content=f"Тикет не закрыт. {role_detail}", view=None
                )
                return
        elif self.reason_key == "inappropriate":
            granted, role_detail = await grant_inappropriate_ticket_role(channel)
            if not granted:
                await interaction.edit_original_response(
                    content=f"Тикет не закрыт. {role_detail}", view=None
                )
                return
        saved, detail = await save_ticket_transcript(channel, self.reason, interaction.user)
        if not saved:
            await interaction.edit_original_response(content=f"Тикет не закрыт. {detail}", view=None)
            return
        prefix = f"{role_detail} " if role_detail else ""
        await interaction.edit_original_response(content=f"{prefix}{detail} Тикет закрывается…", view=None)
        await asyncio.sleep(1)
        try:
            await channel.delete(reason=f"Тикет закрыт пользователем {interaction.user}")
        except Exception as e:
            try:
                await interaction.edit_original_response(content=f"Не получилось закрыть тикет: {short_error(e)}")
            except Exception:
                pass

    @discord.ui.button(label="Отмена", style=discord.ButtonStyle.secondary, custom_id="ticket:close:cancel")
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(content="Закрытие отменено.", view=None)


class TicketApplicationCloseView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.reason_select = discord.ui.Select(
            placeholder="Выбрать причину закрытия тикета",
            min_values=1,
            max_values=1,
            custom_id="ticket:application:close_reason",
            options=[discord.SelectOption(label=label, value=value)
                     for value, label in TICKET_CLOSE_REASONS.items()],
        )
        self.reason_select.callback = self.select_reason
        self.add_item(self.reason_select)

    async def select_reason(self, interaction: discord.Interaction):
        if not can_close_ticket(interaction):
            await interaction.response.send_message("Закрыть тикет может только администратор или настроенная роль доступа.",
                                                    ephemeral=True)
            return
        reason_key = self.reason_select.values[0]
        reason = TICKET_CLOSE_REASONS[reason_key]
        await interaction.response.send_message(
            f"Закрыть тикет с причиной «{reason}»?",
            view=TicketCloseConfirmView(
                interaction.user.id, reason=reason, reason_key=reason_key
            ),
            ephemeral=True,
        )


class TicketCloseView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Закрыть тикет", style=discord.ButtonStyle.danger, custom_id="ticket:close")
    async def close(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not can_close_ticket(interaction):
            await interaction.response.send_message("Закрыть тикет может только администратор или настроенная роль доступа.",
                                                    ephemeral=True)
            return
        await interaction.response.send_message("Точно закрыть и удалить этот тикет?",
                                                view=TicketCloseConfirmView(interaction.user.id), ephemeral=True)


def ticket_sync_source_guild_key(team: str) -> str:
    return f"ticket_sync_source_{team}_guild"


def ticket_sync_source_role_key(team: str, role_slug: str) -> str:
    return f"ticket_sync_source_{team}_{role_slug}"


def ticket_private_invite_channel_key(team: str) -> str:
    return f"ticket_sync_source_{team}_invite_channel"


def ticket_private_member_role_key(team: str) -> str:
    return f"ticket_sync_source_{team}_member_role"


async def apply_pending_private_roles(member: discord.Member) -> bool:
    """Автовыдача ролей в приватках временно отключена."""
    if store.private_invites.pop(str(member.id), None) is not None:
        store.save()
    return False


async def fetch_all_members(guild: discord.Guild) -> dict[int, discord.Member]:
    members = {}
    async for member in guild.fetch_members(limit=None):
        if not member.bot:
            members[member.id] = member
    return members


# ---------- закрепление игроков за 10 модераторами ----------
MODERATOR_BLOCK_COUNT = 10
MODERATOR_ROSTER_LOCKS: dict[int, asyncio.Lock] = {}


def moderator_roster_lock(guild_id: int) -> asyncio.Lock:
    return MODERATOR_ROSTER_LOCKS.setdefault(guild_id, asyncio.Lock())


def moderator_roster(guild_id: int, create: bool = False) -> dict | None:
    key = str(guild_id)
    if create and key not in store.moderator_rosters:
        store.moderator_rosters[key] = {
            "channel_id": 0,
            "last_alert_message_id": 0,
            "excluded_role_ids": [],
            "moderator_role_id": 0,
            "last_auto_sync_slot": "",
            "blocks": {
                str(number): {
                    "moderator_id": 0,
                    "player_ids": [],
                    "message_id": 0,
                }
                for number in range(1, MODERATOR_BLOCK_COUNT + 1)
            },
        }
    roster = store.moderator_rosters.get(key)
    if roster is None:
        return None
    blocks = roster.setdefault("blocks", {})
    for block in blocks.values():
        block.setdefault("moderator_id", 0)
        block.setdefault("player_ids", [])
        block.setdefault("message_id", 0)
    roster.setdefault("channel_id", 0)
    roster.setdefault("last_alert_message_id", 0)
    roster.setdefault("excluded_role_ids", [])
    roster.setdefault("moderator_role_id", 0)
    roster.setdefault("last_auto_sync_slot", "")
    return roster


def moderator_block_numbers(roster: dict) -> list[int]:
    return sorted(int(number) for number in roster.get("blocks", {}) if str(number).isdigit())


def moderator_block_embed(guild: discord.Guild, number: int, block: dict) -> discord.Embed:
    moderator_id = int(block.get("moderator_id") or 0)
    player_ids = [int(uid) for uid in block.get("player_ids", []) if int(uid)]
    moderator = guild.get_member(moderator_id) if moderator_id else None
    if moderator is not None:
        moderator_name = discord.utils.escape_markdown(
            discord.utils.escape_mentions(moderator.display_name)
        )
        moderator_text = f"**{moderator_name}** · {moderator.mention}"
    else:
        moderator_text = "*не назначен*"
    embed = discord.Embed(
        description=f"**Модератор**\n{moderator_text}",
        colour=0x7C3AED,
    )
    lines = []
    for index, uid in enumerate(player_ids, 1):
        member = guild.get_member(uid)
        if member is not None:
            member_name = discord.utils.escape_markdown(
                discord.utils.escape_mentions(member.display_name)
            )
            lines.append(f"**{index:02d}. {member_name}** · {member.mention}")
        else:
            lines.append(f"**{index:02d}.** *участник не найден* (`{uid}`)")
    if not lines:
        embed.add_field(name="Игроки · 0", value="*Игроков пока нет.*", inline=False)
    else:
        chunks, current = [], []
        current_size = 0
        for line in lines:
            if current and current_size + len(line) + 1 > 1000:
                chunks.append("\n".join(current))
                current, current_size = [], 0
            current.append(line)
            current_size += len(line) + 1
        if current:
            chunks.append("\n".join(current))
        for index, chunk in enumerate(chunks):
            name = f"Игроки · {len(player_ids)}" if index == 0 else "Продолжение"
            embed.add_field(name=name, value=chunk, inline=False)
    embed.set_footer(text="CL Team")
    return embed


async def moderator_roster_channel(guild: discord.Guild, roster: dict) -> discord.TextChannel | None:
    channel_id = int(roster.get("channel_id") or 0)
    channel = guild.get_channel(channel_id) if channel_id else None
    return channel if isinstance(channel, discord.TextChannel) else None


async def update_moderator_block(guild: discord.Guild, number: int, roster: dict | None = None):
    roster = roster or moderator_roster(guild.id)
    if roster is None:
        return
    channel = await moderator_roster_channel(guild, roster)
    if channel is None:
        return
    block = roster["blocks"][str(number)]
    message = None
    message_id = int(block.get("message_id") or 0)
    if message_id:
        try:
            message = await channel.fetch_message(message_id)
        except (discord.NotFound, discord.Forbidden):
            message = None
    embed = moderator_block_embed(guild, number, block)
    if message is None:
        message = await channel.send(embed=embed, allowed_mentions=NO_PINGS)
        block["message_id"] = message.id
        store.save()
    else:
        await message.edit(content=None, embed=embed, allowed_mentions=NO_PINGS)


async def update_all_moderator_blocks(guild: discord.Guild, roster: dict | None = None):
    roster = roster or moderator_roster(guild.id)
    if roster is None:
        return
    for number in moderator_block_numbers(roster):
        await update_moderator_block(guild, number, roster)


async def send_moderator_roster_alert(guild: discord.Guild, roster: dict, text: str):
    """В канале остаётся только последнее уведомление с тегом."""
    channel = await moderator_roster_channel(guild, roster)
    if channel is None:
        return
    previous_id = int(roster.get("last_alert_message_id") or 0)
    if previous_id:
        try:
            previous = await channel.fetch_message(previous_id)
            await previous.delete()
        except (discord.NotFound, discord.Forbidden):
            pass
    alert = await channel.send(
        text,
        allowed_mentions=discord.AllowedMentions(users=True, roles=False, everyone=False),
    )
    roster["last_alert_message_id"] = alert.id
    store.save()


def find_moderator_player_block(roster: dict, member_id: int) -> int | None:
    for number in moderator_block_numbers(roster):
        if member_id in [int(uid) for uid in roster["blocks"][str(number)].get("player_ids", [])]:
            return number
    return None


def configured_moderator_blocks(roster: dict) -> list[int]:
    return [
        number for number in moderator_block_numbers(roster)
        if int(roster["blocks"][str(number)].get("moderator_id") or 0)
    ]


def member_is_moderator_excluded(member: discord.Member, roster: dict) -> bool:
    excluded_role_ids = {int(role_id) for role_id in roster.get("excluded_role_ids", [])}
    return any(role.id in excluded_role_ids for role in member.roles)


def member_has_roster_moderator_role(member: discord.Member, roster: dict) -> bool:
    role_id = int(roster.get("moderator_role_id") or 0)
    return bool(role_id and any(role.id == role_id for role in member.roles))


async def auto_assign_moderator_player(member: discord.Member) -> bool:
    roster = moderator_roster(member.guild.id)
    if roster is None or not int(roster.get("channel_id") or 0):
        return False
    async with moderator_roster_lock(member.guild.id):
        if find_moderator_player_block(roster, member.id) is not None:
            return False
        blocks = configured_moderator_blocks(roster)
        moderator_ids = {
            int(roster["blocks"][str(number)].get("moderator_id") or 0)
            for number in blocks
        }
        if not blocks or member.id in moderator_ids or member_is_moderator_excluded(member, roster):
            return False
        number = min(
            blocks,
            key=lambda item: (len(roster["blocks"][str(item)].get("player_ids", [])), item),
        )
        block = roster["blocks"][str(number)]
        block.setdefault("player_ids", []).append(member.id)
        store.save()
        await update_moderator_block(member.guild, number, roster)
        moderator_id = int(block.get("moderator_id") or 0)
        await send_moderator_roster_alert(
            member.guild,
            roster,
            f"<@{moderator_id}> Новый игрок {member.mention} (`{member.id}`) "
            "автоматически закреплён за вами.",
        )
        return True


async def remove_departed_moderator_player(member: discord.Member) -> bool:
    roster = moderator_roster(member.guild.id)
    if roster is None:
        return False
    async with moderator_roster_lock(member.guild.id):
        number = find_moderator_player_block(roster, member.id)
        if number is None:
            return False
        block = roster["blocks"][str(number)]
        block["player_ids"] = [
            uid for uid in block.get("player_ids", []) if int(uid) != member.id
        ]
        store.save()
        await update_moderator_block(member.guild, number, roster)
        moderator_id = int(block.get("moderator_id") or 0)
        await send_moderator_roster_alert(
            member.guild,
            roster,
            f"<@{moderator_id}> Игрок **{discord.utils.escape_markdown(str(member))}** "
            f"(`{member.id}`) вышел с сервера и удалён из вашей карточки.",
        )
        return True


async def synchronize_moderator_roster(guild: discord.Guild) -> tuple[int, int]:
    """Сверить карточки с ролью модератора и заново равномерно раздать игроков."""
    roster = moderator_roster(guild.id)
    if roster is None or not int(roster.get("channel_id") or 0):
        raise RuntimeError("Сначала запусти /moderators_setup в нужном канале.")
    role_id = int(roster.get("moderator_role_id") or 0)
    moderator_role = guild.get_role(role_id) if role_id else None
    if moderator_role is None:
        raise RuntimeError("Сначала настрой роль модераторов через /moderators_role.")
    if not MEMBERS_OK:
        raise RuntimeError("В Discord Developer Portal не включён Server Members Intent.")
    members = await fetch_all_members(guild)
    moderators = sorted(
        (member for member in members.values() if moderator_role in member.roles),
        key=lambda member: (member.display_name.casefold(), member.id),
    )
    async with moderator_roster_lock(guild.id):
        old_blocks = roster.get("blocks", {})
        old_numbers = moderator_block_numbers(roster)
        channel = await moderator_roster_channel(guild, roster)
        new_blocks = {}
        for index, moderator in enumerate(moderators, 1):
            old = old_blocks.get(str(index), {})
            new_blocks[str(index)] = {
                "moderator_id": moderator.id,
                "player_ids": [],
                "message_id": int(old.get("message_id") or 0),
            }
        removed_message_ids = [
            int(old_blocks[str(number)].get("message_id") or 0)
            for number in old_numbers
            if number > len(moderators)
        ]
        moderator_ids = {member.id for member in moderators}
        players = sorted(
            (
                member for member in members.values()
                if member.id not in moderator_ids
                and not member_is_moderator_excluded(member, roster)
            ),
            key=lambda member: (member.display_name.casefold(), member.id),
        )
        if moderators:
            for index, member in enumerate(players):
                number = index % len(moderators) + 1
                new_blocks[str(number)]["player_ids"].append(member.id)
        roster["blocks"] = new_blocks
        store.save()
        if channel is not None:
            for message_id in removed_message_ids:
                if not message_id:
                    continue
                try:
                    message = await channel.fetch_message(message_id)
                    await message.delete()
                except (discord.NotFound, discord.Forbidden):
                    pass
        await update_all_moderator_blocks(guild, roster)
    return len(moderators), len(players)


async def process_scheduled_moderator_rosters(client: discord.Client):
    """Автосверка в 00:00 и 12:00 по Москве; запуск после границы тоже считается."""
    now = dt.datetime.now(MAIN_VOICE_TZ)
    slot_hour = 12 if now.hour >= 12 else 0
    slot_key = f"{now:%Y-%m-%d}-{slot_hour:02d}"
    for guild in client.guilds:
        roster = moderator_roster(guild.id)
        if (
            roster is None
            or not int(roster.get("channel_id") or 0)
            or not int(roster.get("moderator_role_id") or 0)
            or roster.get("last_auto_sync_slot") == slot_key
        ):
            continue
        roster["last_auto_sync_slot"] = slot_key
        store.save()
        try:
            await synchronize_moderator_roster(guild)
        except Exception as e:
            print(f"Не удалось автоматически сверить модераторов {guild.id}: {short_error(e)}", flush=True)


# ---------- VIS: карточки игроков в каналах модераторов ----------
VIS_CATEGORY_NAME = "MODERATORS"
VIS_HUB_LOCKS: dict[int, asyncio.Lock] = {}
STEAM_RE = re.compile(
    r"https?://(?:www\.)?steamcommunity\.com/(?:profiles/\d+|id/[A-Za-z0-9_-]+)/?",
    re.I,
)
STEAM64_RE = re.compile(r"\b(7656119\d{10})\b")
STEAM_LEGACY_RE = re.compile(r"\bSTEAM_[0-5]:([01]):(\d+)\b", re.I)
STEAM_ACCOUNT_RE = re.compile(r"\[U:1:(\d+)\]", re.I)
STEAM64_BASE = 76561197960265728
BM_RE = re.compile(r"https?://(?:www\.)?battlemetrics\.com/players/\d+/?", re.I)
BM_ID_RE = re.compile(r"(?:battlemetrics|battle\s*metrics|бм|бмс)\s*[:#=-]?\s*(\d{5,15})", re.I)


def vis_config(guild_id: int, create: bool = False) -> dict | None:
    key = str(guild_id)
    if create and key not in store.vis_configs:
        store.vis_configs[key] = {
            "farm_channel_id": 0,
            "profile_channel_ids": [],
            "profile_channel_types": {},
            "category_id": 0,
            "moderator_channels": {},
            "card_messages": {},
            "last_change_alerts": {},
            "model": VISION_MODEL,
            "last_hour_slot": "",
        }
    cfg = store.vis_configs.get(key)
    if cfg is None:
        return None
    cfg.setdefault("farm_channel_id", 0)
    cfg.setdefault("profile_channel_ids", [])
    cfg.setdefault("profile_channel_types", {})
    cfg.setdefault("category_id", 0)
    cfg.setdefault("moderator_channels", {})
    cfg.setdefault("card_messages", {})
    cfg.setdefault("last_change_alerts", {})
    cfg.setdefault("model", VISION_MODEL)
    cfg.setdefault("last_hour_slot", "")
    return cfg


def vis_profile(guild_id: int, user_id: int, create: bool = False) -> dict | None:
    guild_profiles = store.vis_profiles.setdefault(str(guild_id), {}) if create else store.vis_profiles.get(str(guild_id), {})
    key = str(user_id)
    if create and key not in guild_profiles:
        guild_profiles[key] = {
            "steam": "",
            "battlemetrics": "",
            "timezone": "",
            "rt_positions": "",
            "updated_at": 0,
        }
    return guild_profiles.get(key)


def vis_voice_key(guild_id: int, user_id: int) -> str:
    return f"{guild_id}:{user_id}"


def vis_voice_start(guild_id: int, user_id: int, now: float | None = None):
    store.vis_voice_open.setdefault(vis_voice_key(guild_id, user_id), now or time.time())
    store.save()


def vis_voice_stop(guild_id: int, user_id: int, now: float | None = None):
    key = vis_voice_key(guild_id, user_id)
    started = store.vis_voice_open.pop(key, None)
    if started is not None:
        store.vis_voice.append([str(guild_id), str(user_id), started, now or time.time()])
    store.save()


def reconcile_vis_voice(client: discord.Client):
    current = {
        vis_voice_key(guild.id, member.id)
        for guild in client.guilds
        for channel in guild.voice_channels
        for member in channel.members
        if not member.bot
    }
    for key, started in list(store.vis_voice_open.items()):
        if key not in current:
            guild_id, user_id = key.split(":", 1)
            store.vis_voice.append([guild_id, user_id, started, max(started, store.heartbeat)])
            del store.vis_voice_open[key]
    now = time.time()
    for key in current:
        store.vis_voice_open.setdefault(key, now)
    store.save()


def vis_voice_today_seconds(guild_id: int, user_id: int, now: float | None = None) -> float:
    current = now or time.time()
    local_now = dt.datetime.fromtimestamp(current, MAIN_VOICE_TZ)
    midnight = local_now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    total = 0.0
    for row in store.vis_voice:
        if len(row) < 4 or int(row[0]) != guild_id or int(row[1]) != user_id:
            continue
        start, end = float(row[2]), float(row[3])
        total += max(0.0, min(end, current) - max(start, midnight))
    started = store.vis_voice_open.get(vis_voice_key(guild_id, user_id))
    if started is not None:
        total += max(0.0, current - max(float(started), midnight))
    return total


def vis_duration(seconds: float) -> str:
    minutes = max(0, int(seconds // 60))
    hours, minutes = divmod(minutes, 60)
    return f"{hours} ч {minutes:02d} мин"


def vis_target_member(message: discord.Message) -> discord.Member | None:
    mentioned = next((member for member in message.mentions if not member.bot), None)
    return mentioned or (message.author if isinstance(message.author, discord.Member) and not message.author.bot else None)


def vis_steam_profile_from_text(text: str) -> str:
    steam = STEAM_RE.search(text)
    if steam:
        return steam.group(0).rstrip("/")
    steam64 = STEAM64_RE.search(text)
    if steam64:
        return f"https://steamcommunity.com/profiles/{steam64.group(1)}"
    legacy = STEAM_LEGACY_RE.search(text)
    if legacy:
        account_id = int(legacy.group(2)) * 2 + int(legacy.group(1))
        return f"https://steamcommunity.com/profiles/{STEAM64_BASE + account_id}"
    account = STEAM_ACCOUNT_RE.search(text)
    if account:
        return f"https://steamcommunity.com/profiles/{STEAM64_BASE + int(account.group(1))}"
    return ""


def vis_battlemetrics_profile_from_text(text: str) -> str:
    battlemetrics = BM_RE.search(text)
    if battlemetrics:
        return battlemetrics.group(0).rstrip("/")
    battlemetrics_id = BM_ID_RE.search(text)
    if battlemetrics_id:
        return f"https://www.battlemetrics.com/players/{battlemetrics_id.group(1)}"
    return ""


def vis_extract_profile_fields(text: str, source_type: str = "data") -> dict[str, str]:
    out: dict[str, str] = {}
    steam = vis_steam_profile_from_text(text)
    battlemetrics = vis_battlemetrics_profile_from_text(text)
    if steam:
        out["steam"] = steam
    if battlemetrics:
        out["battlemetrics"] = battlemetrics
    timezone_match = re.search(
        r"(?:(?:часов(?:ой|ого)\s+пояс\w*|utc|gmt|мск)\s*[:=]?\s*)([+-]?\s*\d{1,2})",
        text,
        re.I,
    )
    if timezone_match:
        offset = timezone_match.group(1).replace(" ", "")
        if not offset.startswith(("+", "-")):
            offset = f"+{offset}"
        out["timezone"] = f"UTC{offset}"
    elif source_type == "timezone":
        simple_offset = re.search(r"[+-]?\s*\d{1,2}", text)
        if simple_offset:
            offset = simple_offset.group(0).replace(" ", "")
            if not offset.startswith(("+", "-")):
                offset = f"+{offset}"
            out["timezone"] = f"UTC{offset}"
        elif "мск" in text.lower():
            out["timezone"] = "UTC+3 (МСК)"
    positions_match = re.search(
        r"(?:позици\w*\s+(?:на\s+)?рт|знаю\s+(?:позици\w*\s+)?(?:на\s+)?рт)\s*[:=-]?\s*(.+)",
        text,
        re.I | re.S,
    )
    if positions_match:
        out["rt_positions"] = positions_match.group(1).strip()[:500]
    elif source_type == "positions":
        cleaned = re.sub(r"<@!?\d+>", "", text).strip(" :-\n\t")
        if cleaned:
            out["rt_positions"] = cleaned[:500]
    return out


def vis_apply_profile_fields(guild_id: int, user_id: int, fields: dict[str, str]) -> bool:
    clean = {key: str(value).strip() for key, value in fields.items() if str(value).strip()}
    if not clean:
        return False
    profile = vis_profile(guild_id, user_id, create=True)
    changed = False
    for key in ("steam", "battlemetrics", "timezone", "rt_positions"):
        value = clean.get(key)
        if key == "steam" and value:
            value = vis_steam_profile_from_text(value) or value
        if key == "battlemetrics" and value:
            value = vis_battlemetrics_profile_from_text(value) or value
        if value and profile.get(key) != value:
            profile[key] = value
            changed = True
    if changed:
        profile["updated_at"] = time.time()
        store.save()
    return changed


def parse_openrouter_json(text: str) -> dict:
    cleaned = text.strip()
    cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.I)
    cleaned = re.sub(r"\s*```$", "", cleaned)
    start, end = cleaned.find("{"), cleaned.rfind("}")
    if start >= 0 and end >= start:
        cleaned = cleaned[start:end + 1]
    data = json.loads(cleaned)
    if not isinstance(data, dict):
        raise ValueError("модель вернула не объект JSON")
    return data


async def vis_openrouter_json(model: str, content: list[dict] | str, prompt: str) -> dict:
    if not OPENROUTER_KEY:
        raise RuntimeError("OPENROUTER_KEY не настроен на сервере")
    user_content = ([{"type": "text", "text": prompt}] + content) if isinstance(content, list) else f"{prompt}\n\n{content}"
    body = {
        "model": model,
        "temperature": 0,
        "messages": [
            {"role": "system", "content": "Отвечай только корректным JSON без markdown."},
            {"role": "user", "content": user_content},
        ],
    }
    headers = {"Authorization": f"Bearer {OPENROUTER_KEY}", "X-Title": "rust-clan-bot-vis"}
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120)) as session:
        async with session.post(OPENROUTER_URL, json=body, headers=headers) as response:
            payload = await response.json(content_type=None)
            if response.status >= 400:
                error = payload.get("error", {}) if isinstance(payload, dict) else {}
                raise RuntimeError(f"OpenRouter {response.status}: {error.get('message') or payload}")
    return parse_openrouter_json(payload["choices"][0]["message"]["content"])


def vis_number(value) -> int:
    if isinstance(value, (int, float)):
        return max(0, int(value))
    text = str(value or "0").strip().lower().replace(" ", "").replace(",", ".")
    multiplier = 1000 if text.endswith(("k", "к", "тыс")) else 1
    text = re.sub(r"(?:k|к|тыс)$", "", text)
    try:
        return max(0, int(float(text) * multiplier))
    except ValueError:
        return 0


def vis_farm_day_key(timestamp: float | None = None) -> str:
    return dt.datetime.fromtimestamp(timestamp or time.time(), MAIN_VOICE_TZ).strftime("%Y-%m-%d")


def vis_add_farm(guild_id: int, user_id: int, values: dict):
    guild_totals = store.vis_farm_totals.setdefault(str(guild_id), {})
    player_totals = guild_totals.setdefault(str(user_id), {})
    day = player_totals.setdefault(
        vis_farm_day_key(),
        {"sulfur": 0, "metal": 0, "stone": 0, "wood": 0, "cupboards": 0},
    )
    cupboards = vis_number(values.get("cupboards"))
    day["sulfur"] += vis_number(values.get("sulfur"))
    day["metal"] += vis_number(values.get("metal"))
    day["stone"] += vis_number(values.get("stone"))
    day["wood"] += vis_number(values.get("wood")) + cupboards * 1000
    day["cupboards"] += cupboards
    store.save()


def vis_farm_sum(guild_id: int, user_id: int, days: int) -> dict[str, int]:
    out = {"sulfur": 0, "metal": 0, "stone": 0, "wood": 0, "cupboards": 0}
    totals = store.vis_farm_totals.get(str(guild_id), {}).get(str(user_id), {})
    today = dt.datetime.now(MAIN_VOICE_TZ).date()
    accepted = {(today - dt.timedelta(days=offset)).isoformat() for offset in range(days)}
    for day_key, values in totals.items():
        if day_key not in accepted:
            continue
        for key in out:
            out[key] += vis_number(values.get(key))
    return out


def vis_farm_text(values: dict[str, int]) -> str:
    return (
        f"Сера: **{values['sulfur']:,}**\n"
        f"Металл: **{values['metal']:,}**\n"
        f"Камень: **{values['stone']:,}**\n"
        f"Дерево: **{values['wood']:,}**\n"
        f"Шкафы: **{values['cupboards']}**"
    ).replace(",", " ")


async def process_vis_farm_message(message: discord.Message):
    cfg = vis_config(message.guild.id) if message.guild else None
    if cfg is None or message.channel.id != int(cfg.get("farm_channel_id") or 0):
        return
    member = vis_target_member(message)
    if member is None:
        return
    image_parts, new_hashes = [], []
    seen = store.vis_seen_images.setdefault(str(message.guild.id), {})
    for attachment in message.attachments:
        content_type = (attachment.content_type or "").lower()
        if not content_type.startswith("image/") and not attachment.filename.lower().endswith(
            (".png", ".jpg", ".jpeg", ".webp")
        ):
            continue
        raw = await attachment.read()
        image_hash = hashlib.sha256(raw).hexdigest()
        if image_hash in seen:
            continue
        mime = content_type if content_type.startswith("image/") else "image/png"
        image_parts.append({
            "type": "image_url",
            "image_url": {"url": f"data:{mime};base64,{base64.b64encode(raw).decode('ascii')}"},
        })
        new_hashes.append(image_hash)
    if not image_parts:
        return
    prompt = (
        "Проанализируй скриншоты Rust и посчитай только явно видимые количества ресурсов. "
        "Верни JSON с целыми числами: sulfur, metal, stone, wood, cupboards. "
        "cupboards — количество предметов Tool Cupboard/шкаф. Не добавляй стоимость шкафа …1256 tokens truncated…lour=0x7C3AED,
    )
    steam_id_match = re.search(r"steamcommunity\.com/profiles/(\d+)", steam)
    if steam.startswith("http"):
        steam_label = steam_id_match.group(1) if steam_id_match else "Открыть профиль"
        steam_value = f"[{steam_label}]({steam})"
    else:
        steam_value = steam
    embed.add_field(name="Steam", value=steam_value, inline=True)
    embed.add_field(
        name="BattleMetrics",
        value=f"[Открыть профиль]({battlemetrics})" if battlemetrics.startswith("http") else battlemetrics,
        inline=True,
    )
    embed.add_field(name="Часовой пояс", value=timezone, inline=True)
    embed.add_field(name="Позиции на РТ", value=str(positions)[:1024], inline=False)
    embed.add_field(
        name="Голосовой онлайн сегодня",
        value=vis_duration(vis_voice_today_seconds(guild.id, member.id)),
        inline=False,
    )
    embed.add_field(name="Фарм сегодня", value=vis_farm_text(vis_farm_sum(guild.id, member.id, 1)), inline=True)
    embed.add_field(name="Фарм за 7 дней", value=vis_farm_text(vis_farm_sum(guild.id, member.id, 7)), inline=True)
    embed.set_footer(text="CL Team")
    return embed


async def scan_vis_profile_history(guild: discord.Guild, cfg: dict,
                                   channel_ids: list[int] | None = None) -> int:
    changed = 0
    seen_channels = set()
    for channel_id in (channel_ids or cfg.get("profile_channel_ids", [])):
        channel = guild.get_channel(int(channel_id))
        if not isinstance(channel, discord.TextChannel) or channel.id in seen_channels:
            continue
        seen_channels.add(channel.id)
        async for message in channel.history(limit=None, oldest_first=True):
            if message.author.bot or not message.content.strip():
                continue
            member = vis_target_member(message)
            if member is None:
                continue
            source_type = str(cfg.get("profile_channel_types", {}).get(str(channel.id), "data"))
            fields = vis_extract_profile_fields(message.content, source_type)
            if fields and vis_apply_profile_fields(guild.id, member.id, fields):
                changed += 1
    return changed


async def ensure_vis_category(guild: discord.Guild, cfg: dict, moderator_role: discord.Role) -> discord.CategoryChannel:
    category = guild.get_channel(int(cfg.get("category_id") or 0))
    if not isinstance(category, discord.CategoryChannel):
        category = discord.utils.get(guild.categories, name=VIS_CATEGORY_NAME)
    overwrites = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False),
        moderator_role: discord.PermissionOverwrite(
            view_channel=True, read_message_history=True, send_messages=False
        ),
    }
    if guild.me is not None:
        overwrites[guild.me] = discord.PermissionOverwrite(
            view_channel=True, read_message_history=True, send_messages=True,
            manage_messages=True, manage_channels=True,
        )
    if category is None:
        category = await guild.create_category(VIS_CATEGORY_NAME, overwrites=overwrites, reason="VIS карточки игроков")
    else:
        await category.edit(overwrites=overwrites, reason="VIS права категории модераторов")
    cfg["category_id"] = category.id
    store.save()
    return category


async def update_vis_card_message(channel: discord.TextChannel, message_id: int,
                                  embed: discord.Embed) -> int:
    if message_id:
        try:
            message = channel.get_partial_message(message_id)
            await message.edit(content=None, embed=embed, allowed_mentions=NO_PINGS)
            return message_id
        except discord.NotFound:
            pass
    message = await channel.send(embed=embed, allowed_mentions=NO_PINGS)
    return message.id


async def refresh_vis_player_card(guild: discord.Guild, user_id: int) -> bool:
    """Точечно обновить карточку игрока после изменения профиля или фарма."""
    cfg = vis_config(guild.id)
    member = guild.get_member(user_id)
    if cfg is None or member is None:
        return False
    lock = VIS_HUB_LOCKS.setdefault(guild.id, asyncio.Lock())
    async with lock:
        for moderator_id, player_cards in cfg.get("card_messages", {}).items():
            message_id = int(player_cards.get(str(user_id), 0) or 0)
            if not message_id:
                continue
            channel_id = int(cfg.get("moderator_channels", {}).get(str(moderator_id), 0) or 0)
            channel = guild.get_channel(channel_id)
            if not isinstance(channel, discord.TextChannel):
                continue
            try:
                player_cards[str(user_id)] = await update_vis_card_message(
                    channel, message_id, vis_player_embed(guild, member)
                )
                alerts = cfg.setdefault("last_change_alerts", {})
                previous_alert_id = int(alerts.get(str(moderator_id), 0) or 0)
                if previous_alert_id:
                    try:
                        await channel.get_partial_message(previous_alert_id).delete()
                    except (discord.NotFound, discord.Forbidden):
                        pass
                moderator = guild.get_member(int(moderator_id))
                if moderator is not None:
                    safe_name = discord.utils.escape_markdown(
                        discord.utils.escape_mentions(member.display_name)
                    )
                    try:
                        alert = await channel.send(
                            f"{moderator.mention} В карточке игрока **{safe_name}** есть изменения.",
                            allowed_mentions=discord.AllowedMentions(
                                users=True, roles=False, everyone=False
                            ),
                        )
                        alerts[str(moderator_id)] = alert.id
                    except Exception as e:
                        print(f"VIS уведомление модератора {moderator_id}: {short_error(e)}", flush=True)
                store.save()
                return True
            except Exception as e:
                print(f"VIS карточка игрока {guild.id}/{user_id}: {short_error(e)}", flush=True)
                return False
    return False


async def _synchronize_vis_hub(guild: discord.Guild) -> tuple[int, int]:
    cfg = vis_config(guild.id)
    roster = moderator_roster(guild.id)
    if cfg is None:
        raise RuntimeError("Сначала настрой /vis.")
    if roster is None:
        raise RuntimeError("Сначала настрой карточки модераторов.")
    moderator_role_id = int(roster.get("moderator_role_id") or 0)
    moderator_role = guild.get_role(moderator_role_id) if moderator_role_id else None
    if moderator_role is None:
        raise RuntimeError("Сначала настрой /moderators_role.")
    category = await ensure_vis_category(guild, cfg, moderator_role)
    members = await fetch_all_members(guild)
    blocks = [roster["blocks"][str(number)] for number in configured_moderator_blocks(roster)]
    active_moderator_ids = {int(block.get("moderator_id") or 0) for block in blocks}
    channels = cfg.setdefault("moderator_channels", {})
    cards = cfg.setdefault("card_messages", {})

    for moderator_id in list(channels):
        if int(moderator_id) in active_moderator_ids:
            continue
        channel = guild.get_channel(int(channels.pop(moderator_id) or 0))
        if isinstance(channel, discord.TextChannel) and channel.category_id == category.id:
            try:
                await channel.delete(reason="Модератор больше не назначен")
            except discord.Forbidden:
                pass
        cards.pop(moderator_id, None)
        cfg.setdefault("last_change_alerts", {}).pop(moderator_id, None)

    total_cards = 0
    for block in blocks:
        moderator_id = int(block.get("moderator_id") or 0)
        moderator = members.get(moderator_id)
        if moderator is None:
            continue
        channel = guild.get_channel(int(channels.get(str(moderator_id), 0) or 0))
        if not isinstance(channel, discord.TextChannel):
            channel = await guild.create_text_channel(
                vis_safe_channel_name(moderator), category=category,
                reason="VIS канал модератора",
            )
            channels[str(moderator_id)] = channel.id
        elif channel.category_id != category.id:
            await channel.edit(category=category, sync_permissions=True, reason="VIS перенос канала")

        player_ids = [int(uid) for uid in block.get("player_ids", [])]
        moderator_cards = cards.setdefault(str(moderator_id), {})
        detected_cards: dict[int, list[discord.Message]] = {}
        async for existing_message in channel.history(limit=500):
            if existing_message.author.id != guild.me.id or not existing_message.embeds:
                continue
            description = existing_message.embeds[0].description or ""
            if "В приватке:" not in description:
                continue
            match = re.search(r"<@!?(\d+)>", description)
            if match:
                detected_cards.setdefault(int(match.group(1)), []).append(existing_message)

        for detected_user_id, messages in detected_cards.items():
            if detected_user_id not in player_ids:
                for duplicate in messages:
                    try:
                        await duplicate.delete()
                    except discord.Forbidden:
                        pass
                moderator_cards.pop(str(detected_user_id), None)
                continue
            saved_id = int(moderator_cards.get(str(detected_user_id), 0) or 0)
            keep = next((message for message in messages if message.id == saved_id), None)
            if keep is None:
                keep = messages[-1]
                moderator_cards[str(detected_user_id)] = keep.id
            for duplicate in messages:
                if duplicate.id == keep.id:
                    continue
                try:
                    await duplicate.delete()
                except discord.Forbidden:
                    pass
        for user_id in list(moderator_cards):
            if int(user_id) in player_ids:
                continue
            message_id = int(moderator_cards.pop(user_id) or 0)
            if message_id:
                try:
                    await channel.get_partial_message(message_id).delete()
                except (discord.NotFound, discord.Forbidden):
                    pass
        for user_id in player_ids:
            member = members.get(user_id)
            if member is None:
                continue
            message_id = int(moderator_cards.get(str(user_id), 0) or 0)
            moderator_cards[str(user_id)] = await update_vis_card_message(
                channel, message_id, vis_player_embed(guild, member)
            )
            total_cards += 1
        store.save()
    return len(active_moderator_ids), total_cards


async def synchronize_vis_hub(guild: discord.Guild) -> tuple[int, int]:
    lock = VIS_HUB_LOCKS.setdefault(guild.id, asyncio.Lock())
    async with lock:
        return await _synchronize_vis_hub(guild)


async def process_hourly_vis_hubs(client: discord.Client):
    now = dt.datetime.now(MAIN_VOICE_TZ)
    slot = now.strftime("%Y-%m-%d-%H")
    for guild in client.guilds:
        cfg = vis_config(guild.id)
        if cfg is None or cfg.get("last_hour_slot") == slot:
            continue
        cfg["last_hour_slot"] = slot
        store.save()
        try:
            await synchronize_vis_hub(guild)
        except Exception as e:
            print(f"VIS почасовое обновление {guild.id}: {short_error(e)}", flush=True)


def sync_member_name(member: discord.Member) -> str:
    return f"{member} ({member.id})"


async def synchronize_ticket_roles(client: discord.Client, target_guild: discord.Guild) -> tuple[str, str]:
    """Синхронизировать составы и направления из двух приваток в Community."""
    if not MEMBERS_OK:
        raise RuntimeError("В Discord Developer Portal не включён Server Members Intent.")
    if cfg_id("ticket_sync_target_guild") != target_guild.id:
        raise RuntimeError("Эта панель находится не на настроенном Community-сервере.")

    source_guilds = {}
    for team in ("main", "second"):
        guild_id = cfg_id(ticket_sync_source_guild_key(team))
        source_guild = client.get_guild(guild_id) if guild_id else None
        if source_guild is None:
            raise RuntimeError(
                f"Не настроена приватка «{TICKET_SYNC_TEAM_LABELS[team]}». "
                f"Запусти в ней `/ticket_roles team:{TICKET_SYNC_TEAM_LABELS[team]}`."
            )
        source_guilds[team] = source_guild

    group_roles = {
        "main": target_guild.get_role(cfg_id("ticket_group_main")),
        "second": target_guild.get_role(cfg_id("ticket_group_second")),
    }
    missing_groups = [TICKET_SYNC_TEAM_LABELS[team] for team, role in group_roles.items() if role is None]
    if missing_groups:
        raise RuntimeError("Не настроены роли составов в Community: " + ", ".join(missing_groups) + ".")

    target_direction_roles = {}
    deleted_target_roles = []
    for role_slug, config_key in TICKET_ROLE_CONFIG_KEYS.items():
        role_id = cfg_id(config_key)
        role = target_guild.get_role(role_id) if role_id else None
        if role_id and role is None:
            deleted_target_roles.append(TICKET_ROLE_LABELS[role_slug])
        elif role is not None:
            target_direction_roles[role_slug] = role
    if deleted_target_roles:
        raise RuntimeError("В Community не найдены настроенные роли: " + ", ".join(deleted_target_roles) + ".")

    source_direction_roles: dict[str, dict[str, discord.Role]] = {"main": {}, "second": {}}
    missing_mappings = []
    for team, source_guild in source_guilds.items():
        for role_slug in target_direction_roles:
            source_role_id = cfg_id(ticket_sync_source_role_key(team, role_slug))
            source_role = source_guild.get_role(source_role_id) if source_role_id else None
            if source_role is None:
                missing_mappings.append(
                    f"{TICKET_SYNC_TEAM_LABELS[team]} — {TICKET_ROLE_LABELS[role_slug]}"
                )
            else:
                source_direction_roles[team][role_slug] = source_role
    if missing_mappings:
        raise RuntimeError("Не сопоставлены роли в приватках: " + ", ".join(missing_mappings) + ".")

    managed_roles = {*group_roles.values(), *target_direction_roles.values()}
    me = target_guild.me
    if me is None or not me.guild_permissions.manage_roles:
        raise RuntimeError("У бота в Community нет права «Управлять ролями».")
    unavailable = [role.name for role in managed_roles if role.managed or role >= me.top_role]
    if unavailable:
        raise RuntimeError("Бот не может управлять ролями выше своей роли: " + ", ".join(unavailable) + ".")

    try:
        main_members, second_members, target_members = await asyncio.gather(
            fetch_all_members(source_guilds["main"]),
            fetch_all_members(source_guilds["second"]),
            fetch_all_members(target_guild),
        )
    except discord.Forbidden as e:
        raise RuntimeError("Бот не может получить список участников одного из серверов.") from e
    if not main_members or not second_members or not target_members:
        raise RuntimeError(
            "Один из серверов вернул пустой список участников. Синхронизация остановлена, чтобы не снять роли ошибочно."
        )

    main_ids = set(main_members)
    second_ids = set(second_members)
    both_ids = main_ids & second_ids
    second_only_ids = second_ids - main_ids
    source_ids = main_ids | second_ids
    missing_in_community = sorted(source_ids - set(target_members))
    managed_role_ids = {role.id for role in managed_roles}
    role_by_id = {role.id: role for role in managed_roles}

    changed_lines = []
    error_lines = []
    added_total = 0
    removed_total = 0
    unchanged = 0
    for member in target_members.values():
        desired_ids = set()
        source_member = None
        source_team = None
        if member.id in main_ids:
            source_team = "main"
            source_member = main_members[member.id]
            desired_ids.add(group_roles["main"].id)
        elif member.id in second_only_ids:
            source_team = "second"
            source_member = second_members[member.id]
            desired_ids.add(group_roles["second"].id)
        if source_member is not None and source_team is not None:
            source_role_ids = {role.id for role in source_member.roles}
            for role_slug, target_role in target_direction_roles.items():
                if source_direction_roles[source_team][role_slug].id in source_role_ids:
                    desired_ids.add(target_role.id)

        current_ids = {role.id for role in member.roles}
        add_ids = desired_ids - current_ids
        remove_ids = (current_ids & managed_role_ids) - desired_ids
        if not add_ids and not remove_ids:
            unchanged += 1
            continue
        add_roles = [role_by_id[role_id] for role_id in sorted(add_ids)]
        remove_roles = [role_by_id[role_id] for role_id in sorted(remove_ids)]
        if add_roles:
            try:
                await member.add_roles(*add_roles, reason="Синхронизация ролей CL Team")
            except Exception as e:
                error_lines.append(f"• {sync_member_name(member)} — роли не выданы: {short_error(e)}")
                continue
            added_total += len(add_roles)
        if remove_roles:
            try:
                await member.remove_roles(*remove_roles, reason="Синхронизация ролей CL Team")
            except Exception as e:
                error_lines.append(f"• {sync_member_name(member)} — роли не сняты: {short_error(e)}")
                remove_roles = []
            else:
                removed_total += len(remove_roles)
        changes = []
        if add_roles:
            changes.append("выданы: " + ", ".join(role.name for role in add_roles))
        if remove_roles:
            changes.append("сняты: " + ", ".join(role.name for role in remove_roles))
        if changes:
            changed_lines.append(f"• {sync_member_name(member)} — {'; '.join(changes)}")

    missing_lines = []
    for member_id in missing_in_community:
        source_member = main_members.get(member_id) or second_members.get(member_id)
        missing_lines.append(f"• {sync_member_name(source_member)}")

    now = dt.datetime.now(TZ).strftime("%d.%m.%Y %H:%M")
    report_lines = [
        f"Синхронизация ролей CL Team — {now}",
        f"Community: {target_guild.name}",
        f"Мейн-приватка: {source_guilds['main'].name} — {len(main_ids)} участников",
        f"Вторая приватка: {source_guilds['second'].name} — {len(second_ids)} участников",
        f"В обеих приватках (приоритет Мейн): {len(both_ids)}",
        f"Выдано ролей: {added_total}",
        f"Снято ролей: {removed_total}",
        f"Без изменений: {unchanged}",
        "",
        "Изменения:",
        *(changed_lines or ["• Нет"]),
        "",
        "Есть в приватке, но отсутствуют в Community:",
        *(missing_lines or ["• Нет"]),
        "",
        "Ошибки:",
        *(error_lines or ["• Нет"]),
    ]
    summary = (f"Выдано ролей: {added_total}, снято: {removed_total}, "
               f"ошибок: {len(error_lines)}, нет в Community: {len(missing_lines)}.")
    return "\n".join(report_lines), summary


async def send_ticket_sync_report(client: discord.Client, owner_id: int, report: str) -> None:
    owner = client.get_user(owner_id)
    if owner is None:
        owner = await client.fetch_user(owner_id)
    if len(report) <= 1900:
        await owner.send(report, allowed_mentions=NO_PINGS)
        return
    stamp = dt.datetime.now(TZ).strftime("%Y%m%d-%H%M")
    await owner.send(
        "Синхронизация завершена. Полный отчёт прикреплён файлом.",
        file=discord.File(io.BytesIO(report.encode("utf-8")), filename=f"role-sync-{stamp}.txt"),
    )


class TicketRoleSyncView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Синхронизировать составы", style=discord.ButtonStyle.primary,
                       custom_id="ticket:roles:sync")
    async def sync_roles(self, interaction: discord.Interaction, button: discord.ui.Button):
        owner_id = cfg_id("ticket_sync_report_owner")
        if not owner_id or interaction.user.id != owner_id:
            await interaction.response.send_message("Эта кнопка доступна только владельцу панели.", ephemeral=True)
            return
        perms = getattr(interaction.user, "guild_permissions", None)
        if not perms or not perms.administrator:
            await interaction.response.send_message("Нужно право «Администратор».", ephemeral=True)
            return
        if interaction.guild is None:
            await interaction.response.send_message("Синхронизация работает только на сервере.", ephemeral=True)
            return
        if _ticket_role_sync_lock.locked():
            await interaction.response.send_message("Синхронизация уже выполняется.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        try:
            async with _ticket_role_sync_lock:
                report, summary = await synchronize_ticket_roles(interaction.client, interaction.guild)
        except Exception as e:
            detail = str(e) if isinstance(e, RuntimeError) else short_error(e)
            await interaction.edit_original_response(content=f"Синхронизация не выполнена: {detail}"[:1900])
            return
        try:
            await send_ticket_sync_report(interaction.client, owner_id, report)
        except Exception as e:
            await interaction.edit_original_response(
                content=f"Роли синхронизированы, но не получилось отправить отчёт в личку: {short_error(e)}. {summary}"
            )
            return
        await interaction.edit_original_response(content=f"Готово. Отчёт отправлен тебе в личку. {summary}")


def main_voice_owner_id() -> int:
    return cfg_id("main_voice_report_owner") or cfg_id("ticket_sync_report_owner")


def main_voice_latest_boundary(timestamp: float) -> float:
    local_now = dt.datetime.fromtimestamp(timestamp, MAIN_VOICE_TZ)
    boundary = local_now.replace(hour=MAIN_VOICE_REPORT_HOUR, minute=0, second=0, microsecond=0)
    if local_now < boundary:
        boundary -= dt.timedelta(days=1)
    return boundary.timestamp()


def main_voice_next_boundary(timestamp: float) -> float:
    latest = main_voice_latest_boundary(timestamp)
    if timestamp - latest < 60:
        return latest
    return latest + 86400


def main_voice_guild(client: discord.Client) -> discord.Guild | None:
    guild_id = cfg_id(ticket_sync_source_guild_key("main"))
    return client.get_guild(guild_id) if guild_id else None


def current_main_voice_members(guild: discord.Guild) -> dict[str, discord.Member]:
    return {
        str(member.id): member
        for channel in guild.voice_channels
        for member in channel.members
        if not member.bot
    }


async def send_pending_main_voice_report(client: discord.Client):
    pending = store.main_voice_pending
    if not pending:
        return
    owner_id = int(pending.get("owner_id") or 0)
    chunks = list(pending.get("chunks") or [])
    next_chunk = int(pending.get("next_chunk") or 0)
    if not owner_id or next_chunk >= len(chunks):
        store.main_voice_pending = {}
        store.save()
        return
    owner = client.get_user(owner_id)
    if owner is None:
        owner = await client.fetch_user(owner_id)
    while next_chunk < len(chunks):
        await owner.send(chunks[next_chunk], allowed_mentions=NO_PINGS)
        next_chunk += 1
        store.main_voice_pending["next_chunk"] = next_chunk
        store.save()
    store.main_voice_pending = {}
    store.save()


async def build_main_voice_report(guild: discord.Guild, totals: dict[str, float],
                                  window_start: float, window_end: float) -> list[str]:
    members = await fetch_all_members(guild)
    rows = [(member, totals.get(str(member.id), 0.0) / 3600) for member in members.values()]
    missed = sorted((row for row in rows if row[1] < MAIN_VOICE_NORM_HOURS), key=lambda row: row[1])
    completed = sorted((row for row in rows if row[1] >= MAIN_VOICE_NORM_HOURS),
                       key=lambda row: row[1], reverse=True)
    start_text = dt.datetime.fromtimestamp(window_start, MAIN_VOICE_TZ).strftime("%d.%m %H:%M")
    end_text = dt.datetime.fromtimestamp(window_end, MAIN_VOICE_TZ).strftime("%d.%m %H:%M")
    lines = [
        f"**Отчёт Мейн-приватки · {start_text} — {end_text} МСК**",
        f"Норма: **{MAIN_VOICE_NORM_HOURS:g} часов в сутки**",
        f"Выполнили: **{len(completed)}/{len(rows)}**",
        "",
        f"**❌ Не выполнили — {len(missed)}**",
    ]
    lines.extend(
        f"• {member.mention} (`{member.id}`) — **{hours:.1f} ч**"
        for member, hours in missed
    )
    lines.extend(["", f"**✅ Выполнили — {len(completed)}**"])
    lines.extend(
        f"• {member.mention} (`{member.id}`) — **{hours:.1f} ч**"
        for member, hours in completed
    )
    return chunk_lines(lines)


async def process_main_voice_tracking(client: discord.Client):
    owner_id = main_voice_owner_id()
    guild = main_voice_guild(client)
    if not owner_id or guild is None or not MEMBERS_OK:
        return

    now = time.time()
    if store.main_voice_window_start <= 0:
        store.main_voice_window_start = main_voice_next_boundary(now)
        store.main_voice_seconds = {}
        store.main_voice_active = {}
        store.save()

    if store.main_voice_pending:
        await send_pending_main_voice_report(client)

    window_start = store.main_voice_window_start
    if now < window_start:
        return

    voice_members = current_main_voice_members(guild)
    window_end = window_start + 86400
    if now >= window_end:
        totals = dict(store.main_voice_seconds)
        for uid, started in store.main_voice_active.items():
            totals[uid] = totals.get(uid, 0.0) + max(0.0, window_end - max(started, window_start))

        chunks = await build_main_voice_report(guild, totals, window_start, window_end)
        new_start = main_voice_latest_boundary(now)
        store.main_voice_window_start = new_start
        store.main_voice_seconds = {}
        store.main_voice_active = {
            uid: max(now, new_start) for uid in voice_members
        }
        store.main_voice_pending = {
            "owner_id": owner_id,
            "chunks": chunks,
            "next_chunk": 0,
        }
        store.save()
        await send_pending_main_voice_report(client)
        return

    changed = False
    cutoff = min(now, max(window_start, store.heartbeat))
    for uid, started in list(store.main_voice_active.items()):
        if uid not in voice_members:
            store.main_voice_seconds[uid] = (
                store.main_voice_seconds.get(uid, 0.0) + max(0.0, cutoff - started)
            )
            del store.main_voice_active[uid]
            changed = True
    for uid in voice_members:
        if uid not in store.main_voice_active:
            store.main_voice_active[uid] = now
            changed = True
    if changed:
        store.save()


# ---------- бот ----------
class Bot(discord.Client):
    def __init__(self):
        intents = discord.Intents.default()  # voice_states входит в default
        intents.message_content = MESSAGE_CONTENT_OK  # нужен для полной переписки в логах тикетов
        intents.members = MEMBERS_OK  # список участников: кто с ролью клана не заполнил анкету
        super().__init__(intents=intents)
        self.tree = app_commands.CommandTree(self)
        self.guild_commands_synced = False

    async def setup_hook(self):
        self.add_view(PanelView())
        self.add_view(WipeView())
        self.add_view(StatusView())
        self.add_view(AdminView())
        self.add_view(TicketCloseView())
        self.add_view(TicketApplicationCloseView())
        self.add_view(TicketRoleSyncView())
        if store.private_invites:
            store.private_invites = {}
            store.save()
        for kind in TICKET_TYPES:
            self.add_view(TicketPanelView(kind))
        for event in store.wipe_events.values():
            if event.get("status") == "open" and event.get("message_id"):
                self.add_view(WipeRsvpView(event["id"]), message_id=int(event["message_id"]))
        await self.tree.sync()
        self.wipe_tick.start()

    async def on_ready(self):
        if not self.guild_commands_synced:
            sync_ok = True
            for guild in self.guilds:
                try:
                    self.tree.copy_global_to(guild=guild)
                    await self.tree.sync(guild=guild)
                except Exception as e:
                    sync_ok = False
                    print(f"Не удалось синхронизировать команды на сервере {guild.id}: {short_error(e)}", flush=True)
            self.guild_commands_synced = sync_ok
        try:
            await process_main_voice_tracking(self)
        except Exception as e:
            print(f"Ошибка учёта войса Мейн-приватки при запуске: {short_error(e)}", flush=True)
        in_voice = {str(m.id) for g in self.guilds for vc in g.voice_channels for m in vc.members if not m.bot}
        store.reconcile(in_voice)
        reconcile_vis_voice(self)
        try:
            await import_wipes_from_sheet()
        except Exception as e:
            print(f"Не удалось прочитать вайпы из таблицы: {e}", flush=True)
        print(f"Бот запущен как {self.user}", flush=True)

    async def on_guild_join(self, guild: discord.Guild):
        """Сразу добавить slash-команды, когда бота пригласили в новую приватку."""
        try:
            self.tree.copy_global_to(guild=guild)
            await self.tree.sync(guild=guild)
        except Exception as e:
            print(f"Не удалось синхронизировать команды на новом сервере {guild.id}: {short_error(e)}", flush=True)

    async def on_member_join(self, member: discord.Member):
        if member.bot:
            return
        await apply_pending_private_roles(member)
        try:
            roster = moderator_roster(member.guild.id)
            if roster is not None and member_has_roster_moderator_role(member, roster):
                await synchronize_moderator_roster(member.guild)
            else:
                await auto_assign_moderator_player(member)
        except Exception as e:
            print(f"Не удалось закрепить нового игрока за модератором: {short_error(e)}", flush=True)

    async def on_member_remove(self, member: discord.Member):
        if member.bot:
            return
        try:
            roster = moderator_roster(member.guild.id)
            if roster is not None and member_has_roster_moderator_role(member, roster):
                await synchronize_moderator_roster(member.guild)
            else:
                await remove_departed_moderator_player(member)
        except Exception as e:
            print(f"Не удалось убрать вышедшего игрока из блока модератора: {short_error(e)}", flush=True)

    async def on_member_update(self, before: discord.Member, after: discord.Member):
        if after.bot or before.roles == after.roles:
            return
        roster = moderator_roster(after.guild.id)
        if roster is None:
            return
        was_moderator = member_has_roster_moderator_role(before, roster)
        is_moderator = member_has_roster_moderator_role(after, roster)
        if was_moderator != is_moderator:
            try:
                await synchronize_moderator_roster(after.guild)
            except Exception as e:
                print(f"Не удалось обновить список модераторов: {short_error(e)}", flush=True)
            return
        was_excluded = member_is_moderator_excluded(before, roster)
        is_excluded = member_is_moderator_excluded(after, roster)
        if was_excluded == is_excluded:
            return
        try:
            if is_excluded:
                async with moderator_roster_lock(after.guild.id):
                    number = find_moderator_player_block(roster, after.id)
                    if number is None:
                        return
                    block = roster["blocks"][str(number)]
                    block["player_ids"] = [
                        uid for uid in block.get("player_ids", []) if int(uid) != after.id
                    ]
                    store.save()
                    await update_moderator_block(after.guild, number, roster)
            else:
                await auto_assign_moderator_player(after)
        except Exception as e:
            print(f"Не удалось применить исключение модераторов: {short_error(e)}", flush=True)

    async def on_raw_message_delete(self, payload: discord.RawMessageDeleteEvent):
        removed = [
            event_id for event_id, event in store.wipe_events.items()
            if int(event.get("guild_id", 0)) == int(payload.guild_id or 0)
            and int(event.get("channel_id", 0)) == payload.channel_id
            and int(event.get("message_id", 0)) == payload.message_id
        ]
        for event_id in removed:
            store.wipe_events.pop(event_id, None)
        if removed:
            store.save()
            print(f"Удалена карточка сбора, очищено событий: {len(removed)}", flush=True)

    async def on_invite_delete(self, invite):
        """Логировать удаление инвайта в закрытый канал; исполнитель берётся из Audit Log."""
        channel_id = INVITE_LOG_CHANNEL_ID
        guild = getattr(invite, "guild", None)
        if not channel_id or guild is None:
            return

        code = getattr(invite, "code", None) or "неизвестный код"
        executor = None
        # Discord может задержать запись в Audit Log относительно gateway-события.
        for attempt in range(5):
            try:
                async for entry in guild.audit_logs(
                    limit=10, action=discord.AuditLogAction.invite_delete
                ):
                    target = getattr(entry, "target", None)
                    target_code = getattr(target, "code", None)
                    if target_code == code and (dt.datetime.now(dt.timezone.utc) - entry.created_at).total_seconds() <= 20:
                        executor = entry.user
                        break
                if executor is not None:
                    break
            except discord.Forbidden:
                print("Не могу читать Audit Log для удаления инвайтов: нет права «Просмотр журнала аудита».", flush=True)
                break
            except Exception as e:
                print(f"Ошибка чтения Audit Log для invite delete: {short_error(e)}", flush=True)
            if attempt < 4:
                await asyncio.sleep(1)

        channel = self.get_channel(channel_id)
        if channel is None:
            try:
                channel = await self.fetch_channel(channel_id)
            except Exception as e:
                print(f"Не найден канал лога инвайтов {channel_id}: {short_error(e)}", flush=True)
                return

        # Не отправлять события других серверов в канал лога CL Community.
        channel_guild = getattr(channel, "guild", None)
        if channel_guild is None or channel_guild.id != guild.id:
            return

        if executor is not None:
            text = f"🛡️ Модератор **{discord.utils.escape_markdown(executor.display_name)}** удалил ссылку-приглашение `{code}`."
        else:
            text = f"⚠️ Ссылка-приглашение `{code}` удалена/истекла, но Discord не предоставил исполнителя в Audit Log."
        try:
            await channel.send(text, allowed_mentions=NO_PINGS)
        except Exception as e:
            print(f"Не удалось отправить лог удаления инвайта: {short_error(e)}", flush=True)

    async def on_voice_state_update(self, member, before, after):
        if member.bot:
            return
        uid = str(member.id)
        if before.channel is None and after.channel is not None:
            store.voice_start(uid)
        elif before.channel is not None and after.channel is None:
            store.voice_stop(uid)

        if before.channel is None and after.channel is not None:
            vis_voice_start(member.guild.id, member.id)
        elif before.channel is not None and after.channel is None:
            vis_voice_stop(member.guild.id, member.id)

        main_guild_id = cfg_id(ticket_sync_source_guild_key("main"))
        if main_guild_id and member.guild.id == main_guild_id:
            if before.channel is None and after.channel is not None:
                store.main_voice_start(uid)
            elif before.channel is not None and after.channel is None:
                store.main_voice_stop(uid)

    async def on_message(self, message):
        if message.author.bot:
            return
        if message.guild is not None:
            cfg = vis_config(message.guild.id)
            if cfg is not None:
                if message.channel.id == int(cfg.get("farm_channel_id") or 0):
                    asyncio.create_task(process_vis_farm_message(message))
                elif message.channel.id in [int(cid) for cid in cfg.get("profile_channel_ids", [])]:
                    asyncio.create_task(process_vis_profile_message(message))
        if not use_editor() or message.channel.id != editor_channel_id():
            return
        try:
            await handle_editor_message(self, message)
        except Exception as e:
            print(f"Ошибка редактора: {short_error(e)}", flush=True)

    async def on_raw_reaction_add(self, payload):
        await apply_check_reaction(self, payload, added=True)
        if use_editor() and payload.channel_id == editor_channel_id():
            await apply_draft_reaction(self, payload)

    async def on_raw_reaction_remove(self, payload):
        await apply_check_reaction(self, payload, added=False)

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
        try:
            await process_wipe_event_reminders(self)
        except Exception as e:
            print(f"Ошибка напоминаний о сборах: {short_error(e)}", flush=True)
        try:
            await process_main_voice_tracking(self)
        except Exception as e:
            print(f"Ошибка ежедневного отчёта войса Мейн-приватки: {short_error(e)}", flush=True)
        try:
            await process_scheduled_moderator_rosters(self)
        except Exception as e:
            print(f"Ошибка автосверки модераторов: {short_error(e)}", flush=True)
        try:
            await process_hourly_vis_hubs(self)
        except Exception as e:
            print(f"Ошибка почасового VIS: {short_error(e)}", flush=True)

    @wipe_tick.before_loop
    async def _wait(self):
        await self.wait_until_ready()


bot = Bot()


async def admin_only(interaction: discord.Interaction) -> bool:
    if not is_admin(interaction):
        await interaction.response.send_message("Только для админов.", ephemeral=True)
        return False
    return True


TEXT_PROFILE = "Без заполнения в клан попасть нельзя."
TEXT_WIPE = ("**Вайп**\nАдмин нажимает «Вайп» и указывает название, дату и время начала и конца. "
             "В момент старта бот отметит, кто пришёл в голосовой канал и кто опоздал, "
             "а по окончании выложит статистику: кто сколько наиграл.")
TEXT_STATUS = "**Статус**\nНе можешь играть? Нажми инактив. Отошёл на время - нажми афк"
TEXT_ADMIN = "**Админ-панель**\nКто сейчас должен играть, кто в АФК и кто в инактиве."


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


@bot.tree.command(name="wipe", description="Создать сбор на вайп с голосованием")
@app_commands.describe(
    name="Название события",
    description="Описание сбора",
    event_date="Дата сбора: ДД.ММ.ГГГГ",
    event_time="Время сбора: ЧЧ:ММ по Москве",
)
@app_commands.default_permissions(manage_guild=True)
async def wipe_event(interaction: discord.Interaction, name: str, description: str,
                     event_date: str, event_time: str):
    if not await admin_only(interaction):
        return
    if interaction.guild is None or interaction.channel is None:
        await interaction.response.send_message("Команда работает только на сервере.", ephemeral=True)
        return
    try:
        start = wipe_event_datetime(event_date, event_time)
    except ValueError:
        await interaction.response.send_message(
            "Не разобрал дату или время. Пример: `дата: 10.11.2026`, `время: 19:30`.",
            ephemeral=True,
        )
        return
    if start <= time.time():
        await interaction.response.send_message("Дата и время сбора должны быть в будущем.", ephemeral=True)
        return
    if not name.strip() or len(name.strip()) > 100 or len(description.strip()) > 1000:
        await interaction.response.send_message(
            "Название должно быть до 100 символов, описание — до 1000.", ephemeral=True)
        return
    duplicate = False
    removed_deleted = False
    for stored_id, event in list(store.wipe_events.items()):
        if (
            int(event.get("guild_id", 0)) == interaction.guild.id
            and event.get("status") == "open"
            and event.get("name", "").casefold() == name.strip().casefold()
        ):
            if await wipe_event_card_exists(interaction.client, event):
                duplicate = True
                break
            del store.wipe_events[stored_id]
            removed_deleted = True
    if removed_deleted:
        store.save()
    if duplicate:
        await interaction.response.send_message(
            "Открытый сбор с таким названием уже существует.", ephemeral=True
        )
        return
    if not MEMBERS_OK:
        await interaction.response.send_message(
            "Не включён Server Members Intent — бот не сможет разослать событие всем участникам.",
            ephemeral=True,
        )
        return

    await interaction.response.defer(ephemeral=True)
    try:
        participants = [member async for member in interaction.guild.fetch_members(limit=None)
                        if not member.bot]
    except Exception as e:
        await interaction.followup.send(
            f"Не удалось получить список участников: {short_error(e)}", ephemeral=True)
        return
    if not participants:
        await interaction.followup.send("На сервере не найдено участников для голосования.", ephemeral=True)
        return

    event_id = f"{interaction.guild.id:x}-{int(time.time()):x}"
    event = {
        "id": event_id,
        "guild_id": interaction.guild.id,
        "channel_id": interaction.channel_id,
        "message_id": 0,
        "name": name.strip(),
        "description": description.strip(),
        "start": start,
        "created": time.time(),
        "created_by": interaction.user.id,
        "participants": [member.id for member in participants],
        "votes": {},
        "status": "open",
        "start_notified": [],
        "start_notifications_done": False,
        "final_sent": False,
        "last_reminder_date": dt.datetime.now(TZ).strftime("%Y-%m-%d"),
    }
    try:
        message = await interaction.channel.send(
            embed=wipe_event_embed(event),
            view=WipeRsvpView(event_id),
            allowed_mentions=NO_PINGS,
        )
    except Exception as e:
        await interaction.followup.send(
            f"Не удалось создать карточку события: {short_error(e)}", ephemeral=True)
        return
    event["message_id"] = message.id
    store.wipe_events[event_id] = event
    store.save()

    delivered = 0
    for member in participants:
        if await send_wipe_event_dm(member, event, message.jump_url):
            delivered += 1
        await asyncio.sleep(0.2)
    failed = len(participants) - delivered
    await interaction.followup.send(
        f"Сбор создан: {message.jump_url}\n"
        f"Участников: **{len(participants)}**. Личные сообщения доставлены: **{delivered}**. "
        f"Не доставлены: **{failed}**.",
        ephemeral=True,
    )


def guild_wipe_events(guild_id: int) -> list[dict]:
    return sorted(
        (event for event in store.wipe_events.values() if int(event.get("guild_id", 0)) == guild_id),
        key=lambda event: event.get("start", 0),
        reverse=True,
    )


@bot.tree.command(name="wipe_reasons", description="Посмотреть причины отсутствия и опоздания")
@app_commands.describe(event="Выбери сбор")
@app_commands.default_permissions(manage_guild=True)
async def wipe_reasons(interaction: discord.Interaction, event: str | None = None):
    if not await admin_only(interaction):
        return
    if interaction.guild is None:
        await interaction.response.send_message("Команда работает только на сервере.", ephemeral=True)
        return
    events = guild_wipe_events(interaction.guild.id)
    selected = store.wipe_events.get(event) if event else (events[0] if events else None)
    if selected is None or int(selected.get("guild_id", 0)) != interaction.guild.id:
        await interaction.response.send_message("Сбор не найден.", ephemeral=True)
        return

    lines = [f"**Причины · {selected['name']}**", f"Сбор: <t:{int(selected['start'])}:F>", ""]
    reason_votes = [(uid, vote) for uid, vote in selected.get("votes", {}).items()
                    if vote.get("status") in ("no", "late")]
    if not reason_votes:
        lines.append("Причин отсутствия или опоздания пока нет.")
    else:
        for uid, vote in reason_votes:
            label = f"{WIPE_RSVP_EMOJIS[vote['status']]} {WIPE_RSVP_LABELS[vote['status']]}"
            reason = discord.utils.escape_markdown(vote.get("reason") or "Причина не указана")
            lines.append(f"• <@{uid}> — `{uid}` · {label}\n  Причина: {reason}")
    await interaction.response.defer(ephemeral=True)
    for chunk in chunk_lines(lines):
        await interaction.followup.send(chunk, ephemeral=True, allowed_mentions=NO_PINGS)


@bot.tree.command(name="wipe_pending", description="Посмотреть, кто не проголосовал за сбор")
@app_commands.describe(event="Выбери сбор")
@app_commands.default_permissions(manage_guild=True)
async def wipe_pending(interaction: discord.Interaction, event: str | None = None):
    if not await admin_only(interaction):
        return
    if interaction.guild is None:
        await interaction.response.send_message("Команда работает только на сервере.", ephemeral=True)
        return
    events = guild_wipe_events(interaction.guild.id)
    selected = store.wipe_events.get(event) if event else (events[0] if events else None)
    if selected is None or int(selected.get("guild_id", 0)) != interaction.guild.id:
        await interaction.response.send_message("Сбор не найден.", ephemeral=True)
        return
    pending_ids = wipe_event_pending_ids(selected)
    lines = [
        f"**Не проголосовали · {discord.utils.escape_markdown(selected['name'])}**",
        f"Сбор: <t:{int(selected['start'])}:F>",
        "",
    ]
    lines.extend((f"• <@{user_id}> — `{user_id}`"
                  for user_id in pending_ids))
    if not pending_ids:
        lines.append("Все участники уже ответили.")
    await interaction.response.defer(ephemeral=True)
    for chunk in chunk_lines(lines):
        await interaction.followup.send(chunk, ephemeral=True, allowed_mentions=NO_PINGS)


@wipe_reasons.autocomplete("event")
@wipe_pending.autocomplete("event")
async def wipe_reasons_autocomplete(interaction: discord.Interaction, current: str):
    if interaction.guild is None:
        return []
    current_folded = current.casefold()
    choices = []
    for event in guild_wipe_events(interaction.guild.id):
        if current_folded and current_folded not in event.get("name", "").casefold():
            continue
        date = dt.datetime.fromtimestamp(event["start"], TZ).strftime("%d.%m %H:%M")
        choices.append(app_commands.Choice(name=f"{event['name']} · {date}"[:100], value=event["id"]))
        if len(choices) >= 25:
            break
    return choices


@bot.tree.command(name="status_panel", description="Выложить кнопки «Инактив» и «АФК» (для админов)")
@app_commands.default_permissions(manage_guild=True)
async def status_panel(interaction: discord.Interaction):
    if not await admin_only(interaction):
        return
    await interaction.channel.send(
        "**Статус**\nНе можешь играть? Нажми инактив. Отошёл на время - нажми афк",
        view=StatusView(),
    )
    await interaction.response.send_message("Кнопки выложены.", ephemeral=True)


async def send_ticket_panel(interaction: discord.Interaction, kind: str) -> None:
    if not await admin_only(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    settings = TICKET_TYPES[kind]
    embed = discord.Embed(
        title=f"{settings['emoji']} {settings['panel_title']}",
        description=settings["description"],
        colour=settings["color"],
    )
    embed.set_author(name="CL TEAM • ПОДАТЬ ЗАЯВКУ" if kind != "support" else "CL TEAM • ПОДДЕРЖКА")
    embed.set_footer(text="CL Team")
    try:
        await interaction.channel.send(
            embed=embed,
            view=TicketPanelView(kind),
        )
    except discord.Forbidden:
        await interaction.followup.send(
            "Не могу отправить панель в этот канал. Проверь права бота.", ephemeral=True)
        return
    except Exception as e:
        print(f"Не удалось выложить панель {kind}: {short_error(e)}", flush=True)
        await interaction.followup.send(
            f"Не получилось выложить панель: {short_error(e)}", ephemeral=True)
        return
    await interaction.followup.send("Панель выложена.", ephemeral=True)


@bot.tree.command(name="main_panel", description="Выложить панель анкеты в Main состав")
@app_commands.default_permissions(manage_guild=True)
async def main_panel(interaction: discord.Interaction):
    await send_ticket_panel(interaction, "main")


@bot.tree.command(name="second_panel", description="Выложить панель анкеты во второй состав")
@app_commands.default_permissions(manage_guild=True)
async def second_panel(interaction: discord.Interaction):
    await send_ticket_panel(interaction, "second")


@bot.tree.command(name="support_panel", description="Выложить панель поддержки")
@app_commands.default_permissions(manage_guild=True)
async def support_panel(interaction: discord.Interaction):
    await send_ticket_panel(interaction, "support")


@bot.tree.command(name="ticket_logs_setup", description="Создать два закрытых канала логов тикетов")
@app_commands.default_permissions(manage_guild=True)
async def ticket_logs_setup(interaction: discord.Interaction):
    if not await admin_only(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    try:
        channels = await ensure_ticket_log_channels(
            interaction.guild, getattr(interaction.channel, "category", None))
    except Exception as e:
        await interaction.followup.send(
            f"Не получилось создать каналы логов: {short_error(e)}. Нужны права «Управлять каналами».",
            ephemeral=True)
        return
    await interaction.followup.send(
        f"Готово: {channels[0].mention} и {channels[1].mention}.", ephemeral=True)


@bot.tree.command(name="ticket_roles", description="Настроить роли анкет или сопоставление ролей приватки")
@app_commands.describe(
    team="В приватке выбери, к какому составу она относится",
    coller="Роль коллера", combat="Роль комбата", fpv="Роль FPV-пилота",
    pilot="Роль пилота", crafter="Роль крафтера", electric="Роль электрика",
    farmer="Роль фермера", builder="Роль билдера",
    invite_channel="Канал, на который бот создаёт приглашение в приватку",
    member_role="Общая роль приватки (автовыдача временно отключена)",
    main_group="Дополнительная роль Мейн состава",
    second_group="Дополнительная роль второго состава",
    inappropriate="Роль «Неадекватный», выдаваемая при такой причине закрытия",
)
@app_commands.choices(team=[
    app_commands.Choice(name="Мейн", value="main"),
    app_commands.Choice(name="Второй состав", value="second"),
])
@app_commands.default_permissions(manage_guild=True)
async def ticket_roles(interaction: discord.Interaction, team: str | None = None,
                       coller: discord.Role | None = None,
                       combat: discord.Role | None = None, fpv: discord.Role | None = None,
                       pilot: discord.Role | None = None, crafter: discord.Role | None = None,
                       electric: discord.Role | None = None, farmer: discord.Role | None = None,
                       builder: discord.Role | None = None,
                       invite_channel: discord.TextChannel | None = None,
                       member_role: discord.Role | None = None,
                       main_group: discord.Role | None = None,
                       second_group: discord.Role | None = None,
                       inappropriate: discord.Role | None = None):
    if not await admin_only(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    direction_supplied = {
        "coller": coller,
        "combat": combat,
        "fpv": fpv,
        "pilot": pilot,
        "crafter": crafter,
        "electric": electric,
        "farmer": farmer,
        "builder": builder,
    }

    if team in TICKET_SYNC_TEAM_LABELS:
        store.cfg[ticket_sync_source_guild_key(team)] = interaction.guild.id
        changed = []
        for role_slug, role in direction_supplied.items():
            if role is not None:
                store.cfg[ticket_sync_source_role_key(team, role_slug)] = role.id
                changed.append(role)
        if invite_channel is not None:
            store.cfg[ticket_private_invite_channel_key(team)] = invite_channel.id
        if member_role is not None:
            store.cfg[ticket_private_member_role_key(team)] = member_role.id
            changed.append(member_role)
        store.save()
        lines = [f"**Роли приватки · {TICKET_SYNC_TEAM_LABELS[team]}:**"]
        for role_slug, label in TICKET_ROLE_LABELS.items():
            role = interaction.guild.get_role(cfg_id(ticket_sync_source_role_key(team, role_slug)))
            if role is not None:
                lines.append(f"• {label}: {role.mention}")
        configured_member_role = interaction.guild.get_role(cfg_id(ticket_private_member_role_key(team)))
        configured_invite_channel = interaction.guild.get_channel(cfg_id(ticket_private_invite_channel_key(team)))
        if configured_member_role is not None:
            lines.append(f"• Общая роль приватки: {configured_member_role.mention}")
        if isinstance(configured_invite_channel, discord.TextChannel):
            lines.append(f"• Канал приглашения: {configured_invite_channel.mention}")
        if len(lines) == 1:
            lines.append("Настроенных ролей пока нет.")
        if main_group is not None or second_group is not None or inappropriate is not None:
            lines.append(
                "Роли составов и «Неадекватный» настраиваются через "
                "`/ticket_roles` только в Community."
            )
        saved = bool(changed or invite_channel is not None)
        lines.insert(0, "Приватка сохранена.\n" if saved else "Приватка выбрана.\n")
    else:
        if invite_channel is not None or member_role is not None:
            await interaction.followup.send(
                "`invite_channel` и `member_role` настраиваются только в приватке вместе с параметром `team`.",
                ephemeral=True,
            )
            return
        supplied = {
            **{TICKET_ROLE_CONFIG_KEYS[role_slug]: role for role_slug, role in direction_supplied.items()},
            "ticket_group_main": main_group,
            "ticket_group_second": second_group,
            "ticket_inappropriate_role": inappropriate,
        }
        changed = [role for role in supplied.values() if role is not None]
        for key, role in supplied.items():
            if role is not None:
                store.cfg[key] = role.id
        if changed:
            store.save()
        labels = {
            **{TICKET_ROLE_CONFIG_KEYS[role_slug]: label for role_slug, label in TICKET_ROLE_LABELS.items()},
            "ticket_group_main": "Доп. роль Мейн",
            "ticket_group_second": "Доп. роль второго состава",
            "ticket_inappropriate_role": "Неадекватный",
        }
        lines = ["**Роли для принятых анкет:**"]
        for key, label in labels.items():
            role = interaction.guild.get_role(cfg_id(key))
            if role is not None:
                lines.append(f"• {label}: {role.mention}")
        if len(lines) == 1:
            lines.append("Настроенных ролей пока нет.")
        if changed:
            lines.insert(0, "Настройки сохранены.\n")
    await interaction.followup.send("\n".join(lines), ephemeral=True, allowed_mentions=NO_PINGS)



async def build_ticket_sync_check_report(client: discord.Client) -> tuple[str, int]:
    """Проверить всю конфигурацию синхронизации без изменения ролей."""
    lines = ["**Проверка синхронизации составов**", ""]
    problems = 0

    def add(ok: bool, text: str):
        nonlocal problems
        lines.append(("✅ " if ok else "❌ ") + text)
        if not ok:
            problems += 1

    add(MEMBERS_OK, "Server Members Intent включён." if MEMBERS_OK else
        "Server Members Intent выключен в Discord Developer Portal.")

    target_id = cfg_id("ticket_sync_target_guild")
    target = client.get_guild(target_id) if target_id else None
    add(bool(target_id), "Community сохранён в настройках." if target_id else
        "Community не настроен: запусти /ticket_sync_panel в паблике.")
    if target_id:
        add(target is not None,
            f"Community: {target.name} ({target.id})." if target is not None else
            f"Бот не видит настроенный Community с ID {target_id}.")

    owner_id = cfg_id("ticket_sync_report_owner")
    add(bool(owner_id),
        f"Получатель отчётов настроен: <@{owner_id}>." if owner_id else
        "Получатель отчётов не настроен: заново запусти /ticket_sync_panel в паблике.")

    active_target_roles: dict[str, discord.Role] = {}
    if target is not None:
        lines += ["", "**Роли в Community**"]
        group_roles = {
            "main": target.get_role(cfg_id("ticket_group_main")),
            "second": target.get_role(cfg_id("ticket_group_second")),
        }
        for team, role in group_roles.items():
            add(role is not None,
                f"{TICKET_SYNC_TEAM_LABELS[team]}: {role.name} ({role.id})." if role else
                f"{TICKET_SYNC_TEAM_LABELS[team]}: роль состава не настроена.")

        for role_slug, key in TICKET_ROLE_CONFIG_KEYS.items():
            role_id = cfg_id(key)
            role = target.get_role(role_id) if role_id else None
            if role is not None:
                active_target_roles[role_slug] = role
                add(True, f"{TICKET_ROLE_LABELS[role_slug]}: {role.name} ({role.id}).")
            elif role_id:
                add(False, f"{TICKET_ROLE_LABELS[role_slug]}: настроенная роль удалена (ID {role_id}).")
            else:
                lines.append(f"➖ {TICKET_ROLE_LABELS[role_slug]}: не используется.")

        add(bool(active_target_roles),
            f"Направлений для синхронизации: {len(active_target_roles)}." if active_target_roles else
            "Не настроено ни одного направления в Community.")

        me = target.me
        add(me is not None and me.guild_permissions.manage_roles,
            "У бота есть право «Управлять ролями» в Community." if
            me is not None and me.guild_permissions.manage_roles else
            "У бота нет права «Управлять ролями» в Community.")
        if me is not None:
            controlled = [*group_roles.values(), *active_target_roles.values()]
            unavailable = [
                role.name for role in controlled
                if role is not None and (role.managed or role >= me.top_role)
            ]
            add(not unavailable,
                "Все настроенные роли находятся ниже роли бота." if not unavailable else
                "Бот не может управлять ролями: " + ", ".join(unavailable) + ".")

    source_ids = []
    for team in ("main", "second"):
        lines += ["", f"**Приватка · {TICKET_SYNC_TEAM_LABELS[team]}**"]
        source_id = cfg_id(ticket_sync_source_guild_key(team))
        source_ids.append(source_id)
        source = client.get_guild(source_id) if source_id else None
        add(bool(source_id),
            "Сервер сохранён в настройках." if source_id else
            f"Приватка не настроена: запусти в ней /ticket_roles team:{TICKET_SYNC_TEAM_LABELS[team]}.")
        if source_id:
            add(source is not None,
                f"Сервер: {source.name} ({source.id})." if source is not None else
                f"Бот не видит настроенную приватку с ID {source_id}.")
        if source is not None:
            for role_slug in active_target_roles:
                role_id = cfg_id(ticket_sync_source_role_key(team, role_slug))
                role = source.get_role(role_id) if role_id else None
                add(role is not None,
                    f"{TICKET_ROLE_LABELS[role_slug]} → {role.name} ({role.id})." if role else
                    f"{TICKET_ROLE_LABELS[role_slug]} → не сопоставлена роль в этой приватке.")

    configured_ids = [guild_id for guild_id in [target_id, *source_ids] if guild_id]
    if len(configured_ids) > 1:
        add(len(configured_ids) == len(set(configured_ids)),
            "Community и приватки настроены как разные серверы." if
            len(configured_ids) == len(set(configured_ids)) else
            "Один сервер ошибочно выбран сразу для нескольких назначений.")

    lines += ["", "**Итог**"]
    if problems:
        lines.append(f"❌ Не готово. Найдено проблем: **{problems}**.")
        lines += [
            "",
            "**Порядок настройки:**",
            "1. В паблике настрой роли через /ticket_roles.",
            "2. В Мейн-приватке запусти /ticket_roles с team:Мейн и сопоставь роли.",
            "3. Во второй приватке запусти /ticket_roles с team:Второй состав и сопоставь роли.",
            "4. В паблике заново запусти /ticket_sync_panel.",
            "5. Повтори /ticket_sync_check.",
        ]
    else:
        lines.append("✅ Всё необходимое настроено. Можно запускать синхронизацию.")
    return "\n".join(lines), problems


@bot.tree.command(name="ticket_sync_check", description="Проверить настройки синхронизации ролей")
@app_commands.default_permissions(administrator=True)
async def ticket_sync_check(interaction: discord.Interaction):
    if not await admin_only(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    report, problems = await build_ticket_sync_check_report(interaction.client)
    if len(report) <= 1900:
        await interaction.followup.send(report, ephemeral=True, allowed_mentions=NO_PINGS)
        return
    await interaction.followup.send(
        f"Проверка завершена. Найдено проблем: {problems}. Полный отчёт прикреплён.",
        file=discord.File(io.BytesIO(report.encode("utf-8")), filename="ticket-sync-check.txt"),
        ephemeral=True,
        allowed_mentions=NO_PINGS,
    )


@bot.tree.command(name="ticket_sync_reset", description="Сбросить только настройки синхронизации")
@app_commands.describe(
    scope="Текущий сервер или вся схема из паблика и двух приваток",
    confirm="Обязательно включи, чтобы подтвердить сброс",
)
@app_commands.choices(scope=[
    app_commands.Choice(name="Только текущий сервер", value="current"),
    app_commands.Choice(name="Вся синхронизация", value="all"),
])
@app_commands.default_permissions(administrator=True)
async def ticket_sync_reset(interaction: discord.Interaction,
                            scope: str = "current", confirm: bool = False):
    if not await admin_only(interaction):
        return
    if interaction.guild is None:
        await interaction.response.send_message("Команда работает только на сервере.", ephemeral=True)
        return
    if not confirm:
        await interaction.response.send_message(
            "Сброс отменён. Запусти команду ещё раз и укажи confirm: Да.",
            ephemeral=True,
        )
        return

    keys: set[str] = set()
    reset_parts = []
    guild_id = interaction.guild.id

    if scope == "all" or cfg_id("ticket_sync_target_guild") == guild_id:
        keys.update({"ticket_sync_target_guild", "ticket_sync_report_owner",
                     "ticket_group_main", "ticket_group_second"})
        keys.update(TICKET_ROLE_CONFIG_KEYS.values())
        reset_parts.append("Community")

    for team in ("main", "second"):
        if scope == "all" or cfg_id(ticket_sync_source_guild_key(team)) == guild_id:
            keys.add(ticket_sync_source_guild_key(team))
            keys.add(ticket_private_member_role_key(team))
            keys.update(ticket_sync_source_role_key(team, slug) for slug in TICKET_ROLE_LABELS)
            reset_parts.append(f"приватка «{TICKET_SYNC_TEAM_LABELS[team]}»")

    if not keys:
        await interaction.response.send_message(
            "Для этого сервера настройки синхронизации не найдены. "
            "Выбери scope: Вся синхронизация для полного сброса.",
            ephemeral=True,
        )
        return

    removed = [key for key in keys if key in store.cfg]
    for key in keys:
        store.cfg.pop(key, None)
    store.save()

    scope_text = ", ".join(dict.fromkeys(reset_parts))
    await interaction.response.send_message(
        f"Настройки сброшены: {scope_text}. Удалено значений: {len(removed)}.\n"
        "Тикеты, логи, вайп, роли доступа и каналы приглашений не затронуты.\n"
        "После повторной настройки запусти /ticket_sync_check.",
        ephemeral=True,
    )


@bot.tree.command(name="ticket_access", description="Разделить роли доступа к Мейн и Секонд тикетам")
@app_commands.describe(
    main_role="Роль, которая видит и закрывает только Мейн-тикеты",
    second_role="Роль, которая видит и закрывает только Секонд-тикеты",
    support_role="Необязательно: отдельная роль для тикетов поддержки",
)
@app_commands.default_permissions(administrator=True)
async def ticket_access(
    interaction: discord.Interaction,
    main_role: discord.Role,
    second_role: discord.Role,
    support_role: discord.Role | None = None,
):
    if not await admin_only(interaction):
        return
    if interaction.guild is None:
        await interaction.response.send_message("Команда работает только на сервере.", ephemeral=True)
        return
    roles = [role for role in (main_role, second_role, support_role) if role is not None]
    if any(role.is_default() for role in roles):
        await interaction.response.send_message("Роль @everyone использовать нельзя.", ephemeral=True)
        return
    if main_role.id == second_role.id:
        await interaction.response.send_message(
            "Для Мейн и Секонд нужно выбрать разные роли.", ephemeral=True
        )
        return

    await interaction.response.defer(ephemeral=True)
    old_role_ids = list(dict.fromkeys((
        staff_role_id(),
        *ticket_access_role_ids(),
        cfg_id("ticket_access_main_role"),
        cfg_id("ticket_access_second_role"),
        cfg_id("ticket_access_support_role"),
    )))
    store.cfg["ticket_access_main_role"] = main_role.id
    store.cfg["ticket_access_second_role"] = second_role.id
    if support_role is None:
        store.cfg.pop("ticket_access_support_role", None)
    else:
        store.cfg["ticket_access_support_role"] = support_role.id
    store.cfg.pop("ticket_access_role", None)
    store.cfg.pop("ticket_access_role_2", None)
    store.save()

    roles_by_kind = {
        "main": [main_role],
        "second": [second_role],
        "support": [support_role] if support_role is not None else [],
    }
    try:
        updated = await apply_ticket_access_roles(
            interaction.guild, old_role_ids, roles_by_kind
        )
    except discord.Forbidden:
        await interaction.followup.send(
            "Роли сохранены, но не удалось обновить открытые тикеты: "
            "боту нужно право «Управлять каналами».",
            ephemeral=True,
        )
        return
    except Exception as e:
        await interaction.followup.send(
            f"Роли сохранены, но не удалось обновить открытые тикеты: {short_error(e)}",
            ephemeral=True,
        )
        return

    support_text = (
        f" Поддержка: {support_role.mention}."
        if support_role is not None else
        " Тикеты поддержки доступны только администраторам."
    )
    await interaction.followup.send(
        f"Готово. Мейн: {main_role.mention}. Секонд: {second_role.mention}."
        f"{support_text} Обновлено открытых тикетов: {updated}.",
        ephemeral=True,
        allowed_mentions=NO_PINGS,
    )


@bot.tree.command(name="ticket_sync_panel", description="Выложить панель синхронизации ролей составов")
@app_commands.default_permissions(administrator=True)
async def ticket_sync_panel(interaction: discord.Interaction):
    perms = getattr(interaction.user, "guild_permissions", None)
    if interaction.guild is None or not perms or not perms.administrator:
        await interaction.response.send_message("Нужно право «Администратор».", ephemeral=True)
        return
    store.cfg["ticket_sync_target_guild"] = interaction.guild.id
    store.cfg["ticket_sync_report_owner"] = interaction.user.id
    store.save()
    embed = discord.Embed(
        title="Синхронизация составов",
        description=("Выдаёт и снимает настроенные роли Community по участникам двух приваток. "
                     "Если участник находится в обеих приватках, приоритет получает Мейн."),
        colour=0x5865F2,
    )
    embed.set_footer(text="Отчёт придёт владельцу панели в личные сообщения")
    await interaction.channel.send(embed=embed, view=TicketRoleSyncView())
    await interaction.response.send_message(
        "Панель выложена. Отчёты будут приходить тебе в личку.", ephemeral=True)


@bot.tree.command(name="main_voice_report", description="Назначить себе ежедневный отчёт войса Мейн-приватки")
@app_commands.default_permissions(administrator=True)
async def main_voice_report(interaction: discord.Interaction):
    main_guild = main_voice_guild(interaction.client)
    if main_guild is None:
        await interaction.response.send_message(
            "Мейн-приватка не настроена. Сначала запусти в ней `/ticket_roles team:Мейн`.",
            ephemeral=True,
        )
        return
    if interaction.guild is None or interaction.guild.id != main_guild.id:
        await interaction.response.send_message(
            f"Команду нужно запускать в Мейн-приватке «{main_guild.name}».",
            ephemeral=True,
        )
        return
    perms = getattr(interaction.user, "guild_permissions", None)
    if not perms or not perms.administrator:
        await interaction.response.send_message("Нужно право «Администратор».", ephemeral=True)
        return
    if not MEMBERS_OK:
        await interaction.response.send_message(
            "Включи Server Members Intent в Discord Developer Portal.", ephemeral=True
        )
        return

    store.cfg["main_voice_report_owner"] = interaction.user.id
    if store.main_voice_window_start <= 0:
        store.main_voice_window_start = main_voice_next_boundary(time.time())
        store.main_voice_seconds = {}
        store.main_voice_active = {}
    store.save()
    start_text = dt.datetime.fromtimestamp(
        store.main_voice_window_start, MAIN_VOICE_TZ
    ).strftime("%d.%m.%Y в %H:%M")
    await interaction.response.send_message(
        f"Готово. Учёт начнётся {start_text} МСК. "
        f"Каждые сутки в 14:00 я пришлю тебе личный отчёт. Норма — {MAIN_VOICE_NORM_HOURS:g} часов.",
        ephemeral=True,
    )


@bot.tree.command(name="mss", description="Отправить личное сообщение всем участникам сервера")
@app_commands.describe(text="Текст личного сообщения")
@app_commands.default_permissions(administrator=True)
async def mss(interaction: discord.Interaction, text: str):
    if interaction.guild is None:
        await interaction.response.send_message(
            "Команда работает только на сервере.", ephemeral=True
        )
        return
    perms = getattr(interaction.user, "guild_permissions", None)
    if not perms or not perms.administrator:
        await interaction.response.send_message(
            "Нужно право «Администратор».", ephemeral=True
        )
        return
    message_text = text.strip()
    if not message_text:
        await interaction.response.send_message(
            "Текст сообщения не может быть пустым.", ephemeral=True
        )
        return
    if len(message_text) > 1900:
        await interaction.response.send_message(
            "Сообщение слишком длинное. Максимум — 1900 символов.", ephemeral=True
        )
        return
    if not MEMBERS_OK:
        await interaction.response.send_message(
            "Бот не видит список участников. Включи Server Members Intent "
            "в Discord Developer Portal.",
            ephemeral=True,
        )
        return

    await interaction.response.defer(ephemeral=True)
    try:
        members = await fetch_all_members(interaction.guild)
    except Exception as e:
        await interaction.followup.send(
            f"Не удалось получить список участников: {short_error(e)}",
            ephemeral=True,
        )
        return

    sent = 0
    failed = []
    for member in members.values():
        try:
            await member.send(message_text, allowed_mentions=NO_PINGS)
            sent += 1
        except Exception:
            failed.append(f"{member} ({member.id})")
        await asyncio.sleep(0.15)

    result = (
        f"Рассылка завершена. Участников: {len(members)}. "
        f"Доставлено: {sent}. Не доставлено: {len(failed)}."
    )
    if failed:
        result += "\nНе удалось отправить:\n" + "\n".join(
            f"• {name}" for name in failed[:30]
        )
        if len(failed) > 30:
            result += f"\n…и ещё {len(failed) - 30}."
    await interaction.followup.send(result[:1900], ephemeral=True)


@bot.tree.command(name="main_dm", description="Разослать сообщение участникам Мейн-приватки")
@app_commands.default_permissions(administrator=True)
async def main_dm(interaction: discord.Interaction):
    main_guild_id = cfg_id(ticket_sync_source_guild_key("main"))
    main_guild = interaction.client.get_guild(main_guild_id) if main_guild_id else None
    if main_guild is None:
        await interaction.response.send_message(
            "Мейн-приватка не настроена. Сначала запусти в ней `/ticket_roles team:Мейн`.",
            ephemeral=True,
        )
        return
    if interaction.guild is None or interaction.guild.id != main_guild.id:
        await interaction.response.send_message(
            f"Команду нужно запускать в настроенной Мейн-приватке «{main_guild.name}».",
            ephemeral=True,
        )
        return

    requester = main_guild.get_member(interaction.user.id)
    requester_perms = getattr(requester, "guild_permissions", None)
    if requester is None or requester_perms is None or not requester_perms.administrator:
        await interaction.response.send_message("Нужно право «Администратор».", ephemeral=True)
        return
    if not MEMBERS_OK:
        await interaction.response.send_message(
            "Бот не видит список участников. Включи Server Members Intent в Discord Developer Portal.",
            ephemeral=True,
        )
        return

    await interaction.response.defer(ephemeral=True)
    try:
        members = await fetch_all_members(main_guild)
    except Exception as e:
        await interaction.followup.send(
            f"Не получилось получить участников приватки: {short_error(e)}",
            ephemeral=True,
        )
        return

    sent = 0
    failed = []
    for member in members.values():
        try:
            await member.send("Заходи в сеть, тварь!", allowed_mentions=NO_PINGS)
            sent += 1
        except Exception:
            failed.append(f"{member} ({member.id})")
        await asyncio.sleep(0.15)

    result = f"Готово. Отправлено: {sent}. Не доставлено: {len(failed)}."
    if failed:
        result += "\nЛичка закрыта или сообщение недоступно:\n" + "\n".join(
            f"• {name}" for name in failed[:30]
        )
        if len(failed) > 30:
            result += f"\n…и ещё {len(failed) - 30}."
    await interaction.followup.send(result[:1900], ephemeral=True)


async def send_who_plays(interaction: discord.Interaction):
    """Список: кто сейчас должен играть (ответ в канал)."""
    await interaction.response.defer()
    profiles = await sheet_players()
    if not profiles:
        await interaction.followup.send("В таблице нет игроков с привязанным Discord.")
        return
    now = time.time()
    play, afk, away = [], [], []
    for uid, p in profiles.items():
        st = active_status(uid, now)
        if st and st["kind"] == "инактив":
            away.append(f"• {p['nick']} до {fmt_dt(st['until'])[:10]}, {st['reason']}")
        elif st:
            afk.append(f"• {p['nick']} до {dt.datetime.fromtimestamp(st['until'], TZ):%H:%M}, {st['reason']}")
        else:
            play.append(f"• {p['nick']} — " + ("🔊 в гс" if uid in store.open_voice else "не в гс"))
    lines = [f"**Должны играть: {len(play)}**"] + (play or ["никого"])
    if afk:
        lines += [f"**В АФК: {len(afk)}**"] + afk
    if away:
        lines += [f"**В инактиве: {len(away)}**"] + away
    for chunk in chunk_lines(lines):
        await interaction.followup.send(chunk, allowed_mentions=NO_PINGS)


@bot.tree.command(name="кто_играет", description="Кто сейчас должен играть (для админов)")
@app_commands.default_permissions(manage_guild=True)
async def who_plays(interaction: discord.Interaction):
    if not await admin_only(interaction):
        return
    await send_who_plays(interaction)


STAFF_ROLE_NAME, CLAN_ROLE_NAME, CATEGORY_NAME = "Лидер", "Клан", "Клан"
# ключ, название, кто видит (all / clan / staff), могут ли писать лидеры, панель
CHANNEL_PLAN = [
    ("profile", "анкета", "all", False, (TEXT_PROFILE, PanelView)),
    ("report", "сводка", "staff", False, None),
    ("wipe", "вайп", "clan", False, (TEXT_WIPE, WipeView)),
    ("status", "статус", "clan", False, (TEXT_STATUS, StatusView)),
    *([("editor", "редактор", "staff", True, None)] if EDITOR_ENABLED else []),
    ("admin", "админ-панель", "staff", False, (TEXT_ADMIN, AdminView)),
    ("ticket_support_logs", "логи-поддержки", "staff", True, None),
    ("ticket_application_logs", "логи-анкет", "staff", True, None),
]


def channel_overwrites(guild, vis: str, staff_write: bool, staff, clan) -> dict:
    P = discord.PermissionOverwrite
    ow = {guild.me: P(view_channel=True, send_messages=True, embed_links=True, add_reactions=True,
                      read_message_history=True, manage_messages=True, attach_files=True)}
    if vis == "all":
        ow[guild.default_role] = P(view_channel=True, send_messages=False, add_reactions=False,
                                   read_message_history=True)
    else:
        ow[guild.default_role] = P(view_channel=False)
        if vis == "clan":
            ow[clan] = P(view_channel=True, send_messages=False, add_reactions=False, read_message_history=True)
    ow[staff] = P(view_channel=True, send_messages=staff_write, add_reactions=True, read_message_history=True,
                  attach_files=staff_write)
    return ow


async def setup_channels(guild) -> list[str]:
    """Создать роли, закрытые каналы и панели. Ничего не дублирует при повторном запуске."""
    notes = []
    staff = guild.get_role(cfg_id("staff")) or discord.utils.get(guild.roles, name=STAFF_ROLE_NAME)
    if staff is None:
        staff = await guild.create_role(name=STAFF_ROLE_NAME, reason="rust-clan-bot")
        notes.append(f"создана роль «{STAFF_ROLE_NAME}»")
    clan = guild.get_role(cfg_id("member")) or discord.utils.get(guild.roles, name=CLAN_ROLE_NAME)
    if clan is None:
        clan = await guild.create_role(name=CLAN_ROLE_NAME, reason="rust-clan-bot")
        notes.append(f"создана роль «{CLAN_ROLE_NAME}»")
    store.cfg["staff"], store.cfg["member"] = staff.id, clan.id
    category = discord.utils.get(guild.categories, name=CATEGORY_NAME)
    if category is None:
        category = await guild.create_category(CATEGORY_NAME, reason="rust-clan-bot")
    for key, name, vis, staff_write, panel in CHANNEL_PLAN:
        ch = discord.utils.get(guild.text_channels, name=name, category=category)
        if ch is None:
            ch = await guild.create_text_channel(name, category=category, reason="rust-clan-bot",
                                                 overwrites=channel_overwrites(guild, vis, staff_write, staff, clan))
            notes.append(f"создан канал #{name} ({'видят все, писать нельзя' if vis == 'all' else 'приватный'})")
            if panel:
                await ch.send(panel[0], view=panel[1]())
        elif panel:  # канал уже есть: обновить текст и кнопки существующей панели
            try:
                async for m in ch.history(limit=30):
                    if m.author.id == guild.me.id:
                        if m.content != panel[0]:
                            notes.append(f"обновлён текст панели в #{name}")
                        await m.edit(content=panel[0], view=panel[1]())
                        break
            except Exception as e:
                print(f"Не удалось обновить панель в #{name}: {short_error(e)}", flush=True)
        store.cfg[key] = ch.id
    store.save()
    return notes


async def apply_roles(guild, staff=None, clan=None) -> list[str]:
    """Сменить роли стаффа и клана и перенастроить доступ в уже созданных каналах."""
    old_staff, old_clan = guild.get_role(cfg_id("staff")), guild.get_role(cfg_id("member"))
    if staff is not None:
        store.cfg["staff"] = staff.id
    if clan is not None:
        store.cfg["member"] = clan.id
    new_staff, new_clan = guild.get_role(cfg_id("staff")), guild.get_role(cfg_id("member"))
    store.save()
    notes = []
    if new_staff is None or new_clan is None:
        return ["каналы пока не созданы или роли не заданы, доступ настроится при /setup_channels"]
    for key, name, vis, staff_write, panel in CHANNEL_PLAN:
        ch = guild.get_channel(cfg_id(key))
        if ch is None:
            continue
        for old, new in ((old_staff, new_staff), (old_clan, new_clan)):
            if old is not None and old.id != new.id and old.id not in (new_staff.id, new_clan.id):
                await ch.set_permissions(old, overwrite=None)
        for target, ow in channel_overwrites(guild, vis, staff_write, new_staff, new_clan).items():
            await ch.set_permissions(target, overwrite=ow)
        notes.append(f"#{ch.name}")
    return notes


@bot.tree.command(name="roles", description="Роли стаффа и клана (для админов)")
@app_commands.describe(staff="Роль лидеров и стаффа", clan="Роль участников клана")
@app_commands.default_permissions(manage_guild=True)
async def roles_cmd(interaction: discord.Interaction, staff: discord.Role | None = None,
                    clan: discord.Role | None = None):
    if not await admin_only(interaction):
        return
    if staff is None and clan is None:
        st, cl = interaction.guild.get_role(cfg_id("staff")), interaction.guild.get_role(cfg_id("member"))
        await interaction.response.send_message(
            f"Стафф: {st.mention if st else 'не задана'}\nКлан: {cl.mention if cl else 'не задана'}",
            ephemeral=True, allowed_mentions=NO_PINGS)
        return
    await interaction.response.defer(ephemeral=True)
    try:
        notes = await apply_roles(interaction.guild, staff, clan)
    except Exception as e:
        await interaction.followup.send(f"Не получилось: {short_error(e)}. Нужны права «Управлять каналами» "
                                        "и «Управлять ролями».", ephemeral=True)
        return
    await interaction.followup.send("Роли сохранены. Доступ обновлён: " + (", ".join(notes) or "каналов пока нет") + ".",
                                    ephemeral=True)


@bot.tree.command(name="setup_channels", description="Создать закрытые каналы, роли и панели (для админов)")
@app_commands.default_permissions(manage_guild=True)
async def setup_channels_cmd(interaction: discord.Interaction):
    if not await admin_only(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    try:
        notes = await setup_channels(interaction.guild)
    except Exception as e:
        await interaction.followup.send(
            f"Не получилось: {short_error(e)}. Дай боту права «Управлять каналами» и «Управлять ролями».",
            ephemeral=True)
        return
    lines = ["Готово." if notes else "Всё уже создано, ничего не менял."] + [f"• {n}" for n in notes]
    lines.append(f"Выдай игрокам роль «{CLAN_ROLE_NAME}», лидерам роль «{STAFF_ROLE_NAME}». "
                 "Сводка теперь выкладывается в #сводка.")
    await interaction.followup.send("\n".join(lines), ephemeral=True)


async def missing_profiles(guild, channel) -> tuple[list, int]:
    """Все, кто видит приватный канал, но не заполнил профиль (нет в таблице с Discord ID). -> (список, всего)."""
    profiles = await sheet_players()
    members = []
    async for m in guild.fetch_members(limit=None):
        if not m.bot and channel.permissions_for(m).view_channel:
            members.append(m)
    return [m for m in members if str(m.id) not in profiles], len(members)


@bot.tree.command(name="registered", description="Кто из приватного канала не заполнил профиль (для админов)")
@app_commands.describe(channel="Приватный канал клана (по умолчанию #статус)",
                       notify="Написать в канал и упомянуть тех, кто не заполнил")
@app_commands.default_permissions(manage_guild=True)
async def registered(interaction: discord.Interaction, channel: discord.TextChannel | None = None,
                     notify: bool = False):
    if not await admin_only(interaction):
        return
    if not MEMBERS_OK:
        await interaction.response.send_message(
            "Бот не видит список участников. В Discord Developer Portal: Bot → Privileged Gateway Intents → "
            "включи Server Members Intent и перезапусти бота (или подожди автоперезапуск).", ephemeral=True)
        return
    channel = channel or interaction.guild.get_channel(cfg_id("status"))
    if channel is None:
        await interaction.response.send_message(
            "Укажи приватный канал клана: /registered channel:#канал", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=not notify)
    try:
        missing, total = await missing_profiles(interaction.guild, channel)
    except Exception as e:
        await interaction.followup.send(f"Не получилось: {short_error(e)}", ephemeral=True)
        return
    head = f"**В #{channel.name}: {total}, заполнили профиль: {total - len(missing)}**"
    if not missing:
        await interaction.followup.send(head + "\nВсе заполнили.")
        return
    lines = [head, f"**Не заполнили: {len(missing)}**"]
    lines += [f"• {m.mention}" if notify else f"• {m.display_name} ({m.name})" for m in missing]
    if notify:
        lines.append("Нажмите «Мой профиль» в канале анкеты и заполните форму.")
    allowed = discord.AllowedMentions(users=True) if notify else NO_PINGS
    for chunk in chunk_lines(lines):
        await interaction.followup.send(chunk, allowed_mentions=allowed)


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


@bot.tree.command(name="report", description="Выложить сводку по норме прямо сейчас (для админов)")
@app_commands.default_permissions(manage_guild=True)
async def report(interaction: discord.Interaction):
    if not await admin_only(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    await post_report(interaction.channel)
    await interaction.followup.send("Сводка выложена.", ephemeral=True)



@bot.tree.command(name="moderators_setup", description="Выбрать текущий канал для карточек модераторов")
@app_commands.default_permissions(administrator=True)
async def moderators_setup(interaction: discord.Interaction):
    if not await admin_only(interaction):
        return
    if interaction.guild is None or not isinstance(interaction.channel, discord.TextChannel):
        await interaction.response.send_message(
            "Запусти команду в готовом текстовом канале.", ephemeral=True
        )
        return
    await interaction.response.defer(ephemeral=True)
    guild = interaction.guild
    channel = interaction.channel
    async with moderator_roster_lock(guild.id):
        roster = moderator_roster(guild.id, create=True)
        if int(roster.get("channel_id") or 0) != channel.id:
            roster["channel_id"] = channel.id
            roster["last_alert_message_id"] = 0
            for block in roster["blocks"].values():
                block["message_id"] = 0
        store.save()
        await update_all_moderator_blocks(guild, roster)
    await interaction.followup.send(
        f"Готово: карточки закреплены за {channel.mention}. "
        "Теперь настрой общую роль через `/moderators_role`.", ephemeral=True,
    )


@bot.tree.command(name="moderators_role", description="Указать роль, по которой бот определяет модераторов")
@app_commands.describe(role="Общая роль всех модераторов")
@app_commands.default_permissions(administrator=True)
async def moderators_role(interaction: discord.Interaction, role: discord.Role):
    if not await admin_only(interaction):
        return
    if interaction.guild is None or role.is_default():
        await interaction.response.send_message("Выбери обычную роль модераторов.", ephemeral=True)
        return
    roster = moderator_roster(interaction.guild.id)
    if roster is None or not int(roster.get("channel_id") or 0):
        await interaction.response.send_message("Сначала запусти `/moderators_setup`.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    roster["moderator_role_id"] = role.id
    roster["last_auto_sync_slot"] = ""
    store.save()
    try:
        moderators_count, players_count = await synchronize_moderator_roster(interaction.guild)
    except Exception as e:
        await interaction.followup.send(f"Роль сохранена, но сверка не удалась: {short_error(e)}", ephemeral=True)
        return
    await interaction.followup.send(
        f"Роль модераторов сохранена: {role.mention}. Карточек: **{moderators_count}**, "
        f"распределено игроков: **{players_count}**.",
        ephemeral=True, allowed_mentions=NO_PINGS,
    )


@bot.tree.command(name="moderators_sync", description="Сверить модераторов и перераспределить игроков сейчас")
@app_commands.default_permissions(administrator=True)
async def moderators_sync(interaction: discord.Interaction):
    if not await admin_only(interaction):
        return
    if interaction.guild is None:
        await interaction.response.send_message("Команда работает только на сервере.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    try:
        moderators_count, players_count = await synchronize_moderator_roster(interaction.guild)
    except Exception as e:
        await interaction.followup.send(f"Не удалось выполнить сверку: {short_error(e)}", ephemeral=True)
        return
    await interaction.followup.send(
        f"Сверка завершена. Модераторов: **{moderators_count}**, игроков: **{players_count}**.",
        ephemeral=True,
    )


@bot.tree.command(name="moderators_reset", description="Сбросить карточки и канал распределения модераторов")
@app_commands.default_permissions(administrator=True)
async def moderators_reset(interaction: discord.Interaction):
    if not await admin_only(interaction):
        return
    if interaction.guild is None:
        await interaction.response.send_message("Команда работает только на сервере.", ephemeral=True)
        return
    roster = moderator_roster(interaction.guild.id)
    if roster is None:
        await interaction.response.send_message("Распределение ещё не настроено.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    async with moderator_roster_lock(interaction.guild.id):
        channel = await moderator_roster_channel(interaction.guild, roster)
        deleted_messages = 0
        if channel is not None:
            message_ids = [
                int(block.get("message_id") or 0) for block in roster.get("blocks", {}).values()
            ]
            message_ids.append(int(roster.get("last_alert_message_id") or 0))
            for message_id in set(message_id for message_id in message_ids if message_id):
                try:
                    await channel.get_partial_message(message_id).delete()
                    deleted_messages += 1
                except (discord.NotFound, discord.Forbidden):
                    pass
        roster["channel_id"] = 0
        roster["last_alert_message_id"] = 0
        roster["blocks"] = {}
        roster["last_auto_sync_slot"] = ""
        store.save()
    await interaction.followup.send(
        f"Распределение сброшено. Удалено старых сообщений: **{deleted_messages}**. "
        "Роль модераторов и роли-исключения сохранены. Создай новый канал и запусти в нём "
        "`/moderators_setup`, затем `/moderators_sync`.",
        ephemeral=True,
    )


@bot.tree.command(name="moderators_unassigned", description="Показать незакреплённых игроков приватки")
@app_commands.default_permissions(administrator=True)
async def moderators_unassigned(interaction: discord.Interaction):
    if not await admin_only(interaction):
        return
    if interaction.guild is None:
        await interaction.response.send_message("Команда работает только на сервере.", ephemeral=True)
        return
    roster = moderator_roster(interaction.guild.id)
    if roster is None or not int(roster.get("channel_id") or 0):
        await interaction.response.send_message("Сначала запусти `/moderators_setup`.", ephemeral=True)
        return
    if not MEMBERS_OK:
        await interaction.response.send_message(
            "Включи Server Members Intent в Discord Developer Portal и перезапусти бота.", ephemeral=True
        )
        return
    await interaction.response.defer(ephemeral=True)
    try:
        members = await fetch_all_members(interaction.guild)
    except Exception as e:
        await interaction.followup.send(
            f"Не удалось получить участников: {short_error(e)}", ephemeral=True
        )
        return
    moderator_ids = {
        member.id for member in members.values()
        if member_has_roster_moderator_role(member, roster)
    }
    excluded_ids = {
        member.id for member in members.values()
        if member.id not in moderator_ids and member_is_moderator_excluded(member, roster)
    }
    assigned_ids = {
        int(uid)
        for block in roster.get("blocks", {}).values()
        for uid in block.get("player_ids", [])
    }
    eligible = {
        member.id: member for member in members.values()
        if member.id not in moderator_ids and member.id not in excluded_ids
    }
    missing = sorted(
        (member for uid, member in eligible.items() if uid not in assigned_ids),
        key=lambda member: (member.display_name.casefold(), member.id),
    )
    lines = [
        "**Проверка распределения приватки**",
        f"Участников без ботов: **{len(members)}**",
        f"Модераторов: **{len(moderator_ids)}**",
        f"По ролям-исключениям: **{len(excluded_ids)}**",
        f"Должны быть распределены: **{len(eligible)}**",
        f"Не распределены: **{len(missing)}**",
    ]
    if missing:
        lines.extend(["", "**Нужно распределить:**"])
        lines.extend(f"• {member.mention} (`{member.id}`)" for member in missing)
    else:
        lines.extend(["", "Все нужные игроки распределены."])
    for chunk in chunk_lines(lines):
        await interaction.followup.send(chunk, ephemeral=True, allowed_mentions=NO_PINGS)


@bot.tree.command(name="vis", description="Настроить карточки игроков и анализ скриншотов фарма")
@app_commands.describe(
    farm_channel="Канал, куда игроки отправляют скриншоты фарма",
    data_channel="Общий канал Steam, BattleMetrics, часового пояса и РТ",
    steam_channel="Существующий отдельный канал со Steam",
    battlemetrics_channel="Существующий отдельный канал BattleMetrics",
    timezone_channel="Существующий отдельный канал часовых поясов",
    positions_channel="Канал с известными позициями на РТ",
    model="Vision-модель OpenRouter; пусто — модель из настроек сервера",
)
@app_commands.default_permissions(administrator=True)
async def vis(
    interaction: discord.Interaction,
    farm_channel: discord.TextChannel,
    data_channel: discord.TextChannel | None = None,
    steam_channel: discord.TextChannel | None = None,
    battlemetrics_channel: discord.TextChannel | None = None,
    timezone_channel: discord.TextChannel | None = None,
    positions_channel: discord.TextChannel | None = None,
    model: str | None = None,
):
    if not await admin_only(interaction):
        return
    if interaction.guild is None:
        await interaction.response.send_message("Команда работает только на сервере.", ephemeral=True)
        return
    roster = moderator_roster(interaction.guild.id)
    if roster is None or not int(roster.get("moderator_role_id") or 0):
        await interaction.response.send_message(
            "Сначала настрой общую роль модераторов через `/moderators_role`.", ephemeral=True
        )
        return
    await interaction.response.defer(ephemeral=True)
    cfg = vis_config(interaction.guild.id, create=True)
    cfg["farm_channel_id"] = farm_channel.id
    channel_types = {}
    for channel, source_type in (
        (data_channel, "data"),
        (steam_channel, "steam"),
        (battlemetrics_channel, "battlemetrics"),
        (timezone_channel, "timezone"),
        (positions_channel, "positions"),
    ):
        if channel is not None and channel.id != farm_channel.id:
            channel_types[str(channel.id)] = source_type
    selected_channels = [int(channel_id) for channel_id in channel_types]
    if selected_channels:
        cfg["profile_channel_ids"] = selected_channels
        cfg["profile_channel_types"] = channel_types
    if model and model.strip():
        cfg["model"] = model.strip()[:150]
    cfg["last_hour_slot"] = ""
    store.save()
    try:
        await interaction.edit_original_response(content="Импортирую существующие Steam и BattleMetrics…")
        imported = await scan_vis_profile_history(interaction.guild, cfg)
        await interaction.edit_original_response(content="Создаю категорию и карточки игроков…")
        moderators_count, cards_count = await synchronize_vis_hub(interaction.guild)
    except Exception as e:
        await interaction.edit_original_response(content=f"Не удалось завершить настройку: {short_error(e)}")
        return
    key_status = "настроен" if OPENROUTER_KEY else "не настроен — скриншоты пока не анализируются"
    await interaction.edit_original_response(
        content=(
            "VIS настроен.\n"
            f"Канал фарма: {farm_channel.mention}\n"
            f"Импортировано сообщений с данными: **{imported}**\n"
            f"Каналов модераторов: **{moderators_count}**\n"
            f"Карточек игроков: **{cards_count}**\n"
            f"OpenRouter: **{key_status}**\n"
            "Карточки обновляются каждый час. В канале фарма бот работает полностью тихо."
        ),
        allowed_mentions=NO_PINGS,
    )


@bot.tree.command(name="vis_sync", description="Заново создать каналы модераторов и обновить VIS-карточки")
@app_commands.default_permissions(administrator=True)
async def vis_sync(interaction: discord.Interaction):
    if not await admin_only(interaction):
        return
    if interaction.guild is None:
        await interaction.response.send_message("Команда работает только на сервере.", ephemeral=True)
        return
    if vis_config(interaction.guild.id) is None:
        await interaction.response.send_message("Сначала настрой систему через `/vis`.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    try:
        cfg = vis_config(interaction.guild.id)
        imported = await scan_vis_profile_history(interaction.guild, cfg)
        moderators_count, cards_count = await synchronize_vis_hub(interaction.guild)
    except Exception as e:
        await interaction.followup.send(
            f"Не удалось восстановить VIS-каналы: {short_error(e)}", ephemeral=True
        )
        return
    await interaction.followup.send(
        f"VIS восстановлен. Каналов модераторов: **{moderators_count}**, "
        f"карточек игроков: **{cards_count}**, импортировано записей: **{imported}**.",
        ephemeral=True,
    )


@bot.tree.command(name="vis_import", description="Импортировать всю историю общего канала в карточки")
@app_commands.describe(channel="Общий канал со Steam, BattleMetrics, часовыми поясами и РТ")
@app_commands.default_permissions(administrator=True)
async def vis_import(
    interaction: discord.Interaction,
    channel: discord.TextChannel,
):
    if not await admin_only(interaction):
        return
    if interaction.guild is None:
        await interaction.response.send_message("Команда работает только на сервере.", ephemeral=True)
        return
    cfg = vis_config(interaction.guild.id)
    if cfg is None:
        await interaction.response.send_message("Сначала настрой систему через `/vis`.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    channel_ids = [int(cid) for cid in cfg.get("profile_channel_ids", [])]
    if channel.id not in channel_ids:
        channel_ids.append(channel.id)
    cfg["profile_channel_ids"] = channel_ids
    cfg.setdefault("profile_channel_types", {})[str(channel.id)] = "data"
    store.save()
    await interaction.edit_original_response(content=f"Читаю всю историю {channel.mention}…")
    try:
        imported = await scan_vis_profile_history(interaction.guild, cfg, channel_ids=[channel.id])
        await interaction.edit_original_response(content="Обновляю карточки игроков…")
        moderators_count, cards_count = await synchronize_vis_hub(interaction.guild)
    except Exception as e:
        await interaction.edit_original_response(content=f"Импорт не завершён: {short_error(e)}")
        return
    bot_member = interaction.guild.me
    permissions = channel.permissions_for(bot_member) if bot_member is not None else None
    missing_permissions = []
    if permissions is not None:
        if not permissions.view_channel:
            missing_permissions.append("Просматривать канал")
        if not permissions.read_message_history:
            missing_permissions.append("Читать историю сообщений")
        if not permissions.send_messages:
            missing_permissions.append("Отправлять сообщения")
        if not permissions.manage_messages:
            missing_permissions.append("Управлять сообщениями")
    permission_text = (
        "Права канала: всё настроено."
        if not missing_permissions else
        "Не хватает прав: " + ", ".join(missing_permissions) + "."
    )
    await interaction.edit_original_response(
        content=(
            f"Импорт завершён. Обновлено записей: **{imported}**. "
            f"Каналов модераторов: **{moderators_count}**, карточек: **{cards_count}**.\n"
            f"{permission_text}\n"
            "Канал сохранён для автоматической обработки новых сообщений."
        )
    )


@bot.tree.command(name="moderators_set", description="Вручную назначить модератора карточке")
@app_commands.describe(slot="Номер карточки", moderator="Модератор этой карточки")
@app_commands.default_permissions(administrator=True)
async def moderators_set(interaction: discord.Interaction,
                         slot: app_commands.Range[int, 1, 50],
                         moderator: discord.Member):
    if not await admin_only(interaction):
        return
    if interaction.guild is None or moderator.bot:
        await interaction.response.send_message("Выбери обычного участника сервера.", ephemeral=True)
        return
    roster = moderator_roster(interaction.guild.id)
    if roster is None or not int(roster.get("channel_id") or 0):
        await interaction.response.send_message("Сначала запусти `/moderators_setup`.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    guild = interaction.guild
    async with moderator_roster_lock(guild.id):
        block = roster["blocks"].setdefault(str(slot), {
            "moderator_id": 0, "player_ids": [], "message_id": 0,
        })
        old_moderator_id = int(block.get("moderator_id") or 0)
        changed_blocks = {int(slot)}
        for number in moderator_block_numbers(roster):
            other = roster["blocks"][str(number)]
            if number != int(slot) and int(other.get("moderator_id") or 0) == moderator.id:
                other["moderator_id"] = 0
                changed_blocks.add(number)
            before = list(other.get("player_ids", []))
            other["player_ids"] = [uid for uid in before if int(uid) != moderator.id]
            if other["player_ids"] != before:
                changed_blocks.add(number)
        block["moderator_id"] = moderator.id
        store.save()
        channel = await moderator_roster_channel(guild, roster)
        if channel is not None:
            try:
                await channel.set_permissions(
                    moderator, view_channel=True, read_message_history=True, send_messages=False,
                    reason="Назначен модератором блока",
                )
                still_used = any(
                    int(item.get("moderator_id") or 0) == old_moderator_id
                    for item in roster["blocks"].values()
                )
                old_moderator = guild.get_member(old_moderator_id)
                if old_moderator is not None and old_moderator_id != moderator.id and not still_used:
                    await channel.set_permissions(
                        old_moderator, overwrite=None, reason="Больше не закреплён за блоком"
                    )
            except discord.Forbidden:
                pass
        for number in sorted(changed_blocks):
            await update_moderator_block(guild, number, roster)
    await interaction.followup.send(
        f"{moderator.mention} назначен на карточку **{slot}**.", ephemeral=True
    )


@bot.tree.command(name="moderators_distribute", description="Равномерно распределить сервер между модераторами")
@app_commands.default_permissions(administrator=True)
async def moderators_distribute(interaction: discord.Interaction):
    if not await admin_only(interaction):
        return
    if interaction.guild is None:
        await interaction.response.send_message("Команда работает только на сервере.", ephemeral=True)
        return
    roster = moderator_roster(interaction.guild.id)
    if roster is None or not int(roster.get("channel_id") or 0):
        await interaction.response.send_message("Сначала запусти `/moderators_setup`.", ephemeral=True)
        return
    blocks = configured_moderator_blocks(roster)
    if not blocks:
        await interaction.response.send_message(
            "Сначала назначь хотя бы одного модератора через `/moderators_set`.", ephemeral=True
        )
        return
    if not MEMBERS_OK:
        await interaction.response.send_message(
            "Включи Server Members Intent в Discord Developer Portal и перезапусти бота.", ephemeral=True
        )
        return
    await interaction.response.defer(ephemeral=True)
    guild = interaction.guild
    try:
        members = await fetch_all_members(guild)
    except Exception as e:
        await interaction.followup.send(
            f"Не удалось получить участников: {short_error(e)}", ephemeral=True
        )
        return
    async with moderator_roster_lock(guild.id):
        moderator_ids = {
            int(roster["blocks"][str(number)].get("moderator_id") or 0) for number in blocks
        }
        players = sorted(
            (
                member for member in members.values()
                if member.id not in moderator_ids
                and not member_is_moderator_excluded(member, roster)
            ),
            key=lambda member: (member.display_name.casefold(), member.id),
        )
        for block in roster["blocks"].values():
            block["player_ids"] = []
        for index, member in enumerate(players):
            number = blocks[index % len(blocks)]
            roster["blocks"][str(number)]["player_ids"].append(member.id)
        store.save()
        await update_all_moderator_blocks(guild, roster)
    counts = ", ".join(
        f"{number}: {len(roster['blocks'][str(number)]['player_ids'])}" for number in blocks
    )
    await interaction.followup.send(
        f"Распределено игроков: **{len(players)}**. По карточкам — {counts}.", ephemeral=True
    )


@bot.tree.command(name="moderators_exclusions", description="Настроить роли, не участвующие в распределении")
@app_commands.describe(
    role_1="Первая исключённая роль, например Лидер",
    role_2="Вторая исключённая роль, например Со-лидер",
    role_3="Третья исключённая роль, например Старший админ",
    clear="Удалить все сохранённые исключения",
)
@app_commands.default_permissions(administrator=True)
async def moderators_exclusions(
    interaction: discord.Interaction,
    role_1: discord.Role | None = None,
    role_2: discord.Role | None = None,
    role_3: discord.Role | None = None,
    clear: bool = False,
):
    if not await admin_only(interaction):
        return
    if interaction.guild is None:
        await interaction.response.send_message("Команда работает только на сервере.", ephemeral=True)
        return
    roster = moderator_roster(interaction.guild.id)
    if roster is None or not int(roster.get("channel_id") or 0):
        await interaction.response.send_message("Сначала запусти `/moderators_setup`.", ephemeral=True)
        return
    selected_roles = list(dict.fromkeys(role for role in (role_1, role_2, role_3) if role is not None))
    if any(role.is_default() for role in selected_roles):
        await interaction.response.send_message("Роль @everyone исключать нельзя.", ephemeral=True)
        return
    if not clear and not selected_roles:
        saved = [
            interaction.guild.get_role(int(role_id))
            for role_id in roster.get("excluded_role_ids", [])
        ]
        saved = [role for role in saved if role is not None]
        text = ", ".join(role.mention for role in saved) if saved else "не настроены"
        await interaction.response.send_message(
            f"**Роли-исключения:** {text}", ephemeral=True, allowed_mentions=NO_PINGS
        )
        return
    await interaction.response.defer(ephemeral=True)
    async with moderator_roster_lock(interaction.guild.id):
        roster["excluded_role_ids"] = [] if clear else [role.id for role in selected_roles]
        changed_blocks = []
        removed = 0
        if not clear:
            for number in moderator_block_numbers(roster):
                block = roster["blocks"][str(number)]
                before_ids = list(block.get("player_ids", []))
                keep_ids = []
                for uid in before_ids:
                    member = interaction.guild.get_member(int(uid))
                    if member is not None and member_is_moderator_excluded(member, roster):
                        removed += 1
                    else:
                        keep_ids.append(uid)
                if keep_ids != before_ids:
                    block["player_ids"] = keep_ids
                    changed_blocks.append(number)
        store.save()
        for number in changed_blocks:
            await update_moderator_block(interaction.guild, number, roster)
    if clear:
        result = "Все роли-исключения удалены."
    else:
        result = "Роли-исключения сохранены: " + ", ".join(role.mention for role in selected_roles)
        result += f"\nУбрано из блоков: **{removed}**."
    await interaction.followup.send(result, ephemeral=True, allowed_mentions=NO_PINGS)


@bot.tree.command(name="moderators_assign", description="Вручную закрепить игрока за нужным модератором")
@app_commands.describe(player="Игрок", slot="Номер карточки модератора")
@app_commands.default_permissions(administrator=True)
async def moderators_assign(interaction: discord.Interaction, player: discord.Member,
                            slot: app_commands.Range[int, 1, 50]):
    if not await admin_only(interaction):
        return
    if interaction.guild is None or player.bot:
        await interaction.response.send_message("Выбери обычного участника сервера.", ephemeral=True)
        return
    roster = moderator_roster(interaction.guild.id)
    if roster is None or not int(roster.get("channel_id") or 0):
        await interaction.response.send_message("Сначала запусти `/moderators_setup`.", ephemeral=True)
        return
    if member_is_moderator_excluded(player, roster):
        await interaction.response.send_message(
            "У этого игрока есть роль-исключение, поэтому закрепить его нельзя.", ephemeral=True
        )
        return
    target = roster["blocks"].get(str(slot))
    if target is None:
        await interaction.response.send_message("Карточки с таким номером нет.", ephemeral=True)
        return
    moderator_id = int(target.get("moderator_id") or 0)
    if not moderator_id:
        await interaction.response.send_message("У этой карточки ещё нет модератора.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    async with moderator_roster_lock(interaction.guild.id):
        changed_blocks = {int(slot)}
        for number in moderator_block_numbers(roster):
            block = roster["blocks"][str(number)]
            before = list(block.get("player_ids", []))
            block["player_ids"] = [uid for uid in before if int(uid) != player.id]
            if block["player_ids"] != before:
                changed_blocks.add(number)
        target.setdefault("player_ids", []).append(player.id)
        store.save()
        for number in sorted(changed_blocks):
            await update_moderator_block(interaction.guild, number, roster)
        await send_moderator_roster_alert(
            interaction.guild, roster,
            f"<@{moderator_id}> Игрок {player.mention} (`{player.id}`) закреплён за вами.",
        )
    await interaction.followup.send(
        f"{player.mention} закреплён за модератором на карточке **{slot}**.", ephemeral=True
    )


@bot.tree.command(name="moderators_remove", description="Открепить игрока от модератора")
@app_commands.describe(player="Игрок, которого нужно открепить")
@app_commands.default_permissions(administrator=True)
async def moderators_remove(interaction: discord.Interaction, player: discord.Member):
    if not await admin_only(interaction):
        return
    if interaction.guild is None:
        await interaction.response.send_message("Команда работает только на сервере.", ephemeral=True)
        return
    roster = moderator_roster(interaction.guild.id)
    if roster is None:
        await interaction.response.send_message("Сначала запусти `/moderators_setup`.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    async with moderator_roster_lock(interaction.guild.id):
        number = find_moderator_player_block(roster, player.id)
        if number is None:
            await interaction.followup.send("Этот игрок ни за кем не закреплён.", ephemeral=True)
            return
        block = roster["blocks"][str(number)]
        block["player_ids"] = [uid for uid in block.get("player_ids", []) if int(uid) != player.id]
        store.save()
        await update_moderator_block(interaction.guild, number, roster)
    await interaction.followup.send(
        f"{player.mention} откреплён от модератора.", ephemeral=True
    )


@bot.tree.command(name="randomizer", description="Разыграть приз среди участников приватки")
@app_commands.describe(
    prize="Что разыгрываем",
    winners="Количество победителей: от 1 до 20",
    pool="Участники всего сервера или только текущего войса",
    exclude_role_1="Первая роль, которая не участвует",
    exclude_role_2="Вторая роль, которая не участвует",
    exclude_role_3="Третья роль, которая не участвует",
    exclude_role_4="Четвёртая роль, которая не участвует",
    exclude_role_5="Пятая роль, которая не участвует",
)
@app_commands.choices(pool=[
    app_commands.Choice(name="Весь сервер", value="server"),
    app_commands.Choice(name="Текущий голосовой канал", value="voice"),
])
@app_commands.default_permissions(administrator=True)
async def randomizer(
    interaction: discord.Interaction,
    prize: str,
    winners: int = 1,
    pool: str = "server",
    exclude_role_1: discord.Role | None = None,
    exclude_role_2: discord.Role | None = None,
    exclude_role_3: discord.Role | None = None,
    exclude_role_4: discord.Role | None = None,
    exclude_role_5: discord.Role | None = None,
):
    if not await admin_only(interaction):
        return
    if interaction.guild is None:
        await interaction.response.send_message(
            "Команда работает только на сервере.", ephemeral=True
        )
        return
    if not 1 <= winners <= 20:
        await interaction.response.send_message(
            "Количество победителей должно быть от 1 до 20.", ephemeral=True
        )
        return
    prize = prize.strip()
    if not prize:
        await interaction.response.send_message(
            "Укажи, что разыгрываем.", ephemeral=True
        )
        return
    if len(prize) > 300:
        await interaction.response.send_message(
            "Название приза должно быть не длиннее 300 символов.", ephemeral=True
        )
        return

    excluded_roles = list(dict.fromkeys(
        role for role in (
            exclude_role_1, exclude_role_2, exclude_role_3,
            exclude_role_4, exclude_role_5,
        )
        if role is not None
    ))
    if any(role.is_default() for role in excluded_roles):
        await interaction.response.send_message(
            "Роль @everyone исключать нельзя — тогда участников не останется.",
            ephemeral=True,
        )
        return

    if pool == "voice":
        invoker = interaction.guild.get_member(interaction.user.id)
        voice_channel = getattr(getattr(invoker, "voice", None), "channel", None)
        if voice_channel is None:
            await interaction.response.send_message(
                "Сначала зайди в нужный голосовой канал.", ephemeral=True
            )
            return
        source_members = list(voice_channel.members)
        pool_name = f"войс «{voice_channel.name}»"
    else:
        if not MEMBERS_OK:
            await interaction.response.send_message(
                "Бот не видит полный список участников. Включи Server Members Intent "
                "в Discord Developer Portal и перезапусти бота.",
                ephemeral=True,
            )
            return
        await interaction.response.defer()
        try:
            source_members = [
                member async for member in interaction.guild.fetch_members(limit=None)
            ]
        except discord.Forbidden:
            await interaction.edit_original_response(
                content="Бот не может получить список участников сервера."
            )
            return
        except Exception as e:
            await interaction.edit_original_response(
                content=f"Не удалось получить участников: {short_error(e)}"
            )
            return
        pool_name = "весь сервер"

    excluded_role_ids = {role.id for role in excluded_roles}
    eligible = [
        member for member in source_members
        if not member.bot
        and not any(role.id in excluded_role_ids for role in member.roles)
    ]

    if len(eligible) < winners:
        message = (
            f"Недостаточно участников: подходит {len(eligible)}, "
            f"а победителей указано {winners}."
        )
        if interaction.response.is_done():
            await interaction.edit_original_response(content=message)
        else:
            await interaction.response.send_message(message, ephemeral=True)
        return

    excluded_text = (
        ", ".join(role.mention for role in excluded_roles)
        if excluded_roles else "нет"
    )
    finish_at = int(time.time()) + 120
    start_embed = discord.Embed(
        title="Розыгрыш начался",
        description=(
            f"**Приз:** {prize}\n\n"
            f"Победители будут выбраны <t:{finish_at}:R>."
        ),
        colour=0x5865F2,
    )
    start_embed.add_field(
        name="Участники",
        value=f"{pool_name} · допущено: **{len(eligible)}**",
        inline=False,
    )
    start_embed.add_field(
        name="Не участвуют роли",
        value=excluded_text,
        inline=False,
    )
    start_embed.set_footer(text="Таймер: 2 минуты")

    no_mentions = discord.AllowedMentions.none()
    if interaction.response.is_done():
        await interaction.edit_original_response(
            content=None, embed=start_embed, allowed_mentions=no_mentions
        )
    else:
        await interaction.response.send_message(
            embed=start_embed, allowed_mentions=no_mentions
        )

    await asyncio.sleep(110)
    for seconds_left in range(10, 0, -1):
        countdown_embed = discord.Embed(
            title="Розыгрыш",
            description=(
                f"**Приз:** {prize}\n\n"
                f"## {seconds_left}"
            ),
            colour=0xF1C40F,
        )
        countdown_embed.set_footer(text="Идёт обратный отсчёт")
        await interaction.edit_original_response(
            content=None, embed=countdown_embed, allowed_mentions=no_mentions
        )
        await asyncio.sleep(1)

    selected = secrets.SystemRandom().sample(eligible, winners)
    winner_lines = [
        f"**{index}.** {member.mention} — Discord ID: {member.id}"
        for index, member in enumerate(selected, 1)
    ]
    winner_text = (
        "## WINNERS\n"
        f"**Приз:** {prize}\n\n"
        + "\n".join(winner_lines)
        + f"\n\n-# Запустил: {interaction.user} · случайный выбор без повторов"
    )
    await interaction.edit_original_response(
        content=winner_text,
        embed=None,
        allowed_mentions=discord.AllowedMentions(
            users=True, roles=False, everyone=False
        ),
    )


bot.run(DISCORD_TOKEN)
