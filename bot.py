import os
import re
import io
import json
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
        tmp = DATA_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps({"voice": self.voice, "open_voice": self.open_voice,
                                   "heartbeat": self.heartbeat, "wipes": self.wipes,
                                   "checks": self.checks, "statuses": self.statuses,
                                   "drafts": self.drafts, "cfg": self.cfg,
                                   "ticket_counters": self.ticket_counters,
                                   "private_invites": self.private_invites,
                                   "wipe_events": self.wipe_events}), encoding="utf-8")
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


def cfg_id(key: str, env: int = 0) -> int:
    return int(store.cfg.get(key) or env or 0)


def report_channel_id() -> int:
    return cfg_id("report", CHANNEL_ID)


def editor_channel_id() -> int:
    return cfg_id("editor", EDITOR_CHANNEL_ID)


def staff_role_id() -> int:
    return cfg_id("staff", STAFF_ROLE_ID)


def ticket_access_role_ids() -> list[int]:
    return list(dict.fromkeys(role_id for role_id in (
        cfg_id("ticket_access_role"),
        cfg_id("ticket_access_role_2"),
    ) if role_id))


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
    if str(interaction.user.id) not in {str(uid) for uid in event.get("participants", [])}:
        await interaction.response.send_message("Ты не входишь в список участников этого сбора.", ephemeral=True)
        return
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


def ticket_overwrites(guild: discord.Guild, opener: discord.Member) -> dict:
    """Доступ к тикету: автор, бот, настроенный стафф и административные роли."""
    P = discord.PermissionOverwrite
    access = P(view_channel=True, send_messages=True, read_message_history=True, attach_files=True,
               embed_links=True)
    overwrites = {guild.default_role: P(view_channel=False), opener: access}
    if guild.me is not None:
        overwrites[guild.me] = P(view_channel=True, send_messages=True, read_message_history=True,
                                 attach_files=True, embed_links=True, manage_channels=True,
                                 manage_messages=True)
    configured_staff = guild.get_role(staff_role_id())
    if configured_staff is not None:
        overwrites[configured_staff] = access
    for role_id in ticket_access_role_ids():
        ticket_access = guild.get_role(role_id)
        if ticket_access is not None:
            overwrites[ticket_access] = access
    for role in guild.roles:
        if role.is_default():
            continue
        if role.permissions.administrator or role.permissions.manage_guild:
            overwrites[role] = access
    return overwrites


async def apply_ticket_access_roles(guild: discord.Guild, old_role_ids: list[int],
                                    roles: list[discord.Role]) -> int:
    """Обновить доступ ролей во всех уже открытых тикетах."""
    access = discord.PermissionOverwrite(
        view_channel=True, send_messages=True, read_message_history=True,
        attach_files=True, embed_links=True,
    )
    new_role_ids = {role.id for role in roles}
    old_roles = [guild.get_role(role_id) for role_id in old_role_ids
                 if role_id not in new_role_ids and role_id != staff_role_id()]
    updated = 0
    for channel in guild.text_channels:
        if not (channel.topic or "").startswith("ticket:"):
            continue
        for old_role in old_roles:
            if old_role is not None:
                await channel.set_permissions(old_role, overwrite=None,
                                              reason="Изменены роли доступа к тикетам")
        for role in roles:
            await channel.set_permissions(role, overwrite=access,
                                          reason="Настроена роль доступа к тикетам")
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
                overwrites=ticket_overwrites(guild, interaction.user),
                topic=(f"ticket:{kind} owner:{interaction.user.id} "
                       f"role:{TICKET_ROLE_SLUGS.get(selected_role or '', 'general')}"),
                reason=f"{settings['title']}: {interaction.user}",
            )
            counter_key = f"{guild.id}:{kind}:{TICKET_ROLE_SLUGS.get(selected_role or '') or 'general'}"
            store.ticket_counters[counter_key] = number
            store.save()
        notify_roles = []
        for role_id in (staff_role_id(), *ticket_access_role_ids()):
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
    access_role_ids = set(ticket_access_role_ids())
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


async def grant_accepted_ticket_roles(channel: discord.TextChannel,
                                      client: discord.Client) -> tuple[bool, str]:
    match = re.search(r"^ticket:(main|second) owner:(\d+) role:([\w-]+)", channel.topic or "")
    if not match:
        return False, "Не удалось определить состав, участника или направление тикета."
    kind, owner_id, role_slug = match.group(1), int(match.group(2)), match.group(3)
    member = channel.guild.get_member(owner_id)
    if member is None:
        try:
            member = await channel.guild.fetch_member(owner_id)
        except Exception:
            return False, "Автор тикета больше не находится на сервере."

    direction_key = TICKET_ROLE_CONFIG_KEYS.get(role_slug)
    group_key = "ticket_group_main" if kind == "main" else "ticket_group_second"
    direction_role = channel.guild.get_role(cfg_id(direction_key)) if direction_key else None
    group_role = channel.guild.get_role(cfg_id(group_key))
    missing = []
    if direction_role is None:
        missing.append("роль направления")
    if group_role is None:
        missing.append("дополнительная роль состава")
    if missing:
        return False, "Не настроена " + " и ".join(missing) + ". Используй `/ticket_roles`."

    private_guild_id = cfg_id(ticket_sync_source_guild_key(kind))
    private_guild = client.get_guild(private_guild_id) if private_guild_id else None
    if private_guild is None:
        return False, (f"Не настроена приватка «{TICKET_SYNC_TEAM_LABELS[kind]}». "
                       "Запусти в ней `/ticket_roles` и выбери состав.")

    private_direction_role = private_guild.get_role(
        cfg_id(ticket_sync_source_role_key(kind, role_slug))
    )
    private_member_role = private_guild.get_role(cfg_id(ticket_private_member_role_key(kind)))
    invite_channel = private_guild.get_channel(cfg_id(ticket_private_invite_channel_key(kind)))
    if private_direction_role is None:
        return False, (f"В приватке «{private_guild.name}» не настроена роль "
                       f"«{TICKET_ROLE_LABELS.get(role_slug, role_slug)}». Используй `/ticket_roles`.")
    if not isinstance(invite_channel, discord.TextChannel):
        return False, (f"В приватке «{private_guild.name}» не настроен канал приглашения. "
                       "Укажи `invite_channel` в `/ticket_roles`.")
    private_me = private_guild.me
    if private_me is None or not private_me.guild_permissions.manage_roles:
        return False, f"У бота в приватке «{private_guild.name}» нет права «Управлять ролями»."
    private_roles_to_manage = [private_direction_role]
    if private_member_role is not None:
        private_roles_to_manage.append(private_member_role)
    unavailable_private_roles = [
        role.name for role in private_roles_to_manage
        if role.managed or role >= private_me.top_role
    ]
    if unavailable_private_roles:
        return False, ("Подними роль бота выше ролей приватки: "
                       + ", ".join(unavailable_private_roles) + ".")
    if not invite_channel.permissions_for(private_me).create_instant_invite:
        return False, (f"У бота нет права «Создавать приглашения» в канале "
                       f"{invite_channel.mention} приватки «{private_guild.name}».")

    community_roles = list({direction_role.id: direction_role, group_role.id: group_role}.values())
    community_role_ids_before = {role.id for role in member.roles}
    private_member = private_guild.get_member(owner_id)
    if private_member is None:
        try:
            private_member = await private_guild.fetch_member(owner_id)
        except discord.NotFound:
            private_member = None
        except discord.Forbidden:
            return False, f"Бот не может проверить участников приватки «{private_guild.name}»."
        except Exception as e:
            return False, f"Не удалось проверить приватку: {short_error(e)}"

    private_roles = list({role.id: role for role in private_roles_to_manage}.values())
    private_role_ids_before = {role.id for role in private_member.roles} if private_member else set()
    invite = None

    try:
        await member.add_roles(*community_roles, reason=f"Принят через тикет #{channel.name}")
    except discord.Forbidden:
        return False, "Бот не может выдать роли. Подними роль бота выше выдаваемых ролей."
    except Exception as e:
        return False, f"Не удалось выдать роли: {short_error(e)}"

    try:
        if private_member is not None:
            await private_member.add_roles(*private_roles, reason=f"Принят через тикет #{channel.name}")
            store.private_invites.pop(str(owner_id), None)
            store.save()
            return True, f"Роли выданы в Community и приватке «{private_guild.name}»."

        invite = await invite_channel.create_invite(
            max_age=7 * 86400,
            max_uses=1,
            unique=True,
            reason=f"Принят через тикет #{channel.name}",
        )
        store.private_invites[str(owner_id)] = {
            "team": kind,
            "role": role_slug,
            "created": time.time(),
            "community_guild_id": channel.guild.id,
        }
        store.save()
        await member.send(
            f"Твоя анкета в **{channel.guild.name}** принята.\n"
            f"Приглашение в **{private_guild.name}**: {invite.url}\n\n"
            "Ссылка одноразовая и действует 7 дней. После входа бот автоматически выдаст роли.",
            allowed_mentions=NO_PINGS,
        )
        return True, f"Роли выданы, приглашение в «{private_guild.name}» отправлено участнику в личку."
    except discord.Forbidden:
        detail = ("Бот не может создать приглашение или написать участнику в личку. "
                  "Проверь право «Создавать приглашения» и попроси участника открыть личные сообщения.")
    except Exception as e:
        detail = f"Не удалось пригласить в приватку: {short_error(e)}"

    store.private_invites.pop(str(owner_id), None)
    store.save()
    if invite is not None:
        try:
            await invite.delete(reason="Не удалось доставить приглашение")
        except Exception:
            pass
    rollback_community = [role for role in community_roles if role.id not in community_role_ids_before]
    if rollback_community:
        try:
            await member.remove_roles(*rollback_community, reason="Откат неудачного принятия в приватку")
        except Exception:
            pass
    if private_member is not None:
        rollback_private = [role for role in private_roles if role.id not in private_role_ids_before]
        if rollback_private:
            try:
                await private_member.remove_roles(*rollback_private, reason="Откат неудачного принятия")
            except Exception:
                pass
    return False, detail


class TicketCloseConfirmView(discord.ui.View):
    def __init__(self, user_id: int, reason: str = "Закрыт", accepted: bool = False):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.reason = reason
        self.accepted = accepted

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
        if self.accepted:
            granted, role_detail = await grant_accepted_ticket_roles(channel, interaction.client)
            if not granted:
                await interaction.edit_original_response(content=f"Тикет не закрыт. {role_detail}", view=None)
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
            view=TicketCloseConfirmView(interaction.user.id, reason=reason,
                                        accepted=reason_key == "accepted"),
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
    """Выдать роли принятому игроку сразу после входа в настроенную приватку."""
    pending = store.private_invites.get(str(member.id))
    if not pending:
        return False
    team = pending.get("team")
    role_slug = pending.get("role")
    if team not in TICKET_SYNC_TEAM_LABELS or role_slug not in TICKET_ROLE_LABELS:
        store.private_invites.pop(str(member.id), None)
        store.save()
        return False
    if cfg_id(ticket_sync_source_guild_key(team)) != member.guild.id:
        return False

    direction_role = member.guild.get_role(cfg_id(ticket_sync_source_role_key(team, role_slug)))
    member_role = member.guild.get_role(cfg_id(ticket_private_member_role_key(team)))
    if direction_role is None:
        print(f"Не выданы роли {member}: в приватке не настроена роль {role_slug}", flush=True)
        return False
    roles = [direction_role]
    if member_role is not None:
        roles.append(member_role)
    roles = [role for role in {role.id: role for role in roles}.values() if role not in member.roles]
    try:
        if roles:
            await member.add_roles(*roles, reason="Автовыдача после принятой анкеты")
    except Exception as e:
        print(f"Не удалось выдать роли в приватке участнику {member}: {short_error(e)}", flush=True)
        return False

    store.private_invites.pop(str(member.id), None)
    store.save()
    try:
        await member.send(
            f"Добро пожаловать в **{member.guild.name}**. Роли выданы автоматически.",
            allowed_mentions=NO_PINGS,
        )
    except Exception:
        pass
    return True


async def fetch_all_members(guild: discord.Guild) -> dict[int, discord.Member]:
    members = {}
    async for member in guild.fetch_members(limit=None):
        if not member.bot:
            members[member.id] = member
    return members


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
        in_voice = {str(m.id) for g in self.guilds for vc in g.voice_channels for m in vc.members if not m.bot}
        store.reconcile(in_voice)
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

    async def on_message(self, message):
        if not use_editor() or message.author.bot or message.channel.id != editor_channel_id():
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
    duplicate = any(
        event.get("guild_id") == interaction.guild.id
        and event.get("status") == "open"
        and event.get("name", "").casefold() == name.strip().casefold()
        for event in store.wipe_events.values()
    )
    if duplicate:
        await interaction.response.send_message("Открытый сбор с таким названием уже существует.", ephemeral=True)
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
    member_role="Общая роль, которую бот выдаёт всем принятым в этой приватке",
    main_group="Дополнительная роль Мейн состава",
    second_group="Дополнительная роль второго состава",
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
                       second_group: discord.Role | None = None):
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
        if main_group is not None or second_group is not None:
            lines.append("Роли составов настраиваются через `/ticket_roles` только в Community.")
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


@bot.tree.command(name="ticket_access", description="Настроить две роли доступа ко всем тикетам")
@app_commands.describe(
    role_1="Первая роль из этого сервера, например Тикет стафф",
    role_2="Вторая роль из этого сервера",
)
@app_commands.default_permissions(administrator=True)
async def ticket_access(interaction: discord.Interaction, role_1: discord.Role,
                        role_2: discord.Role | None = None):
    if not await admin_only(interaction):
        return
    if interaction.guild is None:
        await interaction.response.send_message("Команда работает только на сервере.", ephemeral=True)
        return
    roles = list(dict.fromkeys(role for role in (role_1, role_2) if role is not None))
    if any(role.is_default() for role in roles):
        await interaction.response.send_message("Роль @everyone использовать нельзя.", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    old_role_ids = ticket_access_role_ids()
    store.cfg["ticket_access_role"] = role_1.id
    if role_2 is None:
        store.cfg.pop("ticket_access_role_2", None)
    else:
        store.cfg["ticket_access_role_2"] = role_2.id
    store.save()
    try:
        updated = await apply_ticket_access_roles(interaction.guild, old_role_ids, roles)
    except discord.Forbidden:
        await interaction.followup.send(
            "Роли сохранены, но не удалось обновить открытые тикеты: боту нужно право «Управлять каналами».",
            ephemeral=True)
        return
    except Exception as e:
        await interaction.followup.send(
            f"Роли сохранены, но не удалось обновить открытые тикеты: {short_error(e)}", ephemeral=True)
        return
    mentions = " и ".join(role.mention for role in roles)
    await interaction.followup.send(
        f"Готово. {mentions} теперь видят, отвечают и закрывают все тикеты. "
        f"Обновлено открытых тикетов: {updated}.",
        ephemeral=True, allowed_mentions=NO_PINGS)


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


bot.run(DISCORD_TOKEN)
