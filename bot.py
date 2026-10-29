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
USE_BM = bool(BM_TOKEN)  # API BattleMetrics требует платную подписку; без токена проверка идёт только по гс
BASE = Path(__file__).parent
DATA_FILE = BASE / "data.json"
KEEP_DAYS = 45
TZ = dt.timezone(dt.timedelta(hours=TZ_OFFSET_HOURS))
NO_PINGS = discord.AllowedMentions.none()
STAFF_ROLE_ID = int(os.environ.get("STAFF_ROLE_ID", "0") or 0)  # роль стаффа, её упоминают в тикете (необязательно)

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
        self.statuses: dict[str, dict] = {}  # discord id -> {kind, reason, until, since, nick}
        self.checks: dict[str, dict] = {}  # id сообщения сводки -> {rid, uid, nick, line, decision, by_id}
        if DATA_FILE.exists():
            d = json.loads(DATA_FILE.read_text(encoding="utf-8"))
            self.voice = d.get("voice", [])
            self.open_voice = d.get("open_voice", {})
            self.heartbeat = d.get("heartbeat", time.time())
            self.wipes = d.get("wipes", {})
            self.checks = d.get("checks", {})
            self.statuses = d.get("statuses", {})

    def save(self):
        self.heartbeat = time.time()
        cutoff = time.time() - KEEP_DAYS * 86400
        self.voice = [v for v in self.voice if v[2] >= cutoff]
        self.checks = {k: v for k, v in self.checks.items() if v["rid"] >= cutoff}
        self.statuses = {k: v for k, v in self.statuses.items() if v["until"] >= time.time() - 86400}
        tmp = DATA_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps({"voice": self.voice, "open_voice": self.open_voice,
                                   "heartbeat": self.heartbeat, "wipes": self.wipes,
                                   "checks": self.checks, "statuses": self.statuses}), encoding="utf-8")
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


def active_status(uid: str, now: float | None = None) -> dict | None:
    st = store.statuses.get(uid)
    return st if st and st["until"] > (now or time.time()) else None


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


# ---------- ежедневная сводка ----------
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
        if STAFF_ROLE_ID:
            role = guild.get_role(STAFF_ROLE_ID)
            if role is not None:
                ow[role] = see
        slug = re.sub(r"[^\w\-]+", "-", f"не-играл-{info['nick']}-{day}".lower()).strip("-")[:90]
        ticket = await guild.create_text_channel(slug or "тикет", category=channel.category, overwrites=ow,
                                                 reason=f"не играл {day}: {info['nick']}")
        info["ticket"] = ticket.id
        store.save()
        staff = f"<@&{STAFF_ROLE_ID}> " if STAFF_ROLE_ID else ""
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
        if member is None or not member.guild_permissions.manage_guild:
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
        how = {"discord": "обновил твою строку", "bm": "нашёл твою строку по ссылке BattleMetrics",
               "nick": "нашёл твою строку по нику", "new": "добавил новую строку"}.get(res.get("how"), "записал")
        await interaction.followup.send(
            f"Готово, {how}. Ник: **{name}**, роль: {role}, регион: {self.region.value.strip()}, "
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


class InactiveModal(discord.ui.Modal, title="Инактив"):
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


class AfkModal(discord.ui.Modal, title="АФК"):
    reason = discord.ui.TextInput(label="Причина", placeholder="Например: отошёл поесть", max_length=200)
    minutes = discord.ui.TextInput(label="На сколько минут", placeholder="5", max_length=3, default="5")

    async def on_submit(self, interaction: discord.Interaction):
        try:
            mins = int(self.minutes.value.strip())
            assert 1 <= mins <= 720
        except (ValueError, AssertionError):
            await interaction.response.send_message("Минуты напиши числом от 1 до 720.", ephemeral=True)
            return
        until = time.time() + mins * 60
        await set_status(interaction, "афк", self.reason.value.strip(), until)
        await interaction.response.send_message(
            f"⏳ АФК на {mins} мин (до {dt.datetime.fromtimestamp(until, TZ):%H:%M}). "
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

    @discord.ui.button(label="Кто играет", style=discord.ButtonStyle.primary, custom_id="status:who", row=1)
    async def who(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_admin(interaction):
            await interaction.response.send_message("Список видят только админы.", ephemeral=True)
            return
        await send_who_plays(interaction)

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
        self.add_view(StatusView())
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

    async def on_raw_reaction_add(self, payload):
        await apply_check_reaction(self, payload, added=True)

    async def on_raw_reaction_remove(self, payload):
        await apply_check_reaction(self, payload, added=False)

    @tasks.loop(time=REPORT_TIME)
    async def daily_report(self):
        channel = self.get_channel(CHANNEL_ID) or await self.fetch_channel(CHANNEL_ID)
        await post_report(channel)

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
