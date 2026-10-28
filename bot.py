import os
import asyncio
import datetime as dt

import aiohttp
import discord
from discord.ext import tasks

# ===== НАСТРОЙКИ =====
DISCORD_TOKEN = os.environ["DISCORD_TOKEN"]
BM_TOKEN = os.environ["BM_TOKEN"]            # battlemetrics.com/developers
CHANNEL_ID = int(os.environ["CHANNEL_ID"])   # канал, куда слать отчёт

MIN_HOURS = 8        # норма
WINDOW_HOURS = 24    # за какой период считать (24 = сутки, 168 = неделя)
REPORT_TIME = dt.time(hour=9, minute=0, tzinfo=dt.timezone.utc)  # во сколько слать (UTC)

# Игроки клана: ник -> BattleMetrics player ID
# ID берётся из ссылки: battlemetrics.com/players/123456789
CLAN = {
    "Vasya": 123456789,
    "Petya": 987654321,
}

# ID серверов (battlemetrics.com/servers/rust/XXXXXXX). Пусто = считать все сервера
SERVER_IDS: set[str] = set()
# ======================

API = "https://api.battlemetrics.com"
HEADERS = {"Authorization": f"Bearer {BM_TOKEN}"}


def parse_ts(s: str) -> dt.datetime:
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))


async def hours_played(session: aiohttp.ClientSession, player_id: int,
                       since: dt.datetime, now: dt.datetime) -> float:
    total = 0.0
    url = f"{API}/players/{player_id}/relationships/sessions"
    params = {"page[size]": 100}

    while url:
        async with session.get(url, params=params, headers=HEADERS) as r:
            if r.status == 429:
                await asyncio.sleep(5)
                continue
            r.raise_for_status()
            data = await r.json()

        stop_paging = False
        for s in data.get("data", []):
            a = s["attributes"]
            start = parse_ts(a["start"])
            stop = parse_ts(a["stop"]) if a.get("stop") else now

            if stop < since:
                stop_paging = True  # сессии идут от новых к старым
                continue

            if SERVER_IDS:
                srv = s.get("relationships", {}).get("server", {}).get("data", {}).get("id")
                if srv not in SERVER_IDS:
                    continue

            overlap = (min(stop, now) - max(start, since)).total_seconds()
            if overlap > 0:
                total += overlap / 3600

        url = None if stop_paging else data.get("links", {}).get("next")
        params = None
        await asyncio.sleep(0.5)  # не долбим API

    return total


class Bot(discord.Client):
    async def setup_hook(self):
        self.daily_report.start()

    @tasks.loop(time=REPORT_TIME)
    async def daily_report(self):
        channel = self.get_channel(CHANNEL_ID)
        now = dt.datetime.now(dt.timezone.utc)
        since = now - dt.timedelta(hours=WINDOW_HOURS)

        lazy = []
        async with aiohttp.ClientSession() as session:
            for name, pid in CLAN.items():
                try:
                    h = await hours_played(session, pid, since, now)
                except Exception as e:
                    print(f"Ошибка для {name}: {e}")
                    continue
                if h < MIN_HOURS:
                    lazy.append((name, h))

        if not lazy:
            await channel.send(f"✅ За последние {WINDOW_HOURS} ч все выполнили норму ({MIN_HOURS} ч).")
            return

        lazy.sort(key=lambda x: x[1])
        lines = [f"• **{n}** — {h:.1f} ч" for n, h in lazy]
        await channel.send(
            f"⚠️ Не набрали {MIN_HOURS} ч за последние {WINDOW_HOURS} ч:\n" + "\n".join(lines)
        )


intents = discord.Intents.default()
Bot(intents=intents).run(DISCORD_TOKEN)
