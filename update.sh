#!/usr/bin/env bash
# Обновление бота из GitHub. Запускать на сервере: bash /opt/rust-clan-bot/update.sh
set -euo pipefail
cd "$(dirname "$0")"

git pull --ff-only
./venv/bin/pip install -q -r requirements.txt
cp rust-bot.service /etc/systemd/system/rust-bot.service
systemctl daemon-reload
systemctl restart rust-bot
sleep 2
systemctl --no-pager --lines=5 status rust-bot
