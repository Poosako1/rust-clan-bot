#!/usr/bin/env bash
# Обновление бота из GitHub. Вручную: bash /opt/rust-clan-bot/update.sh
# (автоматически его запускает таймер rust-bot-update)
set -euo pipefail
cd "$(dirname "$0")"

main() {
  git pull --ff-only
  ./venv/bin/pip install -q -r requirements.txt
  cp rust-bot.service /etc/systemd/system/rust-bot.service
  cp rust-bot-update.service rust-bot-update.timer /etc/systemd/system/
  systemctl daemon-reload
  systemctl enable --now rust-bot-update.timer
  systemctl restart rust-bot
  sleep 2
  systemctl --no-pager --lines=5 status rust-bot
}
# функция читается целиком до выполнения, поэтому git pull не ломает работающий скрипт
main
exit
