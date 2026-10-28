#!/usr/bin/env bash
# Автообновление: вызывается таймером systemd раз в минуту.
# Если на GitHub появились новые коммиты — скачивает их и перезапускает бота.
set -euo pipefail
cd "$(dirname "$0")"

git fetch -q origin main
if [ "$(git rev-parse HEAD)" != "$(git rev-parse origin/main)" ]; then
  logger -t rust-bot-update "найдены новые коммиты, обновляю"
  git pull --ff-only -q
  # новый процесс читает уже обновлённый update.sh
  exec bash ./update.sh
fi
