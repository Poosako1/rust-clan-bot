#!/usr/bin/env bash
# Первичная установка на сервере. Запускать из папки проекта: bash install.sh
set -euo pipefail
cd "$(dirname "$0")"

apt-get update -y
apt-get install -y python3-venv git

python3 -m venv venv
./venv/bin/pip install --upgrade pip
./venv/bin/pip install -r requirements.txt

if [ ! -f .env ]; then
  cp .env.example .env
  chmod 600 .env
  echo ">>> Заполни токены: nano $(pwd)/.env"
fi

cp rust-bot.service /etc/systemd/system/rust-bot.service
systemctl daemon-reload
systemctl enable rust-bot
echo ">>> Готово. После заполнения .env запусти: systemctl restart rust-bot"
