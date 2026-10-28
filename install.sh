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
cp rust-bot-update.service rust-bot-update.timer /etc/systemd/system/
systemctl daemon-reload
systemctl enable rust-bot
systemctl enable --now rust-bot-update.timer
echo ">>> Готово. После заполнения .env запусти: systemctl restart rust-bot"
echo ">>> Автообновление включено: сервер сам проверяет GitHub раз в минуту."
