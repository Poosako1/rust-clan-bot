#!/usr/bin/env bash
# Настройка бота: задаёт вопросы и сам пишет .env. Запуск: bash /opt/rust-clan-bot/setup.sh
set -euo pipefail
cd "$(dirname "$0")"

ask() {   # ask "вопрос" [secret]
  local v
  if [ "${2:-}" = "secret" ]; then read -r -s -p "$1: " v; echo >&2; else read -r -p "$1: " v; fi
  echo "$v"
}

echo "Вставляй значения правой кнопкой / долгим тапом и жми Enter. Токены при вводе не видны, это нормально."
DISCORD_TOKEN=$(ask "1/4 Токен бота Дискорда" secret)
CHANNEL_ID=$(ask "2/4 ID канала для сводки")
SCRIPT_URL=$(ask "3/4 Адрес веб-приложения из таблицы (https://script.google.com/...)")
SCRIPT_SECRET=$(ask "4/4 Секрет из скрипта таблицы" secret)

umask 077
cat > .env <<EOF
DISCORD_TOKEN=$DISCORD_TOKEN
CHANNEL_ID=$CHANNEL_ID
SCRIPT_URL=$SCRIPT_URL
SCRIPT_SECRET=$SCRIPT_SECRET
BM_TOKEN=
EOF
chmod 600 .env

systemctl restart rust-bot
sleep 4
systemctl --no-pager --lines=8 status rust-bot
echo
echo "Если выше написано active (running), бот запущен. Логи: journalctl -u rust-bot -n 30 --no-pager"
