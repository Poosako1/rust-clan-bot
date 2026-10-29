#!/usr/bin/env bash
# Включает текстовый редактор: спрашивает ключ OpenRouter и ID канала, пишет их в .env и перезапускает бота.
# Запуск: bash /opt/rust-clan-bot/setup_editor.sh
set -euo pipefail
cd "$(dirname "$0")"
read -r -s -p "Ключ OpenRouter (при вводе не виден): " KEY; echo
read -r -p "ID канала-редактора: " CH
sed -i '/^OPENROUTER_KEY=/d;/^EDITOR_CHANNEL_ID=/d' .env
printf 'OPENROUTER_KEY=%s\nEDITOR_CHANNEL_ID=%s\n' "$KEY" "$CH" >> .env
chmod 600 .env
systemctl restart rust-bot
sleep 4
systemctl --no-pager --lines=8 status rust-bot
echo
echo "Если active (running) и в логе нет PrivilegedIntentsRequired, редактор включён."
