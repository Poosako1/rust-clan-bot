#!/usr/bin/env bash
# Устанавливает отдельный VLESS + REALITY сервер для приложения Happ.
# Discord-бот и его systemd-служба не изменяются.
# Запуск: sudo bash /opt/rust-clan-bot/install_happ_vpn.sh
# Другой порт: sudo VPN_PORT=8443 bash /opt/rust-clan-bot/install_happ_vpn.sh
set -Eeuo pipefail

readonly XRAY_CONFIG="/usr/local/etc/xray/config.json"
readonly RESULT_FILE="/root/happ-vpn.txt"
readonly INSTALL_URL="https://github.com/XTLS/Xray-install/raw/main/install-release.sh"
readonly VPN_PORT="${VPN_PORT:-443}"
readonly REALITY_HOST="${REALITY_HOST:-www.microsoft.com}"
readonly ALLOW_REPLACE_XRAY="${ALLOW_REPLACE_XRAY:-0}"

fail() {
  echo "ОШИБКА: $*" >&2
  exit 1
}

if [ "${EUID}" -ne 0 ]; then
  fail "запусти скрипт через sudo: sudo bash $0"
fi

command -v systemctl >/dev/null 2>&1 || fail "нужен Linux с systemd"
command -v apt-get >/dev/null 2>&1 || fail "поддерживаются Ubuntu и Debian"

case "${VPN_PORT}" in
  ''|*[!0-9]*) fail "VPN_PORT должен быть числом" ;;
esac
if [ "${VPN_PORT}" -lt 1 ] || [ "${VPN_PORT}" -gt 65535 ]; then
  fail "VPN_PORT должен быть от 1 до 65535"
fi

echo "[1/7] Устанавливаю системные зависимости..."
apt-get update -y
DEBIAN_FRONTEND=noninteractive apt-get install -y ca-certificates curl openssl iproute2

if ss -H -ltn "sport = :${VPN_PORT}" 2>/dev/null | grep -q .; then
  echo "Порт ${VPN_PORT}/tcp уже занят:"
  ss -ltnp "sport = :${VPN_PORT}" || true
  fail "освободи порт или запусти, например: sudo VPN_PORT=8443 bash $0"
fi

if [ -s "${XRAY_CONFIG}" ]; then
  if [ "${ALLOW_REPLACE_XRAY}" != "1" ]; then
    fail "Xray уже установлен. Для продолжения после прерванной первой установки запусти: sudo ALLOW_REPLACE_XRAY=1 bash $0"
  fi
  backup_config="${XRAY_CONFIG}.backup-$(date +%Y%m%d-%H%M%S)"
  cp -a "${XRAY_CONFIG}" "${backup_config}"
  echo "Существующая конфигурация сохранена: ${backup_config}"
fi

echo "[2/7] Загружаю официальный установщик Xray..."
installer_file="$(mktemp)"
trap 'rm -f "${installer_file:-}" "${candidate_config:-}"' EXIT
curl --proto '=https' --tlsv1.2 -fsSL "${INSTALL_URL}" -o "${installer_file}"
chmod 700 "${installer_file}"

echo "[3/7] Устанавливаю Xray-core..."
bash "${installer_file}" install
command -v xray >/dev/null 2>&1 || fail "Xray не установился"

echo "[4/7] Создаю персональные ключи..."
client_id="$(xray uuid)"
key_output="$(xray x25519)"
private_key="$(printf '%s\n' "${key_output}" | awk -F': *' 'tolower($1) ~ /^private ?key$/ {print $2; exit}')"
public_key="$(printf '%s\n' "${key_output}" | awk -F': *' 'tolower($1) ~ /^(public ?key|password)/ {print $2; exit}')"
short_id="$(openssl rand -hex 8)"

[ -n "${client_id}" ] || fail "не удалось создать UUID"
[ -n "${private_key}" ] || fail "не удалось получить закрытый ключ REALITY"
[ -n "${public_key}" ] || fail "не удалось получить открытый ключ REALITY"

echo "[5/7] Записываю конфигурацию..."
install -d -m 755 "$(dirname "${XRAY_CONFIG}")"
candidate_config="$(mktemp --suffix=.json)"
cat >"${candidate_config}" <<JSON
{
  "log": {
    "loglevel": "warning"
  },
  "inbounds": [
    {
      "listen": "0.0.0.0",
      "port": ${VPN_PORT},
      "protocol": "vless",
      "settings": {
        "clients": [
          {
            "id": "${client_id}",
            "flow": "xtls-rprx-vision",
            "email": "happ"
          }
        ],
        "decryption": "none"
      },
      "streamSettings": {
        "network": "tcp",
        "security": "reality",
        "realitySettings": {
          "show": false,
          "target": "${REALITY_HOST}:443",
          "xver": 0,
          "serverNames": ["${REALITY_HOST}"],
          "privateKey": "${private_key}",
          "shortIds": ["${short_id}"]
        }
      },
      "sniffing": {
        "enabled": true,
        "destOverride": ["http", "tls", "quic"],
        "routeOnly": true
      }
    }
  ],
  "outbounds": [
    {
      "protocol": "freedom",
      "tag": "direct"
    },
    {
      "protocol": "blackhole",
      "tag": "blocked"
    }
  ]
}
JSON

xray run -test -config "${candidate_config}"
install -m 600 "${candidate_config}" "${XRAY_CONFIG}"

echo "[6/7] Открываю порт и запускаю службу..."
if command -v ufw >/dev/null 2>&1 && ufw status 2>/dev/null | grep -q '^Status: active'; then
  ufw allow "${VPN_PORT}/tcp"
fi
systemctl enable xray
systemctl restart xray
sleep 2
systemctl is-active --quiet xray || {
  journalctl -u xray -n 30 --no-pager >&2 || true
  fail "служба Xray не запустилась"
}

public_ip="${PUBLIC_IP:-}"
if [ -z "${public_ip}" ]; then
  public_ip="$(curl -4fsS --max-time 10 https://api.ipify.org 2>/dev/null || true)"
fi
if [ -z "${public_ip}" ]; then
  public_ip="$(ip -4 route get 1.1.1.1 2>/dev/null | awk '{for (i=1;i<=NF;i++) if ($i=="src") {print $(i+1); exit}}')"
fi
[ -n "${public_ip}" ] || fail "не удалось определить публичный IP; повтори с PUBLIC_IP=твой_IP"

vpn_name="CL-Happ"
vpn_uri="vless://${client_id}@${public_ip}:${VPN_PORT}?encryption=none&flow=xtls-rprx-vision&security=reality&sni=${REALITY_HOST}&fp=chrome&pbk=${public_key}&sid=${short_id}&type=tcp&headerType=none#${vpn_name}"

umask 077
cat >"${RESULT_FILE}" <<EOF
Готовая ссылка для Happ:
${vpn_uri}

Служба: systemctl status xray
Логи: journalctl -u xray -n 50 --no-pager
EOF
chmod 600 "${RESULT_FILE}"

echo "[7/7] Готово. Discord-бот не изменялся."
echo
echo "Скопируй в Happ эту строку:"
echo "${vpn_uri}"
echo
echo "Она также сохранена в ${RESULT_FILE} (доступ только root)."
