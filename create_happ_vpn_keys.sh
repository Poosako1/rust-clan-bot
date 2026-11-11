#!/usr/bin/env bash
# Добавляет к существующему VLESS + REALITY серверу Happ:
# - 3 ключа на 72 часа;
# - 7 бессрочных ключей.
# Временные ключи удаляются автоматически systemd-таймером.
#
# Запуск:
#   sudo bash /opt/rust-clan-bot/create_happ_vpn_keys.sh
set -Eeuo pipefail

readonly XRAY_CONFIG="/usr/local/etc/xray/config.json"
readonly REGISTRY_FILE="/usr/local/etc/xray/happ-users.json"
readonly RESULT_FILE="/root/happ-vpn-family-keys.txt"
readonly MANAGER_SCRIPT="/usr/local/sbin/happ-vpn-keys"
readonly EXPIRE_SERVICE="/etc/systemd/system/happ-vpn-expire.service"
readonly EXPIRE_TIMER="/etc/systemd/system/happ-vpn-expire.timer"
readonly MODE="${1:-create}"

candidate_config=""
candidate_registry=""
spec_file=""

fail() {
  echo "ОШИБКА: $*" >&2
  exit 1
}

cleanup_files() {
  rm -f "${candidate_config:-}" "${candidate_registry:-}" "${spec_file:-}"
}
trap cleanup_files EXIT

if [ "${EUID}" -ne 0 ]; then
  fail "запусти через sudo"
fi

command -v xray >/dev/null 2>&1 || fail "Xray не установлен"
command -v python3 >/dev/null 2>&1 || fail "Python 3 не установлен"
command -v systemctl >/dev/null 2>&1 || fail "нужен systemd"
[ -s "${XRAY_CONFIG}" ] || fail "не найден ${XRAY_CONFIG}"

umask 077
candidate_config="$(mktemp --suffix=.json)"
candidate_registry="$(mktemp --suffix=.json)"

if [ "${MODE}" = "--cleanup" ]; then
  changed="$(
    python3 - "${XRAY_CONFIG}" "${REGISTRY_FILE}" \
      "${candidate_config}" "${candidate_registry}" <<'PY'
import json
import os
import sys
import time

config_path, registry_path, config_out, registry_out = sys.argv[1:]
with open(config_path, "r", encoding="utf-8") as file:
    config = json.load(file)

if os.path.exists(registry_path):
    with open(registry_path, "r", encoding="utf-8") as file:
        registry = json.load(file)
else:
    registry = {"users": []}

now = int(time.time())
users = list(registry.get("users", []))
expired = {
    str(user.get("id"))
    for user in users
    if int(user.get("expires_at") or 0) > 0
    and int(user.get("expires_at") or 0) <= now
}
if expired:
    for inbound in config.get("inbounds", []):
        settings = inbound.get("settings") or {}
        clients = list(settings.get("clients") or [])
        settings["clients"] = [
            client for client in clients
            if str(client.get("id")) not in expired
        ]
    registry["users"] = [
        user for user in users if str(user.get("id")) not in expired
    ]

with open(config_out, "w", encoding="utf-8") as file:
    json.dump(config, file, ensure_ascii=False, indent=2)
    file.write("\n")
with open(registry_out, "w", encoding="utf-8") as file:
    json.dump(registry, file, ensure_ascii=False, indent=2)
    file.write("\n")
print(len(expired))
PY
  )"

  if [ "${changed}" -eq 0 ]; then
    exit 0
  fi

  xray run -test -config "${candidate_config}"
  backup_config="${XRAY_CONFIG}.backup-expire-$(date +%Y%m%d-%H%M%S)"
  cp -a "${XRAY_CONFIG}" "${backup_config}"
  install -o xray -g xray -m 600 "${candidate_config}" "${XRAY_CONFIG}"
  if ! systemctl restart xray || ! systemctl is-active --quiet xray; then
    cp -a "${backup_config}" "${XRAY_CONFIG}"
    systemctl restart xray || true
    fail "не удалось применить удаление временных ключей; конфигурация восстановлена"
  fi
  install -o root -g root -m 600 "${candidate_registry}" "${REGISTRY_FILE}"
  echo "Удалено просроченных VPN-ключей: ${changed}"
  exit 0
fi

if [ "${MODE}" != "create" ]; then
  fail "неизвестный режим: ${MODE}"
fi

echo "[1/6] Создаю 10 отдельных пользователей..."
created_at="$(date +%s)"
trial_expires_at="$((created_at + 3 * 86400))"
batch="$(date -u +%Y%m%d-%H%M%S)"
spec_file="$(mktemp)"

for number in 1 2 3; do
  client_id="$(xray uuid)"
  printf '%s\t%s\t%s\t%s\n' \
    "${client_id}" "family-trial-${number}-${batch}" \
    "${trial_expires_at}" "CL-Family-3d-${number}" >>"${spec_file}"
done
for number in 1 2 3 4 5 6 7; do
  client_id="$(xray uuid)"
  printf '%s\t%s\t0\t%s\n' \
    "${client_id}" "family-permanent-${number}-${batch}" \
    "CL-Family-Permanent-${number}" >>"${spec_file}"
done

echo "[2/6] Добавляю пользователей в существующую конфигурацию..."
python3 - "${XRAY_CONFIG}" "${REGISTRY_FILE}" \
  "${spec_file}" "${candidate_config}" "${candidate_registry}" \
  "${created_at}" <<'PY'
import json
import os
import sys

config_path, registry_path, spec_path, config_out, registry_out, created_at = sys.argv[1:]
with open(config_path, "r", encoding="utf-8") as file:
    config = json.load(file)

inbound = next(
    (
        item for item in config.get("inbounds", [])
        if item.get("protocol") == "vless"
        and (item.get("streamSettings") or {}).get("security") == "reality"
    ),
    None,
)
if inbound is None:
    raise SystemExit("в конфигурации не найден VLESS + REALITY inbound")

settings = inbound.setdefault("settings", {})
clients = settings.setdefault("clients", [])
known_ids = {str(client.get("id")) for client in clients}

if os.path.exists(registry_path):
    with open(registry_path, "r", encoding="utf-8") as file:
        registry = json.load(file)
else:
    registry = {"users": []}
registry_users = registry.setdefault("users", [])

with open(spec_path, "r", encoding="utf-8") as file:
    for raw_line in file:
        client_id, email, expires_at, name = raw_line.rstrip("\n").split("\t")
        if client_id in known_ids:
            raise SystemExit("обнаружен повтор UUID")
        clients.append({
            "id": client_id,
            "flow": "xtls-rprx-vision",
            "email": email,
        })
        registry_users.append({
            "id": client_id,
            "email": email,
            "name": name,
            "created_at": int(created_at),
            "expires_at": int(expires_at),
        })
        known_ids.add(client_id)

with open(config_out, "w", encoding="utf-8") as file:
    json.dump(config, file, ensure_ascii=False, indent=2)
    file.write("\n")
with open(registry_out, "w", encoding="utf-8") as file:
    json.dump(registry, file, ensure_ascii=False, indent=2)
    file.write("\n")
PY

echo "[3/6] Проверяю и применяю конфигурацию..."
xray run -test -config "${candidate_config}"
backup_config="${XRAY_CONFIG}.backup-users-$(date +%Y%m%d-%H%M%S)"
cp -a "${XRAY_CONFIG}" "${backup_config}"
install -o xray -g xray -m 600 "${candidate_config}" "${XRAY_CONFIG}"
if ! systemctl restart xray || ! systemctl is-active --quiet xray; then
  cp -a "${backup_config}" "${XRAY_CONFIG}"
  systemctl restart xray || true
  fail "Xray не запустился; предыдущая конфигурация восстановлена"
fi
install -o root -g root -m 600 "${candidate_registry}" "${REGISTRY_FILE}"

echo "[4/6] Получаю параметры подключения..."
mapfile -t reality_info < <(
  python3 - "${XRAY_CONFIG}" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as file:
    config = json.load(file)
inbound = next(
    item for item in config.get("inbounds", [])
    if item.get("protocol") == "vless"
    and (item.get("streamSettings") or {}).get("security") == "reality"
)
reality = inbound["streamSettings"]["realitySettings"]
print(inbound["port"])
print(reality["privateKey"])
print((reality.get("serverNames") or [""])[0])
print((reality.get("shortIds") or [""])[0])
PY
)
[ "${#reality_info[@]}" -eq 4 ] || fail "не удалось прочитать параметры REALITY"
vpn_port="${reality_info[0]}"
private_key="${reality_info[1]}"
server_name="${reality_info[2]}"
short_id="${reality_info[3]}"

key_output="$(xray x25519 -i "${private_key}" 2>/dev/null || true)"
public_key="$(printf '%s\n' "${key_output}" | awk -F': *' \
  'tolower($1) ~ /^(public ?key|password)/ {print $2; exit}')"
if [ -z "${public_key}" ] && [ -s /root/happ-vpn.txt ]; then
  public_key="$(grep -o 'pbk=[^&]*' /root/happ-vpn.txt | head -n1 | cut -d= -f2- || true)"
fi
[ -n "${public_key}" ] || fail "не удалось получить открытый ключ REALITY"

public_ip="${PUBLIC_IP:-}"
if [ -z "${public_ip}" ]; then
  public_ip="$(curl -4fsS --max-time 10 https://api.ipify.org 2>/dev/null || true)"
fi
if [ -z "${public_ip}" ]; then
  public_ip="$(ip -4 route get 1.1.1.1 2>/dev/null | awk \
    '{for (i=1;i<=NF;i++) if ($i=="src") {print $(i+1); exit}}')"
fi
[ -n "${public_ip}" ] || fail "не удалось определить публичный IP"

echo "[5/6] Записываю ссылки для Happ..."
if [ -s "${RESULT_FILE}" ]; then
  cp -a "${RESULT_FILE}" "${RESULT_FILE}.backup-$(date +%Y%m%d-%H%M%S)"
fi
{
  echo "VPN-ключи для Happ"
  echo "Созданы: $(date -u -d "@${created_at}" '+%Y-%m-%d %H:%M:%S UTC')"
  echo
  echo "=== 3 ключа на 3 дня ==="
  while IFS=$'\t' read -r client_id email expires_at name; do
    [ "${expires_at}" -gt 0 ] || continue
    echo
    echo "${name}"
    echo "Действует до: $(date -u -d "@${expires_at}" '+%Y-%m-%d %H:%M:%S UTC')"
    echo "vless://${client_id}@${public_ip}:${vpn_port}?encryption=none&flow=xtls-rprx-vision&security=reality&sni=${server_name}&fp=chrome&pbk=${public_key}&sid=${short_id}&type=tcp&headerType=none#${name}"
  done <"${spec_file}"
  echo
  echo
  echo "=== 7 бессрочных ключей ==="
  while IFS=$'\t' read -r client_id email expires_at name; do
    [ "${expires_at}" -eq 0 ] || continue
    echo
    echo "${name}"
    echo "Срок: бессрочный, пока ключ не удалён вручную"
    echo "vless://${client_id}@${public_ip}:${vpn_port}?encryption=none&flow=xtls-rprx-vision&security=reality&sni=${server_name}&fp=chrome&pbk=${public_key}&sid=${short_id}&type=tcp&headerType=none#${name}"
  done <"${spec_file}"
} >"${RESULT_FILE}"
chmod 600 "${RESULT_FILE}"

echo "[6/6] Включаю автоматическое отключение временных ключей..."
install -o root -g root -m 700 "$(readlink -f "$0")" "${MANAGER_SCRIPT}"
cat >"${EXPIRE_SERVICE}" <<EOF
[Unit]
Description=Remove expired Happ VPN users
After=network.target xray.service

[Service]
Type=oneshot
ExecStart=/bin/bash ${MANAGER_SCRIPT} --cleanup
EOF
cat >"${EXPIRE_TIMER}" <<'EOF'
[Unit]
Description=Check expired Happ VPN users

[Timer]
OnBootSec=2min
OnUnitActiveSec=5min
Persistent=true

[Install]
WantedBy=timers.target
EOF
chmod 644 "${EXPIRE_SERVICE}" "${EXPIRE_TIMER}"
systemctl daemon-reload
systemctl enable --now happ-vpn-expire.timer

echo
echo "Готово: 3 временных и 7 бессрочных ключей созданы."
echo "Текущий ключ VPN не изменён."
echo "Ссылки сохранены в ${RESULT_FILE}"
echo
echo "Показать ключи:"
echo "sudo cat ${RESULT_FILE}"
