#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-/root/sb-bot-panel}"
ENV_FILE="${PROJECT_DIR}/.env"
ARCHIVE_DIR_DEFAULT="/var/backups/sb-controller/logs"
WINDOW_HOURS_DEFAULT="24"
KEEP_COUNT_DEFAULT="30"

WINDOW_HOURS="${WINDOW_HOURS:-$WINDOW_HOURS_DEFAULT}"
KEEP_COUNT="${KEEP_COUNT:-$KEEP_COUNT_DEFAULT}"
ARCHIVE_DIR="${ARCHIVE_DIR:-$ARCHIVE_DIR_DEFAULT}"

if [[ -f "$ENV_FILE" ]]; then
  while IFS='=' read -r raw_key raw_value; do
    key="$(echo "${raw_key:-}" | tr -d '[:space:]')"
    value="${raw_value:-}"
    case "$key" in
      LOG_ARCHIVE_WINDOW_HOURS)
        if [[ -z "${WINDOW_HOURS:-}" || "${WINDOW_HOURS}" == "$WINDOW_HOURS_DEFAULT" ]]; then
          WINDOW_HOURS="$value"
        fi
        ;;
      LOG_ARCHIVE_RETENTION_COUNT)
        if [[ -z "${KEEP_COUNT:-}" || "${KEEP_COUNT}" == "$KEEP_COUNT_DEFAULT" ]]; then
          KEEP_COUNT="$value"
        fi
        ;;
      LOG_ARCHIVE_DIR)
        if [[ -z "${ARCHIVE_DIR:-}" || "${ARCHIVE_DIR}" == "$ARCHIVE_DIR_DEFAULT" ]]; then
          ARCHIVE_DIR="$value"
        fi
        ;;
    esac
  done < <(grep -E '^(LOG_ARCHIVE_WINDOW_HOURS|LOG_ARCHIVE_RETENTION_COUNT|LOG_ARCHIVE_DIR)=' "$ENV_FILE" || true)
fi

if ! [[ "$WINDOW_HOURS" =~ ^[0-9]+$ ]] || (( WINDOW_HOURS < 1 )); then
  WINDOW_HOURS=24
fi
if (( WINDOW_HOURS > 720 )); then
  WINDOW_HOURS=720
fi
if ! [[ "$KEEP_COUNT" =~ ^[0-9]+$ ]] || (( KEEP_COUNT < 1 )); then
  KEEP_COUNT=30
fi
if (( KEEP_COUNT > 365 )); then
  KEEP_COUNT=365
fi
if [[ -z "${ARCHIVE_DIR:-}" ]]; then
  ARCHIVE_DIR="$ARCHIVE_DIR_DEFAULT"
fi

mkdir -p "$ARCHIVE_DIR"
tmp_dir="$(mktemp -d /tmp/sb-log-archive-XXXXXX)"
cleanup() {
  rm -rf "$tmp_dir"
}
trap cleanup EXIT

now_ts="$(date +%s)"
stamp="$(date +%Y%m%d-%H%M%S)"
archive_name="ops-logs-${stamp}.tar.gz"
archive_path="${ARCHIVE_DIR}/${archive_name}"

meta_file="${tmp_dir}/meta.txt"
{
  echo "generated_at=${now_ts}"
  echo "window_hours=${WINDOW_HOURS}"
  echo "host=$(hostname -f 2>/dev/null || hostname || echo unknown)"
  echo "kernel=$(uname -r 2>/dev/null || echo unknown)"
} > "$meta_file"

collect_unit() {
  local unit="$1"
  local out_file="$2"
  if systemctl list-unit-files | grep -q "^${unit}\.service"; then
    journalctl -u "$unit" --since "${WINDOW_HOURS} hours ago" --no-pager > "$out_file" 2>&1 || true
  else
    echo "unit_not_found=${unit}.service" > "$out_file"
  fi
}

collect_status() {
  local unit="$1"
  local out_file="$2"
  if systemctl list-unit-files | grep -q "^${unit}\.service"; then
    systemctl status "$unit" --no-pager -n 80 > "$out_file" 2>&1 || true
  else
    echo "unit_not_found=${unit}.service" > "$out_file"
  fi
}

collect_unit "sb-controller" "${tmp_dir}/journal-sb-controller.log"
collect_unit "sb-bot" "${tmp_dir}/journal-sb-bot.log"
collect_unit "caddy" "${tmp_dir}/journal-caddy.log"
collect_unit "fail2ban" "${tmp_dir}/journal-fail2ban.log"
collect_unit "ssh" "${tmp_dir}/journal-ssh.log"
collect_unit "sshd" "${tmp_dir}/journal-sshd.log"

collect_status "sb-controller" "${tmp_dir}/status-sb-controller.log"
collect_status "sb-bot" "${tmp_dir}/status-sb-bot.log"
collect_status "caddy" "${tmp_dir}/status-caddy.log"
collect_status "fail2ban" "${tmp_dir}/status-fail2ban.log"

tar -czf "$archive_path" -C "$tmp_dir" .
size_bytes="$(stat -c%s "$archive_path" 2>/dev/null || echo 0)"

cleaned_files=0
mapfile -t old_files < <(ls -1t "${ARCHIVE_DIR}"/ops-logs-*.tar.gz 2>/dev/null | tail -n +$((KEEP_COUNT + 1)) || true)
if (( ${#old_files[@]} > 0 )); then
  for file_path in "${old_files[@]}"; do
    [[ -z "$file_path" ]] && continue
    rm -f "$file_path" || true
    cleaned_files=$((cleaned_files + 1))
  done
fi

echo "OK=1"
echo "ARCHIVE_PATH=${archive_path}"
echo "SIZE_BYTES=${size_bytes}"
echo "CLEANED_FILES=${cleaned_files}"
echo "KEEP_COUNT=${KEEP_COUNT}"
echo "WINDOW_HOURS=${WINDOW_HOURS}"
echo "CREATED_AT=${now_ts}"
