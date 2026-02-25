#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-/root/sb-bot-panel}"
ENV_FILE="${PROJECT_DIR}/.env"
MIGRATE_DIR="/var/backups/sb-migrate"
INCLUDE_CONTROLLER_BACKUPS="N"

msg() { echo -e "\033[1;32m[信息]\033[0m $*"; }
warn() { echo -e "\033[1;33m[警告]\033[0m $*"; }
err() { echo -e "\033[1;31m[错误]\033[0m $*" >&2; }

require_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    err "请用 root 权限运行。"
    exit 1
  fi
}

ask_yes_no() {
  local prompt="$1"
  local default="${2:-Y}"
  local answer
  local hint="[Y/n]"
  if [[ "$default" == "N" ]]; then
    hint="[y/N]"
  fi
  read -r -p "$prompt $hint: " answer
  answer="${answer:-$default}"
  [[ "$answer" =~ ^[Yy]$ ]]
}

get_env_value() {
  local key="$1"
  if [[ -f "$ENV_FILE" ]]; then
    grep -E "^${key}=" "$ENV_FILE" | head -n1 | cut -d= -f2- || true
  fi
}

stop_services() {
  systemctl stop sb-bot 2>/dev/null || true
  systemctl stop sb-controller 2>/dev/null || true
}

start_services() {
  systemctl start sb-controller 2>/dev/null || true
  systemctl start sb-bot 2>/dev/null || true
}

main() {
  require_root

  local input_project
  read -r -p "项目目录 [${PROJECT_DIR}]: " input_project
  PROJECT_DIR="${input_project:-$PROJECT_DIR}"
  ENV_FILE="${PROJECT_DIR}/.env"

  if [[ ! -d "$PROJECT_DIR" ]]; then
    err "项目目录不存在: $PROJECT_DIR"
    exit 1
  fi

  local env_migrate
  env_migrate="$(get_env_value MIGRATE_DIR)"
  if [[ -n "$env_migrate" ]]; then
    MIGRATE_DIR="$env_migrate"
  fi
  mkdir -p "$MIGRATE_DIR"

  if ask_yes_no "是否包含 /var/backups/sb-controller 历史备份（可能较大）？" "N"; then
    INCLUDE_CONTROLLER_BACKUPS="Y"
  fi

  msg "停止服务，避免导出期间数据变更..."
  stop_services

  local ts pkg_name pkg_path stage_dir
  ts="$(date +%Y%m%d-%H%M%S)"
  pkg_name="sb-migrate-${ts}.tar.gz"
  pkg_path="${MIGRATE_DIR}/${pkg_name}"
  stage_dir="$(mktemp -d /tmp/sb-migrate-export-XXXXXX)"
  trap 'rm -rf "$stage_dir"' EXIT

  mkdir -p "${stage_dir}/sb-bot-panel"

  if [[ -d "${PROJECT_DIR}/data" ]]; then
    cp -a "${PROJECT_DIR}/data" "${stage_dir}/sb-bot-panel/"
  fi
  if [[ -f "${PROJECT_DIR}/.env" ]]; then
    cp -a "${PROJECT_DIR}/.env" "${stage_dir}/sb-bot-panel/"
  fi
  if [[ -d "${PROJECT_DIR}/scripts" ]]; then
    cp -a "${PROJECT_DIR}/scripts" "${stage_dir}/sb-bot-panel/"
  fi

  mkdir -p "${stage_dir}/systemd"
  [[ -f /etc/systemd/system/sb-controller.service ]] && cp -a /etc/systemd/system/sb-controller.service "${stage_dir}/systemd/"
  [[ -f /etc/systemd/system/sb-bot.service ]] && cp -a /etc/systemd/system/sb-bot.service "${stage_dir}/systemd/"

  if [[ "$INCLUDE_CONTROLLER_BACKUPS" == "Y" && -d /var/backups/sb-controller ]]; then
    cp -a /var/backups/sb-controller "${stage_dir}/"
  fi

  tar -czf "$pkg_path" -C "$stage_dir" .
  local size_bytes
  size_bytes="$(stat -c%s "$pkg_path" 2>/dev/null || stat -f%z "$pkg_path")"

  msg "迁移包已生成: $pkg_path"
  msg "文件大小: ${size_bytes} bytes"
  echo ""
  echo "示例传输命令："
  echo "scp root@旧服务器IP:${pkg_path} root@新服务器IP:/root/"
  echo ""

  if ask_yes_no "是否立即重新启动 sb-controller 与 sb-bot 服务？" "Y"; then
    start_services
    msg "服务已启动。"
  else
    warn "你选择了不启动服务，请自行恢复。"
  fi
}

main "$@"
