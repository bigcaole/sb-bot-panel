#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-/root/sb-bot-panel}"
MIGRATE_DIR_DEFAULT="/var/backups/sb-migrate"
MIGRATE_DIR="$MIGRATE_DIR_DEFAULT"
ENV_FILE="${PROJECT_DIR}/.env"
VENV_DIR="${PROJECT_DIR}/venv"

CONTROLLER_PORT="8080"
CONTROLLER_URL=""
AUTH_TOKEN=""
BOT_TOKEN=""
ADMIN_CHAT_IDS=""

msg() { echo -e "\033[1;32m[信息]\033[0m $*"; }
warn() { echo -e "\033[1;33m[警告]\033[0m $*"; }
err() { echo -e "\033[1;31m[错误]\033[0m $*" >&2; }

require_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    err "请使用 root 权限运行。"
    exit 1
  fi
}

get_env_value() {
  local key="$1"
  if [[ -f "$ENV_FILE" ]]; then
    grep -E "^${key}=" "$ENV_FILE" | head -n1 | cut -d= -f2- || true
  fi
}

write_env_file() {
  cat >"$ENV_FILE" <<EOF
# 给 bot 调用 controller 的地址
CONTROLLER_URL=${CONTROLLER_URL}

# controller 监听端口（供 systemd 使用）
CONTROLLER_PORT=${CONTROLLER_PORT}

# 轻量鉴权 token（如未启用鉴权也可保留）
AUTH_TOKEN=${AUTH_TOKEN}

# Telegram Bot token（必填）
BOT_TOKEN=${BOT_TOKEN}

# 管理员 chat id，逗号分隔，可空
ADMIN_CHAT_IDS=${ADMIN_CHAT_IDS}

# 迁移包目录
MIGRATE_DIR=${MIGRATE_DIR}
EOF
  chmod 0600 "$ENV_FILE"
}

detect_public_ip() {
  curl -4 -fsSL ifconfig.me 2>/dev/null \
    || curl -4 -fsSL https://api.ipify.org 2>/dev/null \
    || true
}

install_dependencies() {
  msg "安装基础依赖..."
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -y
  apt-get install -y \
    curl \
    jq \
    git \
    python3 \
    python3-venv \
    python3-pip \
    ca-certificates \
    ufw
}

setup_venv() {
  if [[ ! -f "$PROJECT_DIR/requirements.txt" ]]; then
    err "缺少 requirements.txt，请先确保项目代码完整。"
    exit 1
  fi
  if [[ ! -d "$VENV_DIR" ]]; then
    python3 -m venv "$VENV_DIR"
  fi
  "$VENV_DIR/bin/pip" install --upgrade pip setuptools wheel >/dev/null
  "$VENV_DIR/bin/pip" install -r "$PROJECT_DIR/requirements.txt"
}

write_services() {
  cat >/etc/systemd/system/sb-controller.service <<EOF
[Unit]
Description=sb-controller service
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=${ENV_FILE}
WorkingDirectory=${PROJECT_DIR}
ExecStart=${VENV_DIR}/bin/uvicorn controller.app:app --host 0.0.0.0 --port ${CONTROLLER_PORT}
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
EOF

  cat >/etc/systemd/system/sb-bot.service <<EOF
[Unit]
Description=sb-bot service
After=network-online.target sb-controller.service
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=${ENV_FILE}
WorkingDirectory=${PROJECT_DIR}
ExecStart=${VENV_DIR}/bin/python bot/bot.py
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
EOF
}

main() {
  require_root

  local pkg_path="${1:-}"
  if [[ -z "$pkg_path" ]]; then
    read -r -p "请输入迁移包路径（.tar.gz）: " pkg_path
  fi
  if [[ ! -f "$pkg_path" ]]; then
    err "迁移包不存在: $pkg_path"
    exit 1
  fi

  local input_project
  read -r -p "目标项目目录 [${PROJECT_DIR}]: " input_project
  PROJECT_DIR="${input_project:-$PROJECT_DIR}"
  ENV_FILE="${PROJECT_DIR}/.env"
  VENV_DIR="${PROJECT_DIR}/venv"

  mkdir -p "$MIGRATE_DIR_DEFAULT"

  msg "停止服务..."
  systemctl stop sb-bot 2>/dev/null || true
  systemctl stop sb-controller 2>/dev/null || true

  local ts
  ts="$(date +%Y%m%d-%H%M%S)"
  if [[ -d "$PROJECT_DIR" ]]; then
    local project_backup
    project_backup="${MIGRATE_DIR_DEFAULT}/restore-backup-${ts}.tar.gz"
    msg "备份当前项目到: $project_backup"
    tar -czf "$project_backup" -C "$(dirname "$PROJECT_DIR")" "$(basename "$PROJECT_DIR")"
  fi

  local extract_dir
  extract_dir="$(mktemp -d /tmp/sb-migrate-import-XXXXXX)"
  trap 'rm -rf "$extract_dir"' EXIT
  tar -xzf "$pkg_path" -C "$extract_dir"

  mkdir -p "$PROJECT_DIR"
  if [[ -d "$extract_dir/sb-bot-panel/data" ]]; then
    rm -rf "$PROJECT_DIR/data"
    cp -a "$extract_dir/sb-bot-panel/data" "$PROJECT_DIR/"
    msg "已恢复 data/"
  fi
  if [[ -f "$extract_dir/sb-bot-panel/.env" ]]; then
    cp -a "$extract_dir/sb-bot-panel/.env" "$PROJECT_DIR/.env"
    msg "已恢复 .env"
  fi
  if [[ -d "$extract_dir/sb-bot-panel/scripts" ]]; then
    rm -rf "$PROJECT_DIR/scripts"
    cp -a "$extract_dir/sb-bot-panel/scripts" "$PROJECT_DIR/"
    msg "已恢复 scripts/"
  fi

  if [[ -f "$extract_dir/systemd/sb-controller.service" ]]; then
    cp -a "$extract_dir/systemd/sb-controller.service" /etc/systemd/system/sb-controller.service
  fi
  if [[ -f "$extract_dir/systemd/sb-bot.service" ]]; then
    cp -a "$extract_dir/systemd/sb-bot.service" /etc/systemd/system/sb-bot.service
  fi

  CONTROLLER_PORT="$(get_env_value CONTROLLER_PORT)"
  CONTROLLER_PORT="${CONTROLLER_PORT:-8080}"
  AUTH_TOKEN="$(get_env_value AUTH_TOKEN)"
  AUTH_TOKEN="${AUTH_TOKEN:-devtoken123}"
  BOT_TOKEN="$(get_env_value BOT_TOKEN)"
  ADMIN_CHAT_IDS="$(get_env_value ADMIN_CHAT_IDS)"
  MIGRATE_DIR="$(get_env_value MIGRATE_DIR)"
  MIGRATE_DIR="${MIGRATE_DIR:-$MIGRATE_DIR_DEFAULT}"

  local public_ip
  public_ip="$(detect_public_ip)"
  msg "检测到公网 IP: ${public_ip:-未知}"

  local default_controller_url
  default_controller_url="http://127.0.0.1:${CONTROLLER_PORT}"
  CONTROLLER_URL="$(get_env_value CONTROLLER_URL)"
  CONTROLLER_URL="${CONTROLLER_URL:-$default_controller_url}"
  read -r -p "CONTROLLER_URL [${CONTROLLER_URL}]（建议本机回环地址）: " input_url
  CONTROLLER_URL="${input_url:-$CONTROLLER_URL}"
  CONTROLLER_URL="${CONTROLLER_URL%/}"

  read -r -p "BOT_TOKEN [保持现值请回车]: " input_bot
  BOT_TOKEN="${input_bot:-$BOT_TOKEN}"
  while [[ -z "$BOT_TOKEN" ]]; do
    warn "BOT_TOKEN 不能为空。"
    read -r -p "请重新输入 BOT_TOKEN: " BOT_TOKEN
  done

  read -r -p "ADMIN_CHAT_IDS [${ADMIN_CHAT_IDS}]: " input_admin
  ADMIN_CHAT_IDS="${input_admin:-$ADMIN_CHAT_IDS}"

  mkdir -p "$PROJECT_DIR"
  write_env_file

  install_dependencies
  setup_venv
  write_services

  systemctl daemon-reload
  systemctl enable sb-controller >/dev/null
  systemctl enable sb-bot >/dev/null
  systemctl restart sb-controller
  systemctl restart sb-bot

  echo ""
  msg "自检开始..."
  if curl -fsSL "http://127.0.0.1:${CONTROLLER_PORT}/health" >/dev/null 2>&1; then
    msg "controller /health 检查通过。"
  else
    warn "controller /health 检查失败，请查看日志。"
  fi
  systemctl status sb-bot --no-pager || true
}

main "$@"
