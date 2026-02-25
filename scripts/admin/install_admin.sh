#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-install}"
if [[ "$MODE" != "install" && "$MODE" != "--configure-only" ]]; then
  echo "用法:"
  echo "  sudo bash scripts/admin/install_admin.sh"
  echo "  sudo bash scripts/admin/install_admin.sh --configure-only"
  exit 1
fi
if [[ "$MODE" == "--configure-only" ]]; then
  MODE="configure-only"
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

DEFAULT_PROJECT_DIR="/root/sb-bot-panel"
PROJECT_DIR="${PROJECT_DIR:-$DEFAULT_PROJECT_DIR}"
VENV_DIR=""
ENV_FILE=""
MIGRATE_DIR_DEFAULT="/var/backups/sb-migrate"

CONTROLLER_PORT="8080"
CONTROLLER_URL=""
CONTROLLER_PUBLIC_URL=""
AUTH_TOKEN=""
BOT_TOKEN=""
ADMIN_CHAT_IDS=""
MIGRATE_DIR="$MIGRATE_DIR_DEFAULT"

msg() { echo -e "\033[1;32m[信息]\033[0m $*"; }
warn() { echo -e "\033[1;33m[警告]\033[0m $*"; }
err() { echo -e "\033[1;31m[错误]\033[0m $*" >&2; }

require_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    err "请使用 root 权限运行（sudo）。"
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

get_public_ipv4() {
  curl -4 -fsSL ifconfig.me 2>/dev/null \
    || curl -4 -fsSL https://api.ipify.org 2>/dev/null \
    || true
}

ensure_project_dir() {
  read -r -p "项目目录（默认 /root/sb-bot-panel） [${PROJECT_DIR}]: " input_dir
  PROJECT_DIR="${input_dir:-$PROJECT_DIR}"
  VENV_DIR="${PROJECT_DIR}/venv"
  ENV_FILE="${PROJECT_DIR}/.env"

  if [[ ! -d "$PROJECT_DIR" ]]; then
    if [[ -f "$REPO_ROOT/controller/app.py" ]]; then
      msg "目标目录不存在，正在复制当前仓库到 ${PROJECT_DIR}"
      mkdir -p "$(dirname "$PROJECT_DIR")"
      cp -a "$REPO_ROOT" "$PROJECT_DIR"
    else
      err "目标目录不存在且当前目录不是有效仓库，请先 git clone 项目。"
      exit 1
    fi
  fi

  if [[ ! -f "$PROJECT_DIR/controller/app.py" || ! -f "$PROJECT_DIR/bot/bot.py" ]]; then
    err "项目目录不完整：缺少 controller/app.py 或 bot/bot.py"
    exit 1
  fi
}

install_base_packages() {
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

get_env_value() {
  local key="$1"
  if [[ ! -f "$ENV_FILE" ]]; then
    return
  fi
  grep -E "^${key}=" "$ENV_FILE" | head -n1 | cut -d= -f2- || true
}

load_existing_env_defaults() {
  local old_port old_url old_public_url old_auth old_bot old_admin old_migrate
  old_port="$(get_env_value "CONTROLLER_PORT")"
  old_url="$(get_env_value "CONTROLLER_URL")"
  old_public_url="$(get_env_value "CONTROLLER_PUBLIC_URL")"
  old_auth="$(get_env_value "AUTH_TOKEN")"
  old_bot="$(get_env_value "BOT_TOKEN")"
  old_admin="$(get_env_value "ADMIN_CHAT_IDS")"
  old_migrate="$(get_env_value "MIGRATE_DIR")"

  CONTROLLER_PORT="${old_port:-8080}"
  CONTROLLER_URL="${old_url:-http://127.0.0.1:${CONTROLLER_PORT}}"
  CONTROLLER_PUBLIC_URL="${old_public_url:-}"
  AUTH_TOKEN="${old_auth:-devtoken123}"
  BOT_TOKEN="${old_bot:-}"
  ADMIN_CHAT_IDS="${old_admin:-}"
  MIGRATE_DIR="${old_migrate:-$MIGRATE_DIR_DEFAULT}"
}

prompt_env_config() {
  load_existing_env_defaults

  echo ""
  msg "配置向导说明："
  echo "  - CONTROLLER_PORT：controller 对外监听端口（节点 agent 需要访问）"
  echo "  - CONTROLLER_PUBLIC_URL：可选，对外访问 URL（给节点/外部使用）"
  echo "  - CONTROLLER_URL：bot 调用 controller 的地址（通常 127.0.0.1）"
  echo "  - AUTH_TOKEN：可选；用于保护 /admin/*，bot/agent 也可携带"
  echo "  - BOT_TOKEN：必填；Telegram 机器人 token"
  echo "  - ADMIN_CHAT_IDS：可选；用于限制谁可操作 bot"
  echo "  - MIGRATE_DIR：迁移包/备份包输出目录"
  echo ""

  read -r -p "CONTROLLER_PORT（controller 对外监听端口；节点 agent 需要访问） [${CONTROLLER_PORT}]: " input_port
  CONTROLLER_PORT="${input_port:-$CONTROLLER_PORT}"
  if ! [[ "$CONTROLLER_PORT" =~ ^[0-9]+$ ]] || (( CONTROLLER_PORT < 1 || CONTROLLER_PORT > 65535 )); then
    warn "端口无效，已回退为 8080"
    CONTROLLER_PORT="8080"
  fi

  local public_ip default_public_url
  public_ip="$(get_public_ipv4)"
  if [[ -n "$public_ip" ]]; then
    default_public_url="http://${public_ip}:${CONTROLLER_PORT}"
  else
    default_public_url=""
  fi
  read -r -p "CONTROLLER_PUBLIC_URL（可选；给节点/外部访问的完整 URL） [${CONTROLLER_PUBLIC_URL:-$default_public_url}]: " input_public_url
  CONTROLLER_PUBLIC_URL="${input_public_url:-${CONTROLLER_PUBLIC_URL:-$default_public_url}}"
  CONTROLLER_PUBLIC_URL="${CONTROLLER_PUBLIC_URL%/}"

  local default_controller_url="http://127.0.0.1:${CONTROLLER_PORT}"
  read -r -p "CONTROLLER_URL（给 bot 调用，建议本机回环地址） [${CONTROLLER_URL:-$default_controller_url}]: " input_url
  CONTROLLER_URL="${input_url:-${CONTROLLER_URL:-$default_controller_url}}"
  CONTROLLER_URL="${CONTROLLER_URL%/}"

  read -r -p "AUTH_TOKEN（可选；保护 /admin/*；留空=关闭鉴权） [${AUTH_TOKEN}]: " input_auth
  AUTH_TOKEN="${input_auth:-$AUTH_TOKEN}"

  while [[ -z "$BOT_TOKEN" ]]; do
    read -r -p "BOT_TOKEN（必填；Telegram 机器人 token） [保持现值请直接回车]: " input_bot
    BOT_TOKEN="${input_bot:-$BOT_TOKEN}"
    if [[ -z "$BOT_TOKEN" ]]; then
      warn "BOT_TOKEN 不能为空。"
    fi
  done

  read -r -p "ADMIN_CHAT_IDS（可选；逗号分隔，限制谁能操作 bot） [${ADMIN_CHAT_IDS}]: " input_admin
  ADMIN_CHAT_IDS="${input_admin:-$ADMIN_CHAT_IDS}"

  read -r -p "MIGRATE_DIR（迁移包/备份包输出目录） [${MIGRATE_DIR}]: " input_migrate
  MIGRATE_DIR="${input_migrate:-$MIGRATE_DIR}"
  if [[ -z "$MIGRATE_DIR" ]]; then
    MIGRATE_DIR="$MIGRATE_DIR_DEFAULT"
  fi

  echo ""
  msg "UFW/端口放行说明："
  echo "  - 需要放行 ${CONTROLLER_PORT}/tcp（节点 agent 访问 controller）"
  echo "  - 如仅内网使用，建议限制来源 IP，而不是全网开放"
}

write_env_file() {
  msg "写入环境配置: ${ENV_FILE}"
  mkdir -p "$PROJECT_DIR"
  cat >"$ENV_FILE" <<EOF
# 给 bot 调用 controller 的地址
CONTROLLER_URL=${CONTROLLER_URL}

# 对外访问 controller 的地址（可选，给节点/外部使用）
CONTROLLER_PUBLIC_URL=${CONTROLLER_PUBLIC_URL}

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

setup_venv_and_requirements() {
  if [[ ! -f "$PROJECT_DIR/requirements.txt" ]]; then
    err "缺少 requirements.txt，无法安装 Python 依赖。"
    exit 1
  fi

  if [[ ! -d "$VENV_DIR" ]]; then
    msg "创建 venv: $VENV_DIR"
    python3 -m venv "$VENV_DIR"
  fi

  msg "安装 Python 依赖..."
  "$VENV_DIR/bin/pip" install --upgrade pip setuptools wheel >/dev/null
  "$VENV_DIR/bin/pip" install -r "$PROJECT_DIR/requirements.txt"
}

write_systemd_services() {
  msg "写入 systemd 服务文件..."
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

restart_services() {
  systemctl daemon-reload
  systemctl enable sb-controller >/dev/null
  systemctl enable sb-bot >/dev/null
  systemctl restart sb-controller
  systemctl restart sb-bot
}

show_summary() {
  echo ""
  msg "管理服务器安装/配置完成。"
  echo "项目目录: ${PROJECT_DIR}"
  echo "venv 目录: ${VENV_DIR}"
  echo "Controller: 0.0.0.0:${CONTROLLER_PORT}"
  echo "MIGRATE_DIR: ${MIGRATE_DIR}"
  echo ""
  echo "快捷查看："
  echo "  systemctl status sb-controller"
  echo "  systemctl status sb-bot"
  echo "  journalctl -u sb-controller -n 200 --no-pager"
  echo "  journalctl -u sb-bot -n 200 --no-pager"
}

main() {
  require_root
  ensure_project_dir

  if [[ "$MODE" == "install" ]]; then
    install_base_packages
    setup_venv_and_requirements
  else
    msg "仅配置模式：跳过 apt 与依赖安装。"
  fi

  prompt_env_config
  write_env_file
  write_systemd_services
  restart_services
  show_summary
}

main "$@"
