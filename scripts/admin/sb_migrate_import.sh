#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-/root/sb-bot-panel}"
MIGRATE_DIR_DEFAULT="/var/backups/sb-migrate"
MIGRATE_DIR="$MIGRATE_DIR_DEFAULT"
ENV_FILE="${PROJECT_DIR}/.env"
VENV_DIR="${PROJECT_DIR}/venv"
PYTHON_BIN=""

CONTROLLER_PORT="8080"
CONTROLLER_PORT_WHITELIST=""
CONTROLLER_URL=""
CONTROLLER_PUBLIC_URL=""
PANEL_BASE_URL=""
ENABLE_HTTPS="0"
HTTPS_DOMAIN=""
HTTPS_ACME_EMAIL=""
AUTH_TOKEN=""
BOT_TOKEN=""
ADMIN_CHAT_IDS=""
VIEW_ADMIN_CHAT_IDS=""
OPS_ADMIN_CHAT_IDS=""
SUPER_ADMIN_CHAT_IDS=""
BACKUP_RETENTION_COUNT="30"
MIGRATE_RETENTION_COUNT="20"
BOT_MENU_TTL="60"
BOT_NODE_MONITOR_INTERVAL="60"
BOT_NODE_OFFLINE_THRESHOLD="120"
BOT_MUTATION_COOLDOWN="1"
TRUST_X_FORWARDED_FOR="0"
TRUSTED_PROXY_IPS="127.0.0.1,::1"
NODE_TASK_RUNNING_TIMEOUT="120"
NODE_TASK_RETENTION_SECONDS="604800"
NODE_TASK_MAX_PENDING_PER_NODE="50"
SUB_LINK_SIGN_KEY=""
SUB_LINK_REQUIRE_SIGNATURE="0"
SUB_LINK_DEFAULT_TTL_SECONDS="604800"
API_RATE_LIMIT_ENABLED="0"
API_RATE_LIMIT_WINDOW_SECONDS="60"
API_RATE_LIMIT_MAX_REQUESTS="120"
CONTROLLER_HTTP_TIMEOUT="10"
BOT_ACTOR_LABEL="sb-bot"
NON_INTERACTIVE="0"
PACKAGE_PATH=""

msg() { echo -e "\033[1;32m[信息]\033[0m $*"; }
warn() { echo -e "\033[1;33m[警告]\033[0m $*"; }
err() { echo -e "\033[1;31m[错误]\033[0m $*" >&2; }

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --non-interactive)
        NON_INTERACTIVE="1"
        shift
        ;;
      --package)
        if [[ $# -lt 2 ]]; then
          err "--package 需要提供路径参数。"
          exit 1
        fi
        PACKAGE_PATH="$2"
        shift 2
        ;;
      *)
        if [[ -z "$PACKAGE_PATH" ]]; then
          PACKAGE_PATH="$1"
          shift
        else
          err "未知参数: $1"
          exit 1
        fi
        ;;
    esac
  done
}

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

has_env_key() {
  local key="$1"
  if [[ ! -f "$ENV_FILE" ]]; then
    return 1
  fi
  grep -qE "^${key}=" "$ENV_FILE"
}

generate_auth_token() {
  local token
  token=""
  if command -v openssl >/dev/null 2>&1; then
    token="$(openssl rand -hex 24 2>/dev/null || true)"
  fi
  if [[ -z "$token" ]]; then
    token="$( (cat /proc/sys/kernel/random/uuid 2>/dev/null || true) | tr -d '-' )"
    token="${token}$(date +%s)"
    token="${token:0:40}"
  fi
  if [[ -z "$token" ]]; then
    token="token$(date +%s)"
  fi
  echo "$token"
}

write_env_file() {
  cat >"$ENV_FILE" <<EOF
# 给 bot 调用 controller 的地址
CONTROLLER_URL=${CONTROLLER_URL}

# 对外访问 controller 的地址（可选，给节点/外部使用）
CONTROLLER_PUBLIC_URL=${CONTROLLER_PUBLIC_URL}

# Bot 订阅链接基础地址（建议域名）
PANEL_BASE_URL=${PANEL_BASE_URL}

# 启用 Caddy HTTPS（1=启用，0=关闭）
ENABLE_HTTPS=${ENABLE_HTTPS}

# Caddy 证书域名（启用 HTTPS 时使用）
HTTPS_DOMAIN=${HTTPS_DOMAIN}

# Caddy ACME 账号邮箱（可选）
HTTPS_ACME_EMAIL=${HTTPS_ACME_EMAIL}

# controller 监听端口（供 systemd 使用）
CONTROLLER_PORT=${CONTROLLER_PORT}

# controller 端口白名单（可选，逗号分隔）
CONTROLLER_PORT_WHITELIST=${CONTROLLER_PORT_WHITELIST}

# 轻量鉴权 token（可用逗号分隔做轮换过渡）
AUTH_TOKEN=${AUTH_TOKEN}

# Telegram Bot token（必填）
BOT_TOKEN=${BOT_TOKEN}

# 管理员 chat id，逗号分隔，可空
ADMIN_CHAT_IDS=${ADMIN_CHAT_IDS}

VIEW_ADMIN_CHAT_IDS=${VIEW_ADMIN_CHAT_IDS}

OPS_ADMIN_CHAT_IDS=${OPS_ADMIN_CHAT_IDS}

SUPER_ADMIN_CHAT_IDS=${SUPER_ADMIN_CHAT_IDS}

# 迁移包目录
MIGRATE_DIR=${MIGRATE_DIR}

# 控制器备份保留数量（超出自动清理）
BACKUP_RETENTION_COUNT=${BACKUP_RETENTION_COUNT}

# 迁移包保留数量（超出自动清理）
MIGRATE_RETENTION_COUNT=${MIGRATE_RETENTION_COUNT}

# Bot 菜单按钮自动清理秒数
BOT_MENU_TTL=${BOT_MENU_TTL}

# 节点在线检测周期秒数
BOT_NODE_MONITOR_INTERVAL=${BOT_NODE_MONITOR_INTERVAL}

# 节点离线判定阈值秒数
BOT_NODE_OFFLINE_THRESHOLD=${BOT_NODE_OFFLINE_THRESHOLD}

# bot 写操作按钮防抖秒数（防重复提交）
BOT_MUTATION_COOLDOWN=${BOT_MUTATION_COOLDOWN}

# 是否信任 X-Forwarded-For（仅受控反代场景建议开启）
TRUST_X_FORWARDED_FOR=${TRUST_X_FORWARDED_FOR}

# 可信代理 IP 列表（逗号分隔）
TRUSTED_PROXY_IPS=${TRUSTED_PROXY_IPS}

# 节点任务运行超时秒数（超时后自动重试/标记超时）
NODE_TASK_RUNNING_TIMEOUT=${NODE_TASK_RUNNING_TIMEOUT}

# 节点任务历史保留秒数（到期自动清理）
NODE_TASK_RETENTION_SECONDS=${NODE_TASK_RETENTION_SECONDS}

# 单节点任务队列上限（pending+running）
NODE_TASK_MAX_PENDING_PER_NODE=${NODE_TASK_MAX_PENDING_PER_NODE}

# 订阅签名密钥（可选）
SUB_LINK_SIGN_KEY=${SUB_LINK_SIGN_KEY}

# 是否强制订阅签名（1=强制）
SUB_LINK_REQUIRE_SIGNATURE=${SUB_LINK_REQUIRE_SIGNATURE}

# 订阅签名默认有效期（秒）
SUB_LINK_DEFAULT_TTL_SECONDS=${SUB_LINK_DEFAULT_TTL_SECONDS}

# controller 轻量限流
API_RATE_LIMIT_ENABLED=${API_RATE_LIMIT_ENABLED}
API_RATE_LIMIT_WINDOW_SECONDS=${API_RATE_LIMIT_WINDOW_SECONDS}
API_RATE_LIMIT_MAX_REQUESTS=${API_RATE_LIMIT_MAX_REQUESTS}

# bot 调 controller 超时（秒）
CONTROLLER_HTTP_TIMEOUT=${CONTROLLER_HTTP_TIMEOUT}

# bot 审计操作者标识
BOT_ACTOR_LABEL=${BOT_ACTOR_LABEL}
EOF
  chmod 0600 "$ENV_FILE"
}

detect_public_ip() {
  curl -4 -fsSL ifconfig.me 2>/dev/null \
    || curl -4 -fsSL https://api.ipify.org 2>/dev/null \
    || true
}

extract_url_host() {
  local raw="$1"
  raw="${raw#*://}"
  raw="${raw%%/*}"
  raw="${raw%%:*}"
  echo "$raw"
}

normalize_input_url() {
  local raw="$1"
  local default_scheme="${2:-http}"
  raw="${raw//$'\r'/}"
  raw="${raw//$'\n'/}"
  raw="$(echo "$raw" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
  if [[ -z "$raw" ]]; then
    echo ""
    return
  fi
  if [[ "$raw" =~ ^https?:// ]]; then
    echo "${raw%/}"
    return
  fi
  if [[ "$default_scheme" != "https" ]]; then
    default_scheme="http"
  fi
  echo "${default_scheme}://${raw%/}"
}

wait_for_controller_ready() {
  local timeout_seconds="${1:-30}"
  local i
  for i in $(seq 1 "$timeout_seconds"); do
    if curl -fsSL "http://127.0.0.1:${CONTROLLER_PORT}/health" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

sanitize_domain_input() {
  local raw="$1"
  raw="${raw//$'\r'/}"
  raw="${raw//$'\n'/}"
  raw="$(echo "$raw" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
  raw="${raw#http://}"
  raw="${raw#https://}"
  raw="${raw%%/*}"
  raw="${raw%%:*}"
  raw="${raw%.}"
  echo "$raw"
}

is_valid_domain() {
  local value="$1"
  [[ "$value" =~ ^([A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)+[A-Za-z]{2,63}$ ]]
}

python_version_ge_311() {
  local cmd="$1"
  "$cmd" - <<'PY'
import sys
raise SystemExit(0 if sys.version_info >= (3, 11) else 1)
PY
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

ensure_python_311_runtime() {
  local os_id=""
  local os_version=""
  if [[ -f /etc/os-release ]]; then
    # shellcheck disable=SC1091
    . /etc/os-release
    os_id="${ID:-}"
    os_version="${VERSION_ID:-}"
  fi

  if command -v python3.11 >/dev/null 2>&1 && python_version_ge_311 python3.11; then
    PYTHON_BIN="$(command -v python3.11)"
    return
  fi

  export DEBIAN_FRONTEND=noninteractive
  apt-get install -y python3.11 python3.11-venv python3.11-distutils >/dev/null 2>&1 || true

  if [[ "$os_id" == "debian" && "$os_version" == 11* ]]; then
    if [[ ! -f /etc/apt/sources.list.d/bullseye-backports.list ]]; then
      echo "deb http://deb.debian.org/debian bullseye-backports main" >/etc/apt/sources.list.d/bullseye-backports.list
    fi
    apt-get update -y >/dev/null 2>&1 || true
    apt-get install -y -t bullseye-backports python3.11 python3.11-venv >/dev/null 2>&1 || true
  fi

  if [[ "$os_id" == "ubuntu" ]]; then
    apt-get install -y software-properties-common >/dev/null 2>&1 || true
    add-apt-repository -y ppa:deadsnakes/ppa >/dev/null 2>&1 || true
    apt-get update -y >/dev/null 2>&1 || true
    apt-get install -y python3.11 python3.11-venv >/dev/null 2>&1 || true
  fi

  if command -v python3.11 >/dev/null 2>&1 && python_version_ge_311 python3.11; then
    PYTHON_BIN="$(command -v python3.11)"
    return
  fi
  if command -v python3 >/dev/null 2>&1 && python_version_ge_311 python3; then
    PYTHON_BIN="$(command -v python3)"
    return
  fi
  err "未能找到 Python >=3.11。请先安装 python3.11 与 python3.11-venv 后再导入。"
  exit 1
}

setup_venv() {
  if [[ ! -f "$PROJECT_DIR/requirements.txt" ]]; then
    err "缺少 requirements.txt，请先确保项目代码完整。"
    exit 1
  fi
  if [[ -z "$PYTHON_BIN" ]]; then
    ensure_python_311_runtime
  fi
  if [[ -d "$VENV_DIR" && -x "$VENV_DIR/bin/python" ]]; then
    if ! python_version_ge_311 "$VENV_DIR/bin/python"; then
      warn "检测到旧 venv Python < 3.11，正在重建 venv..."
      rm -rf "$VENV_DIR"
    fi
  fi
  if [[ ! -d "$VENV_DIR" ]]; then
    "$PYTHON_BIN" -m venv "$VENV_DIR"
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

install_caddy_if_needed() {
  if [[ "$ENABLE_HTTPS" != "1" ]]; then
    return
  fi
  if command -v caddy >/dev/null 2>&1; then
    return
  fi
  msg "检测到 ENABLE_HTTPS=1，安装 caddy..."
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -y
  apt-get install -y caddy
}

write_caddy_config_if_needed() {
  if [[ "$ENABLE_HTTPS" != "1" ]]; then
    return
  fi
  if [[ -z "$HTTPS_DOMAIN" ]]; then
    warn "ENABLE_HTTPS=1 但 HTTPS_DOMAIN 为空，跳过 caddy 配置。"
    return
  fi
  mkdir -p /etc/caddy
  if [[ -n "$HTTPS_ACME_EMAIL" ]]; then
    cat >/etc/caddy/Caddyfile <<EOF
{
    email ${HTTPS_ACME_EMAIL}
}

${HTTPS_DOMAIN} {
    encode zstd gzip
    reverse_proxy 127.0.0.1:${CONTROLLER_PORT}
}
EOF
  else
    cat >/etc/caddy/Caddyfile <<EOF
${HTTPS_DOMAIN} {
    encode zstd gzip
    reverse_proxy 127.0.0.1:${CONTROLLER_PORT}
}
EOF
  fi
}

restart_caddy_with_diagnostics() {
  if [[ "$ENABLE_HTTPS" != "1" ]]; then
    return
  fi
  if ! command -v caddy >/dev/null 2>&1; then
    err "未检测到 caddy 命令。"
    return 1
  fi
  if ! caddy validate --config /etc/caddy/Caddyfile >/tmp/sb-caddy-validate.log 2>&1; then
    err "Caddyfile 校验失败："
    cat /tmp/sb-caddy-validate.log || true
    return 1
  fi
  systemctl enable caddy >/dev/null || true
  if ! systemctl restart caddy; then
    err "caddy 启动失败，开始输出诊断信息。"
    echo "----- 端口占用(80/443) -----"
    ss -ltnup 2>/dev/null | grep -E ':(80|443)\s' || echo "未发现明显占用"
    echo "----- caddy status -----"
    systemctl status caddy --no-pager || true
    echo "----- caddy 日志 -----"
    journalctl -u caddy -n 120 --no-pager || true
    return 1
  fi
}

main() {
  require_root
  parse_args "$@"

  local pkg_path="${PACKAGE_PATH:-}"
  if [[ -z "$pkg_path" && "$NON_INTERACTIVE" != "1" ]]; then
    read -r -p "请输入迁移包路径（.tar.gz）: " pkg_path
  fi
  if [[ -z "$pkg_path" ]]; then
    err "迁移包路径不能为空。"
    exit 1
  fi
  if [[ ! -f "$pkg_path" ]]; then
    err "迁移包不存在: $pkg_path"
    exit 1
  fi

  if [[ "$NON_INTERACTIVE" != "1" ]]; then
    local input_project
    read -r -p "目标项目目录 [${PROJECT_DIR}]: " input_project
    PROJECT_DIR="${input_project:-$PROJECT_DIR}"
  fi
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
  CONTROLLER_PORT_WHITELIST="$(get_env_value CONTROLLER_PORT_WHITELIST)"
  CONTROLLER_PORT_WHITELIST="${CONTROLLER_PORT_WHITELIST:-}"
  CONTROLLER_PUBLIC_URL="$(get_env_value CONTROLLER_PUBLIC_URL)"
  PANEL_BASE_URL="$(get_env_value PANEL_BASE_URL)"
  ENABLE_HTTPS="$(get_env_value ENABLE_HTTPS)"
  ENABLE_HTTPS="${ENABLE_HTTPS:-0}"
  HTTPS_DOMAIN="$(get_env_value HTTPS_DOMAIN)"
  HTTPS_DOMAIN="$(sanitize_domain_input "$HTTPS_DOMAIN")"
  HTTPS_ACME_EMAIL="$(get_env_value HTTPS_ACME_EMAIL)"
  AUTH_TOKEN="$(get_env_value AUTH_TOKEN)"
  if has_env_key "AUTH_TOKEN"; then
    AUTH_TOKEN="${AUTH_TOKEN:-}"
  else
    AUTH_TOKEN="$(generate_auth_token)"
  fi
  BOT_TOKEN="$(get_env_value BOT_TOKEN)"
  ADMIN_CHAT_IDS="$(get_env_value ADMIN_CHAT_IDS)"
  VIEW_ADMIN_CHAT_IDS="$(get_env_value VIEW_ADMIN_CHAT_IDS)"
  OPS_ADMIN_CHAT_IDS="$(get_env_value OPS_ADMIN_CHAT_IDS)"
  SUPER_ADMIN_CHAT_IDS="$(get_env_value SUPER_ADMIN_CHAT_IDS)"
  MIGRATE_DIR="$(get_env_value MIGRATE_DIR)"
  MIGRATE_DIR="${MIGRATE_DIR:-$MIGRATE_DIR_DEFAULT}"
  BACKUP_RETENTION_COUNT="$(get_env_value BACKUP_RETENTION_COUNT)"
  BACKUP_RETENTION_COUNT="${BACKUP_RETENTION_COUNT:-30}"
  MIGRATE_RETENTION_COUNT="$(get_env_value MIGRATE_RETENTION_COUNT)"
  MIGRATE_RETENTION_COUNT="${MIGRATE_RETENTION_COUNT:-20}"
  BOT_MENU_TTL="$(get_env_value BOT_MENU_TTL)"
  BOT_MENU_TTL="${BOT_MENU_TTL:-60}"
  BOT_NODE_MONITOR_INTERVAL="$(get_env_value BOT_NODE_MONITOR_INTERVAL)"
  BOT_NODE_MONITOR_INTERVAL="${BOT_NODE_MONITOR_INTERVAL:-60}"
  BOT_NODE_OFFLINE_THRESHOLD="$(get_env_value BOT_NODE_OFFLINE_THRESHOLD)"
  BOT_NODE_OFFLINE_THRESHOLD="${BOT_NODE_OFFLINE_THRESHOLD:-120}"
  BOT_MUTATION_COOLDOWN="$(get_env_value BOT_MUTATION_COOLDOWN)"
  BOT_MUTATION_COOLDOWN="${BOT_MUTATION_COOLDOWN:-1}"
  if ! [[ "$BOT_MUTATION_COOLDOWN" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    BOT_MUTATION_COOLDOWN="1"
  fi
  TRUST_X_FORWARDED_FOR="$(get_env_value TRUST_X_FORWARDED_FOR)"
  TRUST_X_FORWARDED_FOR="${TRUST_X_FORWARDED_FOR:-0}"
  TRUSTED_PROXY_IPS="$(get_env_value TRUSTED_PROXY_IPS)"
  TRUSTED_PROXY_IPS="${TRUSTED_PROXY_IPS:-127.0.0.1,::1}"
  NODE_TASK_RUNNING_TIMEOUT="$(get_env_value NODE_TASK_RUNNING_TIMEOUT)"
  NODE_TASK_RUNNING_TIMEOUT="${NODE_TASK_RUNNING_TIMEOUT:-120}"
  NODE_TASK_RETENTION_SECONDS="$(get_env_value NODE_TASK_RETENTION_SECONDS)"
  NODE_TASK_RETENTION_SECONDS="${NODE_TASK_RETENTION_SECONDS:-604800}"
  NODE_TASK_MAX_PENDING_PER_NODE="$(get_env_value NODE_TASK_MAX_PENDING_PER_NODE)"
  NODE_TASK_MAX_PENDING_PER_NODE="${NODE_TASK_MAX_PENDING_PER_NODE:-50}"
  SUB_LINK_SIGN_KEY="$(get_env_value SUB_LINK_SIGN_KEY)"
  SUB_LINK_SIGN_KEY="${SUB_LINK_SIGN_KEY:-}"
  SUB_LINK_REQUIRE_SIGNATURE="$(get_env_value SUB_LINK_REQUIRE_SIGNATURE)"
  SUB_LINK_REQUIRE_SIGNATURE="${SUB_LINK_REQUIRE_SIGNATURE:-0}"
  SUB_LINK_DEFAULT_TTL_SECONDS="$(get_env_value SUB_LINK_DEFAULT_TTL_SECONDS)"
  SUB_LINK_DEFAULT_TTL_SECONDS="${SUB_LINK_DEFAULT_TTL_SECONDS:-604800}"
  API_RATE_LIMIT_ENABLED="$(get_env_value API_RATE_LIMIT_ENABLED)"
  API_RATE_LIMIT_ENABLED="${API_RATE_LIMIT_ENABLED:-0}"
  API_RATE_LIMIT_WINDOW_SECONDS="$(get_env_value API_RATE_LIMIT_WINDOW_SECONDS)"
  API_RATE_LIMIT_WINDOW_SECONDS="${API_RATE_LIMIT_WINDOW_SECONDS:-60}"
  API_RATE_LIMIT_MAX_REQUESTS="$(get_env_value API_RATE_LIMIT_MAX_REQUESTS)"
  API_RATE_LIMIT_MAX_REQUESTS="${API_RATE_LIMIT_MAX_REQUESTS:-120}"
  CONTROLLER_HTTP_TIMEOUT="$(get_env_value CONTROLLER_HTTP_TIMEOUT)"
  CONTROLLER_HTTP_TIMEOUT="${CONTROLLER_HTTP_TIMEOUT:-10}"
  BOT_ACTOR_LABEL="$(get_env_value BOT_ACTOR_LABEL)"
  BOT_ACTOR_LABEL="${BOT_ACTOR_LABEL:-sb-bot}"

  local public_ip
  public_ip="$(detect_public_ip)"
  msg "检测到公网 IP: ${public_ip:-未知}"

  local default_controller_url
  default_controller_url="http://127.0.0.1:${CONTROLLER_PORT}"
  CONTROLLER_URL="$(get_env_value CONTROLLER_URL)"
  CONTROLLER_URL="${CONTROLLER_URL:-$default_controller_url}"
  if [[ "$NON_INTERACTIVE" != "1" ]]; then
    read -r -p "CONTROLLER_URL [${CONTROLLER_URL}]（支持省略 http/https）: " input_url
    CONTROLLER_URL="${input_url:-$CONTROLLER_URL}"
    local controller_host controller_scheme
    controller_host="$(extract_url_host "$CONTROLLER_URL")"
    controller_scheme="http"
    if [[ "$ENABLE_HTTPS" == "1" && "$controller_host" != "127.0.0.1" && "$controller_host" != "localhost" ]]; then
      controller_scheme="https"
    fi
    CONTROLLER_URL="$(normalize_input_url "$CONTROLLER_URL" "$controller_scheme")"

    read -r -p "BOT_TOKEN [保持现值请回车]: " input_bot
    BOT_TOKEN="${input_bot:-$BOT_TOKEN}"
    while [[ -z "$BOT_TOKEN" ]]; do
      warn "BOT_TOKEN 不能为空。"
      read -r -p "请重新输入 BOT_TOKEN: " BOT_TOKEN
    done

    read -r -p "ADMIN_CHAT_IDS [${ADMIN_CHAT_IDS}]: " input_admin
    ADMIN_CHAT_IDS="${input_admin:-$ADMIN_CHAT_IDS}"
    read -r -p "VIEW_ADMIN_CHAT_IDS [${VIEW_ADMIN_CHAT_IDS}]: " input_view_admin
    VIEW_ADMIN_CHAT_IDS="${input_view_admin:-$VIEW_ADMIN_CHAT_IDS}"
    read -r -p "OPS_ADMIN_CHAT_IDS [${OPS_ADMIN_CHAT_IDS}]: " input_ops_admin
    OPS_ADMIN_CHAT_IDS="${input_ops_admin:-$OPS_ADMIN_CHAT_IDS}"
    read -r -p "SUPER_ADMIN_CHAT_IDS [${SUPER_ADMIN_CHAT_IDS}]: " input_super_admin
    SUPER_ADMIN_CHAT_IDS="${input_super_admin:-$SUPER_ADMIN_CHAT_IDS}"

    read -r -p "ENABLE_HTTPS（1=启用 caddy 自动证书，0=关闭） [${ENABLE_HTTPS}]: " input_https_switch
    ENABLE_HTTPS="${input_https_switch:-$ENABLE_HTTPS}"
    if [[ "$ENABLE_HTTPS" != "1" && "$ENABLE_HTTPS" != "0" ]]; then
      warn "ENABLE_HTTPS 无效，回退为 0"
      ENABLE_HTTPS="0"
    fi
    if [[ "$ENABLE_HTTPS" == "1" ]]; then
      read -r -p "HTTPS_DOMAIN（例如 panel.example.com） [${HTTPS_DOMAIN}]: " input_https_domain
      HTTPS_DOMAIN="${input_https_domain:-$HTTPS_DOMAIN}"
      HTTPS_DOMAIN="$(sanitize_domain_input "$HTTPS_DOMAIN")"
      while [[ -z "$HTTPS_DOMAIN" ]] || ! is_valid_domain "$HTTPS_DOMAIN"; do
        warn "HTTPS_DOMAIN 无效，请填写域名（例如 panel.example.com）。"
        read -r -p "请重新输入 HTTPS_DOMAIN: " HTTPS_DOMAIN
        HTTPS_DOMAIN="$(sanitize_domain_input "$HTTPS_DOMAIN")"
      done
      read -r -p "HTTPS_ACME_EMAIL（可选） [${HTTPS_ACME_EMAIL}]: " input_https_email
      HTTPS_ACME_EMAIL="${input_https_email:-$HTTPS_ACME_EMAIL}"
    else
      HTTPS_DOMAIN=""
      HTTPS_ACME_EMAIL=""
    fi
  fi

  if [[ -z "$BOT_TOKEN" ]]; then
    err "BOT_TOKEN 为空，无法继续导入。请先修正 .env 后重试。"
    exit 1
  fi

  if [[ -n "$CONTROLLER_PUBLIC_URL" ]]; then
    local public_scheme
    public_scheme="http"
    if [[ "$ENABLE_HTTPS" == "1" || "$CONTROLLER_PUBLIC_URL" == https://* ]]; then
      public_scheme="https"
    fi
    CONTROLLER_PUBLIC_URL="$(normalize_input_url "$CONTROLLER_PUBLIC_URL" "$public_scheme")"
  fi
  if [[ -n "$PANEL_BASE_URL" ]]; then
    local panel_scheme
    panel_scheme="http"
    if [[ "$ENABLE_HTTPS" == "1" || "$PANEL_BASE_URL" == https://* ]]; then
      panel_scheme="https"
    fi
    PANEL_BASE_URL="$(normalize_input_url "$PANEL_BASE_URL" "$panel_scheme")"
  fi
  if [[ "$ENABLE_HTTPS" == "1" && -n "$HTTPS_DOMAIN" ]]; then
    CONTROLLER_PUBLIC_URL="https://${HTTPS_DOMAIN}"
    if [[ -z "$PANEL_BASE_URL" ]]; then
      PANEL_BASE_URL="$CONTROLLER_PUBLIC_URL"
    fi
  fi

  mkdir -p "$PROJECT_DIR"
  write_env_file

  install_dependencies
  ensure_python_311_runtime
  install_caddy_if_needed
  setup_venv
  write_services
  write_caddy_config_if_needed

  systemctl daemon-reload
  systemctl enable sb-controller >/dev/null
  systemctl enable sb-bot >/dev/null
  systemctl restart sb-controller
  systemctl restart sb-bot
  if [[ "$ENABLE_HTTPS" == "1" ]]; then
    restart_caddy_with_diagnostics
  fi

  echo ""
  msg "自检开始..."
  if wait_for_controller_ready 30; then
    msg "controller /health 检查通过。"
  else
    warn "controller /health 检查失败，请查看日志。"
  fi
  systemctl status sb-bot --no-pager || true
}

main "$@"
