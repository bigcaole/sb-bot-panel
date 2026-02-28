#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-install}"
if [[ "$MODE" != "install" && "$MODE" != "--configure-only" && "$MODE" != "--configure-quick" && "$MODE" != "--reuse-config" ]]; then
  echo "用法:"
  echo "  sudo bash scripts/admin/install_admin.sh"
  echo "  sudo bash scripts/admin/install_admin.sh --configure-only"
  echo "  sudo bash scripts/admin/install_admin.sh --configure-quick"
  echo "  sudo bash scripts/admin/install_admin.sh --reuse-config"
  exit 1
fi
if [[ "$MODE" == "--configure-only" ]]; then
  MODE="configure-only"
fi
if [[ "$MODE" == "--configure-quick" ]]; then
  MODE="configure-quick"
fi
if [[ "$MODE" == "--reuse-config" ]]; then
  MODE="reuse-config"
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
AI_CONTEXT_SCRIPT="${SCRIPT_DIR}/ai_context_export.sh"
AI_CONTEXT_ON_FAIL="${INSTALL_ADMIN_EXPORT_AI_CONTEXT_ON_FAIL:-1}"
AI_CONTEXT_EXPORTED=0

DEFAULT_PROJECT_DIR="/root/sb-bot-panel"
PROJECT_DIR="${PROJECT_DIR:-$DEFAULT_PROJECT_DIR}"
VENV_DIR=""
ENV_FILE=""
PYTHON_BIN=""
MIGRATE_DIR_DEFAULT="/var/backups/sb-migrate"
BOT_TOKEN_PLACEHOLDER="__REPLACE_WITH_TELEGRAM_BOT_TOKEN__"

CONTROLLER_PORT="8080"
CONTROLLER_PORT_WHITELIST=""
ADMIN_API_WHITELIST=""
CONTROLLER_URL=""
CONTROLLER_PUBLIC_URL=""
PANEL_BASE_URL=""
ENABLE_HTTPS="0"
HTTPS_DOMAIN=""
HTTPS_ACME_EMAIL=""
AUTH_TOKEN=""
ADMIN_AUTH_TOKEN=""
NODE_AUTH_TOKEN=""
BOT_TOKEN=""
ADMIN_CHAT_IDS=""
VIEW_ADMIN_CHAT_IDS=""
OPS_ADMIN_CHAT_IDS=""
SUPER_ADMIN_CHAT_IDS=""
MIGRATE_DIR="$MIGRATE_DIR_DEFAULT"
BACKUP_RETENTION_COUNT="30"
MIGRATE_RETENTION_COUNT="20"
LOG_ARCHIVE_WINDOW_HOURS="24"
LOG_ARCHIVE_RETENTION_COUNT="30"
LOG_ARCHIVE_DIR="/var/backups/sb-controller/logs"
BOT_MENU_TTL="60"
BOT_NODE_MONITOR_INTERVAL="60"
BOT_NODE_OFFLINE_THRESHOLD="120"
BOT_NODE_TIME_SYNC_INTERVAL="86400"
BOT_MUTATION_COOLDOWN="1"
TRUST_X_FORWARDED_FOR="0"
TRUSTED_PROXY_IPS="127.0.0.1,::1"
NODE_TASK_RUNNING_TIMEOUT="120"
NODE_TASK_RETENTION_SECONDS="604800"
NODE_TASK_MAX_PENDING_PER_NODE="50"
SUB_LINK_SIGN_KEY=""
SUB_LINK_REQUIRE_SIGNATURE="0"
SUB_LINK_DEFAULT_TTL_SECONDS="604800"
API_RATE_LIMIT_ENABLED="1"
API_RATE_LIMIT_WINDOW_SECONDS="60"
API_RATE_LIMIT_MAX_REQUESTS="120"
ADMIN_OVERVIEW_CACHE_TTL_SECONDS="5"
ADMIN_SECURITY_STATUS_CACHE_TTL_SECONDS="5"
SECURITY_EVENTS_EXCLUDE_LOCAL="1"
SECURITY_BLOCK_PROTECTED_IPS=""
SECURITY_AUTO_BLOCK_ENABLED="0"
SECURITY_AUTO_BLOCK_INTERVAL_SECONDS="60"
SECURITY_AUTO_BLOCK_WINDOW_SECONDS="3600"
SECURITY_AUTO_BLOCK_THRESHOLD="30"
SECURITY_AUTO_BLOCK_DURATION_SECONDS="3600"
SECURITY_AUTO_BLOCK_MAX_PER_INTERVAL="5"
CONTROLLER_HTTP_TIMEOUT="10"
BOT_ACTOR_LABEL="sb-bot"
SELF_CHECK_OK=0
SELF_CHECK_WARN=0
SELF_CHECK_FAIL=0
NODE_DEFAULT_SYNC_SUMMARY="未执行"
INSTALL_SCRIPT_ACTOR="install-admin"
AUTH_DISABLED_EXPLICIT=0

msg() { echo -e "\033[1;32m[信息]\033[0m $*"; }
warn() { echo -e "\033[1;33m[警告]\033[0m $*"; }
err() { echo -e "\033[1;31m[错误]\033[0m $*" >&2; }
check_ok() { echo -e "\033[1;32m[自检-通过]\033[0m $*"; SELF_CHECK_OK=$((SELF_CHECK_OK + 1)); }
check_warn() { echo -e "\033[1;33m[自检-警告]\033[0m $*"; SELF_CHECK_WARN=$((SELF_CHECK_WARN + 1)); }
check_fail() { echo -e "\033[1;31m[自检-失败]\033[0m $*"; SELF_CHECK_FAIL=$((SELF_CHECK_FAIL + 1)); }

emit_ai_context_on_failure() {
  if [[ "${AI_CONTEXT_ON_FAIL}" != "1" ]]; then
    return
  fi
  if [[ "${AI_CONTEXT_EXPORTED}" == "1" ]]; then
    return
  fi
  AI_CONTEXT_EXPORTED=1
  if [[ ! -x "$AI_CONTEXT_SCRIPT" ]]; then
    warn "未找到 AI 诊断包脚本: ${AI_CONTEXT_SCRIPT}"
    return
  fi
  local ai_context_path
  ai_context_path="/tmp/sb-install-admin-ai-context-on-fail-$(date +%Y%m%d-%H%M%S).md"
  if bash "$AI_CONTEXT_SCRIPT" --output "$ai_context_path" >/tmp/sb_install_admin_ai_export.log 2>&1; then
    echo "失败辅助诊断包：${ai_context_path}"
    echo "提示：可将该文件整体粘贴给任意 AI 做继续定位。"
  else
    warn "自动导出 AI 诊断包失败（不影响原始失败结论），可手动执行: bash scripts/admin/ai_context_export.sh"
    cat /tmp/sb_install_admin_ai_export.log || true
  fi
}

on_script_exit() {
  local code=$?
  if (( code != 0 )); then
    emit_ai_context_on_failure
  fi
}

trap on_script_exit EXIT

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

is_bot_token_configured() {
  local token="$1"
  [[ -n "$token" && "$token" != "$BOT_TOKEN_PLACEHOLDER" ]]
}

get_public_ipv4() {
  curl -4 -fsSL ifconfig.me 2>/dev/null \
    || curl -4 -fsSL https://api.ipify.org 2>/dev/null \
    || true
}

detect_current_ssh_client_ip() {
  local ip
  ip="${SSH_CLIENT:-}"
  ip="${ip%% *}"
  if [[ -z "$ip" ]]; then
    ip="${SSH_CONNECTION:-}"
    ip="${ip%% *}"
  fi
  if [[ -z "$ip" ]]; then
    ip="$(who -u am i 2>/dev/null | awk '{print $NF}' | tr -d '()' || true)"
  fi
  echo "$ip"
}

extract_url_host() {
  local raw="$1"
  raw="${raw#*://}"
  raw="${raw%%/*}"
  raw="${raw%%:*}"
  echo "$raw"
}

wait_for_controller_ready() {
  local timeout_seconds="${1:-30}"
  local i
  for i in $(seq 1 "$timeout_seconds"); do
    if curl -fsSL --max-time 3 "http://127.0.0.1:${CONTROLLER_PORT}/health" >/tmp/sb-controller-health.json 2>/dev/null; then
      return 0
    fi
    sleep 1
  done
  return 1
}

normalize_whitelist_csv() {
  local raw="$1"
  local result=""
  IFS=',' read -r -a items <<<"$raw"
  for item in "${items[@]}"; do
    local value
    value="$(echo "$item" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
    [[ -z "$value" ]] && continue
    if [[ -z "$result" ]]; then
      result="$value"
    else
      result="${result},${value}"
    fi
  done
  echo "$result"
}

delete_controller_port_rules() {
  local controller_port="$1"
  local rule_ids
  rule_ids="$(ufw status numbered 2>/dev/null | grep "${controller_port}/tcp" | sed -E 's/^\[ *([0-9]+)\].*/\1/' | sort -rn || true)"
  if [[ -z "$rule_ids" ]]; then
    return
  fi
  while read -r rule_id; do
    [[ -z "${rule_id:-}" ]] && continue
    yes | ufw delete "$rule_id" >/dev/null 2>&1 || true
  done <<<"$rule_ids"
}

is_ipv4() {
  local value="$1"
  [[ "$value" =~ ^([0-9]{1,3}\.){3}[0-9]{1,3}$ ]]
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

ensure_project_dir() {
  if [[ "$MODE" == "reuse-config" && -d "$PROJECT_DIR" && -f "$PROJECT_DIR/controller/app.py" && -f "$PROJECT_DIR/bot/bot.py" ]]; then
    msg "更新模式使用项目目录：${PROJECT_DIR}"
  else
    read -r -p "项目目录（默认 /root/sb-bot-panel） [${PROJECT_DIR}]: " input_dir
    PROJECT_DIR="${input_dir:-$PROJECT_DIR}"
  fi
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

python_version_ge_311() {
  local cmd="$1"
  "$cmd" - <<'PY'
import sys
raise SystemExit(0 if sys.version_info >= (3, 11) else 1)
PY
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
    msg "检测到 Python 3.11: ${PYTHON_BIN}"
    return
  fi

  msg "尝试安装 Python 3.11..."
  export DEBIAN_FRONTEND=noninteractive
  apt-get install -y python3.11 python3.11-venv python3.11-distutils >/dev/null 2>&1 || true

  if command -v python3.11 >/dev/null 2>&1 && python_version_ge_311 python3.11; then
    PYTHON_BIN="$(command -v python3.11)"
    msg "Python 3.11 安装完成: ${PYTHON_BIN}"
    return
  fi

  if [[ "$os_id" == "debian" && "$os_version" == 11* ]]; then
    msg "检测到 Debian 11，尝试启用 bullseye-backports 安装 Python 3.11..."
    if [[ ! -f /etc/apt/sources.list.d/bullseye-backports.list ]]; then
      echo "deb http://deb.debian.org/debian bullseye-backports main" >/etc/apt/sources.list.d/bullseye-backports.list
    fi
    apt-get update -y >/dev/null 2>&1 || true
    apt-get install -y -t bullseye-backports python3.11 python3.11-venv >/dev/null 2>&1 || true
  fi

  if [[ "$os_id" == "ubuntu" ]]; then
    msg "检测到 Ubuntu，尝试 deadsnakes PPA 安装 Python 3.11..."
    apt-get install -y software-properties-common >/dev/null 2>&1 || true
    add-apt-repository -y ppa:deadsnakes/ppa >/dev/null 2>&1 || true
    apt-get update -y >/dev/null 2>&1 || true
    apt-get install -y python3.11 python3.11-venv >/dev/null 2>&1 || true
  fi

  if command -v python3.11 >/dev/null 2>&1 && python_version_ge_311 python3.11; then
    PYTHON_BIN="$(command -v python3.11)"
    msg "Python 3.11 安装完成: ${PYTHON_BIN}"
    return
  fi

  if command -v python3 >/dev/null 2>&1 && python_version_ge_311 python3; then
    PYTHON_BIN="$(command -v python3)"
    warn "未找到 python3.11，回退使用 ${PYTHON_BIN}（版本>=3.11）"
    return
  fi

  err "未能找到 Python >=3.11。请手动安装 python3.11 与 python3.11-venv 后重试。"
  exit 1
}

install_admin_menu_commands() {
  msg "安装菜单快捷命令..."
  # Cleanup legacy shortcut from old versions to avoid confusing entrypoint.
  if [[ -f /usr/local/bin/sb-bot-panel ]] && grep -q "sb-bot-panel-main-shortcut" /usr/local/bin/sb-bot-panel 2>/dev/null; then
    rm -f /usr/local/bin/sb-bot-panel
    msg "已清理历史快捷命令：/usr/local/bin/sb-bot-panel"
  fi

  cat >/usr/local/bin/sb-admin <<EOF
#!/usr/bin/env bash
exec bash "${PROJECT_DIR}/scripts/admin/menu_admin.sh" "\$@"
EOF
  chmod 0755 /usr/local/bin/sb-admin

  local s_ui_path
  s_ui_path="$(command -v s-ui || true)"
  if [[ -z "$s_ui_path" ]]; then
    cat >/usr/local/bin/s-ui <<EOF
#!/usr/bin/env bash
# sb-bot-panel-admin-shortcut
exec bash "${PROJECT_DIR}/scripts/admin/menu_admin.sh" "\$@"
EOF
    chmod 0755 /usr/local/bin/s-ui
  elif [[ "$s_ui_path" == "/usr/local/bin/s-ui" ]] && grep -q "sb-bot-panel-admin-shortcut" /usr/local/bin/s-ui 2>/dev/null; then
    cat >/usr/local/bin/s-ui <<EOF
#!/usr/bin/env bash
# sb-bot-panel-admin-shortcut
exec bash "${PROJECT_DIR}/scripts/admin/menu_admin.sh" "\$@"
EOF
    chmod 0755 /usr/local/bin/s-ui
  else
    warn "检测到已有 s-ui 命令(${s_ui_path})，为避免冲突，跳过覆盖。可使用 sb-admin 打开菜单。"
  fi
}

get_env_value() {
  local key="$1"
  if [[ ! -f "$ENV_FILE" ]]; then
    return
  fi
  grep -E "^${key}=" "$ENV_FILE" | head -n1 | cut -d= -f2- || true
}

has_env_key() {
  local key="$1"
  if [[ ! -f "$ENV_FILE" ]]; then
    return 1
  fi
  grep -qE "^${key}=" "$ENV_FILE"
}

load_existing_env_defaults() {
  local old_port old_port_whitelist old_admin_api_whitelist old_url old_public_url old_panel_base old_enable_https old_https_domain old_https_email old_auth old_admin_auth old_node_auth old_bot old_admin old_view_admin old_ops_admin old_super_admin old_migrate old_backup_retention old_migrate_retention old_log_archive_window old_log_archive_retention old_log_archive_dir old_menu_ttl old_monitor_interval old_offline_threshold old_time_sync_interval old_mutation_cooldown old_trust_xff old_trusted_proxy_ips old_task_timeout old_task_retention old_task_max_pending old_sub_link_sign_key old_sub_link_require old_sub_link_ttl old_rate_limit_enabled old_rate_limit_window old_rate_limit_max old_security_events_exclude_local old_security_block_protected_ips old_security_auto_block_enabled old_security_auto_block_interval old_security_auto_block_window old_security_auto_block_threshold old_security_auto_block_duration old_security_auto_block_max old_controller_http_timeout old_bot_actor_label
  local has_auth_key has_admin_auth_key has_node_auth_key
  old_port="$(get_env_value "CONTROLLER_PORT")"
  old_port_whitelist="$(get_env_value "CONTROLLER_PORT_WHITELIST")"
  old_admin_api_whitelist="$(get_env_value "ADMIN_API_WHITELIST")"
  old_url="$(get_env_value "CONTROLLER_URL")"
  old_public_url="$(get_env_value "CONTROLLER_PUBLIC_URL")"
  old_panel_base="$(get_env_value "PANEL_BASE_URL")"
  old_enable_https="$(get_env_value "ENABLE_HTTPS")"
  old_https_domain="$(get_env_value "HTTPS_DOMAIN")"
  old_https_email="$(get_env_value "HTTPS_ACME_EMAIL")"
  old_auth="$(get_env_value "AUTH_TOKEN")"
  old_admin_auth="$(get_env_value "ADMIN_AUTH_TOKEN")"
  old_node_auth="$(get_env_value "NODE_AUTH_TOKEN")"
  old_bot="$(get_env_value "BOT_TOKEN")"
  old_admin="$(get_env_value "ADMIN_CHAT_IDS")"
  old_view_admin="$(get_env_value "VIEW_ADMIN_CHAT_IDS")"
  old_ops_admin="$(get_env_value "OPS_ADMIN_CHAT_IDS")"
  old_super_admin="$(get_env_value "SUPER_ADMIN_CHAT_IDS")"
  old_migrate="$(get_env_value "MIGRATE_DIR")"
  old_backup_retention="$(get_env_value "BACKUP_RETENTION_COUNT")"
  old_migrate_retention="$(get_env_value "MIGRATE_RETENTION_COUNT")"
  old_log_archive_window="$(get_env_value "LOG_ARCHIVE_WINDOW_HOURS")"
  old_log_archive_retention="$(get_env_value "LOG_ARCHIVE_RETENTION_COUNT")"
  old_log_archive_dir="$(get_env_value "LOG_ARCHIVE_DIR")"
  old_menu_ttl="$(get_env_value "BOT_MENU_TTL")"
  old_monitor_interval="$(get_env_value "BOT_NODE_MONITOR_INTERVAL")"
  old_offline_threshold="$(get_env_value "BOT_NODE_OFFLINE_THRESHOLD")"
  old_time_sync_interval="$(get_env_value "BOT_NODE_TIME_SYNC_INTERVAL")"
  old_mutation_cooldown="$(get_env_value "BOT_MUTATION_COOLDOWN")"
  old_trust_xff="$(get_env_value "TRUST_X_FORWARDED_FOR")"
  old_trusted_proxy_ips="$(get_env_value "TRUSTED_PROXY_IPS")"
  old_task_timeout="$(get_env_value "NODE_TASK_RUNNING_TIMEOUT")"
  old_task_retention="$(get_env_value "NODE_TASK_RETENTION_SECONDS")"
  old_task_max_pending="$(get_env_value "NODE_TASK_MAX_PENDING_PER_NODE")"
  old_sub_link_sign_key="$(get_env_value "SUB_LINK_SIGN_KEY")"
  old_sub_link_require="$(get_env_value "SUB_LINK_REQUIRE_SIGNATURE")"
  old_sub_link_ttl="$(get_env_value "SUB_LINK_DEFAULT_TTL_SECONDS")"
  old_rate_limit_enabled="$(get_env_value "API_RATE_LIMIT_ENABLED")"
  old_rate_limit_window="$(get_env_value "API_RATE_LIMIT_WINDOW_SECONDS")"
  old_rate_limit_max="$(get_env_value "API_RATE_LIMIT_MAX_REQUESTS")"
  old_admin_overview_cache_ttl="$(get_env_value "ADMIN_OVERVIEW_CACHE_TTL_SECONDS")"
  old_admin_security_cache_ttl="$(get_env_value "ADMIN_SECURITY_STATUS_CACHE_TTL_SECONDS")"
  old_security_events_exclude_local="$(get_env_value "SECURITY_EVENTS_EXCLUDE_LOCAL")"
  old_security_block_protected_ips="$(get_env_value "SECURITY_BLOCK_PROTECTED_IPS")"
  old_security_auto_block_enabled="$(get_env_value "SECURITY_AUTO_BLOCK_ENABLED")"
  old_security_auto_block_interval="$(get_env_value "SECURITY_AUTO_BLOCK_INTERVAL_SECONDS")"
  old_security_auto_block_window="$(get_env_value "SECURITY_AUTO_BLOCK_WINDOW_SECONDS")"
  old_security_auto_block_threshold="$(get_env_value "SECURITY_AUTO_BLOCK_THRESHOLD")"
  old_security_auto_block_duration="$(get_env_value "SECURITY_AUTO_BLOCK_DURATION_SECONDS")"
  old_security_auto_block_max="$(get_env_value "SECURITY_AUTO_BLOCK_MAX_PER_INTERVAL")"
  old_controller_http_timeout="$(get_env_value "CONTROLLER_HTTP_TIMEOUT")"
  old_bot_actor_label="$(get_env_value "BOT_ACTOR_LABEL")"

  CONTROLLER_PORT="${old_port:-8080}"
  CONTROLLER_PORT_WHITELIST="${old_port_whitelist:-}"
  ADMIN_API_WHITELIST="${old_admin_api_whitelist:-}"
  CONTROLLER_URL="${old_url:-http://127.0.0.1:${CONTROLLER_PORT}}"
  CONTROLLER_PUBLIC_URL="${old_public_url:-}"
  PANEL_BASE_URL="${old_panel_base:-}"
  ENABLE_HTTPS="${old_enable_https:-0}"
  HTTPS_DOMAIN="$(sanitize_domain_input "${old_https_domain:-}")"
  HTTPS_ACME_EMAIL="${old_https_email:-}"
  has_auth_key=0
  has_admin_auth_key=0
  has_node_auth_key=0
  if has_env_key "AUTH_TOKEN"; then
    has_auth_key=1
    AUTH_TOKEN="$old_auth"
  else
    AUTH_TOKEN=""
  fi
  if has_env_key "ADMIN_AUTH_TOKEN"; then
    has_admin_auth_key=1
    ADMIN_AUTH_TOKEN="$old_admin_auth"
  else
    ADMIN_AUTH_TOKEN=""
  fi
  if has_env_key "NODE_AUTH_TOKEN"; then
    has_node_auth_key=1
    NODE_AUTH_TOKEN="$old_node_auth"
  else
    NODE_AUTH_TOKEN=""
  fi
  if (( has_auth_key == 1 )) && [[ -z "$AUTH_TOKEN" ]] && [[ -z "$ADMIN_AUTH_TOKEN" ]] && [[ -z "$NODE_AUTH_TOKEN" ]]; then
    AUTH_DISABLED_EXPLICIT=1
  else
    AUTH_DISABLED_EXPLICIT=0
  fi
  BOT_TOKEN="${old_bot:-$BOT_TOKEN_PLACEHOLDER}"
  ADMIN_CHAT_IDS="${old_admin:-}"
  VIEW_ADMIN_CHAT_IDS="${old_view_admin:-}"
  OPS_ADMIN_CHAT_IDS="${old_ops_admin:-}"
  SUPER_ADMIN_CHAT_IDS="${old_super_admin:-}"
  MIGRATE_DIR="${old_migrate:-$MIGRATE_DIR_DEFAULT}"
  BACKUP_RETENTION_COUNT="${old_backup_retention:-30}"
  MIGRATE_RETENTION_COUNT="${old_migrate_retention:-20}"
  LOG_ARCHIVE_WINDOW_HOURS="${old_log_archive_window:-24}"
  LOG_ARCHIVE_RETENTION_COUNT="${old_log_archive_retention:-30}"
  LOG_ARCHIVE_DIR="${old_log_archive_dir:-/var/backups/sb-controller/logs}"
  BOT_MENU_TTL="${old_menu_ttl:-60}"
  BOT_NODE_MONITOR_INTERVAL="${old_monitor_interval:-60}"
  BOT_NODE_OFFLINE_THRESHOLD="${old_offline_threshold:-120}"
  BOT_NODE_TIME_SYNC_INTERVAL="${old_time_sync_interval:-86400}"
  BOT_MUTATION_COOLDOWN="${old_mutation_cooldown:-1}"
  TRUST_X_FORWARDED_FOR="${old_trust_xff:-0}"
  TRUSTED_PROXY_IPS="${old_trusted_proxy_ips:-127.0.0.1,::1}"
  NODE_TASK_RUNNING_TIMEOUT="${old_task_timeout:-120}"
  NODE_TASK_RETENTION_SECONDS="${old_task_retention:-604800}"
  NODE_TASK_MAX_PENDING_PER_NODE="${old_task_max_pending:-50}"
  SUB_LINK_SIGN_KEY="${old_sub_link_sign_key:-}"
  SUB_LINK_REQUIRE_SIGNATURE="${old_sub_link_require:-0}"
  SUB_LINK_DEFAULT_TTL_SECONDS="${old_sub_link_ttl:-604800}"
  API_RATE_LIMIT_ENABLED="${old_rate_limit_enabled:-1}"
  API_RATE_LIMIT_WINDOW_SECONDS="${old_rate_limit_window:-60}"
  API_RATE_LIMIT_MAX_REQUESTS="${old_rate_limit_max:-120}"
  ADMIN_OVERVIEW_CACHE_TTL_SECONDS="${old_admin_overview_cache_ttl:-5}"
  ADMIN_SECURITY_STATUS_CACHE_TTL_SECONDS="${old_admin_security_cache_ttl:-5}"
  SECURITY_EVENTS_EXCLUDE_LOCAL="${old_security_events_exclude_local:-1}"
  SECURITY_BLOCK_PROTECTED_IPS="${old_security_block_protected_ips:-}"
  SECURITY_AUTO_BLOCK_ENABLED="${old_security_auto_block_enabled:-0}"
  SECURITY_AUTO_BLOCK_INTERVAL_SECONDS="${old_security_auto_block_interval:-60}"
  SECURITY_AUTO_BLOCK_WINDOW_SECONDS="${old_security_auto_block_window:-3600}"
  SECURITY_AUTO_BLOCK_THRESHOLD="${old_security_auto_block_threshold:-30}"
  SECURITY_AUTO_BLOCK_DURATION_SECONDS="${old_security_auto_block_duration:-3600}"
  SECURITY_AUTO_BLOCK_MAX_PER_INTERVAL="${old_security_auto_block_max:-5}"
  CONTROLLER_HTTP_TIMEOUT="${old_controller_http_timeout:-10}"
  BOT_ACTOR_LABEL="${old_bot_actor_label:-sb-bot}"
}

ensure_split_auth_tokens() {
  if [[ "$AUTH_DISABLED_EXPLICIT" == "1" ]]; then
    AUTH_TOKEN=""
    ADMIN_AUTH_TOKEN=""
    NODE_AUTH_TOKEN=""
    return
  fi

  # 兼容旧配置：已有 AUTH_TOKEN 且未显式配置拆分 token 时，先沿用旧值，避免升级中断。
  if [[ -z "$ADMIN_AUTH_TOKEN" && -n "$AUTH_TOKEN" ]]; then
    ADMIN_AUTH_TOKEN="$AUTH_TOKEN"
  fi
  if [[ -z "$NODE_AUTH_TOKEN" && -n "$AUTH_TOKEN" ]]; then
    NODE_AUTH_TOKEN="$AUTH_TOKEN"
  fi

  # 全新部署：默认生成拆分 token（admin/node 分离）。
  if [[ -z "$ADMIN_AUTH_TOKEN" ]]; then
    ADMIN_AUTH_TOKEN="$(generate_auth_token)"
  fi
  if [[ -z "$NODE_AUTH_TOKEN" ]]; then
    NODE_AUTH_TOKEN="$(generate_auth_token)"
  fi
  if [[ "$ADMIN_AUTH_TOKEN" == "$NODE_AUTH_TOKEN" ]]; then
    NODE_AUTH_TOKEN="$(generate_auth_token)"
  fi

  # 兼容旧脚本：保留 AUTH_TOKEN（默认镜像 admin token）。
  if [[ -z "$AUTH_TOKEN" ]]; then
    AUTH_TOKEN="$ADMIN_AUTH_TOKEN"
  fi
}

normalize_loaded_values() {
  CONTROLLER_PORT_WHITELIST="$(normalize_whitelist_csv "$CONTROLLER_PORT_WHITELIST")"
  ADMIN_API_WHITELIST="$(normalize_whitelist_csv "$ADMIN_API_WHITELIST")"
  if ! [[ "$ENABLE_HTTPS" =~ ^[01]$ ]]; then
    ENABLE_HTTPS="0"
  fi
  if ! [[ "$TRUST_X_FORWARDED_FOR" =~ ^[01]$ ]]; then
    TRUST_X_FORWARDED_FOR="0"
  fi
  if ! [[ "$NODE_TASK_RUNNING_TIMEOUT" =~ ^[0-9]+$ ]] || (( NODE_TASK_RUNNING_TIMEOUT < 30 )); then
    NODE_TASK_RUNNING_TIMEOUT="120"
  fi
  if ! [[ "$NODE_TASK_RETENTION_SECONDS" =~ ^[0-9]+$ ]] || (( NODE_TASK_RETENTION_SECONDS < 3600 )); then
    NODE_TASK_RETENTION_SECONDS="604800"
  fi
  if ! [[ "$NODE_TASK_MAX_PENDING_PER_NODE" =~ ^[0-9]+$ ]] || (( NODE_TASK_MAX_PENDING_PER_NODE < 1 )); then
    NODE_TASK_MAX_PENDING_PER_NODE="50"
  fi
  if ! [[ "$BOT_NODE_TIME_SYNC_INTERVAL" =~ ^[0-9]+$ ]]; then
    BOT_NODE_TIME_SYNC_INTERVAL="86400"
  fi
  if (( BOT_NODE_TIME_SYNC_INTERVAL > 0 && BOT_NODE_TIME_SYNC_INTERVAL < 3600 )); then
    BOT_NODE_TIME_SYNC_INTERVAL="3600"
  fi
  if ! [[ "$SUB_LINK_REQUIRE_SIGNATURE" =~ ^[01]$ ]]; then
    SUB_LINK_REQUIRE_SIGNATURE="0"
  fi
  if ! [[ "$SUB_LINK_DEFAULT_TTL_SECONDS" =~ ^[0-9]+$ ]] || (( SUB_LINK_DEFAULT_TTL_SECONDS < 60 )); then
    SUB_LINK_DEFAULT_TTL_SECONDS="604800"
  fi
  if ! [[ "$API_RATE_LIMIT_ENABLED" =~ ^[01]$ ]]; then
    API_RATE_LIMIT_ENABLED="1"
  fi
  if ! [[ "$API_RATE_LIMIT_WINDOW_SECONDS" =~ ^[0-9]+$ ]] || (( API_RATE_LIMIT_WINDOW_SECONDS < 1 )); then
    API_RATE_LIMIT_WINDOW_SECONDS="60"
  fi
  if ! [[ "$API_RATE_LIMIT_MAX_REQUESTS" =~ ^[0-9]+$ ]] || (( API_RATE_LIMIT_MAX_REQUESTS < 1 )); then
    API_RATE_LIMIT_MAX_REQUESTS="120"
  fi
  if ! [[ "$ADMIN_OVERVIEW_CACHE_TTL_SECONDS" =~ ^[0-9]+$ ]]; then
    ADMIN_OVERVIEW_CACHE_TTL_SECONDS="5"
  fi
  if (( ADMIN_OVERVIEW_CACHE_TTL_SECONDS > 300 )); then
    ADMIN_OVERVIEW_CACHE_TTL_SECONDS="300"
  fi
  if ! [[ "$ADMIN_SECURITY_STATUS_CACHE_TTL_SECONDS" =~ ^[0-9]+$ ]]; then
    ADMIN_SECURITY_STATUS_CACHE_TTL_SECONDS="5"
  fi
  if (( ADMIN_SECURITY_STATUS_CACHE_TTL_SECONDS > 300 )); then
    ADMIN_SECURITY_STATUS_CACHE_TTL_SECONDS="300"
  fi
  if ! [[ "$SECURITY_EVENTS_EXCLUDE_LOCAL" =~ ^[01]$ ]]; then
    SECURITY_EVENTS_EXCLUDE_LOCAL="1"
  fi
  SECURITY_BLOCK_PROTECTED_IPS="$(normalize_whitelist_csv "$SECURITY_BLOCK_PROTECTED_IPS")"
  if ! [[ "$SECURITY_AUTO_BLOCK_ENABLED" =~ ^[01]$ ]]; then
    SECURITY_AUTO_BLOCK_ENABLED="0"
  fi
  if ! [[ "$SECURITY_AUTO_BLOCK_INTERVAL_SECONDS" =~ ^[0-9]+$ ]] || (( SECURITY_AUTO_BLOCK_INTERVAL_SECONDS < 10 )); then
    SECURITY_AUTO_BLOCK_INTERVAL_SECONDS="60"
  fi
  if ! [[ "$SECURITY_AUTO_BLOCK_WINDOW_SECONDS" =~ ^[0-9]+$ ]] || (( SECURITY_AUTO_BLOCK_WINDOW_SECONDS < 60 )); then
    SECURITY_AUTO_BLOCK_WINDOW_SECONDS="3600"
  fi
  if ! [[ "$SECURITY_AUTO_BLOCK_THRESHOLD" =~ ^[0-9]+$ ]] || (( SECURITY_AUTO_BLOCK_THRESHOLD < 1 )); then
    SECURITY_AUTO_BLOCK_THRESHOLD="30"
  fi
  if ! [[ "$SECURITY_AUTO_BLOCK_DURATION_SECONDS" =~ ^[0-9]+$ ]] || (( SECURITY_AUTO_BLOCK_DURATION_SECONDS < 0 )); then
    SECURITY_AUTO_BLOCK_DURATION_SECONDS="3600"
  fi
  if ! [[ "$SECURITY_AUTO_BLOCK_MAX_PER_INTERVAL" =~ ^[0-9]+$ ]] || (( SECURITY_AUTO_BLOCK_MAX_PER_INTERVAL < 1 )); then
    SECURITY_AUTO_BLOCK_MAX_PER_INTERVAL="5"
  fi
  if ! [[ "$BACKUP_RETENTION_COUNT" =~ ^[0-9]+$ ]] || (( BACKUP_RETENTION_COUNT < 1 )); then
    BACKUP_RETENTION_COUNT="30"
  fi
  if ! [[ "$MIGRATE_RETENTION_COUNT" =~ ^[0-9]+$ ]] || (( MIGRATE_RETENTION_COUNT < 1 )); then
    MIGRATE_RETENTION_COUNT="20"
  fi
  if ! [[ "$LOG_ARCHIVE_WINDOW_HOURS" =~ ^[0-9]+$ ]] || (( LOG_ARCHIVE_WINDOW_HOURS < 1 )); then
    LOG_ARCHIVE_WINDOW_HOURS="24"
  fi
  if ! [[ "$LOG_ARCHIVE_RETENTION_COUNT" =~ ^[0-9]+$ ]] || (( LOG_ARCHIVE_RETENTION_COUNT < 1 )); then
    LOG_ARCHIVE_RETENTION_COUNT="30"
  fi
  if [[ -z "$LOG_ARCHIVE_DIR" ]]; then
    LOG_ARCHIVE_DIR="/var/backups/sb-controller/logs"
  fi
  if ! [[ "$CONTROLLER_HTTP_TIMEOUT" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    CONTROLLER_HTTP_TIMEOUT="10"
  fi
  if [[ -z "$BOT_ACTOR_LABEL" ]]; then
    BOT_ACTOR_LABEL="sb-bot"
  fi
  if ! [[ "$BOT_MUTATION_COOLDOWN" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    BOT_MUTATION_COOLDOWN="1"
  fi
  ensure_split_auth_tokens
  CONTROLLER_URL="$(normalize_input_url "$CONTROLLER_URL" "http")"
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
  if [[ "$ENABLE_HTTPS" == "1" ]]; then
    HTTPS_DOMAIN="$(sanitize_domain_input "$HTTPS_DOMAIN")"
    if [[ -n "$HTTPS_DOMAIN" ]]; then
      CONTROLLER_PUBLIC_URL="https://${HTTPS_DOMAIN}"
      if [[ -z "$PANEL_BASE_URL" ]]; then
        PANEL_BASE_URL="$CONTROLLER_PUBLIC_URL"
      fi
    fi
  fi
}

prompt_env_config() {
  load_existing_env_defaults

  echo ""
  msg "配置向导说明："
  echo "  - CONTROLLER_PORT：controller 对外监听端口（节点 agent 需要访问）"
  echo "  - CONTROLLER_PORT_WHITELIST：可选，限制可访问 controller 端口的来源 IP/CIDR"
  echo "  - ADMIN_API_WHITELIST：可选，管理接口来源白名单（应用层二次限制）"
  echo "  - CONTROLLER_PUBLIC_URL：可选，对外访问 URL（给节点/外部使用）"
  echo "  - PANEL_BASE_URL：bot 生成订阅链接使用的基础地址（建议使用域名）"
  echo "  - ENABLE_HTTPS / HTTPS_DOMAIN：启用 Caddy 自动证书（申请+续期）"
  echo "  - HTTPS_ACME_EMAIL：证书账号邮箱（可选，建议填写）"
  echo "  - CONTROLLER_URL：bot 调用 controller 的地址（通常 127.0.0.1）"
  echo "  - ADMIN_AUTH_TOKEN/NODE_AUTH_TOKEN：默认自动生成并拆分（管理/节点分离）"
  echo "  - AUTH_TOKEN：兼容字段（默认镜像 ADMIN_AUTH_TOKEN；输入 - 可同时关闭全部鉴权）"
  echo "  - BOT_TOKEN：建议填写；留空将使用占位值并跳过启动 sb-bot"
  echo "  - ADMIN_CHAT_IDS：可选；用于限制谁可操作 bot"
  echo "  - VIEW/OPS/SUPER_ADMIN_CHAT_IDS：可选；细分只读/运维/超级管理员权限"
  echo "  - MIGRATE_DIR：迁移包/备份包输出目录"
  echo "  - BACKUP_RETENTION_COUNT：控制器备份保留数量（超出自动清理）"
  echo "  - MIGRATE_RETENTION_COUNT：迁移包保留数量（超出自动清理）"
  echo "  - LOG_ARCHIVE_*：运维日志归档窗口/保留/目录"
  echo "  - BOT_MENU_TTL：bot 菜单按钮自动清理秒数"
  echo "  - BOT_NODE_MONITOR_INTERVAL：节点在线检测周期秒数"
  echo "  - BOT_NODE_OFFLINE_THRESHOLD：节点离线判定阈值秒数"
  echo "  - BOT_NODE_TIME_SYNC_INTERVAL：节点自动时间对齐周期秒数（0=关闭）"
  echo "  - BOT_MUTATION_COOLDOWN：bot 写操作按钮防抖秒数（防止重复点击）"
  echo "  - TRUST_X_FORWARDED_FOR/TRUSTED_PROXY_IPS：仅在受控反代场景下才启用 XFF"
  echo "  - SECURITY_EVENTS_EXCLUDE_LOCAL：安全统计默认过滤本机测试来源（建议 1）"
  echo "  - SECURITY_BLOCK_PROTECTED_IPS：封禁保护白名单（IP/CIDR；manual/auto block 均跳过）"
  echo "  - SECURITY_AUTO_BLOCK_*：自动封禁策略（默认关闭，建议先观察后开启）"
  echo "  - NODE_TASK_*：节点任务超时与历史清理参数"
  echo ""

  read -r -p "CONTROLLER_PORT（controller 对外监听端口；节点 agent 需要访问） [${CONTROLLER_PORT}]: " input_port
  CONTROLLER_PORT="${input_port:-$CONTROLLER_PORT}"
  if ! [[ "$CONTROLLER_PORT" =~ ^[0-9]+$ ]] || (( CONTROLLER_PORT < 1 || CONTROLLER_PORT > 65535 )); then
    warn "端口无效，已回退为 8080"
    CONTROLLER_PORT="8080"
  fi
  read -r -p "CONTROLLER_PORT_WHITELIST（可选，逗号分隔 IP/CIDR；留空=公网放行） [${CONTROLLER_PORT_WHITELIST}]: " input_port_whitelist
  CONTROLLER_PORT_WHITELIST="${input_port_whitelist:-$CONTROLLER_PORT_WHITELIST}"
  CONTROLLER_PORT_WHITELIST="$(normalize_whitelist_csv "$CONTROLLER_PORT_WHITELIST")"
  read -r -p "ADMIN_API_WHITELIST（可选，逗号分隔 IP/CIDR；留空=不启用应用层来源限制） [${ADMIN_API_WHITELIST}]: " input_admin_api_whitelist
  ADMIN_API_WHITELIST="${input_admin_api_whitelist:-$ADMIN_API_WHITELIST}"
  ADMIN_API_WHITELIST="$(normalize_whitelist_csv "$ADMIN_API_WHITELIST")"
  read -r -p "SECURITY_BLOCK_PROTECTED_IPS（可选，逗号分隔 IP/CIDR；封禁保护白名单） [${SECURITY_BLOCK_PROTECTED_IPS}]: " input_block_protected
  SECURITY_BLOCK_PROTECTED_IPS="${input_block_protected:-$SECURITY_BLOCK_PROTECTED_IPS}"
  SECURITY_BLOCK_PROTECTED_IPS="$(normalize_whitelist_csv "$SECURITY_BLOCK_PROTECTED_IPS")"

  local public_ip default_public_url
  public_ip="$(get_public_ipv4)"
  if [[ -n "$public_ip" ]]; then
    default_public_url="http://${public_ip}:${CONTROLLER_PORT}"
  else
    default_public_url=""
  fi
  read -r -p "CONTROLLER_PUBLIC_URL（可选；给节点/外部访问，支持省略 http/https） [${CONTROLLER_PUBLIC_URL:-$default_public_url}]: " input_public_url
  CONTROLLER_PUBLIC_URL="${input_public_url:-${CONTROLLER_PUBLIC_URL:-$default_public_url}}"
  CONTROLLER_PUBLIC_URL="$(normalize_input_url "$CONTROLLER_PUBLIC_URL" "http")"

  local public_host enable_https_default
  public_host="$(extract_url_host "$CONTROLLER_PUBLIC_URL")"
  if [[ -z "$HTTPS_DOMAIN" && -n "$public_host" ]] && ! is_ipv4 "$public_host"; then
    HTTPS_DOMAIN="$public_host"
  fi
  if [[ "$ENABLE_HTTPS" == "1" || "$CONTROLLER_PUBLIC_URL" == https://* ]]; then
    enable_https_default="Y"
  else
    enable_https_default="N"
  fi

  if ask_yes_no "是否启用 HTTPS 反向代理（Caddy 自动申请与续期证书）？" "$enable_https_default"; then
    ENABLE_HTTPS="1"
    while [[ -z "$HTTPS_DOMAIN" ]] || is_ipv4 "$HTTPS_DOMAIN"; do
      read -r -p "HTTPS_DOMAIN（证书域名，例如 panel.example.com） [${HTTPS_DOMAIN}]: " input_https_domain
      HTTPS_DOMAIN="${input_https_domain:-$HTTPS_DOMAIN}"
      HTTPS_DOMAIN="$(sanitize_domain_input "$HTTPS_DOMAIN")"
      if [[ -z "$HTTPS_DOMAIN" ]] || is_ipv4 "$HTTPS_DOMAIN" || ! is_valid_domain "$HTTPS_DOMAIN"; then
        warn "HTTPS_DOMAIN 无效，请填写域名（例如 panel.example.com），不要填 IP/路径。"
      fi
    done
    read -r -p "HTTPS_ACME_EMAIL（证书账号邮箱，可选） [${HTTPS_ACME_EMAIL}]: " input_https_email
    HTTPS_ACME_EMAIL="${input_https_email:-$HTTPS_ACME_EMAIL}"
    CONTROLLER_PUBLIC_URL="https://${HTTPS_DOMAIN}"
  else
    ENABLE_HTTPS="0"
    HTTPS_DOMAIN=""
    HTTPS_ACME_EMAIL=""
  fi

  local default_controller_url="http://127.0.0.1:${CONTROLLER_PORT}"
  local default_panel_base
  default_panel_base="${PANEL_BASE_URL:-$CONTROLLER_PUBLIC_URL}"
  if [[ "$ENABLE_HTTPS" == "1" && -n "$HTTPS_DOMAIN" ]]; then
    default_panel_base="https://${HTTPS_DOMAIN}"
  fi
  if [[ -z "$default_panel_base" ]]; then
    default_panel_base="$default_controller_url"
  fi
  read -r -p "PANEL_BASE_URL（bot订阅链接地址；支持省略 http/https） [${default_panel_base}]: " input_panel_base
  PANEL_BASE_URL="${input_panel_base:-$default_panel_base}"
  local panel_scheme
  panel_scheme="http"
  if [[ "$ENABLE_HTTPS" == "1" || "$PANEL_BASE_URL" == https://* ]]; then
    panel_scheme="https"
  fi
  PANEL_BASE_URL="$(normalize_input_url "$PANEL_BASE_URL" "$panel_scheme")"

  read -r -p "CONTROLLER_URL（给 bot 调用，支持省略 http/https） [${CONTROLLER_URL:-$default_controller_url}]: " input_url
  CONTROLLER_URL="${input_url:-${CONTROLLER_URL:-$default_controller_url}}"
  local controller_host controller_scheme
  controller_host="$(extract_url_host "$CONTROLLER_URL")"
  controller_scheme="http"
  if [[ "$ENABLE_HTTPS" == "1" && "$controller_host" != "127.0.0.1" && "$controller_host" != "localhost" ]]; then
    controller_scheme="https"
  fi
  CONTROLLER_URL="$(normalize_input_url "$CONTROLLER_URL" "$controller_scheme")"

  read -r -p "AUTH_TOKEN（可选；保护 controller 接口；输入 - 可清空关闭鉴权） [${AUTH_TOKEN}]: " input_auth
  if [[ "$input_auth" == "-" ]]; then
    AUTH_DISABLED_EXPLICIT=1
    AUTH_TOKEN=""
    ADMIN_AUTH_TOKEN=""
    NODE_AUTH_TOKEN=""
  else
    AUTH_TOKEN="${input_auth:-$AUTH_TOKEN}"
    AUTH_DISABLED_EXPLICIT=0
  fi

  read -r -p "BOT_TOKEN（建议填写；Telegram 机器人 token，直接回车使用默认占位） [${BOT_TOKEN}]: " input_bot
  BOT_TOKEN="${input_bot:-$BOT_TOKEN}"
  if ! is_bot_token_configured "$BOT_TOKEN"; then
    warn "BOT_TOKEN 当前为占位值。脚本会完成安装，但不会启动 sb-bot，需后续在配置向导中填入真实 token。"
  fi

  read -r -p "ADMIN_CHAT_IDS（可选；逗号分隔，限制谁能操作 bot） [${ADMIN_CHAT_IDS}]: " input_admin
  ADMIN_CHAT_IDS="${input_admin:-$ADMIN_CHAT_IDS}"

  read -r -p "VIEW_ADMIN_CHAT_IDS（可选；只读管理员） [${VIEW_ADMIN_CHAT_IDS}]: " input_view_admin
  VIEW_ADMIN_CHAT_IDS="${input_view_admin:-$VIEW_ADMIN_CHAT_IDS}"
  read -r -p "OPS_ADMIN_CHAT_IDS（可选；运维管理员） [${OPS_ADMIN_CHAT_IDS}]: " input_ops_admin
  OPS_ADMIN_CHAT_IDS="${input_ops_admin:-$OPS_ADMIN_CHAT_IDS}"
  read -r -p "SUPER_ADMIN_CHAT_IDS（可选；超级管理员） [${SUPER_ADMIN_CHAT_IDS}]: " input_super_admin
  SUPER_ADMIN_CHAT_IDS="${input_super_admin:-$SUPER_ADMIN_CHAT_IDS}"

  read -r -p "MIGRATE_DIR（迁移包/备份包输出目录，直接回车使用默认） [${MIGRATE_DIR}]: " input_migrate
  MIGRATE_DIR="${input_migrate:-${MIGRATE_DIR:-$MIGRATE_DIR_DEFAULT}}"
  if [[ -z "$MIGRATE_DIR" ]]; then
    MIGRATE_DIR="$MIGRATE_DIR_DEFAULT"
  fi

  read -r -p "BACKUP_RETENTION_COUNT（控制器备份保留数量） [${BACKUP_RETENTION_COUNT}]: " input_backup_retention
  BACKUP_RETENTION_COUNT="${input_backup_retention:-$BACKUP_RETENTION_COUNT}"
  if ! [[ "$BACKUP_RETENTION_COUNT" =~ ^[0-9]+$ ]] || (( BACKUP_RETENTION_COUNT < 1 )); then
    warn "BACKUP_RETENTION_COUNT 无效，回退为 30"
    BACKUP_RETENTION_COUNT="30"
  fi

  read -r -p "MIGRATE_RETENTION_COUNT（迁移包保留数量） [${MIGRATE_RETENTION_COUNT}]: " input_migrate_retention
  MIGRATE_RETENTION_COUNT="${input_migrate_retention:-$MIGRATE_RETENTION_COUNT}"
  if ! [[ "$MIGRATE_RETENTION_COUNT" =~ ^[0-9]+$ ]] || (( MIGRATE_RETENTION_COUNT < 1 )); then
    warn "MIGRATE_RETENTION_COUNT 无效，回退为 20"
    MIGRATE_RETENTION_COUNT="20"
  fi

  read -r -p "LOG_ARCHIVE_WINDOW_HOURS（日志归档窗口小时数） [${LOG_ARCHIVE_WINDOW_HOURS}]: " input_log_window
  LOG_ARCHIVE_WINDOW_HOURS="${input_log_window:-$LOG_ARCHIVE_WINDOW_HOURS}"
  if ! [[ "$LOG_ARCHIVE_WINDOW_HOURS" =~ ^[0-9]+$ ]] || (( LOG_ARCHIVE_WINDOW_HOURS < 1 )); then
    warn "LOG_ARCHIVE_WINDOW_HOURS 无效，回退为 24"
    LOG_ARCHIVE_WINDOW_HOURS="24"
  fi

  read -r -p "LOG_ARCHIVE_RETENTION_COUNT（日志归档保留数量） [${LOG_ARCHIVE_RETENTION_COUNT}]: " input_log_retention
  LOG_ARCHIVE_RETENTION_COUNT="${input_log_retention:-$LOG_ARCHIVE_RETENTION_COUNT}"
  if ! [[ "$LOG_ARCHIVE_RETENTION_COUNT" =~ ^[0-9]+$ ]] || (( LOG_ARCHIVE_RETENTION_COUNT < 1 )); then
    warn "LOG_ARCHIVE_RETENTION_COUNT 无效，回退为 30"
    LOG_ARCHIVE_RETENTION_COUNT="30"
  fi

  read -r -p "LOG_ARCHIVE_DIR（日志归档目录） [${LOG_ARCHIVE_DIR}]: " input_log_dir
  LOG_ARCHIVE_DIR="${input_log_dir:-$LOG_ARCHIVE_DIR}"
  if [[ -z "$LOG_ARCHIVE_DIR" ]]; then
    LOG_ARCHIVE_DIR="/var/backups/sb-controller/logs"
  fi

  read -r -p "BOT_MENU_TTL（bot 菜单自动清理秒数） [${BOT_MENU_TTL}]: " input_menu_ttl
  BOT_MENU_TTL="${input_menu_ttl:-$BOT_MENU_TTL}"
  if ! [[ "$BOT_MENU_TTL" =~ ^[0-9]+$ ]] || (( BOT_MENU_TTL < 5 )); then
    warn "BOT_MENU_TTL 无效，回退为 60"
    BOT_MENU_TTL="60"
  fi

  read -r -p "BOT_NODE_MONITOR_INTERVAL（节点监控轮询秒数） [${BOT_NODE_MONITOR_INTERVAL}]: " input_monitor_interval
  BOT_NODE_MONITOR_INTERVAL="${input_monitor_interval:-$BOT_NODE_MONITOR_INTERVAL}"
  if ! [[ "$BOT_NODE_MONITOR_INTERVAL" =~ ^[0-9]+$ ]] || (( BOT_NODE_MONITOR_INTERVAL < 10 )); then
    warn "BOT_NODE_MONITOR_INTERVAL 无效，回退为 60"
    BOT_NODE_MONITOR_INTERVAL="60"
  fi

  read -r -p "BOT_NODE_OFFLINE_THRESHOLD（离线判定阈值秒数） [${BOT_NODE_OFFLINE_THRESHOLD}]: " input_offline_threshold
  BOT_NODE_OFFLINE_THRESHOLD="${input_offline_threshold:-$BOT_NODE_OFFLINE_THRESHOLD}"
  if ! [[ "$BOT_NODE_OFFLINE_THRESHOLD" =~ ^[0-9]+$ ]] || (( BOT_NODE_OFFLINE_THRESHOLD < 30 )); then
    warn "BOT_NODE_OFFLINE_THRESHOLD 无效，回退为 120"
    BOT_NODE_OFFLINE_THRESHOLD="120"
  fi

  read -r -p "BOT_NODE_TIME_SYNC_INTERVAL（节点自动时间对齐秒数，0=关闭） [${BOT_NODE_TIME_SYNC_INTERVAL}]: " input_time_sync_interval
  BOT_NODE_TIME_SYNC_INTERVAL="${input_time_sync_interval:-$BOT_NODE_TIME_SYNC_INTERVAL}"
  if ! [[ "$BOT_NODE_TIME_SYNC_INTERVAL" =~ ^[0-9]+$ ]]; then
    warn "BOT_NODE_TIME_SYNC_INTERVAL 无效，回退为 86400"
    BOT_NODE_TIME_SYNC_INTERVAL="86400"
  elif (( BOT_NODE_TIME_SYNC_INTERVAL > 0 && BOT_NODE_TIME_SYNC_INTERVAL < 3600 )); then
    warn "BOT_NODE_TIME_SYNC_INTERVAL 过小，最小为 3600（或 0 关闭）"
    BOT_NODE_TIME_SYNC_INTERVAL="3600"
  fi

  read -r -p "BOT_MUTATION_COOLDOWN（写操作防抖秒数） [${BOT_MUTATION_COOLDOWN}]: " input_mutation_cooldown
  BOT_MUTATION_COOLDOWN="${input_mutation_cooldown:-$BOT_MUTATION_COOLDOWN}"
  if ! [[ "$BOT_MUTATION_COOLDOWN" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
    warn "BOT_MUTATION_COOLDOWN 无效，回退为 1"
    BOT_MUTATION_COOLDOWN="1"
  fi

  echo ""
  msg "UFW/端口放行说明："
  if [[ -n "$CONTROLLER_PORT_WHITELIST" ]]; then
    echo "  - ${CONTROLLER_PORT}/tcp 按白名单放行：${CONTROLLER_PORT_WHITELIST}"
  else
    echo "  - ${CONTROLLER_PORT}/tcp 为公网放行（建议生产环境改为白名单）"
  fi
  if [[ "$ENABLE_HTTPS" == "1" ]]; then
    echo "  - 需要放行 80/tcp 与 443/tcp（Caddy 证书申请/HTTPS）"
  fi
  echo "  - 如仅内网使用，建议限制来源 IP，而不是全网开放"
}

prompt_env_config_quick() {
  load_existing_env_defaults

  echo ""
  msg "快速配置（推荐默认值）"
  echo "  - 本模式会优先使用安全默认值，仅提问最关键参数。"
  echo "  - 其余变量会自动按标准默认值写入。"
  echo "  - 后续可在菜单中进入“高级变量设置向导”逐项调整。"
  echo ""

  local public_ip default_public_url default_controller_url default_panel_base
  public_ip="$(get_public_ipv4)"
  if [[ -n "$public_ip" ]]; then
    default_public_url="http://${public_ip}:${CONTROLLER_PORT}"
  else
    default_public_url="http://127.0.0.1:${CONTROLLER_PORT}"
  fi
  read -r -p "CONTROLLER_PUBLIC_URL（节点/外部访问地址，支持省略 http/https） [${CONTROLLER_PUBLIC_URL:-$default_public_url}]: " input_public_url
  CONTROLLER_PUBLIC_URL="${input_public_url:-${CONTROLLER_PUBLIC_URL:-$default_public_url}}"
  CONTROLLER_PUBLIC_URL="$(normalize_input_url "$CONTROLLER_PUBLIC_URL" "http")"

  local enable_https_default
  if [[ "$ENABLE_HTTPS" == "1" ]]; then
    enable_https_default="Y"
  else
    enable_https_default="N"
  fi
  if ask_yes_no "是否启用 HTTPS（Caddy 自动证书）？" "$enable_https_default"; then
    ENABLE_HTTPS="1"
    local public_host
    public_host="$(extract_url_host "$CONTROLLER_PUBLIC_URL")"
    if [[ -z "$HTTPS_DOMAIN" && -n "$public_host" ]] && ! is_ipv4 "$public_host"; then
      HTTPS_DOMAIN="$public_host"
    fi
    read -r -p "HTTPS_DOMAIN（证书域名） [${HTTPS_DOMAIN}]: " input_https_domain
    HTTPS_DOMAIN="$(sanitize_domain_input "${input_https_domain:-$HTTPS_DOMAIN}")"
    if [[ -z "$HTTPS_DOMAIN" ]] || ! is_valid_domain "$HTTPS_DOMAIN"; then
      warn "域名无效，回退为 HTTP 模式。"
      ENABLE_HTTPS="0"
      HTTPS_DOMAIN=""
      HTTPS_ACME_EMAIL=""
    else
      read -r -p "HTTPS_ACME_EMAIL（证书账号邮箱，可选） [${HTTPS_ACME_EMAIL}]: " input_https_email
      HTTPS_ACME_EMAIL="${input_https_email:-$HTTPS_ACME_EMAIL}"
      CONTROLLER_PUBLIC_URL="https://${HTTPS_DOMAIN}"
    fi
  else
    ENABLE_HTTPS="0"
    HTTPS_DOMAIN=""
    HTTPS_ACME_EMAIL=""
  fi

  default_controller_url="http://127.0.0.1:${CONTROLLER_PORT}"
  CONTROLLER_URL="$default_controller_url"
  if [[ "$ENABLE_HTTPS" == "1" && -n "$HTTPS_DOMAIN" ]]; then
    default_panel_base="https://${HTTPS_DOMAIN}"
  else
    default_panel_base="${CONTROLLER_PUBLIC_URL:-$default_controller_url}"
  fi
  PANEL_BASE_URL="$(normalize_input_url "$default_panel_base" "$([[ "$ENABLE_HTTPS" == "1" ]] && echo https || echo http)")"

  read -r -p "BOT_TOKEN（Telegram 机器人 token；可先留空） [${BOT_TOKEN}]: " input_bot
  BOT_TOKEN="${input_bot:-$BOT_TOKEN}"
  if ! is_bot_token_configured "$BOT_TOKEN"; then
    warn "BOT_TOKEN 为空/占位：将跳过 sb-bot 启动，你可稍后在高级向导中补填。"
  fi

  read -r -p "ADMIN_CHAT_IDS（可选，逗号分隔；不填=不限制） [${ADMIN_CHAT_IDS}]: " input_admin
  ADMIN_CHAT_IDS="${input_admin:-$ADMIN_CHAT_IDS}"

  read -r -p "AUTH_TOKEN（接口鉴权 token，回车使用当前值） [${AUTH_TOKEN}]: " input_auth
  if [[ "$input_auth" == "-" ]]; then
    AUTH_DISABLED_EXPLICIT=1
    AUTH_TOKEN=""
    ADMIN_AUTH_TOKEN=""
    NODE_AUTH_TOKEN=""
  else
    AUTH_TOKEN="${input_auth:-$AUTH_TOKEN}"
    AUTH_DISABLED_EXPLICIT=0
  fi

  local current_client_ip
  current_client_ip=""
  if [[ -z "${SECURITY_BLOCK_PROTECTED_IPS:-}" ]]; then
    current_client_ip="$(detect_current_ssh_client_ip)"
    if [[ -n "$current_client_ip" ]]; then
      SECURITY_BLOCK_PROTECTED_IPS="$current_client_ip"
      msg "快速配置安全默认：已自动设置封禁保护白名单为当前来源 IP（${current_client_ip}）。"
    fi
  fi
  if [[ -z "${ADMIN_API_WHITELIST:-}" ]]; then
    if [[ -z "$current_client_ip" ]]; then
      current_client_ip="$(detect_current_ssh_client_ip)"
    fi
    if [[ -n "$current_client_ip" ]]; then
      ADMIN_API_WHITELIST="$current_client_ip"
      msg "快速配置安全默认：已自动设置管理接口来源白名单为当前来源 IP（${current_client_ip}）。"
    fi
  fi

  if [[ -z "$MIGRATE_DIR" ]]; then
    MIGRATE_DIR="$MIGRATE_DIR_DEFAULT"
  fi

  normalize_loaded_values
  echo ""
  msg "快速配置完成：其余变量已使用默认值。"
}

write_env_file() {
  msg "写入环境配置: ${ENV_FILE}"
  mkdir -p "$PROJECT_DIR"
  cat >"$ENV_FILE" <<EOF
# 给 bot 调用 controller 的地址
CONTROLLER_URL=${CONTROLLER_URL}

# 对外访问 controller 的地址（可选，给节点/外部使用）
CONTROLLER_PUBLIC_URL=${CONTROLLER_PUBLIC_URL}

# Bot 订阅链接基础地址（建议域名）
PANEL_BASE_URL=${PANEL_BASE_URL}

# 启用 Caddy HTTPS（1=启用，0=关闭）
ENABLE_HTTPS=${ENABLE_HTTPS}

# Caddy 证书域名（启用 HTTPS 时必填）
HTTPS_DOMAIN=${HTTPS_DOMAIN}

# Caddy ACME 账号邮箱（可选）
HTTPS_ACME_EMAIL=${HTTPS_ACME_EMAIL}

# controller 监听端口（供 systemd 使用）
CONTROLLER_PORT=${CONTROLLER_PORT}

# controller 端口白名单（可选，逗号分隔）
CONTROLLER_PORT_WHITELIST=${CONTROLLER_PORT_WHITELIST}
ADMIN_API_WHITELIST=${ADMIN_API_WHITELIST}
SECURITY_BLOCK_PROTECTED_IPS=${SECURITY_BLOCK_PROTECTED_IPS}

# 轻量鉴权 token（可用逗号分隔做轮换过渡）
AUTH_TOKEN=${AUTH_TOKEN}

# 管理接口鉴权 token（可用逗号分隔做轮换过渡；优先于 AUTH_TOKEN）
ADMIN_AUTH_TOKEN=${ADMIN_AUTH_TOKEN}

# 节点接口鉴权 token（可用逗号分隔做轮换过渡；优先于 AUTH_TOKEN）
NODE_AUTH_TOKEN=${NODE_AUTH_TOKEN}

# Telegram Bot token（必填）
BOT_TOKEN=${BOT_TOKEN}

# 管理员 chat id，逗号分隔，可空
ADMIN_CHAT_IDS=${ADMIN_CHAT_IDS}

# 只读管理员 chat id（可选）
VIEW_ADMIN_CHAT_IDS=${VIEW_ADMIN_CHAT_IDS}

# 运维管理员 chat id（可选）
OPS_ADMIN_CHAT_IDS=${OPS_ADMIN_CHAT_IDS}

# 超级管理员 chat id（可选）
SUPER_ADMIN_CHAT_IDS=${SUPER_ADMIN_CHAT_IDS}

# 迁移包目录
MIGRATE_DIR=${MIGRATE_DIR}

# 控制器备份保留数量（超出自动清理）
BACKUP_RETENTION_COUNT=${BACKUP_RETENTION_COUNT}

# 迁移包保留数量（超出自动清理）
MIGRATE_RETENTION_COUNT=${MIGRATE_RETENTION_COUNT}

# 日志归档窗口小时数
LOG_ARCHIVE_WINDOW_HOURS=${LOG_ARCHIVE_WINDOW_HOURS}

# 日志归档保留数量
LOG_ARCHIVE_RETENTION_COUNT=${LOG_ARCHIVE_RETENTION_COUNT}

# 日志归档目录
LOG_ARCHIVE_DIR=${LOG_ARCHIVE_DIR}

# Bot 菜单按钮自动清理秒数
BOT_MENU_TTL=${BOT_MENU_TTL}

# 节点在线检测周期秒数
BOT_NODE_MONITOR_INTERVAL=${BOT_NODE_MONITOR_INTERVAL}

# 节点离线判定阈值秒数
BOT_NODE_OFFLINE_THRESHOLD=${BOT_NODE_OFFLINE_THRESHOLD}

# 节点自动时间对齐周期秒数（0=关闭）
BOT_NODE_TIME_SYNC_INTERVAL=${BOT_NODE_TIME_SYNC_INTERVAL}

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

# 订阅签名密钥（可选，留空关闭签名）
SUB_LINK_SIGN_KEY=${SUB_LINK_SIGN_KEY}

# 是否强制订阅签名（1=强制，0=兼容）
SUB_LINK_REQUIRE_SIGNATURE=${SUB_LINK_REQUIRE_SIGNATURE}

# 订阅签名默认有效期（秒）
SUB_LINK_DEFAULT_TTL_SECONDS=${SUB_LINK_DEFAULT_TTL_SECONDS}

# controller 轻量限流开关（1=启用，0=关闭）
API_RATE_LIMIT_ENABLED=${API_RATE_LIMIT_ENABLED}

# 限流窗口秒数
API_RATE_LIMIT_WINDOW_SECONDS=${API_RATE_LIMIT_WINDOW_SECONDS}

# 窗口内最大请求数
API_RATE_LIMIT_MAX_REQUESTS=${API_RATE_LIMIT_MAX_REQUESTS}

# 管理端概览缓存秒数（0=关闭缓存）
ADMIN_OVERVIEW_CACHE_TTL_SECONDS=${ADMIN_OVERVIEW_CACHE_TTL_SECONDS}

# 管理端安全状态缓存秒数（0=关闭缓存）
ADMIN_SECURITY_STATUS_CACHE_TTL_SECONDS=${ADMIN_SECURITY_STATUS_CACHE_TTL_SECONDS}

# 安全事件统计是否默认过滤本机来源（1=过滤，0=不过滤）
SECURITY_EVENTS_EXCLUDE_LOCAL=${SECURITY_EVENTS_EXCLUDE_LOCAL}

# 自动封禁开关（1=启用，0=关闭）
SECURITY_AUTO_BLOCK_ENABLED=${SECURITY_AUTO_BLOCK_ENABLED}

# 自动封禁巡检周期秒数
SECURITY_AUTO_BLOCK_INTERVAL_SECONDS=${SECURITY_AUTO_BLOCK_INTERVAL_SECONDS}

# 自动封禁统计窗口秒数
SECURITY_AUTO_BLOCK_WINDOW_SECONDS=${SECURITY_AUTO_BLOCK_WINDOW_SECONDS}

# 自动封禁触发阈值（窗口内未授权次数）
SECURITY_AUTO_BLOCK_THRESHOLD=${SECURITY_AUTO_BLOCK_THRESHOLD}

# 自动封禁时长秒数（0=永久）
SECURITY_AUTO_BLOCK_DURATION_SECONDS=${SECURITY_AUTO_BLOCK_DURATION_SECONDS}

# 单次最多自动封禁IP数量
SECURITY_AUTO_BLOCK_MAX_PER_INTERVAL=${SECURITY_AUTO_BLOCK_MAX_PER_INTERVAL}

# bot 调 controller 请求超时（秒）
CONTROLLER_HTTP_TIMEOUT=${CONTROLLER_HTTP_TIMEOUT}

# bot 审计操作者标识
BOT_ACTOR_LABEL=${BOT_ACTOR_LABEL}
EOF
  chmod 0600 "$ENV_FILE"
}

setup_venv_and_requirements() {
  if [[ ! -f "$PROJECT_DIR/requirements.txt" ]]; then
    err "缺少 requirements.txt，无法安装 Python 依赖。"
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
    msg "创建 venv: $VENV_DIR"
    "$PYTHON_BIN" -m venv "$VENV_DIR"
  fi

  msg "安装 Python 依赖..."
  "$VENV_DIR/bin/pip" install --upgrade pip setuptools wheel >/dev/null
  "$VENV_DIR/bin/pip" install -r "$PROJECT_DIR/requirements.txt"
}

install_caddy_if_needed() {
  if [[ "$ENABLE_HTTPS" != "1" ]]; then
    return
  fi
  if command -v caddy >/dev/null 2>&1; then
    return
  fi
  msg "启用 HTTPS 模式，安装 Caddy..."
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -y
  apt-get install -y caddy
}

configure_ufw_rules() {
  if ! command -v ufw >/dev/null 2>&1; then
    warn "未检测到 ufw，跳过防火墙配置。"
    return
  fi

  msg "配置 UFW 防火墙规则..."
  ufw allow 22/tcp >/dev/null || true
  if [[ "$ENABLE_HTTPS" == "1" ]]; then
    ufw allow 80/tcp >/dev/null || true
    ufw allow 443/tcp >/dev/null || true
  fi
  delete_controller_port_rules "$CONTROLLER_PORT"
  if [[ -n "$CONTROLLER_PORT_WHITELIST" ]]; then
    local item ip
    local -a whitelist_items=()
    IFS=',' read -r -a whitelist_items <<<"$CONTROLLER_PORT_WHITELIST"
    for item in "${whitelist_items[@]}"; do
      ip="$(echo "$item" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
      [[ -z "$ip" ]] && continue
      if ! ufw allow from "$ip" to any port "$CONTROLLER_PORT" proto tcp >/dev/null 2>&1; then
        warn "白名单规则添加失败（已跳过）：${ip}"
      fi
    done
    msg "已按白名单放行 ${CONTROLLER_PORT}/tcp。"
  else
    ufw allow "${CONTROLLER_PORT}/tcp" >/dev/null || true
    warn "已公网放行 ${CONTROLLER_PORT}/tcp（建议配置 CONTROLLER_PORT_WHITELIST 收敛来源）。"
  fi

  local status_line
  status_line="$(ufw status 2>/dev/null | head -n1 || true)"
  if [[ "$status_line" == *"inactive"* ]]; then
    if ask_yes_no "检测到 UFW 未启用，是否现在启用？" "Y"; then
      ufw --force enable >/dev/null
      msg "UFW 已启用。"
    else
      warn "你选择不启用 UFW，请自行确保端口放行。"
    fi
  else
    msg "UFW 已启用，规则已更新。"
  fi
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

write_caddy_config_if_needed() {
  if [[ "$ENABLE_HTTPS" != "1" ]]; then
    return
  fi
  if [[ -z "$HTTPS_DOMAIN" ]]; then
    warn "ENABLE_HTTPS=1 但 HTTPS_DOMAIN 为空，跳过 Caddy 配置。"
    return
  fi
  msg "写入 Caddy 反向代理配置（自动申请/续期证书）..."
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

  msg "校验 Caddy 配置..."
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

restart_services() {
  systemctl daemon-reload
  systemctl enable sb-controller >/dev/null
  systemctl restart sb-controller
  if is_bot_token_configured "$BOT_TOKEN"; then
    systemctl enable sb-bot >/dev/null
    systemctl restart sb-bot
  else
    systemctl disable sb-bot >/dev/null 2>&1 || true
    systemctl stop sb-bot >/dev/null 2>&1 || true
    warn "已跳过 sb-bot 启动：BOT_TOKEN 仍为占位值。"
  fi
  if [[ "$ENABLE_HTTPS" == "1" ]]; then
    restart_caddy_with_diagnostics
  fi
}

extract_primary_auth_token() {
  local raw="${AUTH_TOKEN:-}"
  local primary
  primary="$(echo "${raw%%,*}" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
  echo "$primary"
}

pick_working_auth_token() {
  local raw="$1"
  local trimmed=""
  local code=""
  local -a candidates=()
  local -a raw_items=()
  local item

  raw="${raw//$'\n'/}"
  raw="${raw//$'\r'/}"
  if [[ -z "$raw" ]]; then
    echo ""
    return 0
  fi

  IFS=',' read -r -a raw_items <<<"$raw"
  for item in "${raw_items[@]}"; do
    trimmed="$(echo "$item" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
    [[ -z "$trimmed" ]] && continue
    candidates+=("$trimmed")
  done
  if (( ${#candidates[@]} == 0 )); then
    echo ""
    return 0
  fi
  if (( ${#candidates[@]} == 1 )); then
    echo "${candidates[0]}"
    return 0
  fi

  for item in "${candidates[@]}"; do
    code="$(curl -sS -o /dev/null -w "%{http_code}" --max-time 3 \
      -H "Authorization: Bearer ${item}" \
      "http://127.0.0.1:${CONTROLLER_PORT}/admin/security/status" || true)"
    if [[ "$code" == "200" ]]; then
      echo "$item"
      return 0
    fi
  done

  echo "${candidates[0]}"
  return 1
}

sync_node_agent_defaults_after_config() {
  local primary_token
  local response
  local body
  local http_code
  local selected
  local created
  local deduplicated
  local failed

  if ! wait_for_controller_ready 30; then
    warn "配置后自动同步节点默认参数：controller 未就绪，已跳过。"
    NODE_DEFAULT_SYNC_SUMMARY="跳过（controller 未就绪）"
    return
  fi

  primary_token="$(pick_working_auth_token "${AUTH_TOKEN:-}")" || {
    warn "AUTH_TOKEN 多值模式下未探测到可用 token，回退使用第一个 token。"
  }
  if [[ -z "$primary_token" ]]; then
    primary_token="$(extract_primary_auth_token)"
  fi
  if [[ -n "$primary_token" ]]; then
    response="$(
      curl -sS --max-time 15 -X POST "http://127.0.0.1:${CONTROLLER_PORT}/admin/nodes/sync_agent_defaults" \
        -H "Authorization: Bearer ${primary_token}" \
        -H "X-Actor: ${INSTALL_SCRIPT_ACTOR}" \
        -H "Content-Type: application/json" \
        -w $'\n%{http_code}' 2>/dev/null || true
    )"
  else
    response="$(
      curl -sS --max-time 15 -X POST "http://127.0.0.1:${CONTROLLER_PORT}/admin/nodes/sync_agent_defaults" \
        -H "X-Actor: ${INSTALL_SCRIPT_ACTOR}" \
        -H "Content-Type: application/json" \
        -w $'\n%{http_code}' 2>/dev/null || true
    )"
  fi

  http_code="${response##*$'\n'}"
  body="${response%$'\n'*}"

  if [[ "$http_code" != "200" ]]; then
    warn "配置后自动同步节点默认参数失败（HTTP ${http_code:-unknown}）。可在 bot 菜单手动执行“节点默认参数同步”。"
    if [[ -n "$body" ]]; then
      warn "返回：${body}"
    fi
    NODE_DEFAULT_SYNC_SUMMARY="失败（HTTP ${http_code:-unknown}）"
    return
  fi

  if command -v jq >/dev/null 2>&1; then
    selected="$(echo "$body" | jq -r '.selected // 0' 2>/dev/null || echo "0")"
    created="$(echo "$body" | jq -r '.created // 0' 2>/dev/null || echo "0")"
    deduplicated="$(echo "$body" | jq -r '.deduplicated // 0' 2>/dev/null || echo "0")"
    failed="$(echo "$body" | jq -r '.failed // 0' 2>/dev/null || echo "0")"
  else
    selected="-"
    created="-"
    deduplicated="-"
    failed="-"
  fi

  msg "配置后自动同步节点默认参数已执行：目标=${selected} 新建=${created} 去重=${deduplicated} 失败=${failed}"
  NODE_DEFAULT_SYNC_SUMMARY="已执行（目标=${selected} 新建=${created} 去重=${deduplicated} 失败=${failed}）"
}

check_env_key() {
  local key="$1"
  local value
  value="$(get_env_value "$key")"
  if [[ -n "$value" ]]; then
    check_ok ".env 参数存在：${key}"
  else
    check_fail ".env 参数缺失或为空：${key}"
  fi
}

run_self_checks() {
  echo ""
  msg "开始执行安装后自检..."
  SELF_CHECK_OK=0
  SELF_CHECK_WARN=0
  SELF_CHECK_FAIL=0

  if [[ -f "$ENV_FILE" ]]; then
    check_ok "环境文件存在：$ENV_FILE"
  else
    check_fail "环境文件不存在：$ENV_FILE"
  fi

  if command -v sb-admin >/dev/null 2>&1; then
    check_ok "菜单快捷命令可用：sb-admin"
  else
    check_warn "菜单快捷命令不可用：sb-admin"
  fi

  check_env_key "CONTROLLER_PORT"
  check_env_key "CONTROLLER_URL"
  check_env_key "PANEL_BASE_URL"
  check_env_key "ADMIN_AUTH_TOKEN"
  check_env_key "NODE_AUTH_TOKEN"
  if is_bot_token_configured "$BOT_TOKEN"; then
    check_ok ".env 参数存在：BOT_TOKEN"
  else
    check_warn "BOT_TOKEN 仍为占位值（sb-bot 未启动）"
  fi
  check_env_key "MIGRATE_DIR"
  check_env_key "BACKUP_RETENTION_COUNT"
  check_env_key "MIGRATE_RETENTION_COUNT"
  check_env_key "LOG_ARCHIVE_WINDOW_HOURS"
  check_env_key "LOG_ARCHIVE_RETENTION_COUNT"
  check_env_key "LOG_ARCHIVE_DIR"
  check_env_key "TRUST_X_FORWARDED_FOR"
  check_env_key "TRUSTED_PROXY_IPS"
  check_env_key "NODE_TASK_RUNNING_TIMEOUT"
  check_env_key "NODE_TASK_RETENTION_SECONDS"
  check_env_key "NODE_TASK_MAX_PENDING_PER_NODE"
  check_env_key "SUB_LINK_SIGN_KEY"
  check_env_key "SUB_LINK_REQUIRE_SIGNATURE"
  check_env_key "SUB_LINK_DEFAULT_TTL_SECONDS"
  check_env_key "API_RATE_LIMIT_ENABLED"
  check_env_key "API_RATE_LIMIT_WINDOW_SECONDS"
  check_env_key "API_RATE_LIMIT_MAX_REQUESTS"
  check_env_key "ADMIN_OVERVIEW_CACHE_TTL_SECONDS"
  check_env_key "ADMIN_SECURITY_STATUS_CACHE_TTL_SECONDS"
  check_env_key "SECURITY_EVENTS_EXCLUDE_LOCAL"
  check_env_key "SECURITY_BLOCK_PROTECTED_IPS"
  check_env_key "SECURITY_AUTO_BLOCK_ENABLED"
  check_env_key "SECURITY_AUTO_BLOCK_INTERVAL_SECONDS"
  check_env_key "SECURITY_AUTO_BLOCK_WINDOW_SECONDS"
  check_env_key "SECURITY_AUTO_BLOCK_THRESHOLD"
  check_env_key "SECURITY_AUTO_BLOCK_DURATION_SECONDS"
  check_env_key "SECURITY_AUTO_BLOCK_MAX_PER_INTERVAL"
  check_env_key "CONTROLLER_HTTP_TIMEOUT"
  check_env_key "BOT_ACTOR_LABEL"
  check_env_key "BOT_MUTATION_COOLDOWN"
  check_env_key "BOT_NODE_TIME_SYNC_INTERVAL"
  if [[ -n "$CONTROLLER_PORT_WHITELIST" ]]; then
    check_ok ".env 参数存在：CONTROLLER_PORT_WHITELIST"
  else
    check_warn "CONTROLLER_PORT_WHITELIST 为空（8080 可能为公网开放）"
  fi
  if [[ -n "$ADMIN_API_WHITELIST" ]]; then
    check_ok ".env 参数存在：ADMIN_API_WHITELIST"
  elif [[ -n "$CONTROLLER_PORT_WHITELIST" ]]; then
    check_ok "ADMIN_API_WHITELIST 为空，但将回退 CONTROLLER_PORT_WHITELIST 作为管理接口来源限制"
  else
    check_warn "ADMIN_API_WHITELIST 为空（管理接口未启用应用层来源限制）"
  fi
  if [[ -z "$AUTH_TOKEN" ]]; then
    check_warn "AUTH_TOKEN 为空（controller 接口不鉴权，建议仅内网或配防火墙来源限制）"
  elif [[ "$AUTH_TOKEN" == "devtoken123" || ${#AUTH_TOKEN} -lt 16 ]]; then
    check_warn "AUTH_TOKEN 强度较弱，建议更新为 16 位以上随机串"
  else
    check_ok "AUTH_TOKEN 已设置且强度正常"
  fi
  if [[ -z "$ADMIN_AUTH_TOKEN" ]]; then
    check_warn "ADMIN_AUTH_TOKEN 为空（管理接口将回退到 AUTH_TOKEN）"
  else
    check_ok "ADMIN_AUTH_TOKEN 已设置"
  fi
  if [[ -z "$NODE_AUTH_TOKEN" ]]; then
    check_warn "NODE_AUTH_TOKEN 为空（节点接口将回退到 AUTH_TOKEN）"
  else
    check_ok "NODE_AUTH_TOKEN 已设置"
  fi
  if [[ -n "$ADMIN_AUTH_TOKEN" && -n "$NODE_AUTH_TOKEN" && "$ADMIN_AUTH_TOKEN" == "$NODE_AUTH_TOKEN" ]]; then
    check_warn "ADMIN_AUTH_TOKEN 与 NODE_AUTH_TOKEN 相同（未形成鉴权隔离）"
  else
    check_ok "ADMIN/NODE token 已拆分"
  fi

  if systemctl is-enabled sb-controller >/dev/null 2>&1; then
    check_ok "sb-controller 已设为开机启动"
  else
    check_warn "sb-controller 未启用开机启动"
  fi
  if is_bot_token_configured "$BOT_TOKEN"; then
    if systemctl is-enabled sb-bot >/dev/null 2>&1; then
      check_ok "sb-bot 已设为开机启动"
    else
      check_warn "sb-bot 未启用开机启动"
    fi
  else
    check_warn "BOT_TOKEN 未配置，已按预期跳过 sb-bot 开机启动"
  fi

  if systemctl is-active sb-controller >/dev/null 2>&1; then
    check_ok "sb-controller 运行中"
  else
    check_fail "sb-controller 未运行"
  fi
  if is_bot_token_configured "$BOT_TOKEN"; then
    if systemctl is-active sb-bot >/dev/null 2>&1; then
      check_ok "sb-bot 运行中"
    else
      check_fail "sb-bot 未运行"
    fi
  else
    check_warn "BOT_TOKEN 未配置，已按预期跳过 sb-bot 运行检查"
  fi

  if wait_for_controller_ready 30; then
    if grep -q '"ok"[[:space:]]*:[[:space:]]*true' /tmp/sb-controller-health.json; then
      check_ok "本地 health 检查通过：http://127.0.0.1:${CONTROLLER_PORT}/health"
    else
      check_warn "本地 health 可访问但返回体异常"
    fi
  else
    check_fail "本地 health 检查失败：http://127.0.0.1:${CONTROLLER_PORT}/health"
  fi

  if [[ "$ENABLE_HTTPS" == "1" ]]; then
    check_env_key "HTTPS_DOMAIN"

    if systemctl is-enabled caddy >/dev/null 2>&1; then
      check_ok "caddy 已设为开机启动"
    else
      check_warn "caddy 未启用开机启动"
    fi

    if systemctl is-active caddy >/dev/null 2>&1; then
      check_ok "caddy 运行中"
    else
      check_fail "caddy 未运行"
    fi

    if caddy validate --config /etc/caddy/Caddyfile >/tmp/sb-caddy-selfcheck.log 2>&1; then
      check_ok "Caddyfile 配置校验通过"
    else
      check_fail "Caddyfile 配置校验失败（见 /tmp/sb-caddy-selfcheck.log）"
    fi

    if grep -q "${HTTPS_DOMAIN}" /etc/caddy/Caddyfile 2>/dev/null; then
      check_ok "Caddyfile 已包含域名：${HTTPS_DOMAIN}"
    else
      check_fail "Caddyfile 未包含域名：${HTTPS_DOMAIN}"
    fi

    local cert_count
    cert_count="$(find /var/lib/caddy -type f \( -name "*.crt" -o -name "*.pem" \) 2>/dev/null | grep -F "${HTTPS_DOMAIN}" | wc -l | tr -d '[:space:]')"
    if [[ "${cert_count:-0}" =~ ^[0-9]+$ ]] && (( cert_count > 0 )); then
      check_ok "已发现 ${HTTPS_DOMAIN} 证书文件（自动续期由 caddy 接管）"
    else
      check_warn "暂未发现 ${HTTPS_DOMAIN} 证书文件（可能是首次签发未完成）"
    fi

    if curl -fsSL --max-time 8 "${PANEL_BASE_URL}/health" >/tmp/sb-controller-public-health.json 2>/dev/null; then
      if grep -q '"ok"[[:space:]]*:[[:space:]]*true' /tmp/sb-controller-public-health.json; then
        check_ok "公网 URL health 检查通过：${PANEL_BASE_URL}/health"
      else
        check_warn "公网 URL 可访问但返回体异常：${PANEL_BASE_URL}/health"
      fi
    else
      check_warn "公网 URL health 检查未通过：${PANEL_BASE_URL}/health（可能 DNS/防火墙/端口冲突）"
    fi
  else
    check_warn "未启用 HTTPS（ENABLE_HTTPS=0），跳过证书申请/续期检查"
  fi

  echo ""
  msg "自检完成：通过=${SELF_CHECK_OK} 警告=${SELF_CHECK_WARN} 失败=${SELF_CHECK_FAIL}"
  if (( SELF_CHECK_FAIL > 0 )); then
    warn "存在自检失败项，请按上方提示修复后再试。"
  fi
}

show_summary() {
  echo ""
  msg "管理服务器安装/配置完成。"
  echo "项目目录: ${PROJECT_DIR}"
  echo "venv 目录: ${VENV_DIR}"
  echo "Controller: 0.0.0.0:${CONTROLLER_PORT}"
  echo "CONTROLLER_PORT_WHITELIST: ${CONTROLLER_PORT_WHITELIST}"
  echo "ADMIN_API_WHITELIST: ${ADMIN_API_WHITELIST}"
  if [[ "$ENABLE_HTTPS" == "1" ]]; then
    echo "HTTPS 域名: ${HTTPS_DOMAIN}"
  else
    echo "HTTPS 域名: 未启用（当前为 HTTP）"
  fi
  echo "PANEL_BASE_URL: ${PANEL_BASE_URL}"
  echo "MIGRATE_DIR: ${MIGRATE_DIR}"
  echo "BACKUP_RETENTION_COUNT: ${BACKUP_RETENTION_COUNT}"
  echo "MIGRATE_RETENTION_COUNT: ${MIGRATE_RETENTION_COUNT}"
  echo "LOG_ARCHIVE_WINDOW_HOURS: ${LOG_ARCHIVE_WINDOW_HOURS}"
  echo "LOG_ARCHIVE_RETENTION_COUNT: ${LOG_ARCHIVE_RETENTION_COUNT}"
  echo "LOG_ARCHIVE_DIR: ${LOG_ARCHIVE_DIR}"
  echo "BOT_MENU_TTL: ${BOT_MENU_TTL}"
  echo "BOT_NODE_MONITOR_INTERVAL: ${BOT_NODE_MONITOR_INTERVAL}"
  echo "BOT_NODE_OFFLINE_THRESHOLD: ${BOT_NODE_OFFLINE_THRESHOLD}"
  echo "BOT_NODE_TIME_SYNC_INTERVAL: ${BOT_NODE_TIME_SYNC_INTERVAL}"
  echo "BOT_MUTATION_COOLDOWN: ${BOT_MUTATION_COOLDOWN}"
  if is_bot_token_configured "$BOT_TOKEN"; then
    echo "BOT_TOKEN: 已配置（sb-bot 已启用）"
  else
    echo "BOT_TOKEN: 占位值（sb-bot 未启用，需在配置向导填写真实 token）"
  fi
  echo "TRUST_X_FORWARDED_FOR: ${TRUST_X_FORWARDED_FOR}"
  echo "NODE_TASK_RUNNING_TIMEOUT: ${NODE_TASK_RUNNING_TIMEOUT}"
  echo "NODE_TASK_RETENTION_SECONDS: ${NODE_TASK_RETENTION_SECONDS}"
  echo "NODE_TASK_MAX_PENDING_PER_NODE: ${NODE_TASK_MAX_PENDING_PER_NODE}"
  echo "SUB_LINK_REQUIRE_SIGNATURE: ${SUB_LINK_REQUIRE_SIGNATURE}"
  echo "SUB_LINK_DEFAULT_TTL_SECONDS: ${SUB_LINK_DEFAULT_TTL_SECONDS}"
  echo "API_RATE_LIMIT_ENABLED: ${API_RATE_LIMIT_ENABLED}"
  echo "API_RATE_LIMIT_WINDOW_SECONDS: ${API_RATE_LIMIT_WINDOW_SECONDS}"
  echo "API_RATE_LIMIT_MAX_REQUESTS: ${API_RATE_LIMIT_MAX_REQUESTS}"
  echo "ADMIN_AUTH_TOKEN: $( [[ -n "$ADMIN_AUTH_TOKEN" ]] && echo 已配置 || echo 未配置 )"
  echo "NODE_AUTH_TOKEN: $( [[ -n "$NODE_AUTH_TOKEN" ]] && echo 已配置 || echo 未配置 )"
  echo "TOKEN拆分状态: $( [[ -n "$ADMIN_AUTH_TOKEN" && -n "$NODE_AUTH_TOKEN" && "$ADMIN_AUTH_TOKEN" != "$NODE_AUTH_TOKEN" ]] && echo 已拆分 || echo 未拆分/兼容模式 )"
  echo "ADMIN_OVERVIEW_CACHE_TTL_SECONDS: ${ADMIN_OVERVIEW_CACHE_TTL_SECONDS}"
  echo "ADMIN_SECURITY_STATUS_CACHE_TTL_SECONDS: ${ADMIN_SECURITY_STATUS_CACHE_TTL_SECONDS}"
  echo "SECURITY_EVENTS_EXCLUDE_LOCAL: ${SECURITY_EVENTS_EXCLUDE_LOCAL}"
  echo "SECURITY_BLOCK_PROTECTED_IPS: ${SECURITY_BLOCK_PROTECTED_IPS}"
  echo "SECURITY_AUTO_BLOCK_ENABLED: ${SECURITY_AUTO_BLOCK_ENABLED}"
  echo "SECURITY_AUTO_BLOCK_INTERVAL_SECONDS: ${SECURITY_AUTO_BLOCK_INTERVAL_SECONDS}"
  echo "SECURITY_AUTO_BLOCK_WINDOW_SECONDS: ${SECURITY_AUTO_BLOCK_WINDOW_SECONDS}"
  echo "SECURITY_AUTO_BLOCK_THRESHOLD: ${SECURITY_AUTO_BLOCK_THRESHOLD}"
  echo "SECURITY_AUTO_BLOCK_DURATION_SECONDS: ${SECURITY_AUTO_BLOCK_DURATION_SECONDS}"
  echo "SECURITY_AUTO_BLOCK_MAX_PER_INTERVAL: ${SECURITY_AUTO_BLOCK_MAX_PER_INTERVAL}"
  echo "CONTROLLER_HTTP_TIMEOUT: ${CONTROLLER_HTTP_TIMEOUT}"
  echo "BOT_ACTOR_LABEL: ${BOT_ACTOR_LABEL}"
  echo "节点默认参数同步: ${NODE_DEFAULT_SYNC_SUMMARY}"
  echo ""
  echo "快捷查看："
  echo "  systemctl status sb-controller"
  echo "  systemctl status sb-bot"
  echo "  sb-admin"
  if [[ "$ENABLE_HTTPS" == "1" ]]; then
    echo "  systemctl status caddy"
    echo "  journalctl -u caddy -n 200 --no-pager"
  fi
  echo "  journalctl -u sb-controller -n 200 --no-pager"
  echo "  journalctl -u sb-bot -n 200 --no-pager"
}

main() {
  local should_prompt
  should_prompt="1"

  require_root
  ensure_project_dir

  if [[ "$MODE" == "install" ]]; then
    install_base_packages
    ensure_python_311_runtime
    setup_venv_and_requirements
  elif [[ "$MODE" == "reuse-config" ]]; then
    msg "更新模式：优先复用现有配置（不重复提问）。"
    install_base_packages
    ensure_python_311_runtime
    setup_venv_and_requirements
    load_existing_env_defaults
    normalize_loaded_values
    if [[ -f "$ENV_FILE" && -n "$BOT_TOKEN" ]]; then
      should_prompt="0"
      msg "检测到现有 .env，已复用原配置。要修改参数请使用 --configure-only 或菜单项 2。"
    else
      warn "未检测到可用 .env（或 BOT_TOKEN 为空），将进入交互配置。"
    fi
  else
    msg "仅配置模式：跳过 apt 与依赖安装。"
  fi

  if [[ "$should_prompt" == "1" ]]; then
    if [[ "$MODE" == "configure-quick" ]]; then
      prompt_env_config_quick
    else
      prompt_env_config
    fi
  fi
  normalize_loaded_values
  write_env_file
  install_caddy_if_needed
  configure_ufw_rules
  write_systemd_services
  install_admin_menu_commands
  write_caddy_config_if_needed
  restart_services
  sync_node_agent_defaults_after_config
  show_summary
  run_self_checks
}

main "$@"
