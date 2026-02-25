#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-install}"
if [[ "$MODE" != "install" && "$MODE" != "--configure-only" && "$MODE" != "--reuse-config" ]]; then
  echo "用法:"
  echo "  sudo bash scripts/admin/install_admin.sh"
  echo "  sudo bash scripts/admin/install_admin.sh --configure-only"
  echo "  sudo bash scripts/admin/install_admin.sh --reuse-config"
  exit 1
fi
if [[ "$MODE" == "--configure-only" ]]; then
  MODE="configure-only"
fi
if [[ "$MODE" == "--reuse-config" ]]; then
  MODE="reuse-config"
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
PANEL_BASE_URL=""
ENABLE_HTTPS="0"
HTTPS_DOMAIN=""
HTTPS_ACME_EMAIL=""
AUTH_TOKEN=""
BOT_TOKEN=""
ADMIN_CHAT_IDS=""
MIGRATE_DIR="$MIGRATE_DIR_DEFAULT"
BOT_MENU_TTL="60"
BOT_NODE_MONITOR_INTERVAL="60"
BOT_NODE_OFFLINE_THRESHOLD="120"
SELF_CHECK_OK=0
SELF_CHECK_WARN=0
SELF_CHECK_FAIL=0

msg() { echo -e "\033[1;32m[信息]\033[0m $*"; }
warn() { echo -e "\033[1;33m[警告]\033[0m $*"; }
err() { echo -e "\033[1;31m[错误]\033[0m $*" >&2; }
check_ok() { echo -e "\033[1;32m[自检-通过]\033[0m $*"; SELF_CHECK_OK=$((SELF_CHECK_OK + 1)); }
check_warn() { echo -e "\033[1;33m[自检-警告]\033[0m $*"; SELF_CHECK_WARN=$((SELF_CHECK_WARN + 1)); }
check_fail() { echo -e "\033[1;31m[自检-失败]\033[0m $*"; SELF_CHECK_FAIL=$((SELF_CHECK_FAIL + 1)); }

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

extract_url_host() {
  local raw="$1"
  raw="${raw#*://}"
  raw="${raw%%/*}"
  raw="${raw%%:*}"
  echo "$raw"
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

install_admin_menu_commands() {
  msg "安装菜单快捷命令..."
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

load_existing_env_defaults() {
  local old_port old_url old_public_url old_panel_base old_enable_https old_https_domain old_https_email old_auth old_bot old_admin old_migrate old_menu_ttl old_monitor_interval old_offline_threshold
  old_port="$(get_env_value "CONTROLLER_PORT")"
  old_url="$(get_env_value "CONTROLLER_URL")"
  old_public_url="$(get_env_value "CONTROLLER_PUBLIC_URL")"
  old_panel_base="$(get_env_value "PANEL_BASE_URL")"
  old_enable_https="$(get_env_value "ENABLE_HTTPS")"
  old_https_domain="$(get_env_value "HTTPS_DOMAIN")"
  old_https_email="$(get_env_value "HTTPS_ACME_EMAIL")"
  old_auth="$(get_env_value "AUTH_TOKEN")"
  old_bot="$(get_env_value "BOT_TOKEN")"
  old_admin="$(get_env_value "ADMIN_CHAT_IDS")"
  old_migrate="$(get_env_value "MIGRATE_DIR")"
  old_menu_ttl="$(get_env_value "BOT_MENU_TTL")"
  old_monitor_interval="$(get_env_value "BOT_NODE_MONITOR_INTERVAL")"
  old_offline_threshold="$(get_env_value "BOT_NODE_OFFLINE_THRESHOLD")"

  CONTROLLER_PORT="${old_port:-8080}"
  CONTROLLER_URL="${old_url:-http://127.0.0.1:${CONTROLLER_PORT}}"
  CONTROLLER_PUBLIC_URL="${old_public_url:-}"
  PANEL_BASE_URL="${old_panel_base:-}"
  ENABLE_HTTPS="${old_enable_https:-0}"
  HTTPS_DOMAIN="$(sanitize_domain_input "${old_https_domain:-}")"
  HTTPS_ACME_EMAIL="${old_https_email:-}"
  AUTH_TOKEN="${old_auth:-devtoken123}"
  BOT_TOKEN="${old_bot:-}"
  ADMIN_CHAT_IDS="${old_admin:-}"
  MIGRATE_DIR="${old_migrate:-$MIGRATE_DIR_DEFAULT}"
  BOT_MENU_TTL="${old_menu_ttl:-60}"
  BOT_NODE_MONITOR_INTERVAL="${old_monitor_interval:-60}"
  BOT_NODE_OFFLINE_THRESHOLD="${old_offline_threshold:-120}"
}

normalize_loaded_values() {
  if ! [[ "$ENABLE_HTTPS" =~ ^[01]$ ]]; then
    ENABLE_HTTPS="0"
  fi
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
  echo "  - CONTROLLER_PUBLIC_URL：可选，对外访问 URL（给节点/外部使用）"
  echo "  - PANEL_BASE_URL：bot 生成订阅链接使用的基础地址（建议使用域名）"
  echo "  - ENABLE_HTTPS / HTTPS_DOMAIN：启用 Caddy 自动证书（申请+续期）"
  echo "  - HTTPS_ACME_EMAIL：证书账号邮箱（可选，建议填写）"
  echo "  - CONTROLLER_URL：bot 调用 controller 的地址（通常 127.0.0.1）"
  echo "  - AUTH_TOKEN：可选；用于保护 /admin/*，bot/agent 也可携带"
  echo "  - BOT_TOKEN：必填；Telegram 机器人 token"
  echo "  - ADMIN_CHAT_IDS：可选；用于限制谁可操作 bot"
  echo "  - MIGRATE_DIR：迁移包/备份包输出目录"
  echo "  - BOT_MENU_TTL：bot 菜单按钮自动清理秒数"
  echo "  - BOT_NODE_MONITOR_INTERVAL：节点在线检测周期秒数"
  echo "  - BOT_NODE_OFFLINE_THRESHOLD：节点离线判定阈值秒数"
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

  read -r -p "MIGRATE_DIR（迁移包/备份包输出目录，直接回车使用默认） [${MIGRATE_DIR}]: " input_migrate
  MIGRATE_DIR="${input_migrate:-${MIGRATE_DIR:-$MIGRATE_DIR_DEFAULT}}"
  if [[ -z "$MIGRATE_DIR" ]]; then
    MIGRATE_DIR="$MIGRATE_DIR_DEFAULT"
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

  echo ""
  msg "UFW/端口放行说明："
  echo "  - 需要放行 ${CONTROLLER_PORT}/tcp（节点 agent 直连 controller 时使用）"
  if [[ "$ENABLE_HTTPS" == "1" ]]; then
    echo "  - 需要放行 80/tcp 与 443/tcp（Caddy 证书申请/HTTPS）"
  fi
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

# 轻量鉴权 token（如未启用鉴权也可保留）
AUTH_TOKEN=${AUTH_TOKEN}

# Telegram Bot token（必填）
BOT_TOKEN=${BOT_TOKEN}

# 管理员 chat id，逗号分隔，可空
ADMIN_CHAT_IDS=${ADMIN_CHAT_IDS}

# 迁移包目录
MIGRATE_DIR=${MIGRATE_DIR}

# Bot 菜单按钮自动清理秒数
BOT_MENU_TTL=${BOT_MENU_TTL}

# 节点在线检测周期秒数
BOT_NODE_MONITOR_INTERVAL=${BOT_NODE_MONITOR_INTERVAL}

# 节点离线判定阈值秒数
BOT_NODE_OFFLINE_THRESHOLD=${BOT_NODE_OFFLINE_THRESHOLD}
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
  ufw allow "${CONTROLLER_PORT}/tcp" >/dev/null || true
  if [[ "$ENABLE_HTTPS" == "1" ]]; then
    ufw allow 80/tcp >/dev/null || true
    ufw allow 443/tcp >/dev/null || true
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
  systemctl enable sb-bot >/dev/null
  systemctl restart sb-controller
  systemctl restart sb-bot
  if [[ "$ENABLE_HTTPS" == "1" ]]; then
    restart_caddy_with_diagnostics
  fi
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
  check_env_key "BOT_TOKEN"
  check_env_key "MIGRATE_DIR"

  if systemctl is-enabled sb-controller >/dev/null 2>&1; then
    check_ok "sb-controller 已设为开机启动"
  else
    check_warn "sb-controller 未启用开机启动"
  fi
  if systemctl is-enabled sb-bot >/dev/null 2>&1; then
    check_ok "sb-bot 已设为开机启动"
  else
    check_warn "sb-bot 未启用开机启动"
  fi

  if systemctl is-active sb-controller >/dev/null 2>&1; then
    check_ok "sb-controller 运行中"
  else
    check_fail "sb-controller 未运行"
  fi
  if systemctl is-active sb-bot >/dev/null 2>&1; then
    check_ok "sb-bot 运行中"
  else
    check_fail "sb-bot 未运行"
  fi

  if curl -fsSL --max-time 5 "http://127.0.0.1:${CONTROLLER_PORT}/health" >/tmp/sb-controller-health.json 2>/dev/null; then
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
  if [[ "$ENABLE_HTTPS" == "1" ]]; then
    echo "HTTPS 域名: ${HTTPS_DOMAIN}"
  else
    echo "HTTPS 域名: 未启用（当前为 HTTP）"
  fi
  echo "PANEL_BASE_URL: ${PANEL_BASE_URL}"
  echo "MIGRATE_DIR: ${MIGRATE_DIR}"
  echo "BOT_MENU_TTL: ${BOT_MENU_TTL}"
  echo "BOT_NODE_MONITOR_INTERVAL: ${BOT_NODE_MONITOR_INTERVAL}"
  echo "BOT_NODE_OFFLINE_THRESHOLD: ${BOT_NODE_OFFLINE_THRESHOLD}"
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
    setup_venv_and_requirements
  elif [[ "$MODE" == "reuse-config" ]]; then
    msg "更新模式：优先复用现有配置（不重复提问）。"
    install_base_packages
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
    prompt_env_config
  fi
  normalize_loaded_values
  write_env_file
  install_caddy_if_needed
  configure_ufw_rules
  write_systemd_services
  install_admin_menu_commands
  write_caddy_config_if_needed
  restart_services
  show_summary
  run_self_checks
}

main "$@"
