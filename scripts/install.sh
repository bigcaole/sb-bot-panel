#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

MODE="${1:-install}"
if [[ "$MODE" != "install" && "$MODE" != "--configure-only" && "$MODE" != "--sync-only" ]]; then
  echo "用法:"
  echo "  sudo bash scripts/install.sh              # 完整安装/更新"
  echo "  sudo bash scripts/install.sh --configure-only  # 仅重写配置并重启服务"
  echo "  sudo bash scripts/install.sh --sync-only  # 无交互同步代码并重启（复用现有配置）"
  exit 1
fi
if [[ "$MODE" == "--configure-only" ]]; then
  MODE="configure-only"
fi
if [[ "$MODE" == "--sync-only" ]]; then
  MODE="sync-only"
fi

CONFIG_PATH="/etc/sb-agent/config.json"
AGENT_STATE_PATH="/etc/sb-agent/state.json"
AGENT_DIR="/opt/sb-agent"
AGENT_VENV="$AGENT_DIR/venv"
AGENT_MAIN="$AGENT_DIR/sb_agent.py"
AGENT_LOG_DIR="/var/log/sb-agent"
SINGBOX_LOG_DIR="/var/log/sing-box"
SINGBOX_CONFIG="/etc/sing-box/config.json"
CERTMAGIC_DIR="/var/lib/sing-box/certmagic"
BACKUP_DIR="/var/backups/sb-agent"

SB_AGENT_SERVICE="/etc/systemd/system/sb-agent.service"
SB_CERT_CHECK_SERVICE="/etc/systemd/system/sb-cert-check.service"
SB_CERT_CHECK_TIMER="/etc/systemd/system/sb-cert-check.timer"
SB_CERT_CHECK_BIN="/usr/local/bin/sb-cert-check.sh"

CONTROLLER_URL=""
NODE_CODE=""
AUTH_TOKEN=""
TUIC_DOMAIN=""
ACME_EMAIL=""
TUIC_LISTEN_PORT=8443
POLL_INTERVAL=15
PUBLIC_IP=""
AGENT_PYTHON_BIN=""

msg() { echo -e "\033[1;32m[信息]\033[0m $*"; }
warn() { echo -e "\033[1;33m[警告]\033[0m $*"; }
err() { echo -e "\033[1;31m[错误]\033[0m $*" >&2; }

require_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    err "请使用 root 权限运行（例如 sudo bash scripts/install.sh）"
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
  if [[ "$answer" =~ ^[Yy]$ ]]; then
    return 0
  fi
  return 1
}

install_menu_shortcuts() {
  cat >/usr/local/bin/sb-node <<EOF
#!/usr/bin/env bash
exec bash "${ROOT_DIR}/scripts/menu.sh" "\$@"
EOF
  chmod 0755 /usr/local/bin/sb-node

  local s_ui_path
  s_ui_path="$(command -v s-ui || true)"
  if [[ -z "$s_ui_path" ]]; then
    cat >/usr/local/bin/s-ui <<EOF
#!/usr/bin/env bash
# sb-node-menu-shortcut
exec bash "${ROOT_DIR}/scripts/menu.sh" "\$@"
EOF
    chmod 0755 /usr/local/bin/s-ui
  elif [[ "$s_ui_path" == "/usr/local/bin/s-ui" ]] && grep -q "sb-node-menu-shortcut" /usr/local/bin/s-ui 2>/dev/null; then
    cat >/usr/local/bin/s-ui <<EOF
#!/usr/bin/env bash
# sb-node-menu-shortcut
exec bash "${ROOT_DIR}/scripts/menu.sh" "\$@"
EOF
    chmod 0755 /usr/local/bin/s-ui
  else
    warn "检测到已有 s-ui 命令(${s_ui_path})，跳过覆盖。可使用 sb-node 打开菜单。"
  fi
}

get_public_ipv4() {
  curl -4 -fsSL ifconfig.me 2>/dev/null \
    || curl -4 -fsSL https://api.ipify.org 2>/dev/null \
    || true
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

extract_url_host() {
  local raw="$1"
  raw="${raw#*://}"
  raw="${raw%%/*}"
  raw="${raw%%:*}"
  echo "$raw"
}

resolve_domain_ipv4() {
  local domain="$1"
  local ip=""
  if command -v dig >/dev/null 2>&1; then
    ip="$(dig +short A "$domain" | grep -E '^[0-9.]+$' | head -n1 || true)"
  elif command -v host >/dev/null 2>&1; then
    ip="$(host -t A "$domain" 2>/dev/null | awk '/has address/{print $NF; exit}')"
  fi
  echo "$ip"
}

install_base_packages() {
  msg "安装基础依赖..."
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -y
  apt-get install -y \
    curl \
    jq \
    ufw \
    python3 \
    python3-venv \
    python3-pip \
    ca-certificates \
    openssl \
    dnsutils \
    bind9-host
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
    AGENT_PYTHON_BIN="$(command -v python3.11)"
    msg "检测到 Python 3.11: ${AGENT_PYTHON_BIN}"
    return
  fi

  msg "尝试安装 Python 3.11..."
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
    AGENT_PYTHON_BIN="$(command -v python3.11)"
    msg "Python 3.11 安装完成: ${AGENT_PYTHON_BIN}"
    return
  fi
  if command -v python3 >/dev/null 2>&1 && python_version_ge_311 python3; then
    AGENT_PYTHON_BIN="$(command -v python3)"
    warn "未找到 python3.11，回退使用 ${AGENT_PYTHON_BIN}（版本>=3.11）"
    return
  fi
  err "未能找到 Python >=3.11。请手动安装 python3.11 与 python3.11-venv 后重试。"
  exit 1
}

ensure_dns_tools() {
  if command -v dig >/dev/null 2>&1 || command -v host >/dev/null 2>&1; then
    return
  fi
  warn "系统缺少 dig/host，正在安装 dnsutils/bind9-host..."
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -y
  apt-get install -y dnsutils bind9-host
}

install_sing_box() {
  msg "安装/更新 sing-box（官方脚本）..."
  curl -fsSL https://sing-box.app/install.sh | bash
  if ! command -v sing-box >/dev/null 2>&1; then
    err "sing-box 安装失败，未检测到命令。"
    exit 1
  fi
}

ensure_dirs() {
  mkdir -p /etc/sb-agent
  mkdir -p "$AGENT_DIR"
  mkdir -p "$AGENT_LOG_DIR"
  mkdir -p "$SINGBOX_LOG_DIR"
  mkdir -p "$CERTMAGIC_DIR"
  mkdir -p "$BACKUP_DIR"
}

setup_python_venv() {
  if [[ -z "$AGENT_PYTHON_BIN" ]]; then
    ensure_python_311_runtime
  fi

  if [[ -d "$AGENT_VENV" && -x "$AGENT_VENV/bin/python" ]]; then
    if ! python_version_ge_311 "$AGENT_VENV/bin/python"; then
      warn "检测到旧 venv Python < 3.11，正在重建 venv..."
      rm -rf "$AGENT_VENV"
    fi
  fi

  if [[ ! -d "$AGENT_VENV" ]]; then
    msg "创建 sb-agent 虚拟环境: $AGENT_VENV"
    "$AGENT_PYTHON_BIN" -m venv "$AGENT_VENV"
  fi
  "$AGENT_VENV/bin/pip" install --upgrade pip setuptools wheel >/dev/null
}

install_agent_files() {
  if [[ ! -f "$ROOT_DIR/agent/sb_agent.py" ]]; then
    err "未找到 agent/sb_agent.py，请确认仓库文件完整。"
    exit 1
  fi
  if [[ ! -f "$ROOT_DIR/scripts/sb_cert_check.sh" ]]; then
    err "未找到 scripts/sb_cert_check.sh，请确认仓库文件完整。"
    exit 1
  fi

  install -m 0755 "$ROOT_DIR/agent/sb_agent.py" "$AGENT_MAIN"
  install -m 0755 "$ROOT_DIR/scripts/sb_cert_check.sh" "$SB_CERT_CHECK_BIN"
}

write_bootstrap_singbox_config_if_missing() {
  if [[ -f "$SINGBOX_CONFIG" ]]; then
    return
  fi
  msg "写入 sing-box 初始配置（占位）..."
  cat >"$SINGBOX_CONFIG" <<'EOF'
{
  "log": {
    "disabled": false,
    "level": "info",
    "timestamp": true,
    "output": "/var/log/sing-box/sing-box.log"
  },
  "inbounds": [],
  "outbounds": [
    {
      "type": "direct",
      "tag": "direct"
    }
  ],
  "route": {
    "final": "direct"
  }
}
EOF
  chmod 0644 "$SINGBOX_CONFIG"
}

read_old_config_value() {
  local key="$1"
  if [[ -f "$CONFIG_PATH" ]] && command -v jq >/dev/null 2>&1; then
    jq -r --arg k "$key" '.[$k] // ""' "$CONFIG_PATH" 2>/dev/null || true
  fi
}

load_existing_config_or_fail() {
  if [[ ! -f "$CONFIG_PATH" ]]; then
    err "未找到现有配置: $CONFIG_PATH，无法执行 --sync-only。请先运行完整安装。"
    exit 1
  fi
  CONTROLLER_URL="$(read_old_config_value "controller_url")"
  NODE_CODE="$(read_old_config_value "node_code")"
  AUTH_TOKEN="$(read_old_config_value "auth_token")"
  TUIC_DOMAIN="$(read_old_config_value "tuic_domain")"
  ACME_EMAIL="$(read_old_config_value "acme_email")"
  TUIC_LISTEN_PORT="$(read_old_config_value "tuic_listen_port")"
  POLL_INTERVAL="$(read_old_config_value "poll_interval")"

  CONTROLLER_URL="${CONTROLLER_URL:-http://127.0.0.1:8080}"
  NODE_CODE="${NODE_CODE:-JP1}"
  TUIC_DOMAIN="${TUIC_DOMAIN:-}"
  ACME_EMAIL="${ACME_EMAIL:-}"

  if ! [[ "${TUIC_LISTEN_PORT}" =~ ^[0-9]+$ ]] || (( TUIC_LISTEN_PORT < 1 || TUIC_LISTEN_PORT > 65535 )); then
    TUIC_LISTEN_PORT=8443
  fi
  if ! [[ "${POLL_INTERVAL}" =~ ^[0-9]+$ ]] || (( POLL_INTERVAL < 5 )); then
    POLL_INTERVAL=15
  fi
}

prompt_config() {
  local old_controller old_node old_token old_domain old_email old_tuic_port old_poll
  old_controller="$(read_old_config_value "controller_url")"
  old_node="$(read_old_config_value "node_code")"
  old_token="$(read_old_config_value "auth_token")"
  old_domain="$(read_old_config_value "tuic_domain")"
  old_email="$(read_old_config_value "acme_email")"
  old_tuic_port="$(read_old_config_value "tuic_listen_port")"
  old_poll="$(read_old_config_value "poll_interval")"

  read -r -p "1) 请输入 controller_url（支持省略 http/https，例如 panel.cwzs.de:8080） [${old_controller:-http://127.0.0.1:8080}]: " CONTROLLER_URL
  CONTROLLER_URL="${CONTROLLER_URL:-${old_controller:-http://127.0.0.1:8080}}"
  local controller_scheme controller_host
  controller_host="$(extract_url_host "$CONTROLLER_URL")"
  controller_scheme="https"
  if [[ "$controller_host" == "127.0.0.1" || "$controller_host" == "localhost" || "$CONTROLLER_URL" == *":8080"* || "$CONTROLLER_URL" == *":80"* ]]; then
    controller_scheme="http"
  fi
  CONTROLLER_URL="$(normalize_input_url "$CONTROLLER_URL" "$controller_scheme")"

  while [[ -z "$NODE_CODE" ]]; do
    read -r -p "2) 请输入 node_code（例如 JP1） [${old_node:-JP1}]: " NODE_CODE
    NODE_CODE="${NODE_CODE:-${old_node:-JP1}}"
    NODE_CODE="$(echo "$NODE_CODE" | tr -d '[:space:]')"
  done

  read -r -p "3) 请输入 auth_token（用于拉取 sync） [${old_token}]: " AUTH_TOKEN
  AUTH_TOKEN="${AUTH_TOKEN:-$old_token}"

  read -r -p "4) 请输入 tuic_domain（例如 jp1.cwzs.de；留空=不启用TUIC） [${old_domain}]: " TUIC_DOMAIN
  TUIC_DOMAIN="${TUIC_DOMAIN:-$old_domain}"
  TUIC_DOMAIN="${TUIC_DOMAIN//[$'\r\n']}"

  if [[ -n "$TUIC_DOMAIN" ]]; then
    while [[ -z "$ACME_EMAIL" ]]; do
      read -r -p "5) 请输入 acme_email（例如 admin@cwzs.de） [${old_email}]: " ACME_EMAIL
      ACME_EMAIL="${ACME_EMAIL:-$old_email}"
      ACME_EMAIL="$(echo "$ACME_EMAIL" | tr -d '[:space:]')"
      if [[ -z "$ACME_EMAIL" ]]; then
        warn "启用 TUIC 时，acme_email 不能为空。"
      fi
    done
  else
    ACME_EMAIL=""
    msg "已选择不启用 TUIC，跳过证书邮箱。"
  fi

  read -r -p "6) 请输入 tuic_listen_port（默认 8443） [${old_tuic_port:-8443}]: " TUIC_LISTEN_PORT
  TUIC_LISTEN_PORT="${TUIC_LISTEN_PORT:-${old_tuic_port:-8443}}"
  if ! [[ "$TUIC_LISTEN_PORT" =~ ^[0-9]+$ ]] || (( TUIC_LISTEN_PORT < 1 || TUIC_LISTEN_PORT > 65535 )); then
    warn "端口无效，已回退为 8443"
    TUIC_LISTEN_PORT=8443
  fi

  read -r -p "7) 请输入 poll_interval（秒，默认 15） [${old_poll:-15}]: " POLL_INTERVAL
  POLL_INTERVAL="${POLL_INTERVAL:-${old_poll:-15}}"
  if ! [[ "$POLL_INTERVAL" =~ ^[0-9]+$ ]] || (( POLL_INTERVAL < 5 )); then
    warn "轮询间隔无效，已回退为 15 秒"
    POLL_INTERVAL=15
  fi
}

check_domain_resolution_interactive() {
  if [[ -z "$TUIC_DOMAIN" ]]; then
    return
  fi
  ensure_dns_tools
  PUBLIC_IP="$(get_public_ipv4)"
  if [[ -z "$PUBLIC_IP" ]]; then
    warn "无法获取本机公网 IPv4，跳过自动解析比对。"
    return
  fi
  while true; do
    local dns_ip
    dns_ip="$(resolve_domain_ipv4 "$TUIC_DOMAIN")"
    msg "域名解析检查: $TUIC_DOMAIN -> ${dns_ip:-未解析}"
    msg "本机公网 IP: $PUBLIC_IP"
    if [[ -n "$dns_ip" && "$dns_ip" == "$PUBLIC_IP" ]]; then
      msg "解析检查通过。"
      break
    fi
    warn "解析不正确。请到 Cloudflare 关闭代理（小黄云置灰），并将 A 记录指向当前公网 IP。"
    read -r -p "修复后按回车重试，输入 skip 跳过该检查: " retry
    if [[ "$retry" == "skip" ]]; then
      warn "你选择了跳过解析检查，证书申请可能失败。"
      break
    fi
  done
}

configure_ufw_rules() {
  if ! command -v ufw >/dev/null 2>&1; then
    warn "未检测到 ufw，跳过防火墙配置。"
    return
  fi
  msg "配置 UFW 防火墙规则..."
  ufw allow 22/tcp >/dev/null
  ufw allow 443/tcp >/dev/null
  if [[ -n "$TUIC_DOMAIN" ]]; then
    ufw allow "${TUIC_LISTEN_PORT}/udp" >/dev/null
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

write_config_json() {
  msg "写入配置文件: $CONFIG_PATH"
  jq -n \
    --arg controller_url "$CONTROLLER_URL" \
    --arg node_code "$NODE_CODE" \
    --arg auth_token "$AUTH_TOKEN" \
    --arg tuic_domain "$TUIC_DOMAIN" \
    --arg acme_email "$ACME_EMAIL" \
    --argjson tuic_listen_port "$TUIC_LISTEN_PORT" \
    --argjson poll_interval "$POLL_INTERVAL" \
    '{
      controller_url: $controller_url,
      node_code: $node_code,
      auth_token: $auth_token,
      poll_interval: $poll_interval,
      tuic_domain: $tuic_domain,
      tuic_listen_port: $tuic_listen_port,
      acme_email: $acme_email
    }' >"$CONFIG_PATH"
  chmod 0600 "$CONFIG_PATH"
}

install_singbox_service_if_missing() {
  if systemctl list-unit-files | grep -q '^sing-box.service'; then
    msg "检测到系统已有 sing-box.service，跳过创建。"
    return
  fi
  local sb_bin
  sb_bin="$(command -v sing-box || true)"
  if [[ -z "$sb_bin" ]]; then
    err "找不到 sing-box 可执行文件，无法创建服务。"
    exit 1
  fi
  msg "未检测到 sing-box.service，创建 systemd 服务..."
  cat > /etc/systemd/system/sing-box.service <<EOF
[Unit]
Description=sing-box service
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=${sb_bin} run -c ${SINGBOX_CONFIG}
ExecReload=/bin/kill -HUP \$MAINPID
Restart=on-failure
RestartSec=3
LimitNOFILE=1048576
AmbientCapabilities=CAP_NET_BIND_SERVICE CAP_NET_ADMIN CAP_NET_RAW
CapabilityBoundingSet=CAP_NET_BIND_SERVICE CAP_NET_ADMIN CAP_NET_RAW
NoNewPrivileges=true

[Install]
WantedBy=multi-user.target
EOF
}

install_sb_agent_service() {
  msg "写入 sb-agent.service ..."
  cat >"$SB_AGENT_SERVICE" <<EOF
[Unit]
Description=sb-agent (pull config from controller and render sing-box)
After=network-online.target sing-box.service
Wants=network-online.target

[Service]
Type=simple
ExecStartPre=/usr/bin/install -m 0755 ${ROOT_DIR}/agent/sb_agent.py ${AGENT_MAIN}
ExecStart=${AGENT_VENV}/bin/python ${AGENT_MAIN}
Restart=always
RestartSec=5
WorkingDirectory=${AGENT_DIR}
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF
}

install_cert_check_timer_files() {
  msg "写入 sb-cert-check.service / timer ..."
  cat >"$SB_CERT_CHECK_SERVICE" <<EOF
[Unit]
Description=sb-agent cert health check
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
ExecStart=/bin/bash -c '${SB_CERT_CHECK_BIN} >> ${AGENT_LOG_DIR}/cert-check.log 2>&1'
EOF

  cat >"$SB_CERT_CHECK_TIMER" <<'EOF'
[Unit]
Description=Run sb-cert-check daily

[Timer]
OnCalendar=daily
Persistent=true

[Install]
WantedBy=timers.target
EOF
}

reload_and_enable_services() {
  systemctl daemon-reload

  if command -v sing-box >/dev/null 2>&1; then
    if sing-box check -c "$SINGBOX_CONFIG" >/dev/null 2>&1; then
      systemctl enable --now sing-box >/dev/null || true
    else
      warn "当前 sing-box 配置检查未通过，先等待 sb-agent 下发新配置。"
    fi
  fi

  systemctl enable sb-agent >/dev/null
  systemctl restart sb-agent

  if ask_yes_no "是否启用每日证书健康检查定时器（sb-cert-check.timer）？" "Y"; then
    systemctl enable --now sb-cert-check.timer >/dev/null
    msg "已启用 sb-cert-check.timer"
  else
    warn "未启用 sb-cert-check.timer，可后续手动启用。"
  fi
}

reload_and_enable_services_noninteractive() {
  systemctl daemon-reload

  if command -v sing-box >/dev/null 2>&1; then
    if sing-box check -c "$SINGBOX_CONFIG" >/dev/null 2>&1; then
      systemctl enable --now sing-box >/dev/null || true
    else
      warn "当前 sing-box 配置检查未通过，先等待 sb-agent 下发新配置。"
    fi
  fi

  systemctl enable sb-agent >/dev/null
  systemctl restart sb-agent

  if systemctl is-enabled sb-cert-check.timer >/dev/null 2>&1; then
    systemctl restart sb-cert-check.timer >/dev/null || true
  fi
}

show_summary() {
  local ip
  ip="$(get_public_ipv4)"
  echo ""
  msg "安装完成。"
  echo "----------------------------------------"
  echo "公网 IP: ${ip:-未知}"
  echo "节点代码: ${NODE_CODE}"
  echo "Controller: ${CONTROLLER_URL}"
  echo "TUIC 域名: ${TUIC_DOMAIN:-未启用}"
  echo "TUIC 端口: ${TUIC_LISTEN_PORT}/udp"
  echo "VLESS 端口: 443/tcp"
  echo ""
  echo "常用命令："
  echo "  systemctl status sb-agent"
  echo "  journalctl -u sb-agent -f"
  echo "  systemctl status sing-box"
  echo "  journalctl -u sing-box -f"
  echo "  /usr/local/bin/sb-cert-check.sh"
  echo ""
  echo "下一步："
  echo "  1) 在控制端（bot/controller）确认 node_code=${NODE_CODE} 已创建并启用"
  echo "  2) 在面板绑定用户到该节点"
  echo "  3) 使用脚本菜单管理：bash ${ROOT_DIR}/scripts/menu.sh"
  echo "----------------------------------------"
}

main() {
  require_root
  msg "开始执行节点侧部署（模式: ${MODE}）"

  if [[ "$MODE" == "install" ]]; then
    install_base_packages
    install_sing_box
  elif [[ "$MODE" == "sync-only" ]]; then
    msg "同步模式：复用现有配置，不进行交互提问。"
    load_existing_config_or_fail
  else
    msg "仅配置模式：跳过依赖与 sing-box 安装步骤"
    if ! command -v jq >/dev/null 2>&1; then
      install_base_packages
    fi
  fi

  ensure_python_311_runtime
  ensure_dirs
  setup_python_venv
  install_agent_files
  write_bootstrap_singbox_config_if_missing

  if [[ "$MODE" == "sync-only" ]]; then
    msg "已加载现有配置：$CONFIG_PATH"
  else
    prompt_config
    check_domain_resolution_interactive
    configure_ufw_rules
    write_config_json
  fi

  install_singbox_service_if_missing
  install_sb_agent_service
  install_cert_check_timer_files
  install_menu_shortcuts
  if [[ "$MODE" == "sync-only" ]]; then
    reload_and_enable_services_noninteractive
  else
    reload_and_enable_services
  fi

  show_summary
}

main "$@"
