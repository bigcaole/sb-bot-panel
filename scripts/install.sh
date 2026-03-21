#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
AI_CONTEXT_SCRIPT="$ROOT_DIR/scripts/ai_context_export.sh"
AI_CONTEXT_ON_FAIL="${INSTALL_NODE_EXPORT_AI_CONTEXT_ON_FAIL:-1}"
AI_CONTEXT_EXPORTED=0

MODE="${1:-install}"
if [[ "$MODE" != "install" && "$MODE" != "--configure-only" && "$MODE" != "--configure-quick" && "$MODE" != "--sync-only" ]]; then
  echo "用法:"
  echo "  sudo bash scripts/install.sh              # 完整安装/更新"
  echo "  sudo bash scripts/install.sh --configure-only  # 仅重写配置并重启服务"
  echo "  sudo bash scripts/install.sh --configure-quick # 快速配置（默认值）并重启服务"
  echo "  sudo bash scripts/install.sh --sync-only  # 无交互同步代码并重启（复用现有配置）"
  exit 1
fi
if [[ "$MODE" == "--configure-only" ]]; then
  MODE="configure-only"
fi
if [[ "$MODE" == "--configure-quick" ]]; then
  MODE="configure-quick"
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
SSH_HARDEN_FILE="/etc/ssh/sshd_config.d/99-sb-agent-hardening.conf"
FAIL2BAN_JAIL_FILE="/etc/fail2ban/jail.d/sb-agent-sshd.local"

SB_AGENT_SERVICE="/etc/systemd/system/sb-agent.service"
SB_CERT_CHECK_SERVICE="/etc/systemd/system/sb-cert-check.service"
SB_CERT_CHECK_TIMER="/etc/systemd/system/sb-cert-check.timer"
SB_CERT_CHECK_BIN="/usr/local/bin/sb-cert-check.sh"

CONTROLLER_URL=""
NODE_CODE=""
AUTH_TOKEN=""
TUIC_DOMAIN=""
ACME_EMAIL=""
TUIC_DEFAULT_PORT=24443
TUIC_LISTEN_PORT=$TUIC_DEFAULT_PORT
POLL_INTERVAL=15
PUBLIC_IP=""
AGENT_PYTHON_BIN=""
OS_ID=""
OS_VERSION=""
INIT_SYSTEM="systemd"

msg() { echo -e "\033[1;32m[信息]\033[0m $*"; }
warn() { echo -e "\033[1;33m[警告]\033[0m $*"; }
err() { echo -e "\033[1;31m[错误]\033[0m $*" >&2; }

detect_os() {
  if [[ -f /etc/os-release ]]; then
    # shellcheck disable=SC1091
    . /etc/os-release
    OS_ID="${ID:-}"
    OS_VERSION="${VERSION_ID:-}"
  fi
  if [[ "$OS_ID" == "alpine" ]]; then
    INIT_SYSTEM="openrc"
  elif command -v systemctl >/dev/null 2>&1 && [[ -d /run/systemd/system ]]; then
    INIT_SYSTEM="systemd"
  else
    INIT_SYSTEM="openrc"
  fi
}

is_alpine() {
  [[ "$OS_ID" == "alpine" ]]
}

strip_unit_name() {
  local unit="$1"
  unit="${unit##*/}"
  unit="${unit%.service}"
  unit="${unit%.timer}"
  echo "$unit"
}

openrc_service_exists() {
  local svc="$1"
  [[ -x "/etc/init.d/${svc}" ]]
}

openrc_is_enabled() {
  local svc="$1"
  rc-update show default 2>/dev/null | grep -E "[[:space:]]${svc}[[:space:]]" >/dev/null 2>&1
}

openrc_enable() {
  local svc="$1"
  rc-update add "$svc" default >/dev/null 2>&1 || true
}

openrc_disable() {
  local svc="$1"
  rc-update del "$svc" default >/dev/null 2>&1 || true
}

openrc_start() {
  local svc="$1"
  rc-service "$svc" start >/dev/null 2>&1 || true
}

openrc_stop() {
  local svc="$1"
  rc-service "$svc" stop >/dev/null 2>&1 || true
}

openrc_restart() {
  local svc="$1"
  rc-service "$svc" restart >/dev/null 2>&1 || rc-service "$svc" start >/dev/null 2>&1 || true
}

openrc_status() {
  local svc="$1"
  rc-service "$svc" status 2>/dev/null || true
}

openrc_is_active() {
  local svc="$1"
  rc-service "$svc" status >/dev/null 2>&1
}

openrc_cert_timer_enabled() {
  [[ -f /etc/periodic/daily/sb-cert-check ]]
}

openrc_cert_timer_enable() {
  mkdir -p /etc/periodic/daily
  cat >/etc/periodic/daily/sb-cert-check <<EOF
#!/bin/sh
${SB_CERT_CHECK_BIN} >> ${AGENT_LOG_DIR}/cert-check.log 2>&1
EOF
  chmod 0755 /etc/periodic/daily/sb-cert-check
  openrc_enable crond
  openrc_start crond
}

openrc_cert_timer_disable() {
  rm -f /etc/periodic/daily/sb-cert-check
}

if ! command -v systemctl >/dev/null 2>&1; then
  systemctl() {
    local sub="$1"
    shift || true
    case "$sub" in
      show)
        local prop=""
        if [[ "${1:-}" == "-p" ]]; then
          prop="$2"; shift 2
        fi
        if [[ "${1:-}" == "--value" ]]; then
          shift
        fi
        local unit="${1:-}"
        unit="$(strip_unit_name "$unit")"
        if [[ "$prop" == "LoadState" ]]; then
          if openrc_service_exists "$unit"; then
            echo "loaded"
          else
            echo "not-found"
          fi
        else
          echo ""
        fi
        ;;
      list-unit-files)
        if [[ -d /etc/init.d ]]; then
          for svc in /etc/init.d/*; do
            svc="$(basename "$svc")"
            echo "${svc}.service enabled"
          done
        fi
        ;;
      is-active)
        openrc_is_active "$(strip_unit_name "${1:-}")"
        ;;
      is-enabled)
        local unit="${1:-}"
        if [[ "$unit" == *.timer ]]; then
          openrc_cert_timer_enabled
        else
          openrc_is_enabled "$(strip_unit_name "$unit")"
        fi
        ;;
      enable)
        local now=0
        if [[ "${1:-}" == "--now" ]]; then
          now=1
          shift
        fi
        local unit="${1:-}"
        if [[ "$unit" == *.timer ]]; then
          openrc_cert_timer_enable
        else
          local svc
          svc="$(strip_unit_name "$unit")"
          openrc_enable "$svc"
          if (( now == 1 )); then
            openrc_start "$svc"
          fi
        fi
        ;;
      disable)
        local unit="${1:-}"
        if [[ "$unit" == *.timer ]]; then
          openrc_cert_timer_disable
        else
          openrc_disable "$(strip_unit_name "$unit")"
        fi
        ;;
      start)
        openrc_start "$(strip_unit_name "${1:-}")"
        ;;
      stop)
        openrc_stop "$(strip_unit_name "${1:-}")"
        ;;
      restart|reload)
        openrc_restart "$(strip_unit_name "${1:-}")"
        ;;
      status)
        openrc_status "$(strip_unit_name "${1:-}")"
        ;;
      daemon-reload)
        return 0
        ;;
      *)
        return 1
        ;;
    esac
  }
fi

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
  ai_context_path="/tmp/sb-install-node-ai-context-on-fail-$(date +%Y%m%d-%H%M%S).md"
  if bash "$AI_CONTEXT_SCRIPT" --output "$ai_context_path" >/tmp/sb_install_node_ai_export.log 2>&1; then
    echo "失败辅助诊断包：${ai_context_path}"
    echo "提示：可将该文件整体粘贴给任意 AI 做继续定位。"
  else
    warn "自动导出 AI 诊断包失败（不影响原始失败结论），可手动执行: bash scripts/ai_context_export.sh"
    cat /tmp/sb_install_node_ai_export.log || true
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
    err "请使用 root 权限运行（例如 sudo bash scripts/install.sh）"
    exit 1
  fi
}

systemd_unit_exists() {
  local unit_name="$1"
  local load_state
  load_state="$(systemctl show -p LoadState --value "$unit_name" 2>/dev/null || true)"
  [[ -n "$load_state" && "$load_state" != "not-found" ]]
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

ask_yes_no_with_back() {
  local prompt="$1"
  local default="${2:-Y}"
  local answer
  local hint="[Y/n]"
  if [[ "$default" == "N" ]]; then
    hint="[y/N]"
  fi
  read -r -p "$prompt $hint（输入 b 返回上一步）: " answer
  answer="${answer:-$default}"
  if [[ "$answer" == "b" || "$answer" == "B" ]]; then
    return 2
  fi
  if [[ "$answer" =~ ^[Yy]$ ]]; then
    return 0
  fi
  return 1
}

prompt_with_back() {
  local prompt="$1"
  local default_value="${2:-}"
  local input
  read -r -p "${prompt} [${default_value}]: " input
  if [[ "$input" == "b" || "$input" == "B" ]]; then
    echo "__SB_BACK__"
    return 0
  fi
  if [[ -z "$input" ]]; then
    echo "$default_value"
  else
    echo "$input"
  fi
}

install_menu_shortcuts() {
  # Cleanup legacy shortcut from old versions to avoid confusing entrypoint.
  if [[ -f /usr/local/bin/sb-bot-panel ]] && grep -q "sb-node-main-shortcut" /usr/local/bin/sb-bot-panel 2>/dev/null; then
    rm -f /usr/local/bin/sb-bot-panel
    msg "已清理历史快捷命令：/usr/local/bin/sb-bot-panel"
  fi

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
  if is_alpine; then
    apk add --no-cache \
      bash \
      curl \
      jq \
      python3 \
      py3-virtualenv \
      py3-pip \
      ca-certificates \
      openssl \
      bind-tools \
      tar \
      gzip \
      coreutils \
      iproute2
  else
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
      fail2ban \
      dnsutils \
      bind9-host
  fi
}

detect_ssh_service() {
  if systemctl list-unit-files 2>/dev/null | grep -q '^sshd.service'; then
    echo "sshd"
    return
  fi
  echo "ssh"
}

has_authorized_keys_for_user() {
  local user_name="${1:-root}"
  local user_home auth_file
  user_home="$(getent passwd "$user_name" | awk -F: '{print $6}' || true)"
  if [[ -z "$user_home" ]]; then
    return 1
  fi
  auth_file="${user_home}/.ssh/authorized_keys"
  [[ -s "$auth_file" ]]
}

get_authorized_keys_path_for_user() {
  local user_name="${1:-root}"
  local user_home
  user_home="$(getent passwd "$user_name" | awk -F: '{print $6}' || true)"
  if [[ -z "$user_home" ]]; then
    echo ""
    return
  fi
  echo "${user_home}/.ssh/authorized_keys"
}

detect_sshd_port() {
  local port
  port=""
  if command -v sshd >/dev/null 2>&1; then
    port="$(sshd -T 2>/dev/null | awk '/^port /{print $2; exit}' || true)"
  fi
  if [[ "$port" =~ ^[0-9]+$ ]]; then
    echo "$port"
  else
    echo "22"
  fi
}

detect_current_ssh_client_ip() {
  local ip
  ip=""
  if [[ -n "${SSH_CONNECTION:-}" ]]; then
    ip="$(echo "$SSH_CONNECTION" | awk '{print $1}')"
  elif [[ -n "${SSH_CLIENT:-}" ]]; then
    ip="$(echo "$SSH_CLIENT" | awk '{print $1}')"
  else
    ip="$(who -m 2>/dev/null | sed -n 's/.*(\([0-9.]*\)).*/\1/p' | head -n1 || true)"
  fi
  echo "$ip"
}

is_fail2ban_banned_ip() {
  local ip="$1"
  if ! command -v fail2ban-client >/dev/null 2>&1; then
    return 1
  fi
  if ! systemctl is-active fail2ban >/dev/null 2>&1; then
    return 1
  fi
  local banned_line
  banned_line="$(fail2ban-client status sshd 2>/dev/null | awk -F: '/Banned IP list/{print $2; exit}' || true)"
  [[ " ${banned_line} " == *" ${ip} "* ]]
}

ufw_allows_ssh_for_ip() {
  local ip="$1"
  local ssh_port="$2"
  if ! command -v ufw >/dev/null 2>&1; then
    return 0
  fi
  local status_line
  status_line="$(ufw status 2>/dev/null | head -n1 || true)"
  if [[ "$status_line" == *"inactive"* ]]; then
    return 0
  fi
  ufw status 2>/dev/null | awk -v p="$ssh_port" -v ip="$ip" '
    BEGIN { ok=0 }
    $0 ~ ("^ *" p "(/tcp)?([[:space:]]|$)") && $0 ~ /ALLOW/ {
      if ($0 ~ /Anywhere/ || $0 ~ ("(^|[[:space:]])" ip "($|[[:space:]])") || $0 ~ (ip "/32")) {
        ok=1
      }
    }
    END { exit(ok ? 0 : 1) }
  '
}

precheck_ssh_lockout_risk() {
  local client_ip ssh_port
  local step=1 answer_rc
  client_ip="$(detect_current_ssh_client_ip)"
  ssh_port="$(detect_sshd_port)"

  while (( step <= 2 )); do
    if (( step == 1 )); then
      if [[ -n "$client_ip" ]]; then
        msg "当前 SSH 会话来源 IP: ${client_ip}，sshd 端口: ${ssh_port}"
        if is_fail2ban_banned_ip "$client_ip"; then
          err "当前来源 IP(${client_ip}) 已在 fail2ban 封禁列表，请先解封后再启用仅密钥登录。"
          return 1
        fi
        if ! ufw_allows_ssh_for_ip "$client_ip" "$ssh_port"; then
          warn "UFW 未明确放行当前来源 IP(${client_ip}) 到 SSH 端口 ${ssh_port}。"
          ask_yes_no_with_back "仍继续启用仅密钥登录？（可能导致失联）" "N"
          answer_rc=$?
          if (( answer_rc == 2 )); then
            return 2
          fi
          if (( answer_rc != 0 )); then
            warn "已取消启用。"
            return 1
          fi
        fi
      else
        warn "未检测到当前 SSH 会话来源 IP（可能是本机控制台）。"
        ask_yes_no_with_back "仍继续启用仅密钥登录？" "N"
        answer_rc=$?
        if (( answer_rc == 2 )); then
          return 2
        fi
        if (( answer_rc != 0 )); then
          warn "已取消启用。"
          return 1
        fi
      fi
      step=2
      continue
    fi

    ask_yes_no_with_back "是否已在另一个终端验证公钥可登录？" "N"
    answer_rc=$?
    if (( answer_rc == 2 )); then
      step=1
      continue
    fi
    if (( answer_rc != 0 )); then
      warn "未确认公钥可登录，已取消启用。"
      return 1
    fi
    step=3
  done
  return 0
}

configure_ssh_key_only_login() {
  local user_name="${1:-root}"
  local ssh_service auth_file
  local precheck_rc=0
  ssh_service="$(detect_ssh_service)"
  auth_file="$(get_authorized_keys_path_for_user "$user_name")"

  if ! has_authorized_keys_for_user "$user_name"; then
    warn "用户 ${user_name} 尚无可用 authorized_keys（${auth_file}），跳过启用仅密钥登录，避免锁死 SSH。"
    return 1
  fi
  precheck_ssh_lockout_risk || precheck_rc=$?
  if (( precheck_rc == 2 )); then
    return 2
  fi
  if (( precheck_rc != 0 )); then
    return 1
  fi

  mkdir -p /etc/ssh/sshd_config.d
  cat >"$SSH_HARDEN_FILE" <<'EOF'
# Managed by sb-agent install/menu
PubkeyAuthentication yes
PasswordAuthentication no
KbdInteractiveAuthentication no
ChallengeResponseAuthentication no
UsePAM yes
PermitRootLogin prohibit-password
EOF

  if command -v sshd >/dev/null 2>&1 && ! sshd -t; then
    rm -f "$SSH_HARDEN_FILE"
    err "sshd 配置校验失败，已回滚 SSH 加固配置。"
    return 1
  fi

  systemctl restart "$ssh_service" >/dev/null 2>&1 || true
  msg "SSH 已切换为仅密钥登录（已禁用密码登录，用户=${user_name}）。"
  return 0
}

install_and_enable_fail2ban() {
  if command -v fail2ban-client >/dev/null 2>&1; then
    msg "检测到 fail2ban 已安装，执行配置同步并确保服务启用..."
  else
    msg "安装并启用 fail2ban（SSH 防爆破）..."
  fi
  if ! command -v fail2ban-client >/dev/null 2>&1; then
    if is_alpine; then
      apk add --no-cache fail2ban >/dev/null 2>&1
    else
      export DEBIAN_FRONTEND=noninteractive
      apt-get update -y >/dev/null 2>&1
      apt-get install -y fail2ban >/dev/null 2>&1
    fi
  fi

  mkdir -p /etc/fail2ban/jail.d
  cat >"$FAIL2BAN_JAIL_FILE" <<'EOF'
[sshd]
enabled = true
mode = normal
port = ssh
filter = sshd
logpath = %(sshd_log)s
backend = auto
maxretry = 5
findtime = 10m
bantime = 1h
EOF

  systemctl enable --now fail2ban >/dev/null
  msg "fail2ban 已启用（jail=sshd, maxretry=5, bantime=1h）。"
}

configure_security_interactive() {
  local step=1 choice_rc ssh_user input
  echo "SSH 公钥存放路径提示："
  echo "  - root: /root/.ssh/authorized_keys"
  echo "  - 普通用户: /home/<用户名>/.ssh/authorized_keys"
  echo ""

  while (( step <= 2 )); do
    case "$step" in
      1)
        ask_yes_no_with_back "是否安装并启用 fail2ban（推荐，用于 SSH 防爆破）？" "Y"
        choice_rc=$?
        if (( choice_rc == 2 )); then
          return 2
        fi
        if (( choice_rc == 0 )); then
          install_and_enable_fail2ban
        else
          warn "已跳过 fail2ban 安装。"
        fi
        step=2
        ;;
      2)
        ask_yes_no_with_back "是否现在启用 SSH 仅密钥登录（将禁用密码登录）？" "N"
        choice_rc=$?
        if (( choice_rc == 2 )); then
          step=1
          continue
        fi
        if (( choice_rc == 0 )); then
          input="$(prompt_with_back "请输入用于校验公钥的用户名" "root")"
          if [[ "$input" == "__SB_BACK__" ]]; then
            continue
          fi
          ssh_user="${input:-root}"
          configure_ssh_key_only_login "$ssh_user"
          choice_rc=$?
          if (( choice_rc == 2 )); then
            continue
          fi
          if (( choice_rc != 0 )); then
            warn "SSH 仅密钥登录未生效。请先为 ${ssh_user} 配置公钥后再执行。"
          fi
        else
          warn "已跳过 SSH 仅密钥登录设置。"
        fi
        step=3
        ;;
    esac
  done
  return 0
}

python_version_ge_311() {
  local cmd="$1"
  "$cmd" - <<'PY'
import sys
raise SystemExit(0 if sys.version_info >= (3, 11) else 1)
PY
}

ensure_python_311_runtime() {
  local os_id="$OS_ID"
  local os_version="$OS_VERSION"

  if command -v python3.11 >/dev/null 2>&1 && python_version_ge_311 python3.11; then
    AGENT_PYTHON_BIN="$(command -v python3.11)"
    msg "检测到 Python 3.11: ${AGENT_PYTHON_BIN}"
    return
  fi

  msg "尝试安装 Python 3.11..."
  if is_alpine; then
    apk add --no-cache python3 py3-virtualenv py3-pip >/dev/null 2>&1 || true
  else
    export DEBIAN_FRONTEND=noninteractive
    apt-get install -y python3.11 python3.11-venv python3.11-distutils >/dev/null 2>&1 || true
  fi

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
  if is_alpine; then
    apk add --no-cache bind-tools
  else
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -y
    apt-get install -y dnsutils bind9-host
  fi
}

find_latest_singbox_deb() {
  find "$PWD" /tmp -maxdepth 2 -type f -name 'sing-box_*_linux_*.deb' -printf '%T@ %p\n' 2>/dev/null \
    | sort -nr \
    | head -n1 \
    | cut -d' ' -f2-
}

retry_singbox_install_keep_local_config() {
  local deb_path
  deb_path="$(find_latest_singbox_deb || true)"
  if [[ -z "$deb_path" || ! -f "$deb_path" ]]; then
    return 1
  fi
  warn "检测到 conffile 冲突，使用保留本地配置策略重试: ${deb_path}"
  if ! dpkg -i --force-confdef --force-confold "$deb_path"; then
    apt-get install -f -y || true
    dpkg -i --force-confdef --force-confold "$deb_path"
  fi
}

install_sing_box() {
  if is_alpine; then
    msg "检测到 Alpine，使用二进制包方式安装 sing-box..."
    local arch dl_url tmp_dir
    arch="$(uname -m)"
    case "$arch" in
      x86_64) arch="amd64" ;;
      aarch64|arm64) arch="arm64" ;;
      *) err "当前架构(${arch})暂未适配 Alpine sing-box 自动安装"; exit 1 ;;
    esac
    dl_url="https://github.com/SagerNet/sing-box/releases/latest/download/sing-box-linux-${arch}.tar.gz"
    tmp_dir="$(mktemp -d)"
    if ! curl -fsSL "$dl_url" -o "${tmp_dir}/sing-box.tgz"; then
      err "sing-box 下载失败：${dl_url}"
      exit 1
    fi
    tar -xzf "${tmp_dir}/sing-box.tgz" -C "$tmp_dir"
    if [[ -f "${tmp_dir}/sing-box" ]]; then
      install -m 0755 "${tmp_dir}/sing-box" /usr/local/bin/sing-box
    else
      local bin_path
      bin_path="$(find "$tmp_dir" -maxdepth 2 -type f -name 'sing-box' | head -n1 || true)"
      if [[ -z "$bin_path" ]]; then
        err "sing-box 解压后未找到二进制文件。"
        exit 1
      fi
      install -m 0755 "$bin_path" /usr/local/bin/sing-box
    fi
    rm -rf "$tmp_dir"
  else
    msg "安装/更新 sing-box（官方脚本）..."
    export DEBIAN_FRONTEND=noninteractive
    if ! curl -fsSL https://sing-box.app/install.sh | bash; then
      warn "官方安装脚本返回失败，尝试非交互重试（保留本地 config.json）..."
      if ! retry_singbox_install_keep_local_config; then
        err "sing-box 安装失败，且未能自动完成非交互重试。"
        exit 1
      fi
    fi
  fi
  if ! command -v sing-box >/dev/null 2>&1; then
    err "sing-box 安装失败，未检测到命令。"
    exit 1
  fi
}

ensure_dirs() {
  mkdir -p /etc/sb-agent
  mkdir -p "$(dirname "$SINGBOX_CONFIG")"
  mkdir -p "$AGENT_DIR"
  mkdir -p "$AGENT_LOG_DIR"
  mkdir -p "$SINGBOX_LOG_DIR"
  mkdir -p "$CERTMAGIC_DIR"
  mkdir -p "$BACKUP_DIR"
}

resolve_singbox_run_user() {
  local service_user
  service_user="$(systemctl show -p User --value sing-box.service 2>/dev/null || true)"
  service_user="$(echo "$service_user" | xargs)"
  if [[ -n "$service_user" ]]; then
    echo "$service_user"
    return
  fi
  if id -u sing-box >/dev/null 2>&1; then
    echo "sing-box"
    return
  fi
  echo "root"
}

ensure_singbox_log_permissions() {
  local run_user run_group
  run_user="$(resolve_singbox_run_user)"
  run_group="$(id -gn "$run_user" 2>/dev/null || echo "$run_user")"

  mkdir -p "$SINGBOX_LOG_DIR"
  touch "${SINGBOX_LOG_DIR}/sing-box.log"

  if chown "${run_user}:${run_group}" "$SINGBOX_LOG_DIR" "${SINGBOX_LOG_DIR}/sing-box.log" >/dev/null 2>&1; then
    :
  else
    warn "无法设置 sing-box 日志目录属主为 ${run_user}:${run_group}，将继续尝试启动。"
  fi
  chmod 0755 "$SINGBOX_LOG_DIR" || true
  chmod 0644 "${SINGBOX_LOG_DIR}/sing-box.log" || true
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
  NODE_CODE="${NODE_CODE:-N1}"
  TUIC_DOMAIN="${TUIC_DOMAIN:-}"
  ACME_EMAIL="${ACME_EMAIL:-}"

  if ! [[ "${TUIC_LISTEN_PORT}" =~ ^[0-9]+$ ]] || (( TUIC_LISTEN_PORT < 1 || TUIC_LISTEN_PORT > 65535 )); then
    TUIC_LISTEN_PORT=$TUIC_DEFAULT_PORT
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

  echo ""
  msg "配置向导说明（每项都可回车采用默认值，输入 b 可回到上一步）："
  echo "  1) controller_url：节点拉取配置的地址。获取：管理服务器公网地址（如 https://panel.example.com）"
  echo "  2) node_code：节点唯一标识。获取：管理端节点列表里的节点编码（需完全一致）"
  echo "  3) auth_token：节点鉴权 token。获取：管理服务器 .env 中 NODE_AUTH_TOKEN（兼容模式用 AUTH_TOKEN）"
  echo "  4) tuic_domain：TUIC 证书域名。获取：你已解析到本机公网 IP 的域名（留空=不启用 TUIC）"
  echo "  5) acme_email：证书申请邮箱。获取：你常用邮箱（用于证书到期通知）"
  echo "  6) tuic_listen_port：TUIC UDP 端口。建议高位端口（默认 ${TUIC_DEFAULT_PORT}）"
  echo "  7) poll_interval：agent 轮询间隔秒数。越小同步越快，默认 15"
  echo ""

  local step=1
  local input controller_scheme controller_host
  CONTROLLER_URL="${old_controller:-http://127.0.0.1:8080}"
  NODE_CODE="${old_node:-N1}"
  AUTH_TOKEN="${old_token:-devtoken123}"
  TUIC_DOMAIN="${old_domain:-}"
  ACME_EMAIL="${old_email:-admin@example.com}"
  TUIC_LISTEN_PORT="${old_tuic_port:-$TUIC_DEFAULT_PORT}"
  POLL_INTERVAL="${old_poll:-15}"

  while (( step <= 7 )); do
    case "$step" in
      1)
        input="$(prompt_with_back "1) 请输入 controller_url（支持省略 http/https，例如 panel.example.com:8080）" "$CONTROLLER_URL")"
        if [[ "$input" == "__SB_BACK__" ]]; then
          warn "已经是第一步。"
          continue
        fi
        CONTROLLER_URL="$input"
        controller_host="$(extract_url_host "$CONTROLLER_URL")"
        controller_scheme="https"
        if [[ "$controller_host" == "127.0.0.1" || "$controller_host" == "localhost" || "$CONTROLLER_URL" == *":8080"* || "$CONTROLLER_URL" == *":80"* ]]; then
          controller_scheme="http"
        fi
        CONTROLLER_URL="$(normalize_input_url "$CONTROLLER_URL" "$controller_scheme")"
        step=$((step + 1))
        ;;
      2)
        input="$(prompt_with_back "2) 请输入 node_code（例如 N1）" "$NODE_CODE")"
        if [[ "$input" == "__SB_BACK__" ]]; then
          step=$((step - 1)); continue
        fi
        NODE_CODE="$(echo "$input" | tr -d '[:space:]')"
        if [[ -z "$NODE_CODE" ]]; then
          warn "node_code 不能为空。"
          continue
        fi
        step=$((step + 1))
        ;;
      3)
        input="$(prompt_with_back "3) 请输入 auth_token（用于拉取 sync）" "$AUTH_TOKEN")"
        if [[ "$input" == "__SB_BACK__" ]]; then
          step=$((step - 1)); continue
        fi
        AUTH_TOKEN="$input"
        step=$((step + 1))
        ;;
      4)
        input="$(prompt_with_back "4) 请输入 tuic_domain（例如 node1.example.com；留空=不启用TUIC）" "$TUIC_DOMAIN")"
        if [[ "$input" == "__SB_BACK__" ]]; then
          step=$((step - 1)); continue
        fi
        TUIC_DOMAIN="$(echo "$input" | tr -d '[:space:]')"
        if [[ -z "$TUIC_DOMAIN" ]]; then
          ACME_EMAIL=""
          msg "已选择不启用 TUIC，跳过证书邮箱。"
        fi
        step=$((step + 1))
        ;;
      5)
        if [[ -z "$TUIC_DOMAIN" ]]; then
          step=$((step + 1))
          continue
        fi
        input="$(prompt_with_back "5) 请输入 acme_email（例如 admin@example.com）" "${ACME_EMAIL:-admin@example.com}")"
        if [[ "$input" == "__SB_BACK__" ]]; then
          step=$((step - 1)); continue
        fi
        ACME_EMAIL="$(echo "$input" | tr -d '[:space:]')"
        if [[ "$ACME_EMAIL" == "admin@example.com" ]]; then
          warn "当前 acme_email 使用默认占位邮箱，建议后续改为你的真实邮箱。"
        fi
        step=$((step + 1))
        ;;
      6)
        input="$(prompt_with_back "6) 请输入 tuic_listen_port（默认 ${TUIC_DEFAULT_PORT}）" "$TUIC_LISTEN_PORT")"
        if [[ "$input" == "__SB_BACK__" ]]; then
          step=$((step - 1)); continue
        fi
        TUIC_LISTEN_PORT="$input"
        if ! [[ "$TUIC_LISTEN_PORT" =~ ^[0-9]+$ ]] || (( TUIC_LISTEN_PORT < 1 || TUIC_LISTEN_PORT > 65535 )); then
          warn "端口无效，已回退为 ${TUIC_DEFAULT_PORT}"
          TUIC_LISTEN_PORT=$TUIC_DEFAULT_PORT
        fi
        step=$((step + 1))
        ;;
      7)
        input="$(prompt_with_back "7) 请输入 poll_interval（秒，默认 15）" "$POLL_INTERVAL")"
        if [[ "$input" == "__SB_BACK__" ]]; then
          step=$((step - 1)); continue
        fi
        POLL_INTERVAL="$input"
        if ! [[ "$POLL_INTERVAL" =~ ^[0-9]+$ ]] || (( POLL_INTERVAL < 5 )); then
          warn "轮询间隔无效，已回退为 15 秒"
          POLL_INTERVAL=15
        fi
        step=$((step + 1))
        ;;
    esac
  done
}

prompt_config_quick() {
  local old_controller old_node old_token old_domain old_email old_tuic_port old_poll
  local enable_tuic_quick
  old_controller="$(read_old_config_value "controller_url")"
  old_node="$(read_old_config_value "node_code")"
  old_token="$(read_old_config_value "auth_token")"
  old_domain="$(read_old_config_value "tuic_domain")"
  old_email="$(read_old_config_value "acme_email")"
  old_tuic_port="$(read_old_config_value "tuic_listen_port")"
  old_poll="$(read_old_config_value "poll_interval")"

  echo ""
  msg "快速配置（推荐默认值，输入 b 可回到上一步）"
  echo "  - 仅提问关键参数：controller_url / node_code / auth_token"
  echo "  - auth_token 获取：管理服务器 .env 的 NODE_AUTH_TOKEN（兼容模式可用 AUTH_TOKEN）"
  echo "  - 其余变量自动按默认值写入（可在高级向导再改）"
  echo ""

  local step=1 input controller_scheme controller_host enable_tuic_answer
  CONTROLLER_URL="${old_controller:-http://127.0.0.1:8080}"
  NODE_CODE="${old_node:-N1}"
  AUTH_TOKEN="${old_token:-devtoken123}"
  TUIC_DOMAIN="${old_domain:-}"
  ACME_EMAIL="${old_email:-}"
  TUIC_LISTEN_PORT="${old_tuic_port:-$TUIC_DEFAULT_PORT}"

  while (( step <= 4 )); do
    case "$step" in
      1)
        input="$(prompt_with_back "controller_url（节点拉取配置地址）" "$CONTROLLER_URL")"
        if [[ "$input" == "__SB_BACK__" ]]; then
          warn "已经是第一步。"
          continue
        fi
        CONTROLLER_URL="$input"
        controller_host="$(extract_url_host "$CONTROLLER_URL")"
        controller_scheme="https"
        if [[ "$controller_host" == "127.0.0.1" || "$controller_host" == "localhost" || "$CONTROLLER_URL" == *":8080"* || "$CONTROLLER_URL" == *":80"* ]]; then
          controller_scheme="http"
        fi
        CONTROLLER_URL="$(normalize_input_url "$CONTROLLER_URL" "$controller_scheme")"
        step=$((step + 1))
        ;;
      2)
        input="$(prompt_with_back "node_code（节点唯一标识）" "$NODE_CODE")"
        if [[ "$input" == "__SB_BACK__" ]]; then
          step=$((step - 1)); continue
        fi
        NODE_CODE="$(echo "$input" | tr -d '[:space:]')"
        [[ -z "$NODE_CODE" ]] && NODE_CODE="N1"
        step=$((step + 1))
        ;;
      3)
        input="$(prompt_with_back "auth_token（用于拉取 sync）" "$AUTH_TOKEN")"
        if [[ "$input" == "__SB_BACK__" ]]; then
          step=$((step - 1)); continue
        fi
        AUTH_TOKEN="$input"
        step=$((step + 1))
        ;;
      4)
        if [[ -n "$old_domain" ]]; then
          enable_tuic_quick="Y"
        else
          enable_tuic_quick="N"
        fi
        read -r -p "是否在快速配置中启用/修改 TUIC 证书参数（tuic_domain/acme_email/端口）？[Y/n]（输入 b 返回上一步）: " enable_tuic_answer
        enable_tuic_answer="${enable_tuic_answer:-$enable_tuic_quick}"
        if [[ "$enable_tuic_answer" == "b" || "$enable_tuic_answer" == "B" ]]; then
          step=$((step - 1))
          continue
        fi
        if [[ "$enable_tuic_answer" =~ ^[Yy]$ ]]; then
          input="$(prompt_with_back "tuic_domain（留空=关闭TUIC）" "${old_domain}")"
          if [[ "$input" == "__SB_BACK__" ]]; then
            step=$((step - 1)); continue
          fi
          TUIC_DOMAIN="$(echo "$input" | tr -d '[:space:]')"
          if [[ -n "$TUIC_DOMAIN" ]]; then
            input="$(prompt_with_back "acme_email（证书邮箱）" "${old_email:-admin@example.com}")"
            if [[ "$input" == "__SB_BACK__" ]]; then
              continue
            fi
            ACME_EMAIL="$(echo "$input" | tr -d '[:space:]')"
            input="$(prompt_with_back "tuic_listen_port（默认 ${TUIC_DEFAULT_PORT}）" "${old_tuic_port:-$TUIC_DEFAULT_PORT}")"
            if [[ "$input" == "__SB_BACK__" ]]; then
              continue
            fi
            TUIC_LISTEN_PORT="$input"
          else
            ACME_EMAIL=""
            TUIC_LISTEN_PORT="${old_tuic_port:-$TUIC_DEFAULT_PORT}"
            warn "已按你的输入关闭 TUIC（tuic_domain 为空）。"
          fi
        else
          TUIC_DOMAIN="${old_domain:-}"
          ACME_EMAIL="${old_email:-}"
          TUIC_LISTEN_PORT="${old_tuic_port:-$TUIC_DEFAULT_PORT}"
        fi
        step=$((step + 1))
        ;;
    esac
  done

  POLL_INTERVAL="${old_poll:-15}"
  if ! [[ "$TUIC_LISTEN_PORT" =~ ^[0-9]+$ ]] || (( TUIC_LISTEN_PORT < 1 || TUIC_LISTEN_PORT > 65535 )); then
    TUIC_LISTEN_PORT=$TUIC_DEFAULT_PORT
  fi
  if ! [[ "$POLL_INTERVAL" =~ ^[0-9]+$ ]] || (( POLL_INTERVAL < 5 )); then
    POLL_INTERVAL=15
  fi
}

check_domain_resolution_interactive() {
  if [[ -z "$TUIC_DOMAIN" ]]; then
    return 0
  fi
  ensure_dns_tools
  PUBLIC_IP="$(get_public_ipv4)"
  if [[ -z "$PUBLIC_IP" ]]; then
    warn "无法获取本机公网 IPv4，跳过自动解析比对。"
    return 0
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
    read -r -p "修复后按回车重试，输入 skip 跳过，输入 b 返回上一步: " retry
    if [[ "$retry" == "skip" ]]; then
      warn "你选择了跳过解析检查，证书申请可能失败。"
      break
    elif [[ "$retry" == "b" || "$retry" == "B" ]]; then
      return 2
    fi
  done
  return 0
}

configure_ufw_rules() {
  if is_alpine; then
    warn "检测到 Alpine：跳过 UFW 配置，请自行配置防火墙放行端口。"
    return 0
  fi
  if ! command -v ufw >/dev/null 2>&1; then
    warn "未检测到 ufw，跳过防火墙配置。"
    return 0
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
    ask_yes_no_with_back "检测到 UFW 未启用，是否现在启用？" "Y"
    local ufw_choice_rc=$?
    if (( ufw_choice_rc == 2 )); then
      return 2
    fi
    if (( ufw_choice_rc == 0 )); then
      ufw --force enable >/dev/null
      msg "UFW 已启用。"
    else
      warn "你选择不启用 UFW，请自行确保端口放行。"
    fi
  else
    msg "UFW 已启用，规则已更新。"
  fi
  return 0
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
  local sb_bin
  sb_bin="$(command -v sing-box || true)"
  if [[ -z "$sb_bin" ]]; then
    err "找不到 sing-box 可执行文件，无法创建服务。"
    exit 1
  fi
  if [[ "$INIT_SYSTEM" == "openrc" ]]; then
    if openrc_service_exists "sing-box"; then
      msg "检测到系统已有 sing-box（OpenRC），跳过创建。"
      return
    fi
    msg "未检测到 sing-box（OpenRC），创建服务..."
    cat >/etc/init.d/sing-box <<EOF
#!/sbin/openrc-run
description="sing-box service"
command="${sb_bin}"
command_args="run -c ${SINGBOX_CONFIG}"
command_background=yes
pidfile="/run/sing-box.pid"
depend() {
  need net
}
EOF
    chmod 0755 /etc/init.d/sing-box
    return
  fi

  if systemd_unit_exists "sing-box.service"; then
    msg "检测到系统已有 sing-box.service，跳过创建。"
    return
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
  if [[ "$INIT_SYSTEM" == "openrc" ]]; then
    msg "写入 sb-agent（OpenRC）..."
    cat >/etc/init.d/sb-agent <<EOF
#!/sbin/openrc-run
description="sb-agent (pull config from controller and render sing-box)"
command="${AGENT_VENV}/bin/python"
command_args="${AGENT_MAIN}"
command_background=yes
pidfile="/run/sb-agent.pid"
depend() {
  need net
}
start_pre() {
  /usr/bin/install -m 0755 ${ROOT_DIR}/agent/sb_agent.py ${AGENT_MAIN} || return 1
}
EOF
    chmod 0755 /etc/init.d/sb-agent
    return
  fi

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
  if [[ "$INIT_SYSTEM" == "openrc" ]]; then
    msg "写入 sb-cert-check（OpenRC daily）..."
    openrc_cert_timer_enable
    return
  fi

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
    systemctl enable sing-box >/dev/null 2>&1 || true
    if sing-box check -c "$SINGBOX_CONFIG" >/dev/null 2>&1; then
      systemctl restart sing-box >/dev/null 2>&1 || systemctl start sing-box >/dev/null 2>&1 || true
    else
      warn "当前 sing-box 配置检查未通过，已先设置开机自启，等待 sb-agent 下发新配置后再启动。"
    fi
  fi

  systemctl enable sb-agent >/dev/null
  systemctl restart sb-agent

  ask_yes_no_with_back "是否启用每日证书健康检查定时器（sb-cert-check.timer）？" "Y"
  local cert_timer_choice_rc=$?
  if (( cert_timer_choice_rc == 0 )); then
    systemctl enable --now sb-cert-check.timer >/dev/null
    msg "已启用 sb-cert-check.timer"
  elif (( cert_timer_choice_rc == 2 )); then
    warn "已返回上一步并默认不启用定时器。你可稍后在菜单中手动启用。"
  else
    warn "未启用 sb-cert-check.timer，可后续手动启用。"
  fi
}

reload_and_enable_services_noninteractive() {
  systemctl daemon-reload

  if command -v sing-box >/dev/null 2>&1; then
    systemctl enable sing-box >/dev/null 2>&1 || true
    if sing-box check -c "$SINGBOX_CONFIG" >/dev/null 2>&1; then
      systemctl restart sing-box >/dev/null 2>&1 || systemctl start sing-box >/dev/null 2>&1 || true
    else
      warn "当前 sing-box 配置检查未通过，已先设置开机自启，等待 sb-agent 下发新配置后再启动。"
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
  if [[ "$INIT_SYSTEM" == "openrc" ]]; then
    echo "  rc-service sb-agent status"
    echo "  rc-service sing-box status"
    echo "  rc-service fail2ban status"
    echo "  fail2ban-client status sshd"
    echo "  /usr/local/bin/sb-cert-check.sh"
  else
    echo "  systemctl status sb-agent"
    echo "  journalctl -u sb-agent -f"
    echo "  systemctl status sing-box"
    echo "  journalctl -u sing-box -f"
    echo "  systemctl status fail2ban"
    echo "  fail2ban-client status sshd"
    echo "  /usr/local/bin/sb-cert-check.sh"
  fi
  if systemctl is-active caddy >/dev/null 2>&1; then
    warn "检测到本机 caddy 处于运行中。节点侧一般不需要 caddy，若占用 443 可能影响 sing-box。"
  fi
  if [[ -f "$SSH_HARDEN_FILE" ]]; then
    echo "SSH 登录策略: 仅密钥（密码登录已禁用）"
  else
    echo "SSH 登录策略: 未启用仅密钥（允许密码登录）"
  fi
  if systemctl is-active fail2ban >/dev/null 2>&1; then
    echo "fail2ban 状态: 已启用"
  else
    echo "fail2ban 状态: 未启用"
  fi
  echo ""
  echo "下一步："
  echo "  1) 在控制端（bot/controller）确认 node_code=${NODE_CODE} 已创建并启用"
  echo "  2) 在面板绑定用户到该节点"
  echo "  3) 使用脚本菜单管理：bash ${ROOT_DIR}/scripts/menu.sh"
  echo "----------------------------------------"
}

main() {
  require_root
  detect_os
  msg "开始执行节点侧部署（模式: ${MODE}）"

  if [[ "$MODE" == "install" ]]; then
    install_base_packages
    install_sing_box
  elif [[ "$MODE" == "sync-only" ]]; then
    msg "同步模式：复用现有配置，不进行交互提问。"
    load_existing_config_or_fail
  else
    msg "仅配置模式：默认跳过依赖与 sing-box 安装（缺失时会自动补齐）"
    if ! command -v jq >/dev/null 2>&1; then
      install_base_packages
    fi
  fi

  if ! command -v sing-box >/dev/null 2>&1; then
    warn "当前未检测到 sing-box，脚本将自动安装以完成服务创建。"
    install_base_packages
    install_sing_box
  fi

  ensure_python_311_runtime
  ensure_dirs
  setup_python_venv
  install_agent_files
  write_bootstrap_singbox_config_if_missing

  if [[ "$MODE" == "sync-only" ]]; then
    msg "已加载现有配置：$CONFIG_PATH"
  else
    if [[ "$MODE" == "configure-quick" ]]; then
      prompt_config_quick
    else
      prompt_config
    fi
    local post_step=1 post_rc=0
    while (( post_step <= 3 )); do
      case "$post_step" in
        1)
          check_domain_resolution_interactive || post_rc=$?
          if (( post_rc == 2 )); then
            warn "已在当前步骤起点，无法再回退。"
            post_rc=0
            continue
          fi
          post_rc=0
          post_step=2
          ;;
        2)
          configure_ufw_rules || post_rc=$?
          if (( post_rc == 2 )); then
            post_step=1
            post_rc=0
            continue
          fi
          post_rc=0
          post_step=3
          ;;
        3)
          configure_security_interactive || post_rc=$?
          if (( post_rc == 2 )); then
            post_step=2
            post_rc=0
            continue
          fi
          post_rc=0
          post_step=4
          ;;
      esac
    done
    write_config_json
  fi

  install_singbox_service_if_missing
  ensure_singbox_log_permissions
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
