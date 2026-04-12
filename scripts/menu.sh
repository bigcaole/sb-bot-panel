#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
INSTALL_SCRIPT="$ROOT_DIR/scripts/install.sh"
LOCAL_CERT_CHECK_SCRIPT="$ROOT_DIR/scripts/sb_cert_check.sh"
OPS_SNAPSHOT_SCRIPT="$ROOT_DIR/scripts/ops_snapshot.sh"
AI_CONTEXT_SCRIPT="$ROOT_DIR/scripts/ai_context_export.sh"
RUNTIME_SELF_CHECK_SCRIPT="$ROOT_DIR/scripts/node_self_check.sh"
SYSTEM_CERT_CHECK_SCRIPT="/usr/local/bin/sb-cert-check.sh"
MENU_VIEW_MODE="${MENU_VIEW_MODE:-basic}"

AGENT_SERVICE="sb-agent"
SINGBOX_SERVICE="sing-box"
CERT_TIMER="sb-cert-check.timer"
CERT_SERVICE="sb-cert-check.service"

CONFIG_PATH="/etc/sb-agent/config.json"
CERTMAGIC_DIR="/var/lib/sing-box/certmagic"
BACKUP_DIR="/var/backups/sb-agent"
SSH_HARDEN_FILE="/etc/ssh/sshd_config.d/99-sb-agent-hardening.conf"
FAIL2BAN_JAIL_FILE="/etc/fail2ban/jail.d/sb-agent-sshd.local"

OS_ID=""
OS_VERSION=""
INIT_SYSTEM="systemd"
AGENT_LOG_DIR="/var/log/sb-agent"

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
${SYSTEM_CERT_CHECK_SCRIPT} >> ${AGENT_LOG_DIR}/cert-check.log 2>&1
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

detect_os

pause() {
  echo ""
  read -r -p "按回车继续..." _
}

confirm_action() {
  local prompt="$1"
  local default="${2:-N}"
  local answer
  local hint="[y/N]"
  if [[ "$default" == "Y" ]]; then
    hint="[Y/n]"
  fi
  read -r -p "${prompt} ${hint}: " answer
  answer="${answer:-$default}"
  [[ "$answer" =~ ^[Yy]$ ]]
}

update_repo_from_origin_main() {
  local repo_dir="$1"
  local before_rev after_rev
  if ! command -v git >/dev/null 2>&1; then
    err "未安装 git，无法执行更新。"
    return 1
  fi
  if [[ ! -d "${repo_dir}/.git" ]]; then
    err "未检测到 Git 仓库：${repo_dir}/.git"
    return 1
  fi
  before_rev="$(git -C "$repo_dir" rev-parse --short HEAD 2>/dev/null || true)"
  msg "当前版本: ${before_rev:-unknown}"
  msg "拉取远端代码: origin/main（仅快进）..."
  if ! git -C "$repo_dir" pull --ff-only origin main; then
    err "git pull 失败，已中止更新（请先处理本地改动/分支状态后重试）。"
    return 1
  fi
  after_rev="$(git -C "$repo_dir" rev-parse --short HEAD 2>/dev/null || true)"
  if [[ -n "$before_rev" && "$before_rev" == "$after_rev" ]]; then
    msg "代码已是最新版本: ${after_rev:-unknown}"
  else
    msg "代码已更新到: ${after_rev:-unknown}"
  fi
  return 0
}

get_project_version_label() {
  local repo_dir="$1"
  local version_file="${repo_dir}/VERSION"
  local version="dev"
  local rev="unknown"
  if [[ -f "$version_file" ]]; then
    version="$(head -n1 "$version_file" | tr -d '\r' | xargs || true)"
    [[ -z "$version" ]] && version="dev"
  fi
  rev="$(git -C "$repo_dir" rev-parse --short HEAD 2>/dev/null || true)"
  [[ -z "$rev" ]] && rev="unknown"
  echo "${version} (${rev})"
}

print_update_success_summary() {
  local repo_dir="$1"
  msg "升级结果: 成功"
  msg "当前项目版本: $(get_project_version_label "$repo_dir")"
}

mask_secret_value() {
  local value="$1"
  local n="${#value}"
  if [[ -z "$value" ]]; then
    echo "未设置"
    return
  fi
  if (( n <= 8 )); then
    echo "$value"
    return
  fi
  echo "${value:0:4}****${value:n-4:4}"
}

normalize_bool_input() {
  local raw="$1"
  raw="$(echo "$raw" | tr '[:upper:]' '[:lower:]' | xargs)"
  case "$raw" in
    1|true|yes|y|on) echo "true" ;;
    0|false|no|n|off) echo "false" ;;
    *) echo "" ;;
  esac
}

read_node_config_value() {
  local key="$1"
  if [[ ! -f "$CONFIG_PATH" ]]; then
    echo ""
    return
  fi
  jq -r --arg k "$key" 'if has($k) then (.[ $k ] | if . == null then "" else tostring end) else "" end' "$CONFIG_PATH" 2>/dev/null || true
}

write_node_config_value() {
  local key="$1"
  local value="$2"
  local tmp
  if [[ ! -f "$CONFIG_PATH" ]]; then
    err "未检测到节点配置: $CONFIG_PATH"
    return 1
  fi
  tmp="$(mktemp)"
  if [[ "$key" == "tuic_listen_port" || "$key" == "poll_interval" ]]; then
    jq --arg k "$key" --argjson v "$value" '.[$k]=$v' "$CONFIG_PATH" >"$tmp" || {
      rm -f "$tmp"
      return 1
    }
  elif [[ "$key" == "enable_tuic" || "$key" == "enable_vless" ]]; then
    jq --arg k "$key" --argjson v "$value" '.[$k]=$v' "$CONFIG_PATH" >"$tmp" || {
      rm -f "$tmp"
      return 1
    }
  else
    jq --arg k "$key" --arg v "$value" '.[$k]=$v' "$CONFIG_PATH" >"$tmp" || {
      rm -f "$tmp"
      return 1
    }
  fi
  mv "$tmp" "$CONFIG_PATH"
  chmod 0600 "$CONFIG_PATH" || true
}

normalize_controller_url_input() {
  local raw="$1"
  local host scheme
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
  host="${raw%%/*}"
  if [[ "$host" == "127.0.0.1"* || "$host" == "localhost"* || "$host" == *":8080"* || "$host" == *":80"* ]]; then
    scheme="http"
  else
    scheme="https"
  fi
  echo "${scheme}://${raw%/}"
}

node_param_hint() {
  local key="$1"
  case "$key" in
    controller_url) echo "管理服务器地址（节点拉取 sync 用）" ;;
    node_code) echo "节点唯一标识，必须与管理端节点编码一致" ;;
    auth_token) echo "节点鉴权 token（管理端 NODE_AUTH_TOKEN）" ;;
    enable_vless) echo "是否启用 VLESS+Reality 协议（true/false）" ;;
    enable_tuic) echo "是否启用 TUIC 协议（true/false）" ;;
    tuic_domain) echo "TUIC 域名（留空=不启用 TUIC 证书）" ;;
    acme_email) echo "证书邮箱（启用 TUIC 域名时建议填写）" ;;
    tuic_listen_port) echo "TUIC 监听 UDP 端口（1-65535）" ;;
    poll_interval) echo "agent 轮询间隔秒数（建议 >=5）" ;;
    *) echo "参数说明未定义" ;;
  esac
}

node_param_brief() {
  local key="$1"
  case "$key" in
    controller_url) echo "控制器地址" ;;
    node_code) echo "节点编码" ;;
    auth_token) echo "节点鉴权Token" ;;
    enable_vless) echo "启用VLESS" ;;
    enable_tuic) echo "启用TUIC" ;;
    tuic_domain) echo "TUIC域名" ;;
    acme_email) echo "证书邮箱" ;;
    tuic_listen_port) echo "TUIC端口" ;;
    poll_interval) echo "轮询间隔秒数" ;;
    *) echo "参数" ;;
  esac
}

edit_single_node_param() {
  if [[ ! -f "$CONFIG_PATH" ]]; then
    err "未检测到节点配置 ${CONFIG_PATH}，请先执行菜单 1 完成配置。"
    return 1
  fi
  if ! command -v jq >/dev/null 2>&1; then
    err "缺少 jq，无法执行单项参数修改。"
    return 1
  fi

  local -a keys=(controller_url node_code auth_token enable_vless enable_tuic tuic_domain acme_email tuic_listen_port poll_interval)
  local choice idx key current_value display_value new_value

  while true; do
    echo "----- 节点参数单项修改 -----"
    for idx in "${!keys[@]}"; do
      key="${keys[$idx]}"
      current_value="$(read_node_config_value "$key")"
      if [[ "$key" == "enable_tuic" || "$key" == "enable_vless" ]]; then
        if [[ -z "$current_value" ]] && jq -e --arg k "$key" 'has($k) and .[$k] == false' "$CONFIG_PATH" >/dev/null 2>&1; then
          current_value="false"
        fi
      fi
      if [[ "$key" == "auth_token" ]]; then
        display_value="$(mask_secret_value "$current_value")"
      else
        display_value="${current_value:-未设置}"
      fi
      printf "%d) %s｜%s = %s\n" "$((idx + 1))" "$(node_param_brief "$key")" "$key" "$display_value"
    done
    echo "q) 返回"
    read -r -p "请选择要修改的参数编号: " choice
    if [[ "$choice" == "q" || "$choice" == "Q" ]]; then
      break
    fi
    if ! [[ "$choice" =~ ^[0-9]+$ ]]; then
      warn "请输入参数编号或 q。"
      continue
    fi
    idx=$((choice - 1))
    if (( idx < 0 || idx >= ${#keys[@]} )); then
      warn "编号超出范围。"
      continue
    fi
    key="${keys[$idx]}"
    current_value="$(read_node_config_value "$key")"
    echo "参数: ${key}"
    echo "说明: $(node_param_hint "$key")"
    if [[ "$key" == "auth_token" ]]; then
      echo "当前值: $(mask_secret_value "$current_value")"
    else
      if [[ "$key" == "enable_tuic" || "$key" == "enable_vless" ]]; then
        if [[ -z "$current_value" ]] && jq -e --arg k "$key" 'has($k) and .[$k] == false' "$CONFIG_PATH" >/dev/null 2>&1; then
          current_value="false"
        fi
      fi
      echo "当前值: ${current_value:-未设置}"
    fi
    read -r -p "请输入新值（输入 __EMPTY__ 清空，直接回车取消）: " new_value
    if [[ -z "$new_value" ]]; then
      warn "已取消修改。"
      continue
    fi
    if [[ "$new_value" == "__EMPTY__" ]]; then
      new_value=""
    fi

    if [[ "$key" == "controller_url" && -n "$new_value" ]]; then
      new_value="$(normalize_controller_url_input "$new_value")"
    fi
    if [[ "$key" == "tuic_listen_port" ]]; then
      if ! [[ "$new_value" =~ ^[0-9]+$ ]] || (( new_value < 1 || new_value > 65535 )); then
        warn "tuic_listen_port 需为 1-65535。"
        continue
      fi
    fi
    if [[ "$key" == "poll_interval" ]]; then
      if ! [[ "$new_value" =~ ^[0-9]+$ ]] || (( new_value < 5 )); then
        warn "poll_interval 需为整数且 >= 5。"
        continue
      fi
    fi
    if [[ "$key" == "enable_tuic" || "$key" == "enable_vless" ]]; then
      local_bool="$(normalize_bool_input "$new_value")"
      if [[ -z "$local_bool" ]]; then
        warn "请输入 true/false 或 1/0。"
        continue
      fi
      new_value="$local_bool"
    fi

    if ! write_node_config_value "$key" "$new_value"; then
      err "写入配置失败。"
      continue
    fi
    if [[ "$key" == "enable_tuic" || "$key" == "enable_vless" ]]; then
      persisted_value="$(read_node_config_value "$key")"
      if [[ -z "$persisted_value" ]]; then
        warn "检测到布尔值未正确回显，尝试强制写入字符串值..."
        tmp_fix="$(mktemp)"
        if jq --arg k "$key" --arg v "$new_value" '.[$k]=$v' "$CONFIG_PATH" >"$tmp_fix" 2>/dev/null; then
          mv "$tmp_fix" "$CONFIG_PATH"
          chmod 0600 "$CONFIG_PATH" || true
        else
          rm -f "$tmp_fix"
        fi
      fi
    fi
    msg "已更新 ${key}。"
    systemctl restart "$AGENT_SERVICE" >/dev/null 2>&1 || true
    if [[ "$key" == "tuic_domain" || "$key" == "acme_email" || "$key" == "tuic_listen_port" || "$key" == "enable_tuic" || "$key" == "enable_vless" ]]; then
      systemctl restart "$SINGBOX_SERVICE" >/dev/null 2>&1 || true
      msg "已尝试重启 sb-agent / sing-box 使变更生效。"
    else
      msg "已尝试重启 sb-agent 使变更生效。"
    fi
    echo ""
  done
}

require_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    err "请使用 root 权限运行菜单，例如：sudo bash scripts/menu.sh"
    exit 1
  fi
}

systemd_unit_exists() {
  local unit_name="$1"
  local load_state
  load_state="$(systemctl show -p LoadState --value "$unit_name" 2>/dev/null || true)"
  [[ -n "$load_state" && "$load_state" != "not-found" ]]
}

detect_ssh_service() {
  if systemd_unit_exists "sshd.service"; then
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

ufw_has_allow_for_port() {
  local ssh_port="$1"
  if ! command -v ufw >/dev/null 2>&1; then
    return 1
  fi
  ufw status 2>/dev/null | grep -E "^ *${ssh_port}(/tcp)?[[:space:]]+ALLOW" >/dev/null 2>&1
}

remove_ufw_allow_rules_for_port() {
  local ssh_port="$1"
  local removed_count=0
  if ! command -v ufw >/dev/null 2>&1; then
    echo "0"
    return
  fi
  local -a delete_nums=()
  local line num rule
  while IFS= read -r line; do
    num="$(echo "$line" | sed -n 's/^\[ *\([0-9][0-9]*\)\].*/\1/p')"
    rule="$(echo "$line" | sed -n 's/^\[ *[0-9][0-9]*\] *//p')"
    if [[ -z "$num" || -z "$rule" ]]; then
      continue
    fi
    if echo "$rule" | grep -E "^${ssh_port}(/tcp)?[[:space:]]+ALLOW" >/dev/null 2>&1; then
      delete_nums+=("$num")
    fi
  done < <(ufw status numbered 2>/dev/null || true)

  if (( ${#delete_nums[@]} > 0 )); then
    local sorted_num
    while IFS= read -r sorted_num; do
      if [[ -n "$sorted_num" ]]; then
        ufw --force delete "$sorted_num" >/dev/null 2>&1 || true
        removed_count=$((removed_count + 1))
      fi
    done < <(printf '%s\n' "${delete_nums[@]}" | sort -rn)
  fi
  echo "$removed_count"
}

precheck_ssh_lockout_risk() {
  local client_ip ssh_port
  client_ip="$(detect_current_ssh_client_ip)"
  ssh_port="$(detect_sshd_port)"

  if [[ -n "$client_ip" ]]; then
    msg "当前 SSH 会话来源 IP: ${client_ip}，sshd 端口: ${ssh_port}"
    if is_fail2ban_banned_ip "$client_ip"; then
      err "当前来源 IP(${client_ip}) 已在 fail2ban 封禁列表，请先解封后再启用仅密钥登录。"
      return 1
    fi
    if ! ufw_allows_ssh_for_ip "$client_ip" "$ssh_port"; then
      warn "UFW 未明确放行当前来源 IP(${client_ip}) 到 SSH 端口 ${ssh_port}。"
      if ! confirm_action "仍继续启用仅密钥登录？（可能导致失联）" "N"; then
        warn "已取消启用。"
        return 1
      fi
    fi
  else
    warn "未检测到当前 SSH 会话来源 IP（可能是本机控制台）。"
    if ! confirm_action "仍继续启用仅密钥登录？" "N"; then
      warn "已取消启用。"
      return 1
    fi
  fi

  if ! confirm_action "是否已在另一个终端验证公钥可登录？" "N"; then
    warn "未确认公钥可登录，已取消启用。"
    return 1
  fi
  return 0
}

install_or_enable_fail2ban() {
  local backend_preferred=""
  local backend_fallback=""
  local used_backend=""
  local logpath=""
  if command -v fail2ban-client >/dev/null 2>&1; then
    msg "检测到 fail2ban 已安装，执行配置同步并确保服务启用..."
  else
    msg "安装并启用 fail2ban（SSH 防爆破）..."
  fi
  if ! command -v fail2ban-client >/dev/null 2>&1; then
    if is_alpine; then
      apk add --no-cache fail2ban
    else
      export DEBIAN_FRONTEND=noninteractive
      apt-get update -y
      apt-get install -y fail2ban
    fi
  fi

  mkdir -p /etc/fail2ban/jail.d
  find_fail2ban_logpath() {
    if [[ -f /var/log/auth.log ]]; then
      echo "/var/log/auth.log"; return
    fi
    if [[ -f /var/log/secure ]]; then
      echo "/var/log/secure"; return
    fi
    if [[ -f /var/log/messages ]]; then
      echo "/var/log/messages"; return
    fi
    mkdir -p /var/log
    touch /var/log/auth.log
    echo "/var/log/auth.log"
  }

  if command -v systemctl >/dev/null 2>&1 && command -v journalctl >/dev/null 2>&1; then
    if command -v python3 >/dev/null 2>&1; then
      if ! python3 - <<'PY' >/dev/null 2>&1
import systemd.journal  # noqa: F401
PY
      then
        export DEBIAN_FRONTEND=noninteractive
        apt-get update -y >/dev/null 2>&1 || true
        apt-get install -y python3-systemd >/dev/null 2>&1 || true
      fi
      if python3 - <<'PY' >/dev/null 2>&1
import systemd.journal  # noqa: F401
PY
      then
        backend_preferred="systemd"
        backend_fallback="auto"
      else
        backend_preferred="auto"
        backend_fallback="systemd"
      fi
    else
      backend_preferred="auto"
      backend_fallback="systemd"
    fi
  else
    backend_preferred="auto"
    backend_fallback="systemd"
  fi

  write_fail2ban_jail() {
    local backend="$1"
    if [[ "$backend" == "systemd" ]]; then
      cat >"$FAIL2BAN_JAIL_FILE" <<'EOF'
[sshd]
enabled = true
mode = normal
port = ssh
filter = sshd
backend = systemd
maxretry = 5
findtime = 10m
bantime = 1h
EOF
    else
      logpath="$(find_fail2ban_logpath)"
      cat >"$FAIL2BAN_JAIL_FILE" <<EOF
[sshd]
enabled = true
mode = normal
port = ssh
filter = sshd
logpath = ${logpath}
backend = auto
maxretry = 5
findtime = 10m
bantime = 1h
EOF
    fi
  }

  used_backend="$backend_preferred"
  write_fail2ban_jail "$used_backend"

  systemctl enable --now fail2ban >/dev/null
  if ! systemctl is-active fail2ban >/dev/null 2>&1; then
    warn "fail2ban 启动失败，尝试切换 backend=${backend_fallback} 自动修复..."
    used_backend="$backend_fallback"
    write_fail2ban_jail "$used_backend"
    systemctl restart fail2ban >/dev/null 2>&1 || true
  fi
  if ! systemctl is-active fail2ban >/dev/null 2>&1; then
    warn "fail2ban 仍未运行，请查看日志：journalctl -u fail2ban -n 120 --no-pager"
    return 1
  fi
  msg "fail2ban 已启用（backend=${used_backend}）。"
}

uninstall_fail2ban() {
  if ! confirm_action "确认卸载 fail2ban？" "N"; then
    warn "已取消卸载。"
    return 0
  fi
  if is_alpine; then
    rc-service fail2ban stop >/dev/null 2>&1 || true
    rc-update del fail2ban default >/dev/null 2>&1 || true
    apk del --no-cache fail2ban >/dev/null 2>&1 || true
  else
    systemctl stop fail2ban >/dev/null 2>&1 || true
    systemctl disable fail2ban >/dev/null 2>&1 || true
    export DEBIAN_FRONTEND=noninteractive
    apt-get purge -y fail2ban >/dev/null 2>&1 || true
    apt-get autoremove -y >/dev/null 2>&1 || true
  fi
  rm -rf /etc/fail2ban
  msg "fail2ban 已卸载（如需再次启用请用菜单重新安装）。"
}

manage_fail2ban_menu() {
  echo "fail2ban 管理："
  echo "  1) 安装/启用"
  echo "  2) 卸载"
  echo "  q) 返回主菜单"
  local choice
  read -r -p "请选择 [1/2/q]（默认 1）: " choice
  choice="${choice:-1}"
  if [[ "$choice" == "q" || "$choice" == "Q" ]]; then
    return
  fi
  if [[ "$choice" == "2" ]]; then
    uninstall_fail2ban
  else
    install_or_enable_fail2ban
  fi
}

show_fail2ban_status() {
  systemctl status fail2ban --no-pager || true
  echo ""
  if command -v fail2ban-client >/dev/null 2>&1; then
    msg "fail2ban 总状态："
    fail2ban-client status || true
    echo ""
    msg "sshd jail 状态："
    fail2ban-client status sshd || true
  else
    warn "未检测到 fail2ban-client。"
  fi
}

show_ssh_security_status() {
  local ssh_service ssh_port client_ip pass_auth permit_root pubkey_auth
  local fail2ban_bans_24h
  local risk_score risk_level
  local root_has_keys=0
  local fail2ban_ok=0
  local ufw_ok=1
  local ufw_installed=1
  local ssh_active=0
  local client_ip_allowed=1
  local need_fix_ssh_service=0
  local need_root_keys=0
  local need_enable_pubkey=0
  local need_disable_password=0
  local need_fix_permit_root=0
  local need_start_fail2ban=0
  local need_install_fail2ban=0
  local need_open_ssh_port=0
  local need_allow_current_ip=0
  local need_install_ufw=0
  local need_reduce_attack_surface=0
  local need_close_legacy_ssh_port=0
  ssh_service="$(detect_ssh_service)"
  ssh_port="$(detect_sshd_port)"
  client_ip="$(detect_current_ssh_client_ip)"
  pass_auth="unknown"
  permit_root="unknown"
  pubkey_auth="unknown"
  fail2ban_bans_24h="-1"
  risk_score=0
  risk_level="低"

  echo "----- SSH 安全状态总览 -----"
  echo "sshd 服务名: ${ssh_service}"
  echo "sshd 端口: ${ssh_port}"
  echo "当前会话来源 IP: ${client_ip:-未知}"

  if systemctl is-active "$ssh_service" >/dev/null 2>&1; then
    ssh_active=1
    msg "SSH 服务状态：运行中"
  else
    risk_score=$((risk_score + 3))
    need_fix_ssh_service=1
    warn "SSH 服务状态：未运行"
  fi

  if command -v sshd >/dev/null 2>&1; then
    pass_auth="$(sshd -T 2>/dev/null | awk '/^passwordauthentication /{print $2; exit}' || true)"
    permit_root="$(sshd -T 2>/dev/null | awk '/^permitrootlogin /{print $2; exit}' || true)"
    pubkey_auth="$(sshd -T 2>/dev/null | awk '/^pubkeyauthentication /{print $2; exit}' || true)"
  fi

  echo ""
  echo "----- SSH 策略（生效值）-----"
  echo "PubkeyAuthentication: ${pubkey_auth}"
  echo "PasswordAuthentication: ${pass_auth}"
  echo "PermitRootLogin: ${permit_root}"
  if [[ "$pubkey_auth" == "yes" && "$pass_auth" == "no" ]]; then
    msg "当前策略符合“仅密钥登录”基本要求。"
  else
    if [[ "$pubkey_auth" != "yes" ]]; then
      risk_score=$((risk_score + 2))
      need_enable_pubkey=1
    fi
    if [[ "$pass_auth" != "no" ]]; then
      risk_score=$((risk_score + 2))
      need_disable_password=1
    fi
    warn "当前策略不是严格仅密钥登录（建议 PasswordAuthentication=no 且 PubkeyAuthentication=yes）。"
  fi
  if [[ "$permit_root" == "yes" ]]; then
    risk_score=$((risk_score + 1))
    need_fix_permit_root=1
  fi

  echo ""
  echo "----- authorized_keys -----"
  echo "公钥存放路径提示："
  echo "  - root: /root/.ssh/authorized_keys"
  echo "  - 普通用户: /home/<用户名>/.ssh/authorized_keys"
  if has_authorized_keys_for_user root; then
    root_has_keys=1
    msg "root 用户已检测到 authorized_keys。"
  else
    risk_score=$((risk_score + 3))
    need_root_keys=1
    warn "root 用户未检测到 authorized_keys。"
  fi

  echo ""
  echo "----- fail2ban（sshd）-----"
  if command -v fail2ban-client >/dev/null 2>&1; then
    if systemctl is-active fail2ban >/dev/null 2>&1; then
      fail2ban_ok=1
      fail2ban-client status sshd 2>/dev/null || warn "未检测到 sshd jail（可能未启用）。"
      fail2ban_bans_24h="$(get_fail2ban_ban_count_24h)"
      if [[ "$fail2ban_bans_24h" =~ ^[0-9]+$ ]]; then
        echo "近24小时封禁次数: ${fail2ban_bans_24h}"
        if (( fail2ban_bans_24h >= 30 )); then
          risk_score=$((risk_score + 1))
          need_reduce_attack_surface=1
          warn "近24小时封禁次数较高，建议收敛 SSH 暴露面（来源IP白名单/变更端口）。"
        fi
      else
        warn "未能统计近24小时封禁次数（journalctl 可能不可用）。"
      fi
    else
      risk_score=$((risk_score + 1))
      need_start_fail2ban=1
      warn "fail2ban 服务未运行。"
    fi
  else
    risk_score=$((risk_score + 1))
    need_install_fail2ban=1
    warn "系统未安装 fail2ban。"
  fi

  echo ""
  echo "----- UFW SSH 放行 -----"
  if command -v ufw >/dev/null 2>&1; then
    local ufw_state
    ufw_state="$(ufw status 2>/dev/null | head -n1 || true)"
    echo "UFW 状态: ${ufw_state:-未知}"
    if ! ufw status 2>/dev/null | grep -E "^ *${ssh_port}(/tcp)?[[:space:]]" >/dev/null; then
      ufw_ok=0
      risk_score=$((risk_score + 2))
      need_open_ssh_port=1
      warn "未发现 SSH 端口(${ssh_port})放行规则。"
    else
      ufw status 2>/dev/null | grep -E "^ *${ssh_port}(/tcp)?[[:space:]]" || true
    fi
    if [[ -n "$client_ip" ]]; then
      if ufw_allows_ssh_for_ip "$client_ip" "$ssh_port"; then
        msg "当前来源 IP(${client_ip}) 对 SSH 端口放行状态：允许。"
      else
        client_ip_allowed=0
        risk_score=$((risk_score + 2))
        need_allow_current_ip=1
        warn "当前来源 IP(${client_ip}) 对 SSH 端口放行状态：不明确允许。"
      fi
    fi
    if [[ "$ssh_port" != "22" ]] && ufw_has_allow_for_port "22"; then
      need_close_legacy_ssh_port=1
      risk_score=$((risk_score + 1))
      warn "检测到 22/tcp 仍放行，当前 SSH 端口为 ${ssh_port}，建议清理遗留 22 规则。"
    fi
  else
    ufw_installed=0
    risk_score=$((risk_score + 1))
    need_install_ufw=1
    warn "系统未安装 UFW。"
  fi

  echo ""
  echo "----- 风险评估 -----"
  if (( risk_score >= 6 )); then
    risk_level="高"
  elif (( risk_score >= 3 )); then
    risk_level="中"
  else
    risk_level="低"
  fi
  echo "风险等级: ${risk_level}（评分=${risk_score}）"
  if [[ "$risk_level" == "低" ]]; then
    msg "当前 SSH 安全基线较好，可按流程执行仅密钥切换。"
  elif [[ "$risk_level" == "中" ]]; then
    warn "存在中等风险，建议先修复后再做仅密钥切换。"
  else
    warn "存在高风险，暂不建议切换仅密钥登录。"
  fi

  if (( need_root_keys + need_enable_pubkey + need_disable_password + need_fix_permit_root + need_fix_ssh_service + need_install_ufw + need_open_ssh_port + need_allow_current_ip + need_install_fail2ban + need_start_fail2ban + need_reduce_attack_surface + need_close_legacy_ssh_port > 0 )); then
    echo ""
    echo "----- 修复建议（按顺序）-----"
    local i=1
    if (( need_root_keys == 1 )); then
      echo "${i}) 先为 root 写入公钥再启用仅密钥登录：mkdir -p /root/.ssh && chmod 700 /root/.ssh && 编辑 /root/.ssh/authorized_keys"
      i=$((i + 1))
    fi
    if (( need_enable_pubkey == 1 )); then
      echo "${i}) 启用公钥认证：菜单 17（启用仅密钥登录）或在 sshd 配置中设置 PubkeyAuthentication yes"
      i=$((i + 1))
    fi
    if (( need_disable_password == 1 )); then
      echo "${i}) 禁用密码登录：菜单 17（启用仅密钥登录）后验证 PasswordAuthentication=no"
      i=$((i + 1))
    fi
    if (( need_fix_permit_root == 1 )); then
      echo "${i}) 建议将 PermitRootLogin 调整为 prohibit-password（菜单 17 会自动处理）"
      i=$((i + 1))
    fi
    if (( need_fix_ssh_service == 1 )); then
      echo "${i}) 检查 SSH 服务并恢复：systemctl status ${ssh_service} && systemctl restart ${ssh_service}"
      i=$((i + 1))
    fi
    if (( need_install_ufw == 1 )); then
      echo "${i}) 安装并启用 UFW 后仅放行必要端口（SSH/443/TUIC）"
      i=$((i + 1))
    fi
    if (( need_open_ssh_port == 1 )); then
      echo "${i}) 放行 SSH 端口：ufw allow ${ssh_port}/tcp"
      i=$((i + 1))
    fi
    if (( need_allow_current_ip == 1 )) && [[ -n "$client_ip" ]]; then
      echo "${i}) 放行当前运维来源 IP：ufw allow from ${client_ip} to any port ${ssh_port} proto tcp"
      i=$((i + 1))
    fi
    if (( need_install_fail2ban == 1 )); then
      echo "${i}) 安装 fail2ban：菜单 11（安装/启用 fail2ban）"
      i=$((i + 1))
    fi
    if (( need_start_fail2ban == 1 )); then
      echo "${i}) 启动 fail2ban：菜单 11（安装/启用 fail2ban）"
      i=$((i + 1))
    fi
    if (( need_reduce_attack_surface == 1 )); then
      echo "${i}) 收敛 SSH 暴露面：仅放行管理来源IP，必要时变更 SSH 端口并启用自动封禁。"
      i=$((i + 1))
    fi
    if (( need_close_legacy_ssh_port == 1 )); then
      echo "${i}) 如确认已迁移到 ${ssh_port}，请清理遗留 22 规则：ufw status numbered 后删除 22/tcp 项。"
      i=$((i + 1))
    fi
  fi

  # Keep variables referenced for shellcheck clarity.
  : "${ssh_active}" "${root_has_keys}" "${fail2ban_ok}" "${ufw_ok}" "${ufw_installed}" "${client_ip_allowed}"
}

run_ssh_security_quick_fix() {
  local ssh_port client_ip ufw_state removed_ufw_rules removed_legacy_22
  ssh_port="$(detect_sshd_port)"
  client_ip="$(detect_current_ssh_client_ip)"

  msg "开始执行半自动安全修复（不包含仅密钥切换）..."
  echo "目标：修复 fail2ban 运行状态、确保 SSH 防火墙放行。"
  echo ""

  if command -v ufw >/dev/null 2>&1; then
    msg "同步 UFW SSH 规则..."
    ufw allow "${ssh_port}/tcp" >/dev/null || true
    if [[ -n "$client_ip" ]]; then
      ufw allow from "$client_ip" to any port "$ssh_port" proto tcp >/dev/null || true
      msg "已尝试放行当前来源 IP(${client_ip}) 到 SSH 端口 ${ssh_port}。"
    fi
    removed_ufw_rules="$(cleanup_ufw_duplicate_ssh_rules "$ssh_port")"
    if [[ "$removed_ufw_rules" =~ ^[0-9]+$ ]] && (( removed_ufw_rules > 0 )); then
      msg "已清理重复 SSH 防火墙规则：${removed_ufw_rules} 条。"
    fi
    if [[ "$ssh_port" != "22" ]] && ufw_has_allow_for_port "22"; then
      if confirm_action "检测到遗留 22/tcp 放行规则，是否清理？" "N"; then
        removed_legacy_22="$(remove_ufw_allow_rules_for_port "22")"
        if [[ "$removed_legacy_22" =~ ^[0-9]+$ ]] && (( removed_legacy_22 > 0 )); then
          msg "已清理遗留 22 端口放行规则：${removed_legacy_22} 条。"
        else
          warn "未清理到 22 端口规则（可能不存在或删除失败）。"
        fi
      fi
    fi
    ufw_state="$(ufw status 2>/dev/null | head -n1 || true)"
    if [[ "$ufw_state" == *"inactive"* ]]; then
      if confirm_action "检测到 UFW 未启用，是否立即启用？" "Y"; then
        ufw --force enable >/dev/null
        msg "UFW 已启用。"
      else
        warn "你选择不启用 UFW。"
      fi
    else
      msg "UFW 已启用，SSH 规则已同步。"
    fi
  else
    warn "系统未安装 UFW，跳过防火墙修复。"
  fi

  if command -v fail2ban-client >/dev/null 2>&1 && systemctl is-active fail2ban >/dev/null 2>&1; then
    msg "fail2ban 已运行。"
  else
    if confirm_action "fail2ban 未就绪，是否安装/启用？" "Y"; then
      install_or_enable_fail2ban
    else
      warn "你选择跳过 fail2ban 安装/启用。"
    fi
  fi

  echo ""
  msg "半自动修复执行完成，建议立即查看菜单 15（SSH 安全状态总览）确认结果。"
}

unban_fail2ban_ip() {
  if ! command -v fail2ban-client >/dev/null 2>&1; then
    err "未检测到 fail2ban-client，请先安装 fail2ban。"
    return
  fi
  local ip
  read -r -p "请输入要解封的 IP: " ip
  ip="$(echo "$ip" | tr -d '[:space:]')"
  if [[ -z "$ip" ]]; then
    warn "IP 不能为空。"
    return
  fi
  fail2ban-client set sshd unbanip "$ip"
  msg "已尝试从 sshd jail 解封: $ip"
}

get_fail2ban_ban_count_24h() {
  if ! command -v journalctl >/dev/null 2>&1; then
    echo "-1"
    return
  fi
  journalctl -u fail2ban --since "24 hours ago" --no-pager 2>/dev/null | awk '
    / Ban / {count++}
    END {print count + 0}
  '
}

cleanup_ufw_duplicate_ssh_rules() {
  local ssh_port removed_count
  ssh_port="${1:-22}"
  removed_count=0

  if ! command -v ufw >/dev/null 2>&1; then
    echo "0"
    return
  fi

  local line num rule normalized
  local -a delete_nums=()
  local -A seen_rules=()

  while IFS= read -r line; do
    num="$(echo "$line" | sed -n 's/^\[ *\([0-9][0-9]*\)\].*/\1/p')"
    rule="$(echo "$line" | sed -n 's/^\[ *[0-9][0-9]*\] *//p')"
    if [[ -z "$num" || -z "$rule" ]]; then
      continue
    fi
    if ! echo "$rule" | grep -E "^${ssh_port}(/tcp)?[[:space:]]+ALLOW" >/dev/null 2>&1; then
      continue
    fi
    normalized="$(echo "$rule" | tr -s ' ' ' ' | sed 's/^ //; s/ $//')"
    if [[ -n "${seen_rules[$normalized]+x}" ]]; then
      delete_nums+=("$num")
    else
      seen_rules["$normalized"]=1
    fi
  done < <(ufw status numbered 2>/dev/null || true)

  if (( ${#delete_nums[@]} > 0 )); then
    local sorted_num
    while IFS= read -r sorted_num; do
      if [[ -n "$sorted_num" ]]; then
        ufw --force delete "$sorted_num" >/dev/null 2>&1 || true
        removed_count=$((removed_count + 1))
      fi
    done < <(printf '%s\n' "${delete_nums[@]}" | sort -rn)
  fi

  echo "$removed_count"
}

generate_ssh_keypair() {
  local user_name user_home key_path passphrase comment overwrite auth_file
  read -r -p "请输入要生成密钥的用户名 [root]: " user_name
  user_name="${user_name:-root}"
  user_home="$(getent passwd "$user_name" | awk -F: '{print $6}' || true)"
  if [[ -z "$user_home" ]]; then
    err "用户不存在: $user_name"
    return
  fi
  key_path="${user_home}/.ssh/id_ed25519"
  read -r -p "请输入私钥保存路径 [${key_path}]: " key_path
  key_path="${key_path:-${user_home}/.ssh/id_ed25519}"

  if [[ -f "$key_path" ]]; then
    read -r -p "密钥已存在，是否覆盖？[y/N]: " overwrite
    overwrite="${overwrite:-N}"
    if [[ ! "$overwrite" =~ ^[Yy]$ ]]; then
      warn "已取消生成密钥。"
      return
    fi
  fi

  read -r -p "请输入密钥口令（留空=无口令）: " passphrase
  comment="${user_name}@$(hostname)-sb-agent"
  mkdir -p "$(dirname "$key_path")"
  chmod 700 "$(dirname "$key_path")"
  ssh-keygen -t ed25519 -a 100 -f "$key_path" -N "$passphrase" -C "$comment"
  chown -R "$user_name":"$user_name" "$(dirname "$key_path")"
  chmod 600 "$key_path"
  chmod 644 "${key_path}.pub"

  auth_file="$(get_authorized_keys_path_for_user "$user_name")"
  msg "公钥如下（请追加到服务器文件：${auth_file}）："
  cat "${key_path}.pub"
  echo ""
  echo "推荐在目标节点服务器执行："
  echo "  mkdir -p $(dirname "$auth_file") && chmod 700 $(dirname "$auth_file")"
  echo "  # 将上方公钥追加到 ${auth_file}"
  echo "  chmod 600 ${auth_file}"
}

enable_ssh_key_only_login() {
  local user_name ssh_service auth_file
  read -r -p "请输入用于校验 authorized_keys 的用户名 [root]: " user_name
  user_name="${user_name:-root}"
  auth_file="$(get_authorized_keys_path_for_user "$user_name")"

  if ! has_authorized_keys_for_user "$user_name"; then
    warn "用户 ${user_name} 没有可用 authorized_keys（${auth_file}），拒绝启用（避免锁死 SSH）。"
    return
  fi
  if ! precheck_ssh_lockout_risk; then
    return
  fi

  mkdir -p /etc/ssh/sshd_config.d
  cat >"$SSH_HARDEN_FILE" <<'EOF'
# Managed by sb-agent menu
PubkeyAuthentication yes
PasswordAuthentication no
KbdInteractiveAuthentication no
ChallengeResponseAuthentication no
UsePAM yes
PermitRootLogin prohibit-password
EOF

  if command -v sshd >/dev/null 2>&1 && ! sshd -t; then
    rm -f "$SSH_HARDEN_FILE"
    err "sshd 配置校验失败，已回滚。"
    return
  fi

  ssh_service="$(detect_ssh_service)"
  systemctl restart "$ssh_service" >/dev/null 2>&1 || true
  msg "SSH 已切换为仅密钥登录（密码登录已禁用）。"
}

disable_ssh_key_only_login() {
  local ssh_service
  read -r -p "确认恢复 SSH 密码登录（应急用途）？[y/N]: " answer
  answer="${answer:-N}"
  if [[ ! "$answer" =~ ^[Yy]$ ]]; then
    warn "已取消恢复密码登录。"
    return
  fi

  rm -f "$SSH_HARDEN_FILE"
  if command -v sshd >/dev/null 2>&1 && ! sshd -t; then
    err "sshd 配置校验失败，请手动检查 /etc/ssh/sshd_config*"
    return
  fi
  ssh_service="$(detect_ssh_service)"
  systemctl restart "$ssh_service" >/dev/null 2>&1 || true
  msg "已移除仅密钥策略，SSH 密码登录恢复。"
}

run_install() {
  if [[ ! -f "$INSTALL_SCRIPT" ]]; then
    err "未找到 install.sh: $INSTALL_SCRIPT"
    return
  fi
  if ! update_repo_from_origin_main "$ROOT_DIR"; then
    err "更新已中止：代码拉取失败。"
    return 1
  fi

  if [[ -f "$CONFIG_PATH" ]]; then
    msg "检测到现有配置，执行无交互更新（复用原参数）..."
    bash "$INSTALL_SCRIPT" --sync-only
  else
    msg "未检测到现有配置，执行首次安装流程..."
    bash "$INSTALL_SCRIPT"
  fi
  print_update_success_summary "$ROOT_DIR"
}

run_reconfigure() {
  if [[ ! -f "$INSTALL_SCRIPT" ]]; then
    err "未找到 install.sh: $INSTALL_SCRIPT"
    return
  fi
  msg "配置模式选择："
  echo "  1) 快速配置（推荐默认值，最少提问）"
  echo "  2) 高级变量设置向导（逐项说明，全部可调）"
  echo "  3) 参数单项修改（点选一项直接改）"
  echo "  q) 返回主菜单"
  local cfg_mode
  read -r -p "请选择 [1/2/3/q]（默认 1）: " cfg_mode
  cfg_mode="${cfg_mode:-1}"
  if [[ "$cfg_mode" == "q" || "$cfg_mode" == "Q" ]]; then
    return
  fi
  if [[ "$cfg_mode" == "3" ]]; then
    edit_single_node_param
  elif [[ "$cfg_mode" == "2" ]]; then
    bash "$INSTALL_SCRIPT" --configure-only
  else
    bash "$INSTALL_SCRIPT" --configure-quick
  fi
}

install_or_update_singbox() {
  if is_alpine; then
    msg "检测到 Alpine，使用二进制包方式安装 sing-box..."
    if ! command -v curl >/dev/null 2>&1; then
      apk add --no-cache curl
    fi
    apk add --no-cache tar gzip >/dev/null 2>&1 || true
    local arch dl_url tmp_dir
    arch="$(uname -m)"
    case "$arch" in
      x86_64) arch="amd64" ;;
      aarch64|arm64) arch="arm64" ;;
      *) err "当前架构(${arch})暂未适配 Alpine sing-box 自动安装"; return 1 ;;
    esac
    dl_url="https://github.com/SagerNet/sing-box/releases/latest/download/sing-box-linux-${arch}.tar.gz"
    tmp_dir="$(mktemp -d)"
    if ! curl -fsSL "$dl_url" -o "${tmp_dir}/sing-box.tgz"; then
      err "sing-box 下载失败：${dl_url}"
      rm -rf "$tmp_dir"
      return 1
    fi
    tar -xzf "${tmp_dir}/sing-box.tgz" -C "$tmp_dir"
    if [[ -f "${tmp_dir}/sing-box" ]]; then
      install -m 0755 "${tmp_dir}/sing-box" /usr/local/bin/sing-box
    else
      local bin_path
      bin_path="$(find "$tmp_dir" -maxdepth 2 -type f -name 'sing-box' | head -n1 || true)"
      if [[ -z "$bin_path" ]]; then
        err "sing-box 解压后未找到二进制文件。"
        rm -rf "$tmp_dir"
        return 1
      fi
      install -m 0755 "$bin_path" /usr/local/bin/sing-box
    fi
    rm -rf "$tmp_dir"
  else
    msg "开始安装/更新 sing-box（官方脚本）..."
    export DEBIAN_FRONTEND=noninteractive
    if ! command -v curl >/dev/null 2>&1; then
      apt-get update -y
      apt-get install -y curl
    fi
    if ! curl -fsSL https://sing-box.app/install.sh | bash; then
      local deb_path
      warn "官方安装脚本返回失败，尝试非交互重试（保留本地 config.json）..."
      deb_path="$(find "$PWD" /tmp -maxdepth 2 -type f -name 'sing-box_*_linux_*.deb' -printf '%T@ %p\n' 2>/dev/null | sort -nr | head -n1 | cut -d' ' -f2-)"
      if [[ -z "$deb_path" || ! -f "$deb_path" ]]; then
        err "sing-box 安装脚本执行失败，且未找到可重试的 deb 包。"
        return 1
      fi
      if ! dpkg -i --force-confdef --force-confold "$deb_path"; then
        apt-get install -f -y || true
        if ! dpkg -i --force-confdef --force-confold "$deb_path"; then
          err "sing-box 非交互重试失败。"
          return 1
        fi
      fi
    fi
  fi
  if ! command -v sing-box >/dev/null 2>&1; then
    err "sing-box 安装后仍未检测到可执行文件。"
    return 1
  fi
  msg "sing-box 已安装：$(command -v sing-box)"

  if [[ ! -f "$CONFIG_PATH" ]]; then
    warn "未检测到节点配置 ${CONFIG_PATH}，请先执行菜单 1 完成配置。"
    return 0
  fi

  if ! systemd_unit_exists "sing-box.service"; then
    if [[ -f "$INSTALL_SCRIPT" ]]; then
      msg "未检测到 sing-box.service，尝试使用 install.sh --sync-only 自动补齐服务..."
      if ! bash "$INSTALL_SCRIPT" --sync-only; then
        warn "通过 --sync-only 补齐服务失败，请执行菜单 1 重新配置。"
        return 1
      fi
    else
      warn "未找到 install.sh，无法自动补齐 sing-box.service。"
      return 1
    fi
  fi

  if systemd_unit_exists "sing-box.service"; then
    systemctl daemon-reload
    systemctl enable --now "$SINGBOX_SERVICE" >/dev/null 2>&1 || true
    msg "sing-box 服务已启用并尝试启动。"
  fi
  return 0
}

uninstall_singbox() {
  if ! confirm_action "确认卸载 sing-box？" "N"; then
    warn "已取消卸载。"
    return 0
  fi
  if [[ "$INIT_SYSTEM" == "openrc" ]]; then
    rc-service sing-box stop >/dev/null 2>&1 || true
    rc-update del sing-box default >/dev/null 2>&1 || true
    apk del --no-cache sing-box >/dev/null 2>&1 || true
    rm -f /etc/init.d/sing-box
  else
    systemctl stop "$SINGBOX_SERVICE" >/dev/null 2>&1 || true
    systemctl disable "$SINGBOX_SERVICE" >/dev/null 2>&1 || true
    export DEBIAN_FRONTEND=noninteractive
    apt-get purge -y sing-box >/dev/null 2>&1 || true
    apt-get autoremove -y >/dev/null 2>&1 || true
    rm -f /etc/systemd/system/sing-box.service
  fi
  rm -rf /etc/sing-box /var/lib/sing-box /var/log/sing-box
  rm -f /usr/local/bin/sing-box /usr/bin/sing-box
  systemctl daemon-reload >/dev/null 2>&1 || true
  msg "sing-box 已卸载（如需再用请重新安装）。"
}

manage_singbox_menu() {
  echo "sing-box 管理："
  echo "  1) 安装/更新"
  echo "  2) 卸载"
  echo "  q) 返回主菜单"
  local choice
  read -r -p "请选择 [1/2/q]（默认 1）: " choice
  choice="${choice:-1}"
  if [[ "$choice" == "q" || "$choice" == "Q" ]]; then
    return
  fi
  if [[ "$choice" == "2" ]]; then
    uninstall_singbox
  else
    install_or_update_singbox
  fi
}

ensure_singbox_installed_interactive() {
  if command -v sing-box >/dev/null 2>&1 && systemd_unit_exists "sing-box.service"; then
    return 0
  fi
  warn "检测到 sing-box 组件不完整（可执行文件或服务缺失）。"
  if confirm_action "是否现在安装/更新 sing-box 并自动补齐服务？" "Y"; then
    install_or_update_singbox
    return $?
  fi
  warn "已取消 sing-box 安装/更新。"
  return 1
}

show_agent_status() {
  local state enabled
  if systemctl is-active "$AGENT_SERVICE" >/dev/null 2>&1; then
    state="active"
  else
    state="inactive"
  fi
  if systemctl is-enabled "$AGENT_SERVICE" >/dev/null 2>&1; then
    enabled="是"
  else
    enabled="否"
  fi
  echo "sb-agent 状态: ${state}"
  echo "开机自启: ${enabled}"
  echo "提示：查看详细日志请执行："
  echo "  journalctl -u sb-agent -n 120 --no-pager"
}

show_singbox_status_logs() {
  if ! ensure_singbox_installed_interactive; then
    return 1
  fi
  local state enabled
  if systemctl is-active "$SINGBOX_SERVICE" >/dev/null 2>&1; then
    state="active"
  else
    state="inactive"
  fi
  if systemctl is-enabled "$SINGBOX_SERVICE" >/dev/null 2>&1; then
    enabled="是"
  else
    enabled="否"
  fi
  echo "sing-box 状态: ${state}"
  echo "开机自启: ${enabled}"
  if [[ "$state" != "active" ]]; then
    echo "提示：可执行菜单 24（自检修复）自动修复常见问题。"
    if command -v journalctl >/dev/null 2>&1; then
      if journalctl -u "$SINGBOX_SERVICE" -n 40 --no-pager 2>/dev/null | grep -qi "permission denied" \
        && journalctl -u "$SINGBOX_SERVICE" -n 40 --no-pager 2>/dev/null | grep -qi "sing-box.log"; then
        echo "提示：检测到日志权限问题（sing-box.log），菜单 24 可自动修复。"
      fi
    fi
  fi
  echo "提示：查看详细日志请执行："
  echo "  journalctl -u ${SINGBOX_SERVICE} -n 120 --no-pager"
}

tail_agent_log() {
  if [[ -f /var/log/sb-agent/agent.log ]]; then
    tail -f /var/log/sb-agent/agent.log
  else
    warn "未找到 /var/log/sb-agent/agent.log，改用 journalctl 跟随输出。"
    journalctl -u "$AGENT_SERVICE" -f
  fi
}

run_cert_check() {
  if ! ensure_singbox_installed_interactive; then
    return 1
  fi
  if [[ -x "$SYSTEM_CERT_CHECK_SCRIPT" ]]; then
    "$SYSTEM_CERT_CHECK_SCRIPT"
    return
  fi
  if [[ -x "$LOCAL_CERT_CHECK_SCRIPT" ]]; then
    "$LOCAL_CERT_CHECK_SCRIPT"
    return
  fi
  err "未找到证书检查脚本。"
}

refresh_certificate() {
  if ! ensure_singbox_installed_interactive; then
    return 1
  fi
  msg "开始执行证书重新申请/刷新流程（安全模式：先备份）..."
  mkdir -p "$BACKUP_DIR"
  local ts backup_tar
  ts="$(date +%Y%m%d-%H%M%S)"
  backup_tar="$BACKUP_DIR/certmagic-${ts}.tar.gz"

  if [[ -d "$CERTMAGIC_DIR" ]]; then
    tar -czf "$backup_tar" -C "$(dirname "$CERTMAGIC_DIR")" "$(basename "$CERTMAGIC_DIR")"
    msg "已备份 certmagic: $backup_tar"
  else
    warn "未检测到 certmagic 目录，跳过备份。"
  fi

  if [[ -f /etc/sing-box/config.json ]]; then
    cp /etc/sing-box/config.json "$BACKUP_DIR/config-${ts}.json.bak"
    msg "已备份配置: $BACKUP_DIR/config-${ts}.json.bak"
  fi

  read -r -p "确认清理 ACME 缓存并重启 sing-box？[Y/n]: " answer
  answer="${answer:-Y}"
  if [[ ! "$answer" =~ ^[Yy]$ ]]; then
    warn "已取消证书刷新。"
    return
  fi

  if [[ -d "$CERTMAGIC_DIR/acme" ]]; then
    rm -rf "$CERTMAGIC_DIR/acme"
    msg "已清理: $CERTMAGIC_DIR/acme"
  else
    warn "未发现 $CERTMAGIC_DIR/acme，无需清理。"
  fi

  systemctl restart "$SINGBOX_SERVICE" || true
  msg "已触发 sing-box 重启。可使用菜单 9/8 查看证书与日志状态。"
}

uninstall_all() {
  local answer remove_shared_pkgs s_ui_path
  warn "将执行深度卸载（节点服务器）：sb-agent/sing-box/证书与日志目录/快捷命令/安全加固文件。"
  warn "该操作不可逆，建议先确认备份已转移到外部位置。"
  read -r -p "确认继续？[y/N]: " answer
  answer="${answer:-N}"
  if [[ ! "$answer" =~ ^[Yy]$ ]]; then
    warn "已取消卸载。"
    return
  fi

  systemctl stop "$AGENT_SERVICE" 2>/dev/null || true
  systemctl disable "$AGENT_SERVICE" 2>/dev/null || true
  systemctl stop "$CERT_SERVICE" 2>/dev/null || true
  systemctl disable "$CERT_SERVICE" 2>/dev/null || true
  systemctl stop "$CERT_TIMER" 2>/dev/null || true
  systemctl disable "$CERT_TIMER" 2>/dev/null || true
  systemctl stop "$SINGBOX_SERVICE" 2>/dev/null || true
  systemctl disable "$SINGBOX_SERVICE" 2>/dev/null || true
  systemctl stop fail2ban 2>/dev/null || true
  systemctl disable fail2ban 2>/dev/null || true

  if [[ "$INIT_SYSTEM" == "openrc" ]]; then
    rm -f /etc/init.d/sb-agent
    rm -f /etc/init.d/sing-box
    rm -f /etc/periodic/daily/sb-cert-check
  fi
  rm -f /etc/systemd/system/sb-agent.service
  rm -f /etc/systemd/system/sb-cert-check.service
  rm -f /etc/systemd/system/sb-cert-check.timer
  rm -f /etc/systemd/system/sing-box.service
  rm -f "$SYSTEM_CERT_CHECK_SCRIPT"

  rm -rf /opt/sb-agent
  rm -rf /etc/sb-agent
  rm -rf /var/log/sb-agent
  rm -rf /etc/sing-box
  rm -rf /var/log/sing-box
  rm -rf /var/lib/sing-box
  rm -rf "$BACKUP_DIR"
  rm -f "$SSH_HARDEN_FILE"
  rm -f "$FAIL2BAN_JAIL_FILE"
  rm -f /usr/local/bin/sing-box
  rm -f /usr/bin/sing-box

  rm -f /usr/local/bin/sb-node
  if [[ -f /usr/local/bin/sb-bot-panel ]] && grep -q "sb-node-main-shortcut" /usr/local/bin/sb-bot-panel 2>/dev/null; then
    rm -f /usr/local/bin/sb-bot-panel
  fi
  s_ui_path="$(command -v s-ui || true)"
  if [[ "$s_ui_path" == "/usr/local/bin/s-ui" ]] && grep -q "sb-node-menu-shortcut" /usr/local/bin/s-ui 2>/dev/null; then
    rm -f /usr/local/bin/s-ui
  fi

  systemctl daemon-reload
  msg "节点组件与相关文件已移除。"

  if confirm_action "是否一并卸载脚本安装的系统组件包（fail2ban/ufw/qrencode）？" "Y"; then
    export DEBIAN_FRONTEND=noninteractive
    apt-get purge -y fail2ban ufw qrencode >/dev/null 2>&1 || true
    apt-get autoremove -y >/dev/null 2>&1 || true
    msg "系统组件包卸载命令已执行。"
  fi

  remove_shared_pkgs="N"
  read -r -p "是否额外卸载通用依赖包（python3-venv/git/curl/jq/socat）？[y/N]: " remove_shared_pkgs
  remove_shared_pkgs="${remove_shared_pkgs:-N}"
  if [[ "$remove_shared_pkgs" =~ ^[Yy]$ ]]; then
    export DEBIAN_FRONTEND=noninteractive
    apt-get purge -y python3-venv git curl jq socat >/dev/null 2>&1 || true
    apt-get autoremove -y >/dev/null 2>&1 || true
    msg "通用依赖包卸载命令已执行。"
  fi

  msg "节点深度卸载完成。"
}

show_menu() {
  clear
  if [[ "$MENU_VIEW_MODE" == "advanced" ]]; then
    echo "========================================"
    echo " sb-agent 中文管理菜单"
    echo "========================================"
    echo "【运行与配置】"
    echo " 1) 配置（快速默认 / 高级变量向导）"
    echo " 2) 启动 sb-agent"
    echo " 3) 停止 sb-agent"
    echo " 4) 重启 sb-agent"
    echo " 5) 查看 sb-agent 状态"
    echo " 6) 查看 sb-agent 日志（tail -f）"
    echo " 7) 重启 sing-box"
    echo " 8) 查看 sing-box 状态（摘要）"
    echo " 9) 证书状态检查（强判定）"
    echo "10) 触发证书重新申请/刷新（先备份）"
    echo ""
    echo "【安全工具】"
    echo "11) fail2ban 管理（安装/卸载）"
    echo "12) 查看 fail2ban 状态与封禁列表"
    echo "13) 解封 fail2ban 封禁 IP"
    echo "14) 生成 SSH 密钥（ed25519）"
    echo "15) SSH 安全状态总览（只读）"
    echo "16) 一键安全修复（半自动）"
    echo "17) 启用 SSH 仅密钥登录（禁用密码）"
    echo "18) 恢复 SSH 密码登录（应急）"
    echo ""
    echo "【系统级操作（谨慎）】"
    echo "19) 节点运维快照（导出关键状态）"
    echo "20) AI诊断包导出（可粘贴给任意AI）"
    echo "21) 更新同步（保留原配置，自动 git pull）"
    echo "22) 深度卸载"
    echo "23) sing-box 管理（安装/更新/卸载）"
    echo "24) 部署参数自检与修复向导（循环到通过）"
    echo "25) 退出"
    echo "========================================"
    echo "视图切换：输入 B 返回简化视图（仅常用项）"
    return
  fi

  echo "========================================"
  echo " sb-agent 中文管理菜单（简化视图）"
  echo "========================================"
  echo "【常用项（核心流程）】"
  echo " 1) 配置（快速默认 / 高级变量向导）"
  echo " 5) 查看 sb-agent 状态"
  echo " 8) 查看 sing-box 状态（摘要）"
  echo " 9) 证书状态检查（强判定）"
  echo "24) 部署参数自检与修复向导（循环到通过）"
  echo "21) 更新同步（保留原配置，自动 git pull）"
  echo "25) 退出"
  echo "========================================"
  echo "提示：日志/卸载/安全工具/组件管理已下沉到高级视图。"
  echo "视图切换：输入 A 查看全部功能（高级视图）"
}

main() {
  require_root
  while true; do
    show_menu
    local choice_prompt
    if [[ "$MENU_VIEW_MODE" == "advanced" ]]; then
      choice_prompt="请选择操作 [1-25/b]: "
    else
      choice_prompt="请选择操作 [常用编号/a]: "
    fi
    read -r -p "$choice_prompt" choice
    case "$choice" in
      [Aa])
        MENU_VIEW_MODE="advanced"
        msg "已切换到高级视图。"
        pause
        ;;
      [Bb])
        MENU_VIEW_MODE="basic"
        msg "已切换到简化视图。"
        pause
        ;;
      1)
        run_reconfigure
        pause
        ;;
      2)
        systemctl start "$AGENT_SERVICE" || true
        msg "已执行启动。"
        pause
        ;;
      3)
        systemctl stop "$AGENT_SERVICE" || true
        msg "已执行停止。"
        pause
        ;;
      4)
        systemctl restart "$AGENT_SERVICE" || true
        msg "已执行重启。"
        pause
        ;;
      5)
        show_agent_status
        pause
        ;;
      6)
        tail_agent_log
        ;;
      7)
        if ! ensure_singbox_installed_interactive; then
          pause
          continue
        fi
        systemctl restart "$SINGBOX_SERVICE" || true
        msg "已执行 sing-box 重启。"
        pause
        ;;
      8)
        show_singbox_status_logs
        pause
        ;;
      9)
        run_cert_check
        pause
        ;;
      10)
        refresh_certificate
        pause
        ;;
      11)
        manage_fail2ban_menu
        pause
        ;;
      12)
        show_fail2ban_status
        pause
        ;;
      13)
        unban_fail2ban_ip
        pause
        ;;
      14)
        generate_ssh_keypair
        pause
        ;;
      15)
        show_ssh_security_status
        pause
        ;;
      16)
        run_ssh_security_quick_fix
        pause
        ;;
      17)
        enable_ssh_key_only_login
        pause
        ;;
      18)
        disable_ssh_key_only_login
        pause
        ;;
      19)
        if [[ -f "$OPS_SNAPSHOT_SCRIPT" ]]; then
          bash "$OPS_SNAPSHOT_SCRIPT"
        else
          err "未找到节点运维快照脚本: $OPS_SNAPSHOT_SCRIPT"
        fi
        pause
        ;;
      20)
        if [[ -f "$AI_CONTEXT_SCRIPT" ]]; then
          bash "$AI_CONTEXT_SCRIPT"
        else
          err "未找到 AI 诊断包脚本: $AI_CONTEXT_SCRIPT"
        fi
        pause
        ;;
      21)
        if confirm_action "确认执行更新同步？" "N"; then
          if ! run_install; then
            warn "更新同步失败，请先处理上述报错后重试。"
          fi
        else
          warn "已取消更新同步。"
        fi
        pause
        ;;
      22)
        uninstall_all
        pause
        ;;
      23)
        manage_singbox_menu
        pause
        ;;
      24)
        if [[ -x "$RUNTIME_SELF_CHECK_SCRIPT" ]]; then
          bash "$RUNTIME_SELF_CHECK_SCRIPT"
        else
          err "未找到部署参数自检脚本: $RUNTIME_SELF_CHECK_SCRIPT"
        fi
        pause
        ;;
      25)
        msg "已退出。"
        exit 0
        ;;
      *)
        if [[ "$MENU_VIEW_MODE" == "advanced" ]]; then
          warn "无效选项，请输入 1-25 或 b。"
        else
          warn "无效选项，请输入简化视图中的编号，或输入 a 切换高级视图。"
        fi
        pause
        ;;
    esac
  done
}

main "$@"
