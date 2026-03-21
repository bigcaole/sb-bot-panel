#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INSTALL_SCRIPT="${ROOT_DIR}/scripts/install.sh"
CONFIG_PATH="/etc/sb-agent/config.json"
SINGBOX_CONFIG="/etc/sing-box/config.json"
SINGBOX_LOG_PATH="/var/lib/sing-box/sing-box.log"
SINGBOX_LOG_DIR="$(dirname "$SINGBOX_LOG_PATH")"
AGENT_LOG_DIR="/var/log/sb-agent"
OS_ID=""
OS_VERSION=""
INIT_SYSTEM="systemd"

declare -a ISSUE_ORDER=()
declare -A ISSUE_SEVERITY=()
declare -A ISSUE_MSG=()
declare -A ISSUE_FIX=()

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
/usr/local/bin/sb-cert-check.sh >> ${AGENT_LOG_DIR}/cert-check.log 2>&1
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

require_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    err "请用 root 执行。"
    exit 1
  fi
}

add_issue() {
  local id="$1"
  local severity="$2"
  local text="$3"
  local fix="$4"
  ISSUE_ORDER+=("$id")
  ISSUE_SEVERITY["$id"]="$severity"
  ISSUE_MSG["$id"]="$text"
  ISSUE_FIX["$id"]="$fix"
}

clear_issues() {
  ISSUE_ORDER=()
  ISSUE_SEVERITY=()
  ISSUE_MSG=()
  ISSUE_FIX=()
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

get_public_ipv4() {
  curl -4 -fsSL ifconfig.me 2>/dev/null \
    || curl -4 -fsSL https://api.ipify.org 2>/dev/null \
    || true
}

update_config_value() {
  local tmp
  tmp="$(mktemp)"
  jq "$@" "$CONFIG_PATH" >"$tmp"
  mv "$tmp" "$CONFIG_PATH"
}

refresh_singbox_log_path_from_config() {
  if ! command -v jq >/dev/null 2>&1; then
    return
  fi
  if [[ -f "$SINGBOX_CONFIG" ]]; then
    local log_output
    log_output="$(jq -r '.log.output // empty' "$SINGBOX_CONFIG" 2>/dev/null || true)"
    if [[ -n "$log_output" && "$log_output" != "null" ]]; then
      SINGBOX_LOG_PATH="$log_output"
      SINGBOX_LOG_DIR="$(dirname "$SINGBOX_LOG_PATH")"
    fi
  fi
}

repair_singbox_log_permissions() {
  local run_user run_group
  local log_file="$SINGBOX_LOG_PATH"
  local dynamic_user="false"
  if command -v systemctl >/dev/null 2>&1; then
    dynamic_user="$(systemctl show -p DynamicUser --value sing-box.service 2>/dev/null | xargs || true)"
  fi
  if [[ "${dynamic_user,,}" == "yes" ]]; then
    dynamic_user="true"
  else
    dynamic_user="false"
  fi
  run_user="$(get_singbox_run_user)"
  run_group="$(get_singbox_run_group "$run_user")"
  mkdir -p "$SINGBOX_LOG_DIR"
  touch "$log_file"
  if [[ "$dynamic_user" == "true" ]]; then
    chmod 0777 "$SINGBOX_LOG_DIR" || true
    chmod 0666 "$log_file" || true
    return 0
  fi
  if chown "$run_user:$run_group" "$SINGBOX_LOG_DIR" "$log_file" >/dev/null 2>&1; then
    chmod 755 "$SINGBOX_LOG_DIR" || true
    chmod 644 "$log_file" || true
    return 0
  fi
  chmod 0777 "$SINGBOX_LOG_DIR" || true
  chmod 0666 "$log_file" || true
}

get_singbox_run_user() {
  local run_user
  run_user="$(systemctl show -p User --value sing-box.service 2>/dev/null | xargs || true)"
  if [[ -z "$run_user" ]]; then
    run_user="$(systemctl cat sing-box.service 2>/dev/null | awk -F= '/^User=/{print $2; exit}' | xargs || true)"
  fi
  [[ -z "$run_user" ]] && run_user="sing-box"
  id -u "$run_user" >/dev/null 2>&1 || run_user="root"
  echo "$run_user"
}

get_singbox_run_group() {
  local run_user="$1"
  id -gn "$run_user" 2>/dev/null || echo "$run_user"
}

detect_singbox_log_permission_issue() {
  local log_file="$SINGBOX_LOG_PATH"
  local dynamic_user="false"
  if command -v systemctl >/dev/null 2>&1; then
    dynamic_user="$(systemctl show -p DynamicUser --value sing-box.service 2>/dev/null | xargs || true)"
  fi
  if [[ "${dynamic_user,,}" == "yes" ]]; then
    dynamic_user="true"
  else
    dynamic_user="false"
  fi
  if command -v journalctl >/dev/null 2>&1; then
    if journalctl -u sing-box -n 50 --no-pager 2>/dev/null | grep -qi "permission denied" \
      && journalctl -u sing-box -n 50 --no-pager 2>/dev/null | grep -qi "sing-box.log"; then
      return 0
    fi
  fi
  if [[ -f "$log_file" ]]; then
    local owner
    owner="$(stat -c %U "$log_file" 2>/dev/null || ls -l "$log_file" 2>/dev/null | awk '{print $3}')"
    if [[ -n "$owner" ]]; then
      local run_user
      run_user="$(get_singbox_run_user)"
      if [[ "$run_user" != "root" && "$owner" != "$run_user" ]]; then
        return 0
      fi
    fi
  fi
  if [[ "$dynamic_user" == "true" && -f "$log_file" ]]; then
    if [[ ! -w "$log_file" ]]; then
      return 0
    fi
  fi
  return 1
}

ensure_singbox_systemd_rw_override() {
  if [[ "$INIT_SYSTEM" != "systemd" ]]; then
    return
  fi
  local dir="/etc/systemd/system/sing-box.service.d"
  mkdir -p "$dir"
  cat >"${dir}/sb-bot-panel.conf" <<'EOF'
[Service]
ReadWritePaths=/var/lib/sing-box /var/log/sing-box
StateDirectory=sing-box
LogsDirectory=sing-box
EOF
  systemctl daemon-reload >/dev/null 2>&1 || true
}

detect_singbox_config_error() {
  if ! command -v sing-box >/dev/null 2>&1; then
    return 1
  fi
  if [[ ! -f "$SINGBOX_CONFIG" ]]; then
    return 1
  fi
  if sing-box check -c "$SINGBOX_CONFIG" >/dev/null 2>&1; then
    return 1
  fi
  return 0
}

get_port_conflict_summary() {
  local tuic_port="$1"
  local summary=""
  if ! command -v ss >/dev/null 2>&1; then
    return 0
  fi
  local line proc
  line="$(ss -lntp 2>/dev/null | awk '$4 ~ /:443$/ {print $0}' | head -n1)"
  if [[ -n "$line" && "$line" != *"sing-box"* ]]; then
    proc="$(echo "$line" | sed -n 's/.*users:(\(.*\))$/\1/p')"
    summary="tcp/443 -> ${proc:-$line}"
  fi
  if [[ -n "$tuic_port" && "$tuic_port" =~ ^[0-9]+$ ]]; then
    line="$(ss -lnup 2>/dev/null | awk -v p=":${tuic_port}$" '$4 ~ p {print $0}' | head -n1)"
    if [[ -n "$line" && "$line" != *"sing-box"* ]]; then
      proc="$(echo "$line" | sed -n 's/.*users:(\(.*\))$/\1/p')"
      if [[ -n "$summary" ]]; then
        summary="${summary}; udp/${tuic_port} -> ${proc:-$line}"
      else
        summary="udp/${tuic_port} -> ${proc:-$line}"
      fi
    fi
  fi
  echo "$summary"
}

run_checks() {
  clear_issues

  if [[ ! -f "$CONFIG_PATH" ]]; then
    add_issue "config_missing" "required_missing" "缺少 /etc/sb-agent/config.json" "运行安装配置向导（菜单 1）"
    return
  fi
  if ! command -v jq >/dev/null 2>&1; then
    add_issue "jq_missing" "config_error" "系统缺少 jq，无法解析配置" "安装 jq 后重试"
    return
  fi

  local controller_url node_code auth_token tuic_domain acme_email tuic_port
  controller_url="$(jq -r '.controller_url // ""' "$CONFIG_PATH")"
  node_code="$(jq -r '.node_code // ""' "$CONFIG_PATH")"
  auth_token="$(jq -r '.auth_token // ""' "$CONFIG_PATH")"
  tuic_domain="$(jq -r '.tuic_domain // ""' "$CONFIG_PATH")"
  acme_email="$(jq -r '.acme_email // ""' "$CONFIG_PATH")"
  tuic_port="$(jq -r '.tuic_listen_port // 0' "$CONFIG_PATH")"
  refresh_singbox_log_path_from_config

  if [[ -z "$controller_url" ]]; then
    add_issue "controller_url_missing" "required_missing" "controller_url 未配置" "改为管理服务器地址（如 https://panel.example.com）"
  fi
  if [[ -z "$node_code" ]]; then
    add_issue "node_code_missing" "required_missing" "node_code 未配置" "需与管理端节点编码一致"
  fi
  if [[ -z "$auth_token" ]]; then
    add_issue "auth_token_missing" "required_missing" "auth_token 未配置" "填写管理端 NODE_AUTH_TOKEN"
  fi
  if ! [[ "$tuic_port" =~ ^[0-9]+$ ]] || (( tuic_port < 1 || tuic_port > 65535 )); then
    add_issue "tuic_port_invalid" "config_error" "tuic_listen_port 非法：${tuic_port}" "建议设置 1-65535 的 UDP 端口"
  fi

  if ! command -v sing-box >/dev/null 2>&1; then
    add_issue "singbox_missing" "required_missing" "未检测到 sing-box 可执行文件" "执行菜单 23 安装/更新 sing-box"
  fi
  if ! systemctl is-active sb-agent >/dev/null 2>&1; then
    add_issue "sb_agent_inactive" "config_error" "sb-agent 未运行" "重启 sb-agent"
  fi
  if ! systemctl is-active sing-box >/dev/null 2>&1; then
    if detect_singbox_log_permission_issue; then
      add_issue "singbox_log_permission_denied" "config_error" "sing-box 未运行（疑似日志权限问题）" "自动修复日志权限并重启 sing-box"
    elif detect_singbox_config_error; then
      add_issue "singbox_config_invalid" "config_error" "sing-box 配置校验失败" "自动重拉配置并重启 sing-box"
    else
      add_issue "singbox_inactive" "config_error" "sing-box 未运行" "修复权限后重启 sing-box"
    fi

    local port_conflict
    port_conflict="$(get_port_conflict_summary "$tuic_port")"
    if [[ -n "$port_conflict" ]]; then
      add_issue "singbox_port_conflict" "config_error" "检测到端口冲突" "占用进程：${port_conflict}"
    fi
  fi
  if ! systemctl is-enabled sb-agent >/dev/null 2>&1; then
    add_issue "sb_agent_not_enabled" "config_error" "sb-agent 未设置开机自启" "启用 sb-agent 开机自启"
  fi
  if systemctl list-unit-files 2>/dev/null | grep -q '^sing-box\.service'; then
    if ! systemctl is-enabled sing-box >/dev/null 2>&1; then
      add_issue "singbox_not_enabled" "config_error" "sing-box 未设置开机自启" "启用 sing-box 开机自启"
    fi
  fi

  if [[ -n "$controller_url" ]]; then
    local health_code
    health_code="$(curl -ks -o /dev/null -w "%{http_code}" --max-time 5 "${controller_url}/health" || true)"
    if [[ "$health_code" != "200" ]]; then
      add_issue "controller_unreachable" "config_error" "controller /health 不可达（HTTP=${health_code}）" "检查 controller_url 与网络连通"
    fi
  fi

  if [[ -n "$controller_url" && -n "$node_code" && -n "$auth_token" ]]; then
    local sync_code
    local sync_ok="false"
    sync_code="$(curl -ks -o /dev/null -w "%{http_code}" --max-time 8 \
      -H "Authorization: Bearer ${auth_token}" \
      "${controller_url}/nodes/${node_code}/sync" || true)"
    case "$sync_code" in
      200) sync_ok="true" ;;
      401) add_issue "sync_unauthorized" "config_error" "节点拉取 sync 401（token 无效）" "更新 auth_token 为管理端 NODE_AUTH_TOKEN" ;;
      403) add_issue "sync_forbidden" "config_error" "节点拉取 sync 403（来源受限）" "检查节点 agent_ip/白名单策略" ;;
      404) add_issue "sync_not_found" "config_error" "节点拉取 sync 404（接口未找到）" "检查 controller_url 是否指向管理服务器控制器地址/反代规则" ;;
      *) add_issue "sync_failed" "config_error" "节点拉取 sync 失败（HTTP=${sync_code}）" "检查 controller 状态、地址与鉴权" ;;
    esac
  fi

  if [[ -z "$tuic_domain" ]]; then
    add_issue "tuic_domain_empty" "optional_missing" "tuic_domain 为空（未启用 TUIC 证书）" "如仅使用 VLESS 可忽略"
  else
    if [[ -z "$acme_email" ]]; then
      add_issue "acme_email_missing" "required_missing" "已设置 tuic_domain 但 acme_email 为空" "填写证书邮箱后重启 sb-agent"
    fi

    local dns_ip public_ip
    dns_ip="$(resolve_domain_ipv4 "$tuic_domain")"
    public_ip="$(get_public_ipv4)"
    if [[ -n "$dns_ip" && -n "$public_ip" && "$dns_ip" != "$public_ip" ]]; then
      add_issue "tuic_dns_mismatch" "config_error" "tuic_domain A记录(${dns_ip})与本机公网IP(${public_ip})不一致" "修正 DNS 后重试"
    fi

    if [[ "${sync_code:-}" == "200" ]]; then
      if [[ -f "$SINGBOX_CONFIG" ]]; then
        local tuic_inbound_count acme_match_count
        tuic_inbound_count="$(jq '[.inbounds[]? | select(.type=="tuic")] | length' "$SINGBOX_CONFIG" 2>/dev/null || echo 0)"
        acme_match_count="$(jq --arg d "$tuic_domain" '[.inbounds[]? | select(.type=="tuic" and (.tls.acme.domain[]?==$d))] | length' "$SINGBOX_CONFIG" 2>/dev/null || echo 0)"
        if ! [[ "$tuic_inbound_count" =~ ^[0-9]+$ ]] || (( tuic_inbound_count < 1 )); then
          add_issue "tuic_inbound_missing" "config_error" "运行配置无 TUIC 入站" "请在管理端执行节点同步后重启 sb-agent"
        fi
        if ! [[ "$acme_match_count" =~ ^[0-9]+$ ]] || (( acme_match_count < 1 )); then
          add_issue "tuic_acme_missing" "config_error" "运行配置未包含当前域名 ACME" "检查 tuic_domain/acme_email 并重载"
        fi
      fi

      local cert_file
      cert_file="$(find /var/lib/sing-box/certmagic -type f -name "${tuic_domain}.crt" 2>/dev/null | head -n1 || true)"
      if [[ -z "$cert_file" ]]; then
        add_issue "certificate_missing" "config_error" "未检测到 TUIC 证书文件" "确认 TUIC 入站+ACME 后等待或触发重载"
      fi
    else
      add_issue "tuic_checks_skipped" "config_error" "同步未成功，TUIC 入站/证书无法判定" "先修复 sync，再重检 TUIC 相关项"
    fi
  fi

  if ! systemctl is-active fail2ban >/dev/null 2>&1; then
    add_issue "fail2ban_inactive" "optional_missing" "fail2ban 未运行" "建议启用以防 SSH 爆破"
  fi
  if ! systemctl is-enabled sb-cert-check.timer >/dev/null 2>&1; then
    add_issue "cert_timer_disabled" "optional_missing" "sb-cert-check.timer 未启用" "建议启用每日证书健康检查"
  fi
}

apply_fix() {
  local id="$1"
  case "$id" in
    config_missing|controller_url_missing|node_code_missing)
      bash "$INSTALL_SCRIPT" --configure-quick || true
      ;;
    jq_missing)
      export DEBIAN_FRONTEND=noninteractive
      apt-get update -y >/dev/null 2>&1 || true
      apt-get install -y jq >/dev/null 2>&1 || true
      ;;
    auth_token_missing|sync_unauthorized)
      local new_token
      read -r -p "请输入管理端生效的 NODE_AUTH_TOKEN: " new_token
      if [[ -n "$new_token" ]]; then
        update_config_value --arg t "$new_token" '.auth_token=$t'
        systemctl restart sb-agent || true
      else
        warn "未输入 token，跳过。"
      fi
      ;;
    tuic_port_invalid)
      update_config_value '.tuic_listen_port=24443'
      systemctl restart sb-agent || true
      ;;
    singbox_missing)
      bash "$INSTALL_SCRIPT" --sync-only || true
      ;;
    sb_agent_inactive)
      systemctl enable --now sb-agent >/dev/null 2>&1 || true
      ;;
    sb_agent_not_enabled)
      systemctl enable sb-agent >/dev/null 2>&1 || true
      ;;
    singbox_inactive)
      repair_singbox_log_permissions
      ensure_singbox_systemd_rw_override
      systemctl restart sb-agent >/dev/null 2>&1 || true
      sleep 2
      refresh_singbox_log_path_from_config
      repair_singbox_log_permissions
      ensure_singbox_systemd_rw_override
      systemctl reset-failed sing-box >/dev/null 2>&1 || true
      systemctl enable --now sing-box >/dev/null 2>&1 || true
      systemctl restart sing-box >/dev/null 2>&1 || true
      if ! systemctl is-active sing-box >/dev/null 2>&1; then
        warn "sing-box 仍未运行，请检查日志：journalctl -u sing-box -n 120 --no-pager"
      fi
      ;;
    singbox_log_permission_denied)
      repair_singbox_log_permissions
      ensure_singbox_systemd_rw_override
      systemctl restart sb-agent >/dev/null 2>&1 || true
      sleep 2
      refresh_singbox_log_path_from_config
      repair_singbox_log_permissions
      ensure_singbox_systemd_rw_override
      systemctl reset-failed sing-box >/dev/null 2>&1 || true
      systemctl restart sing-box >/dev/null 2>&1 || true
      if ! systemctl is-active sing-box >/dev/null 2>&1; then
        warn "sing-box 仍未运行，请检查日志：journalctl -u sing-box -n 120 --no-pager"
      fi
      ;;
    singbox_config_invalid)
      warn "检测到 sing-box 配置校验失败，尝试重拉配置..."
      systemctl restart sb-agent >/dev/null 2>&1 || true
      sleep 2
      systemctl restart sing-box >/dev/null 2>&1 || true
      if ! sing-box check -c "$SINGBOX_CONFIG" >/dev/null 2>&1; then
        warn "配置仍不通过，请检查节点同步与下发配置。"
      fi
      ;;
    singbox_port_conflict)
      warn "端口冲突无法自动修复，请手动处理占用进程后重试。"
      ;;
    singbox_not_enabled)
      systemctl enable sing-box >/dev/null 2>&1 || true
      ;;
    controller_unreachable|sync_failed|sync_forbidden|sync_not_found)
      local new_url
      read -r -p "请输入管理端 controller_url（例如 https://panel.example.com 或 https://panel.example.com:8080）: " new_url
      if [[ -n "$new_url" ]]; then
        update_config_value --arg u "$new_url" '.controller_url=$u'
        systemctl restart sb-agent >/dev/null 2>&1 || true
      else
        warn "未输入 controller_url，跳过。"
      fi
      ;;
    acme_email_missing)
      local new_email
      read -r -p "请输入 acme_email: " new_email
      if [[ -n "$new_email" ]]; then
        update_config_value --arg e "$new_email" '.acme_email=$e'
        systemctl restart sb-agent || true
      fi
      ;;
    tuic_dns_mismatch)
      warn "请先在 DNS 服务商处修正 tuic_domain A 记录，再重新自检。"
      ;;
    tuic_inbound_missing|tuic_acme_missing)
      systemctl restart sb-agent >/dev/null 2>&1 || true
      sleep 2
      systemctl restart sing-box >/dev/null 2>&1 || true
      ;;
    certificate_missing)
      systemctl restart sing-box >/dev/null 2>&1 || true
      sleep 5
      ;;
    tuic_checks_skipped|tuic_domain_empty|fail2ban_inactive|cert_timer_disabled)
      warn "该项为可选缺失，可跳过。"
      ;;
    *)
      warn "未支持自动修复项: $id"
      return 1
      ;;
  esac
  return 0
}

print_issues() {
  local idx=1
  local id sev
  for id in "${ISSUE_ORDER[@]}"; do
    sev="${ISSUE_SEVERITY[$id]}"
    case "$sev" in
      required_missing) echo "[${idx}] [必需未配置] ${ISSUE_MSG[$id]}" ;;
      optional_missing) echo "[${idx}] [可选未配置] ${ISSUE_MSG[$id]}" ;;
      config_error) echo "[${idx}] [配置错误] ${ISSUE_MSG[$id]}" ;;
      *) echo "[${idx}] [未知] ${ISSUE_MSG[$id]}" ;;
    esac
    echo "     处理建议: ${ISSUE_FIX[$id]}"
    idx=$((idx + 1))
  done
}

main_loop() {
  require_root
  msg "开始节点部署参数自检（支持循环修复）。"
  while true; do
    run_checks
    local required_count=0 optional_count=0 error_count=0
    local id sev
    for id in "${ISSUE_ORDER[@]}"; do
      sev="${ISSUE_SEVERITY[$id]}"
      case "$sev" in
        required_missing) required_count=$((required_count + 1)) ;;
        optional_missing) optional_count=$((optional_count + 1)) ;;
        config_error) error_count=$((error_count + 1)) ;;
      esac
    done

    echo "----------------------------------------"
    msg "自检汇总：必需未配置=${required_count} 可选未配置=${optional_count} 配置错误=${error_count}"
    if (( ${#ISSUE_ORDER[@]} > 0 )); then
      print_issues
    fi
    echo "----------------------------------------"

    if (( required_count == 0 && error_count == 0 )); then
      msg "自检通过：无阻断问题。"
      if (( optional_count > 0 )); then
        warn "仍有可选项未配置，可按需处理。"
      fi
      break
    fi

    local choice
    read -r -p "输入 a=自动修复可修项, 编号=修复单项, r=仅重检, q=退出: " choice
    case "$choice" in
      a|A)
        for id in "${ISSUE_ORDER[@]}"; do
          if [[ "${ISSUE_SEVERITY[$id]}" != "optional_missing" ]]; then
            apply_fix "$id" || true
          fi
        done
        ;;
      r|R|"")
        ;;
      q|Q)
        warn "已退出自检。"
        exit 1
        ;;
      *)
        if [[ "$choice" =~ ^[0-9]+$ ]]; then
          local idx=$((choice - 1))
          if (( idx >= 0 && idx < ${#ISSUE_ORDER[@]} )); then
            apply_fix "${ISSUE_ORDER[$idx]}" || true
          else
            warn "编号无效。"
          fi
        else
          warn "输入无效。"
        fi
        ;;
    esac
  done
}

main_loop "$@"
