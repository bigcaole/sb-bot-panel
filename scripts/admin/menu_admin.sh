#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
PROJECT_DIR_DEFAULT="/root/sb-bot-panel"
PROJECT_DIR="${PROJECT_DIR:-$PROJECT_DIR_DEFAULT}"

if [[ ! -f "${PROJECT_DIR}/scripts/admin/install_admin.sh" && -f "${SCRIPT_PROJECT_DIR}/scripts/admin/install_admin.sh" ]]; then
  PROJECT_DIR="$SCRIPT_PROJECT_DIR"
fi

INSTALL_SCRIPT="${PROJECT_DIR}/scripts/admin/install_admin.sh"
EXPORT_SCRIPT="${PROJECT_DIR}/scripts/admin/sb_migrate_export.sh"
IMPORT_SCRIPT="${PROJECT_DIR}/scripts/admin/sb_migrate_import.sh"
SMOKE_SCRIPT="${PROJECT_DIR}/scripts/admin/smoke_test.sh"
DB_CHECK_SCRIPT="${PROJECT_DIR}/scripts/admin/db_consistency_check.sh"
HARDEN_SCRIPT="${PROJECT_DIR}/scripts/admin/harden_security.sh"
TOKEN_COLLAPSE_SCRIPT="${PROJECT_DIR}/scripts/admin/auth_token_collapse.sh"
TOKEN_SPLIT_MIGRATE_SCRIPT="${PROJECT_DIR}/scripts/admin/auth_token_split_migrate.sh"
LOG_ARCHIVE_SCRIPT="${PROJECT_DIR}/scripts/admin/log_archive.sh"
OPS_SNAPSHOT_SCRIPT="${PROJECT_DIR}/scripts/admin/ops_snapshot.sh"
AI_CONTEXT_SCRIPT="${PROJECT_DIR}/scripts/admin/ai_context_export.sh"
ADMIN_SCRIPT_ACTOR="sb-admin"

msg() { echo -e "\033[1;32m[信息]\033[0m $*"; }
warn() { echo -e "\033[1;33m[警告]\033[0m $*"; }
err() { echo -e "\033[1;31m[错误]\033[0m $*" >&2; }

require_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    err "请使用 root 权限运行（sudo）。"
    exit 1
  fi
}

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

get_controller_port() {
  local env_file="${PROJECT_DIR}/.env"
  local port="8080"
  if [[ -f "$env_file" ]]; then
    local value
    value="$(grep -E '^CONTROLLER_PORT=' "$env_file" | tail -n1 | cut -d= -f2- || true)"
    if [[ "${value:-}" =~ ^[0-9]+$ ]]; then
      port="$value"
    fi
  fi
  echo "$port"
}

get_admin_token_raw_from_env() {
  local env_file="${PROJECT_DIR}/.env"
  local admin_raw=""
  local auth_raw=""
  local node_raw=""
  if [[ -f "$env_file" ]]; then
    admin_raw="$(grep -E '^ADMIN_AUTH_TOKEN=' "$env_file" | tail -n1 | cut -d= -f2- || true)"
    auth_raw="$(grep -E '^AUTH_TOKEN=' "$env_file" | tail -n1 | cut -d= -f2- || true)"
    node_raw="$(grep -E '^NODE_AUTH_TOKEN=' "$env_file" | tail -n1 | cut -d= -f2- || true)"
  fi
  if [[ -n "${admin_raw//[[:space:]]/}" ]]; then
    echo "$admin_raw"
  elif [[ -n "${auth_raw//[[:space:]]/}" ]]; then
    echo "$auth_raw"
  else
    echo "$node_raw"
  fi
}

first_auth_token() {
  local raw="${1:-}"
  raw="${raw//$'\n'/}"
  raw="${raw//$'\r'/}"
  raw="$(echo "$raw" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
  if [[ "$raw" == *","* ]]; then
    echo "$(echo "${raw%%,*}" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
  else
    echo "$raw"
  fi
}

pick_working_auth_token() {
  local controller_port="$1"
  local raw="$2"
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
      "http://127.0.0.1:${controller_port}/admin/security/status" || true)"
    if [[ "$code" == "200" ]]; then
      echo "$item"
      return 0
    fi
  done

  echo "${candidates[0]}"
  return 1
}

detect_ssh_service() {
  if systemctl list-unit-files | grep -q '^sshd\.service'; then
    echo "sshd"
  else
    echo "ssh"
  fi
}

detect_sshd_port() {
  local port
  port="$(sshd -T 2>/dev/null | awk '/^port /{print $2; exit}' || true)"
  if [[ -z "$port" ]]; then
    port="$(grep -E '^[[:space:]]*Port[[:space:]]+[0-9]+' /etc/ssh/sshd_config 2>/dev/null | tail -n1 | awk '{print $2}' || true)"
  fi
  echo "${port:-22}"
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

has_authorized_keys_for_user() {
  local user_name="$1"
  local user_home
  user_home="$(getent passwd "$user_name" | awk -F: '{print $6}' || true)"
  if [[ -z "$user_home" ]]; then
    return 1
  fi
  if [[ -s "${user_home}/.ssh/authorized_keys" ]]; then
    return 0
  fi
  return 1
}

ufw_allows_ssh_for_ip() {
  local ip="$1"
  local ssh_port="$2"
  if ! command -v ufw >/dev/null 2>&1; then
    return 1
  fi
  ufw status 2>/dev/null | awk -v p="$ssh_port" -v ip="$ip" '
    BEGIN {found=0}
    $0 ~ p"/tcp" && $0 ~ /ALLOW/ {
      if ($0 ~ ip) { found=1; exit 0; }
    }
    END { exit found ? 0 : 1 }
  '
}

ufw_has_allow_for_port() {
  local ssh_port="$1"
  if ! command -v ufw >/dev/null 2>&1; then
    return 1
  fi
  ufw status 2>/dev/null | grep -E "^ *${ssh_port}(/tcp)?[[:space:]]+ALLOW" >/dev/null 2>&1
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
      [[ -z "$sorted_num" ]] && continue
      ufw --force delete "$sorted_num" >/dev/null 2>&1 || true
      removed_count=$((removed_count + 1))
    done < <(printf '%s\n' "${delete_nums[@]}" | sort -rn)
  fi
  echo "$removed_count"
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
      [[ -z "$sorted_num" ]] && continue
      ufw --force delete "$sorted_num" >/dev/null 2>&1 || true
      removed_count=$((removed_count + 1))
    done < <(printf '%s\n' "${delete_nums[@]}" | sort -rn)
  fi
  echo "$removed_count"
}

wait_for_controller_ready() {
  local timeout_seconds="${1:-20}"
  local port
  port="$(get_controller_port)"
  local i
  for i in $(seq 1 "$timeout_seconds"); do
    if curl -fsSL --max-time 2 "http://127.0.0.1:${port}/health" >/dev/null 2>&1; then
      msg "controller 已就绪（127.0.0.1:${port}）"
      return 0
    fi
    sleep 1
  done
  warn "controller 启动超时（${timeout_seconds}s），请执行：journalctl -u sb-controller -n 120 --no-pager"
  return 1
}

run_security_maintenance_cleanup() {
  local auth_token_raw=""
  local auth_token=""
  local controller_port
  controller_port="$(get_controller_port)"

  auth_token_raw="$(get_admin_token_raw_from_env)"
  auth_token="$(pick_working_auth_token "$controller_port" "$auth_token_raw")" || {
    warn "管理 token 多值模式下未探测到可用 token，回退使用第一个 token。"
  }
  if [[ -z "$auth_token" ]]; then
    auth_token="$(first_auth_token "$auth_token_raw")"
  fi
  auth_token="${auth_token#"${auth_token%%[![:space:]]*}"}"
  auth_token="${auth_token%"${auth_token##*[![:space:]]}"}"

  local url="http://127.0.0.1:${controller_port}/admin/security/maintenance_cleanup"
  local response=""
  if [[ -n "$auth_token" ]]; then
    response="$(curl -fsSL -X POST "$url" -H "Authorization: Bearer ${auth_token}" -H "X-Actor: ${ADMIN_SCRIPT_ACTOR}")" || {
      err "执行手动安全清理失败，请检查管理鉴权 token（ADMIN_AUTH_TOKEN/AUTH_TOKEN）或 controller 状态。"
      return 1
    }
  else
    response="$(curl -fsSL -X POST "$url" -H "X-Actor: ${ADMIN_SCRIPT_ACTOR}")" || {
      err "执行手动安全清理失败，请检查 controller 状态。"
      return 1
    }
  fi

  msg "手动安全清理已执行。"
  if command -v jq >/dev/null 2>&1; then
    echo "$response" | jq
  else
    echo "$response"
  fi
}

run_sync_node_defaults() {
  local auth_token_raw=""
  local auth_token=""
  local controller_port
  local include_disabled="0"
  local force_new="0"
  local answer
  local response
  local body
  local http_code

  controller_port="$(get_controller_port)"
  auth_token_raw="$(get_admin_token_raw_from_env)"
  auth_token="$(pick_working_auth_token "$controller_port" "$auth_token_raw")" || {
    warn "管理 token 多值模式下未探测到可用 token，回退使用第一个 token。"
  }
  if [[ -z "$auth_token" ]]; then
    auth_token="$(first_auth_token "$auth_token_raw")"
  fi

  read -r -p "是否包含已禁用节点？[y/N]: " answer
  answer="${answer:-N}"
  if [[ "$answer" =~ ^[Yy]$ ]]; then
    include_disabled="1"
  fi
  read -r -p "是否强制新建任务（忽略去重）？[y/N]: " answer
  answer="${answer:-N}"
  if [[ "$answer" =~ ^[Yy]$ ]]; then
    force_new="1"
  fi

  if [[ -n "$auth_token" ]]; then
    response="$(
      curl -sS --max-time 15 -X POST \
        "http://127.0.0.1:${controller_port}/admin/nodes/sync_agent_defaults?include_disabled=${include_disabled}&force_new=${force_new}" \
        -H "Authorization: Bearer ${auth_token}" \
        -H "X-Actor: ${ADMIN_SCRIPT_ACTOR}" \
        -H "Content-Type: application/json" \
        -w $'\n%{http_code}' 2>/dev/null || true
    )"
  else
    response="$(
      curl -sS --max-time 15 -X POST \
        "http://127.0.0.1:${controller_port}/admin/nodes/sync_agent_defaults?include_disabled=${include_disabled}&force_new=${force_new}" \
        -H "X-Actor: ${ADMIN_SCRIPT_ACTOR}" \
        -H "Content-Type: application/json" \
        -w $'\n%{http_code}' 2>/dev/null || true
    )"
  fi

  http_code="${response##*$'\n'}"
  body="${response%$'\n'*}"
  if [[ "$http_code" != "200" ]]; then
    err "节点默认参数同步失败（HTTP ${http_code:-unknown}）。"
    if [[ -n "$body" ]]; then
      echo "$body"
    fi
    return 1
  fi

  msg "节点默认参数同步已执行。"
  if command -v jq >/dev/null 2>&1; then
    echo "$body" | jq
  else
    echo "$body"
  fi
}

run_sync_node_tokens() {
  local auth_token_raw=""
  local auth_token=""
  local controller_port
  local include_disabled="1"
  local force_new="1"
  local answer
  local response
  local body
  local http_code

  controller_port="$(get_controller_port)"
  auth_token_raw="$(get_admin_token_raw_from_env)"
  auth_token="$(pick_working_auth_token "$controller_port" "$auth_token_raw")" || {
    warn "管理 token 多值模式下未探测到可用 token，回退使用第一个 token。"
  }
  if [[ -z "$auth_token" ]]; then
    auth_token="$(first_auth_token "$auth_token_raw")"
  fi

  read -r -p "是否包含已禁用节点？[Y/n]: " answer
  answer="${answer:-Y}"
  if [[ ! "$answer" =~ ^[Yy]$ ]]; then
    include_disabled="0"
  fi
  read -r -p "是否强制新建任务（忽略去重）？[Y/n]: " answer
  answer="${answer:-Y}"
  if [[ ! "$answer" =~ ^[Yy]$ ]]; then
    force_new="0"
  fi

  if [[ -n "$auth_token" ]]; then
    response="$(
      curl -sS --max-time 15 -X POST \
        "http://127.0.0.1:${controller_port}/admin/auth/sync_node_tokens?include_disabled=${include_disabled}&force_new=${force_new}" \
        -H "Authorization: Bearer ${auth_token}" \
        -H "X-Actor: ${ADMIN_SCRIPT_ACTOR}" \
        -H "Content-Type: application/json" \
        -w $'\n%{http_code}' 2>/dev/null || true
    )"
  else
    response="$(
      curl -sS --max-time 15 -X POST \
        "http://127.0.0.1:${controller_port}/admin/auth/sync_node_tokens?include_disabled=${include_disabled}&force_new=${force_new}" \
        -H "X-Actor: ${ADMIN_SCRIPT_ACTOR}" \
        -H "Content-Type: application/json" \
        -w $'\n%{http_code}' 2>/dev/null || true
    )"
  fi

  http_code="${response##*$'\n'}"
  body="${response%$'\n'*}"
  if [[ "$http_code" != "200" ]]; then
    err "节点 Token 同步失败（HTTP ${http_code:-unknown}）。"
    if [[ -n "$body" ]]; then
      echo "$body"
    fi
    return 1
  fi

  msg "节点 Token 同步已执行。"
  if command -v jq >/dev/null 2>&1; then
    echo "$body" | jq
  else
    echo "$body"
  fi
}

run_sync_node_time() {
  local auth_token_raw=""
  local auth_token=""
  local controller_port
  local include_disabled="0"
  local force_new="0"
  local answer
  local response
  local body
  local http_code

  controller_port="$(get_controller_port)"
  auth_token_raw="$(get_admin_token_raw_from_env)"
  auth_token="$(pick_working_auth_token "$controller_port" "$auth_token_raw")" || {
    warn "管理 token 多值模式下未探测到可用 token，回退使用第一个 token。"
  }
  if [[ -z "$auth_token" ]]; then
    auth_token="$(first_auth_token "$auth_token_raw")"
  fi

  read -r -p "是否包含已禁用节点？[y/N]: " answer
  answer="${answer:-N}"
  if [[ "$answer" =~ ^[Yy]$ ]]; then
    include_disabled="1"
  fi
  read -r -p "是否强制新建任务（忽略去重）？[y/N]: " answer
  answer="${answer:-N}"
  if [[ "$answer" =~ ^[Yy]$ ]]; then
    force_new="1"
  fi

  if [[ -n "$auth_token" ]]; then
    response="$(
      curl -sS --max-time 15 -X POST \
        "http://127.0.0.1:${controller_port}/admin/nodes/sync_time?include_disabled=${include_disabled}&force_new=${force_new}" \
        -H "Authorization: Bearer ${auth_token}" \
        -H "X-Actor: ${ADMIN_SCRIPT_ACTOR}" \
        -H "Content-Type: application/json" \
        -w $'\n%{http_code}' 2>/dev/null || true
    )"
  else
    response="$(
      curl -sS --max-time 15 -X POST \
        "http://127.0.0.1:${controller_port}/admin/nodes/sync_time?include_disabled=${include_disabled}&force_new=${force_new}" \
        -H "X-Actor: ${ADMIN_SCRIPT_ACTOR}" \
        -H "Content-Type: application/json" \
        -w $'\n%{http_code}' 2>/dev/null || true
    )"
  fi

  http_code="${response##*$'\n'}"
  body="${response%$'\n'*}"
  if [[ "$http_code" != "200" ]]; then
    err "节点时间同步失败（HTTP ${http_code:-unknown}）。"
    if [[ -n "$body" ]]; then
      echo "$body"
    fi
    return 1
  fi

  msg "节点时间同步任务已下发。"
  if command -v jq >/dev/null 2>&1; then
    echo "$body" | jq
  else
    echo "$body"
  fi
}

run_sync_menu() {
  echo "同步操作："
  echo "  1) 同步节点默认参数（auth/url/poll）"
  echo "  2) 仅同步节点 Token（auth）"
  echo "  3) 同步节点系统时间（以管理服务器为准）"
  read -r -p "请选择 [1/2/3]（默认 1）: " sync_choice
  sync_choice="${sync_choice:-1}"
  case "$sync_choice" in
    2)
      run_sync_node_tokens
      ;;
    3)
      run_sync_node_time
      ;;
    *)
      run_sync_node_defaults
      ;;
  esac
}

show_ops_audit_events() {
  local auth_token_raw=""
  local auth_token=""
  local controller_port
  local window_seconds="604800"
  local window_raw=""
  local response

  controller_port="$(get_controller_port)"
  auth_token_raw="$(get_admin_token_raw_from_env)"
  local env_file="${PROJECT_DIR}/.env"
  if [[ -f "$env_file" ]]; then
    window_raw="$(grep -E '^BOT_OPS_AUDIT_WINDOW_SECONDS=' "$env_file" | tail -n1 | cut -d= -f2- || true)"
  fi
  if [[ "$window_raw" =~ ^[0-9]+$ ]] && (( window_raw >= 3600 )) && (( window_raw <= 2592000 )); then
    window_seconds="$window_raw"
  fi
  auth_token="$(pick_working_auth_token "$controller_port" "$auth_token_raw")" || {
    warn "管理 token 多值模式下未探测到可用 token，回退使用第一个 token。"
  }
  if [[ -z "$auth_token" ]]; then
    auth_token="$(first_auth_token "$auth_token_raw")"
  fi

  if [[ -n "$auth_token" ]]; then
    response="$(curl -fsSL \
      "http://127.0.0.1:${controller_port}/admin/audit?limit=100&action_prefix=ops.&window_seconds=${window_seconds}" \
      -H "Authorization: Bearer ${auth_token}" \
      -H "X-Actor: ${ADMIN_SCRIPT_ACTOR}" 2>/dev/null || true)"
  else
    response="$(curl -fsSL \
      "http://127.0.0.1:${controller_port}/admin/audit?limit=100&action_prefix=ops.&window_seconds=${window_seconds}" \
      -H "X-Actor: ${ADMIN_SCRIPT_ACTOR}" 2>/dev/null || true)"
  fi

  if [[ -z "$response" ]]; then
    err "读取审计日志失败，请检查 controller 状态与鉴权。"
    return 1
  fi

  if command -v jq >/dev/null 2>&1; then
    echo "----- 运维审计速览（ops.*，最近 ${window_seconds} 秒，最新 20 条）-----"
    echo "$response" | jq '
      .[:20]
      | if length == 0 then
          "暂无 ops.* 审计记录"
        else
          map({
            id: .id,
            time: (.created_at | tostring),
            actor: (.actor // ""),
            action: .action,
            resource: ((.resource_type // "") + ":" + (.resource_id // "")),
            detail: (.detail // "")
          })
        end
    '
  else
    echo "$response"
  fi
}

show_admin_ssh_security_status() {
  local ssh_service ssh_port client_ip pass_auth permit_root pubkey_auth
  local risk_score risk_level fail2ban_bans_24h
  ssh_service="$(detect_ssh_service)"
  ssh_port="$(detect_sshd_port)"
  client_ip="$(detect_current_ssh_client_ip)"
  pass_auth="$(sshd -T 2>/dev/null | awk '/^passwordauthentication /{print $2; exit}' || true)"
  permit_root="$(sshd -T 2>/dev/null | awk '/^permitrootlogin /{print $2; exit}' || true)"
  pubkey_auth="$(sshd -T 2>/dev/null | awk '/^pubkeyauthentication /{print $2; exit}' || true)"
  pass_auth="${pass_auth:-unknown}"
  permit_root="${permit_root:-unknown}"
  pubkey_auth="${pubkey_auth:-unknown}"
  risk_score=0
  fail2ban_bans_24h="-1"

  echo "----- SSH 安全状态总览（管理服务器）-----"
  echo "sshd 服务名: ${ssh_service}"
  echo "sshd 端口: ${ssh_port}"
  echo "当前会话来源 IP: ${client_ip:-未知}"
  if systemctl is-active "$ssh_service" >/dev/null 2>&1; then
    msg "SSH 服务状态：运行中"
  else
    risk_score=$((risk_score + 3))
    warn "SSH 服务状态：未运行"
  fi

  echo ""
  echo "----- SSH 策略（生效值）-----"
  echo "PubkeyAuthentication: ${pubkey_auth}"
  echo "PasswordAuthentication: ${pass_auth}"
  echo "PermitRootLogin: ${permit_root}"
  if [[ "$pubkey_auth" != "yes" ]]; then
    risk_score=$((risk_score + 2))
  fi
  if [[ "$pass_auth" != "no" ]]; then
    risk_score=$((risk_score + 2))
  fi
  if [[ "$permit_root" == "yes" ]]; then
    risk_score=$((risk_score + 1))
  fi

  echo ""
  echo "----- authorized_keys -----"
  if has_authorized_keys_for_user root; then
    msg "root 用户已检测到 authorized_keys。"
  else
    risk_score=$((risk_score + 3))
    warn "root 用户未检测到 authorized_keys。"
  fi

  echo ""
  echo "----- fail2ban（sshd）-----"
  if command -v fail2ban-client >/dev/null 2>&1; then
    if systemctl is-active fail2ban >/dev/null 2>&1; then
      fail2ban-client status sshd 2>/dev/null || warn "未检测到 sshd jail（可能未启用）。"
      fail2ban_bans_24h="$(get_fail2ban_ban_count_24h)"
      if [[ "$fail2ban_bans_24h" =~ ^[0-9]+$ ]]; then
        echo "近24小时封禁次数: ${fail2ban_bans_24h}"
        if (( fail2ban_bans_24h >= 30 )); then
          risk_score=$((risk_score + 1))
        fi
      fi
    else
      risk_score=$((risk_score + 1))
      warn "fail2ban 服务未运行。"
    fi
  else
    risk_score=$((risk_score + 1))
    warn "系统未安装 fail2ban。"
  fi

  echo ""
  echo "----- UFW SSH 放行 -----"
  if command -v ufw >/dev/null 2>&1; then
    local ufw_state
    ufw_state="$(ufw status 2>/dev/null | head -n1 || true)"
    echo "UFW 状态: ${ufw_state:-未知}"
    if ! ufw status 2>/dev/null | grep -E "^ *${ssh_port}(/tcp)?[[:space:]]" >/dev/null; then
      risk_score=$((risk_score + 2))
      warn "未发现 SSH 端口(${ssh_port})放行规则。"
    else
      ufw status 2>/dev/null | grep -E "^ *${ssh_port}(/tcp)?[[:space:]]" || true
    fi
    if [[ -n "$client_ip" ]] && ! ufw_allows_ssh_for_ip "$client_ip" "$ssh_port"; then
      risk_score=$((risk_score + 2))
      warn "当前来源 IP(${client_ip}) 对 SSH 端口放行状态：不明确允许。"
    fi
    if [[ "$ssh_port" != "22" ]] && ufw_has_allow_for_port "22"; then
      risk_score=$((risk_score + 1))
      warn "检测到 22/tcp 仍放行，当前 SSH 端口为 ${ssh_port}，建议清理遗留 22 规则。"
    fi
  else
    risk_score=$((risk_score + 1))
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
    msg "管理服务器 SSH 安全基线较好。"
  elif [[ "$risk_level" == "中" ]]; then
    warn "存在中等风险，建议先执行一键安全修复。"
  else
    warn "存在高风险，建议先执行一键安全修复并补齐密钥登录。"
  fi
}

run_admin_ssh_security_quick_fix() {
  local ssh_port client_ip ufw_state removed_ufw_rules removed_legacy_22
  ssh_port="$(detect_sshd_port)"
  client_ip="$(detect_current_ssh_client_ip)"
  msg "开始执行管理服务器 SSH 半自动安全修复..."

  if command -v ufw >/dev/null 2>&1; then
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
    fi
  else
    warn "系统未安装 UFW，跳过防火墙修复。"
  fi

  if command -v fail2ban-client >/dev/null 2>&1 && systemctl is-active fail2ban >/dev/null 2>&1; then
    msg "fail2ban 已运行。"
  else
    if confirm_action "fail2ban 未就绪，是否安装/启用？" "Y"; then
      export DEBIAN_FRONTEND=noninteractive
      apt-get update -y >/dev/null 2>&1 || true
      apt-get install -y fail2ban >/dev/null 2>&1 || true
      systemctl enable --now fail2ban >/dev/null 2>&1 || true
      msg "已尝试安装并启用 fail2ban。"
    else
      warn "你选择跳过 fail2ban 安装/启用。"
    fi
  fi

  msg "半自动修复执行完成，建议查看菜单 17（SSH 安全状态总览）确认结果。"
}

show_config_guide() {
  echo "配置项用途说明："
  echo "  - CONTROLLER_PORT（controller 对外监听端口；节点 agent 需要访问）"
  echo "  - CONTROLLER_PORT_WHITELIST（可选；限制可访问 controller 端口的来源 IP/CIDR）"
  echo "  - ADMIN_API_WHITELIST（可选；限制可访问管理接口的来源 IP/CIDR，应用层二次限制）"
  echo "  - SECURITY_BLOCK_PROTECTED_IPS（可选；封禁保护白名单 IP/CIDR）"
  echo "  - CONTROLLER_PUBLIC_URL（可选，给节点/外部访问的完整 URL）"
  echo "  - ENABLE_HTTPS（是否启用 Caddy 自动申请/续期证书）"
  echo "  - HTTPS_DOMAIN（管理端证书域名，如 panel.example.com）"
  echo "  - HTTPS_ACME_EMAIL（可选，证书账号邮箱）"
  echo "  - ADMIN_AUTH_TOKEN（管理接口鉴权 token，推荐）"
  echo "  - NODE_AUTH_TOKEN（节点接口鉴权 token，推荐）"
  echo "  - AUTH_TOKEN（兼容回退字段；仅在 ADMIN/NODE 未设置时生效）"
  echo "  - SECURITY_EVENTS_EXCLUDE_LOCAL（是否过滤本机测试来源，建议 1）"
  echo "  - BOT_TOKEN（建议填写；留空将使用占位值并跳过启动 sb-bot）"
  echo "  - ADMIN_CHAT_IDS（可选；限制谁能使用 bot）"
  echo "  - MIGRATE_DIR（迁移包/备份包输出目录）"
  echo "  - BOT_MENU_TTL（bot 菜单按钮自动清理秒数）"
  echo "  - BOT_NODE_MONITOR_INTERVAL（节点在线检测周期秒数）"
  echo "  - BOT_NODE_OFFLINE_THRESHOLD（节点离线判定阈值秒数）"
  echo "  - UFW/端口放行（按需开放 controller 端口，并限制来源）"
  echo ""
}

get_env_value_local() {
  local key="$1"
  local env_file="${PROJECT_DIR}/.env"
  if [[ -f "$env_file" ]]; then
    grep -E "^${key}=" "$env_file" | tail -n1 | cut -d= -f2- || true
  fi
}

mask_secret_local() {
  local value="$1"
  local n
  n="${#value}"
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

show_current_config_overview() {
  local env_file="${PROJECT_DIR}/.env"
  if [[ ! -f "$env_file" ]]; then
    warn "未检测到 ${env_file}，请先执行配置。"
    return
  fi
  local controller_port controller_url controller_public panel_base enable_https https_domain
  local admin_token node_token auth_token bot_token super_admin admin_whitelist port_whitelist
  controller_port="$(get_env_value_local CONTROLLER_PORT)"; controller_port="${controller_port:-8080}"
  controller_url="$(get_env_value_local CONTROLLER_URL)"
  controller_public="$(get_env_value_local CONTROLLER_PUBLIC_URL)"
  panel_base="$(get_env_value_local PANEL_BASE_URL)"
  enable_https="$(get_env_value_local ENABLE_HTTPS)"; enable_https="${enable_https:-0}"
  https_domain="$(get_env_value_local HTTPS_DOMAIN)"
  admin_token="$(get_env_value_local ADMIN_AUTH_TOKEN)"
  node_token="$(get_env_value_local NODE_AUTH_TOKEN)"
  auth_token="$(get_env_value_local AUTH_TOKEN)"
  bot_token="$(get_env_value_local BOT_TOKEN)"
  super_admin="$(get_env_value_local SUPER_ADMIN_CHAT_IDS)"
  admin_whitelist="$(get_env_value_local ADMIN_API_WHITELIST)"
  port_whitelist="$(get_env_value_local CONTROLLER_PORT_WHITELIST)"

  echo "----- 当前关键配置（含建议） -----"
  echo "CONTROLLER_PORT: ${controller_port}（建议保持 8080）"
  echo "CONTROLLER_URL: ${controller_url:-未设置}（建议同机部署用 http://127.0.0.1:${controller_port}）"
  echo "CONTROLLER_PUBLIC_URL: ${controller_public:-未设置}（建议填节点可访问地址，优先域名）"
  echo "PANEL_BASE_URL: ${panel_base:-未设置}（建议填用户实际访问地址，避免 127.0.0.1）"
  echo "ENABLE_HTTPS: ${enable_https}（1=启用；未切DNS前建议 0）"
  echo "HTTPS_DOMAIN: ${https_domain:-未设置}"
  echo "CONTROLLER_PORT_WHITELIST: ${port_whitelist:-未设置（建议配置）}"
  echo "ADMIN_API_WHITELIST: ${admin_whitelist:-未设置（建议配置）}"
  echo "ADMIN_AUTH_TOKEN: $(mask_secret_local "$admin_token")（建议设置）"
  echo "NODE_AUTH_TOKEN: $(mask_secret_local "$node_token")（建议设置）"
  echo "AUTH_TOKEN(兼容): $(mask_secret_local "$auth_token")（建议留空或仅过渡）"
  if [[ -n "$bot_token" && "$bot_token" != "__REPLACE_WITH_TELEGRAM_BOT_TOKEN__" ]]; then
    echo "BOT_TOKEN: 已设置（建议）"
  else
    echo "BOT_TOKEN: 未设置/占位（将无法启动 bot）"
  fi
  echo "SUPER_ADMIN_CHAT_IDS: ${super_admin:-未设置（建议至少填你的 chat id）}"
}

show_menu() {
  clear
  cat <<'EOF'
========================================
 sb-bot-panel 管理服务器菜单
========================================
【日常运维】
1. 配置（快速默认 / 高级变量向导）
2. 启动 controller
3. 停止 controller
4. 启动 bot
5. 停止 bot
6. 状态查看（controller/bot）
7. 查看日志（controller/bot/归档）
8. HTTPS 证书状态（Caddy）
9. HTTPS 证书刷新（重载 Caddy）
10. 迁移：导出迁移包
11. 迁移：导入迁移包
12. 一键验收自检（语法/单测/API）
13. 数据库一致性校验（迁移前建议）
14. 节点同步（默认参数 / Token / 时间）
15. 安全加固向导（token轮换 + 8080收敛）
16. Token 工具（收敛 token / 拆分迁移）
17. 手动安全清理（过期封禁 + 审计日志）
18. SSH 安全状态总览（只读）
19. SSH 一键安全修复（半自动）
20. 运维快照（导出关键状态）
21. AI诊断包导出（可粘贴给任意AI）
22. 组件自检与自动修复（controller/bot/caddy）

【系统级操作（谨慎）】
23. 安装/重装（交互配置 + 依赖 + venv + 重启）
24. 更新（git pull + 复用现有配置 + 重启）
25. 卸载
26. 退出
========================================
EOF
}

do_install() {
  if [[ -f "$INSTALL_SCRIPT" ]]; then
    bash "$INSTALL_SCRIPT"
  else
    err "未找到安装脚本: $INSTALL_SCRIPT"
  fi
}

do_update_reuse_config() {
  if [[ -d "${PROJECT_DIR}/.git" ]]; then
    msg "检测到 Git 仓库，执行 git pull..."
    git -C "$PROJECT_DIR" pull --ff-only || warn "git pull 失败，请手动处理。"
  else
    warn "未检测到 .git，跳过 git pull。"
  fi

  if [[ -f "$INSTALL_SCRIPT" ]]; then
    bash "$INSTALL_SCRIPT" --reuse-config
  else
    err "未找到安装脚本: $INSTALL_SCRIPT"
  fi
}

configure_only() {
  if [[ -f "$INSTALL_SCRIPT" ]]; then
    msg "配置模式选择："
    echo "  1) 快速配置（推荐默认值，最少提问）"
    echo "  2) 高级变量设置向导（逐项说明，全部可调）"
    echo "  3) 查看当前关键配置（只读，含建议）"
    local cfg_mode
    read -r -p "请选择 [1/2/3]（默认 1）: " cfg_mode
    cfg_mode="${cfg_mode:-1}"
    if [[ "$cfg_mode" == "3" ]]; then
      show_current_config_overview
    elif [[ "$cfg_mode" == "2" ]]; then
      msg "即将进入高级变量设置向导（修改参数并重启服务）。"
      show_config_guide
      bash "$INSTALL_SCRIPT" --configure-only
    else
      msg "即将进入快速配置（推荐默认值并重启服务）。"
      bash "$INSTALL_SCRIPT" --configure-quick
    fi
  else
    err "未找到安装脚本: $INSTALL_SCRIPT"
  fi
}

show_status() {
  echo "----- sb-controller -----"
  systemctl status sb-controller --no-pager || true
  echo ""
  echo "----- sb-bot -----"
  systemctl status sb-bot --no-pager || true
}

show_logs() {
  local choice
  read -r -p "查看哪个日志？1=controller 2=bot 3=日志归档 4=运维审计(ops.*) [1]: " choice
  choice="${choice:-1}"
  if [[ "$choice" == "2" ]]; then
    journalctl -u sb-bot -n 200 --no-pager || true
  elif [[ "$choice" == "3" ]]; then
    if [[ -f "$LOG_ARCHIVE_SCRIPT" ]]; then
      bash "$LOG_ARCHIVE_SCRIPT"
    else
      err "未找到日志归档脚本: $LOG_ARCHIVE_SCRIPT"
    fi
  elif [[ "$choice" == "4" ]]; then
    show_ops_audit_events
  else
    journalctl -u sb-controller -n 200 --no-pager || true
  fi
}

show_https_status() {
  if ! systemctl list-unit-files | grep -q '^caddy.service'; then
    warn "系统未安装 caddy.service。"
    return
  fi
  echo "----- caddy 状态 -----"
  systemctl status caddy --no-pager || true
  echo ""
  echo "----- Caddyfile -----"
  if [[ -f /etc/caddy/Caddyfile ]]; then
    cat /etc/caddy/Caddyfile
  else
    warn "未找到 /etc/caddy/Caddyfile"
  fi
  echo ""
  echo "----- 最近 120 行 caddy 日志 -----"
  journalctl -u caddy -n 120 --no-pager || true
}

reload_https_cert() {
  if ! systemctl list-unit-files | grep -q '^caddy.service'; then
    warn "系统未安装 caddy.service。"
    return
  fi
  msg "执行 caddy reload（触发配置重载并由 caddy 自动处理证书续期）。"
  if caddy validate --config /etc/caddy/Caddyfile >/dev/null 2>&1; then
    systemctl reload caddy || systemctl restart caddy || true
  else
    warn "Caddyfile 校验失败，改为直接重启 caddy。"
    systemctl restart caddy || true
  fi
  systemctl status caddy --no-pager || true
}

run_component_self_check_and_repair() {
  local env_file enable_https https_domain need_repair repair_failed
  local cert_count
  env_file="${PROJECT_DIR}/.env"
  enable_https="$(get_env_value_local ENABLE_HTTPS)"
  https_domain="$(get_env_value_local HTTPS_DOMAIN)"
  enable_https="${enable_https:-0}"
  need_repair=0
  repair_failed=0

  echo "----- 组件自检（管理服务器）-----"
  if [[ ! -f "$env_file" ]]; then
    warn "未检测到 ${env_file}，建议先执行菜单 23（安装/重装）完成初始化。"
    return 1
  fi

  if [[ ! -f "${PROJECT_DIR}/venv/bin/python3" ]]; then
    warn "未检测到 venv Python：${PROJECT_DIR}/venv/bin/python3"
    need_repair=1
  else
    msg "venv Python 已存在。"
  fi

  if ! systemctl list-unit-files | grep -q '^sb-controller\.service'; then
    warn "未检测到 sb-controller.service"
    need_repair=1
  else
    msg "sb-controller.service 已安装。"
  fi

  if ! systemctl list-unit-files | grep -q '^sb-bot\.service'; then
    warn "未检测到 sb-bot.service"
    need_repair=1
  else
    msg "sb-bot.service 已安装。"
  fi

  if [[ "$enable_https" == "1" ]]; then
    if ! command -v caddy >/dev/null 2>&1 || ! systemctl list-unit-files | grep -q '^caddy\.service'; then
      warn "ENABLE_HTTPS=1 但 caddy/caddy.service 缺失。"
      need_repair=1
    else
      msg "caddy 组件已安装。"
    fi
    if [[ -z "$https_domain" ]]; then
      warn "ENABLE_HTTPS=1 但 HTTPS_DOMAIN 为空。"
    fi
  else
    msg "ENABLE_HTTPS=0：跳过 caddy 安装要求检查。"
  fi

  if (( need_repair == 1 )); then
    if [[ ! -f "$INSTALL_SCRIPT" ]]; then
      err "未找到安装脚本，无法自动修复：$INSTALL_SCRIPT"
      return 1
    fi
    msg "检测到组件缺失，开始自动修复（复用现有参数，不重复提问）..."
    if ! bash "$INSTALL_SCRIPT" --reuse-config; then
      err "自动修复失败，请查看上方输出。"
      repair_failed=1
    else
      msg "自动修复执行完成。"
    fi
  else
    msg "未发现必需组件缺失。"
  fi

  echo ""
  echo "----- 修复后状态检查 -----"
  if systemctl is-active sb-controller >/dev/null 2>&1; then
    msg "sb-controller：运行中"
  else
    warn "sb-controller：未运行，尝试启动..."
    systemctl start sb-controller >/dev/null 2>&1 || true
    wait_for_controller_ready 20 || repair_failed=1
  fi

  if systemctl is-active sb-bot >/dev/null 2>&1; then
    msg "sb-bot：运行中"
  else
    warn "sb-bot：未运行，尝试启动..."
    systemctl start sb-bot >/dev/null 2>&1 || true
    if ! systemctl is-active sb-bot >/dev/null 2>&1; then
      err "sb-bot 启动失败，请查看：journalctl -u sb-bot -n 120 --no-pager"
      repair_failed=1
    fi
  fi

  if [[ "$enable_https" == "1" ]]; then
    if ! command -v caddy >/dev/null 2>&1; then
      err "HTTPS 已启用，但未检测到 caddy 命令。"
      repair_failed=1
    elif [[ ! -f /etc/caddy/Caddyfile ]]; then
      err "HTTPS 已启用，但未找到 /etc/caddy/Caddyfile。"
      repair_failed=1
    else
      systemctl enable caddy >/dev/null 2>&1 || true
      if caddy validate --config /etc/caddy/Caddyfile >/tmp/sb-admin-caddy-validate.log 2>&1; then
        if systemctl is-active caddy >/dev/null 2>&1; then
          systemctl reload caddy >/dev/null 2>&1 || systemctl restart caddy >/dev/null 2>&1 || true
        else
          systemctl restart caddy >/dev/null 2>&1 || true
        fi
        if systemctl is-active caddy >/dev/null 2>&1; then
          msg "caddy：运行中（证书自动续期由 caddy 接管）"
        else
          err "caddy 未运行，请查看：journalctl -u caddy -n 120 --no-pager"
          repair_failed=1
        fi
      else
        err "Caddyfile 校验失败（/tmp/sb-admin-caddy-validate.log）"
        cat /tmp/sb-admin-caddy-validate.log || true
        repair_failed=1
      fi
      if [[ -n "$https_domain" ]]; then
        cert_count="$(find /var/lib/caddy -type f \( -name "*.crt" -o -name "*.pem" \) 2>/dev/null | grep -F "$https_domain" | wc -l | tr -d '[:space:]')"
        if [[ "$cert_count" =~ ^[0-9]+$ ]] && (( cert_count > 0 )); then
          msg "已发现域名证书文件：${https_domain}（自动续期有效）。"
        else
          warn "暂未发现 ${https_domain} 证书文件（新部署时可能需等待首次签发）。"
        fi
      fi
    fi
  fi

  if (( repair_failed == 0 )); then
    msg "组件自检与自动修复完成：未发现阻断性问题。"
  else
    err "组件自检完成，但仍存在失败项，请按上方日志处理。"
    return 1
  fi
}

do_uninstall() {
  read -r -p "确认卸载服务？[y/N]: " answer
  answer="${answer:-N}"
  if [[ ! "$answer" =~ ^[Yy]$ ]]; then
    warn "已取消。"
    return
  fi

  systemctl stop sb-bot 2>/dev/null || true
  systemctl stop sb-controller 2>/dev/null || true
  systemctl disable sb-bot 2>/dev/null || true
  systemctl disable sb-controller 2>/dev/null || true
  rm -f /etc/systemd/system/sb-bot.service
  rm -f /etc/systemd/system/sb-controller.service
  systemctl daemon-reload
  msg "服务已卸载。"

  read -r -p "是否删除项目目录 ${PROJECT_DIR}？[y/N]: " remove_proj
  remove_proj="${remove_proj:-N}"
  if [[ "$remove_proj" =~ ^[Yy]$ ]]; then
    rm -rf "$PROJECT_DIR"
    msg "项目目录已删除。"
  fi
}

main() {
  require_root

  while true; do
    show_menu
    read -r -p "请输入选项 [1-26]: " action
    case "$action" in
      1)
        configure_only
        pause
        ;;
      2)
        systemctl start sb-controller || true
        wait_for_controller_ready 20 || true
        msg "已执行启动 controller。"
        pause
        ;;
      3)
        systemctl stop sb-controller || true
        msg "已执行停止 controller。"
        pause
        ;;
      4)
        systemctl start sb-bot || true
        msg "已执行启动 bot。"
        pause
        ;;
      5)
        systemctl stop sb-bot || true
        msg "已执行停止 bot。"
        pause
        ;;
      6)
        show_status
        pause
        ;;
      7)
        show_logs
        pause
        ;;
      8)
        show_https_status
        pause
        ;;
      9)
        reload_https_cert
        pause
        ;;
      10)
        if [[ -f "$EXPORT_SCRIPT" ]]; then
          bash "$EXPORT_SCRIPT"
        else
          err "未找到导出脚本: $EXPORT_SCRIPT"
        fi
        pause
        ;;
      11)
        if [[ -f "$IMPORT_SCRIPT" ]]; then
          local default_pkg migrate_dir_env
          migrate_dir_env="$(grep -E '^MIGRATE_DIR=' "${PROJECT_DIR}/.env" 2>/dev/null | tail -n1 | cut -d= -f2- || true)"
          default_pkg="$(ls -1t "${migrate_dir_env:-/var/backups/sb-migrate}"/sb-migrate-*.tar.gz 2>/dev/null | head -n1 || true)"
          read -r -p "请输入迁移包路径 [${default_pkg}]: " pkg_path
          pkg_path="${pkg_path:-$default_pkg}"
          if [[ -z "$pkg_path" ]]; then
            warn "未提供迁移包路径。"
            pause
            continue
          fi
          bash "$IMPORT_SCRIPT" "$pkg_path"
        else
          err "未找到导入脚本: $IMPORT_SCRIPT"
        fi
        pause
        ;;
      12)
        if [[ -f "$SMOKE_SCRIPT" ]]; then
          local require_split
          read -r -p "是否要求 token 拆分通过（ADMIN/NODE 分离）？[y/N]: " require_split
          require_split="${require_split:-N}"
          if [[ "$require_split" =~ ^[Yy]$ ]]; then
            bash "$SMOKE_SCRIPT" --require-api --require-token-split
          else
            bash "$SMOKE_SCRIPT" --require-api
          fi
        else
          err "未找到验收脚本: $SMOKE_SCRIPT"
        fi
        pause
        ;;
      13)
        if [[ -f "$DB_CHECK_SCRIPT" ]]; then
          bash "$DB_CHECK_SCRIPT"
        else
          err "未找到校验脚本: $DB_CHECK_SCRIPT"
        fi
        pause
        ;;
      14)
        run_sync_menu
        pause
        ;;
      15)
        if [[ -f "$HARDEN_SCRIPT" ]]; then
          bash "$HARDEN_SCRIPT"
        else
          err "未找到安全加固脚本: $HARDEN_SCRIPT"
        fi
        pause
        ;;
      16)
        echo "请选择 token 操作："
        echo "  1) 收敛 token（AUTH/ADMIN/NODE 多值 -> 单值）"
        echo "  2) 拆分迁移（兼容模式 -> ADMIN/NODE 拆分过渡）"
        read -r -p "请输入 [1/2]（默认 1）: " token_action
        token_action="${token_action:-1}"
        if [[ "$token_action" == "2" ]]; then
          if [[ -f "$TOKEN_SPLIT_MIGRATE_SCRIPT" ]]; then
            bash "$TOKEN_SPLIT_MIGRATE_SCRIPT"
          else
            err "未找到 token 拆分迁移脚本: $TOKEN_SPLIT_MIGRATE_SCRIPT"
          fi
        else
          if [[ -f "$TOKEN_COLLAPSE_SCRIPT" ]]; then
            bash "$TOKEN_COLLAPSE_SCRIPT"
          else
            err "未找到 token 收敛脚本: $TOKEN_COLLAPSE_SCRIPT"
          fi
        fi
        pause
        ;;
      17)
        run_security_maintenance_cleanup
        pause
        ;;
      18)
        show_admin_ssh_security_status
        pause
        ;;
      19)
        run_admin_ssh_security_quick_fix
        pause
        ;;
      20)
        if [[ -f "$OPS_SNAPSHOT_SCRIPT" ]]; then
          bash "$OPS_SNAPSHOT_SCRIPT"
        else
          err "未找到运维快照脚本: $OPS_SNAPSHOT_SCRIPT"
        fi
        pause
        ;;
      21)
        if [[ -f "$AI_CONTEXT_SCRIPT" ]]; then
          bash "$AI_CONTEXT_SCRIPT"
        else
          err "未找到 AI 诊断包脚本: $AI_CONTEXT_SCRIPT"
        fi
        pause
        ;;
      22)
        run_component_self_check_and_repair
        pause
        ;;
      23)
        if confirm_action "确认执行安装/重装？（会进入交互配置）" "N"; then
          do_install
        else
          warn "已取消安装/重装。"
        fi
        pause
        ;;
      24)
        if confirm_action "确认执行更新？（自动复用现有 .env 参数）" "Y"; then
          do_update_reuse_config
        else
          warn "已取消更新。"
        fi
        pause
        ;;
      25)
        do_uninstall
        pause
        ;;
      26)
        msg "已退出。"
        exit 0
        ;;
      *)
        warn "无效选项，请输入 1-26。"
        pause
        ;;
    esac
  done
}

main "$@"
