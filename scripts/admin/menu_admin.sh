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
RUNTIME_SELF_CHECK_SCRIPT="${PROJECT_DIR}/scripts/admin/runtime_self_check.sh"
ADMIN_SCRIPT_ACTOR="sb-admin"
MENU_VIEW_MODE="${MENU_VIEW_MODE:-basic}"

msg() { echo -e "\033[1;32m[信息]\033[0m $*"; }
warn() { echo -e "\033[1;33m[警告]\033[0m $*"; }
err() { echo -e "\033[1;31m[错误]\033[0m $*" >&2; }

require_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    err "请使用 root 权限运行（sudo）。"
    exit 1
  fi
}

systemd_unit_exists() {
  local unit_name="$1"
  local load_state
  load_state="$(systemctl show -p LoadState --value "$unit_name" 2>/dev/null || true)"
  [[ -n "$load_state" && "$load_state" != "not-found" ]]
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

resolve_admin_auth_token() {
  local controller_port="$1"
  local auth_token_raw auth_token
  auth_token_raw="$(get_admin_token_raw_from_env)"
  auth_token="$(pick_working_auth_token "$controller_port" "$auth_token_raw")" || {
    warn "管理 token 多值模式下未探测到可用 token，回退使用第一个 token。"
  }
  if [[ -z "$auth_token" ]]; then
    auth_token="$(first_auth_token "$auth_token_raw")"
  fi
  echo "$auth_token"
}

detect_ssh_service() {
  if systemd_unit_exists "sshd.service"; then
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
  echo "  4) 查看节点部署对齐参数（给新节点抄写）"
  echo "  5) 新节点上线一条龙向导（最少切换）"
  read -r -p "请选择 [1/2/3/4/5]（默认 1）: " sync_choice
  sync_choice="${sync_choice:-1}"
  case "$sync_choice" in
    2)
      run_sync_node_tokens
      ;;
    3)
      run_sync_node_time
      ;;
    4)
      show_node_alignment_params
      ;;
    5)
      show_node_onboarding_playbook
      ;;
    *)
      run_sync_node_defaults
      ;;
  esac
}

show_full_auth_tokens() {
  local env_file="${PROJECT_DIR}/.env"
  local admin_token node_token auth_token
  local admin_primary node_primary auth_primary
  local confirm_text=""
  local controller_port auth_token_for_api status_response status_http status_body
  if [[ ! -f "$env_file" ]]; then
    warn "未检测到 ${env_file}，请先完成安装/配置。"
    return 1
  fi
  if ! confirm_action "将显示完整 token（终端历史可见），是否继续？" "N"; then
    warn "已取消显示完整 token。"
    return 0
  fi
  read -r -p "请输入 SHOW 确认显示: " confirm_text
  if [[ "$confirm_text" != "SHOW" ]]; then
    warn "确认口令不匹配，已取消。"
    return 1
  fi

  admin_token="$(get_env_value_local ADMIN_AUTH_TOKEN)"
  node_token="$(get_env_value_local NODE_AUTH_TOKEN)"
  auth_token="$(get_env_value_local AUTH_TOKEN)"
  admin_primary="$(first_auth_token "$admin_token")"
  node_primary="$(first_auth_token "$node_token")"
  auth_primary="$(first_auth_token "$auth_token")"

  echo "----- 完整 Token（敏感） -----"
  echo "ADMIN_AUTH_TOKEN=${admin_token:-<empty>}"
  echo "NODE_AUTH_TOKEN=${node_token:-<empty>}"
  echo "AUTH_TOKEN=${auth_token:-<empty>}"
  echo ""
  echo "主值（逗号前第一段）："
  echo "ADMIN_PRIMARY=${admin_primary:-<empty>}"
  echo "NODE_PRIMARY=${node_primary:-<empty>}"
  echo "AUTH_PRIMARY=${auth_primary:-<empty>}"
  echo ""
  echo "节点推荐对齐："
  echo "  - /etc/sb-agent/config.json 的 auth_token 应使用 NODE_PRIMARY"
  echo "  - 管理接口调用应使用 ADMIN_PRIMARY"

  controller_port="$(get_controller_port)"
  auth_token_for_api="$(resolve_admin_auth_token "$controller_port")"
  if [[ -n "$auth_token_for_api" ]]; then
    status_response="$(
      curl -sS --max-time 8 \
        "http://127.0.0.1:${controller_port}/admin/security/status" \
        -H "Authorization: Bearer ${auth_token_for_api}" \
        -H "X-Actor: ${ADMIN_SCRIPT_ACTOR}" \
        -w $'\n%{http_code}' 2>/dev/null || true
    )"
    status_http="${status_response##*$'\n'}"
    status_body="${status_response%$'\n'*}"
    if [[ "$status_http" == "200" ]] && command -v jq >/dev/null 2>&1; then
      echo ""
      echo "拆分状态（来自 /admin/security/status）："
      echo "$status_body" | jq '{auth_token_split_active,admin_auth_source,node_auth_source,admin_auth_token_count,node_auth_token_count}'
    fi
  fi
}

show_node_alignment_params() {
  local env_file="${PROJECT_DIR}/.env"
  local controller_port controller_public controller_url poll_interval
  local admin_token node_token auth_token node_primary
  if [[ ! -f "$env_file" ]]; then
    warn "未检测到 ${env_file}，请先完成安装/配置。"
    return 1
  fi
  controller_port="$(get_env_value_local CONTROLLER_PORT)"
  controller_port="${controller_port:-8080}"
  controller_public="$(get_env_value_local CONTROLLER_PUBLIC_URL)"
  controller_url="$(get_env_value_local CONTROLLER_URL)"
  poll_interval="$(get_env_value_local AGENT_DEFAULT_POLL_INTERVAL)"
  poll_interval="${poll_interval:-15}"
  admin_token="$(get_env_value_local ADMIN_AUTH_TOKEN)"
  node_token="$(get_env_value_local NODE_AUTH_TOKEN)"
  auth_token="$(get_env_value_local AUTH_TOKEN)"
  node_primary="$(first_auth_token "$node_token")"
  if [[ -z "$node_primary" ]]; then
    node_primary="$(first_auth_token "$auth_token")"
  fi

  echo "----- 节点部署对齐参数（管理端 -> 节点端） -----"
  echo "1) 节点 config.json 必填项："
  if [[ -n "$controller_public" ]]; then
    echo "   controller_url: ${controller_public}"
  else
    echo "   controller_url: <未设置 CONTROLLER_PUBLIC_URL>"
    echo "   建议：在菜单 1 配置 CONTROLLER_PUBLIC_URL（填节点可访问地址），再执行菜单 14 同步默认参数。"
  fi
  echo "   auth_token: ${node_primary:-<未设置 NODE_AUTH_TOKEN/AUTH_TOKEN>}"
  echo "   poll_interval: ${poll_interval}"
  echo "   node_code: 与管理端节点编码完全一致（区分大小写）"
  echo ""
  echo "2) 管理端关键字段："
  echo "   CONTROLLER_PORT=${controller_port}"
  echo "   CONTROLLER_URL=${controller_url:-未设置}"
  echo "   CONTROLLER_PUBLIC_URL=${controller_public:-未设置}"
  echo "   ADMIN_AUTH_TOKEN=$(mask_secret_local "$admin_token")"
  echo "   NODE_AUTH_TOKEN=$(mask_secret_local "$node_token")"
  echo "   AUTH_TOKEN(兼容)=$(mask_secret_local "$auth_token")"
  echo ""
  echo "3) 建议流程："
  echo "   - 菜单 14 -> 1（同步默认参数）批量下发 auth_token/controller_url/poll_interval"
  echo "   - 菜单 14 -> 2（仅同步节点 Token）在 token 轮换后执行"
  echo "   - 菜单 6（状态查看）检查节点连接统计，确认新节点已上报 last_seen"
}

show_node_onboarding_playbook() {
  local controller_public controller_port node_token auth_token node_primary
  local poll_interval
  local node_code_input
  controller_public="$(get_env_value_local CONTROLLER_PUBLIC_URL)"
  controller_port="$(get_env_value_local CONTROLLER_PORT)"
  controller_port="${controller_port:-8080}"
  node_token="$(get_env_value_local NODE_AUTH_TOKEN)"
  auth_token="$(get_env_value_local AUTH_TOKEN)"
  node_primary="$(first_auth_token "$node_token")"
  if [[ -z "$node_primary" ]]; then
    node_primary="$(first_auth_token "$auth_token")"
  fi
  poll_interval="$(get_env_value_local AGENT_DEFAULT_POLL_INTERVAL)"
  poll_interval="${poll_interval:-15}"
  read -r -p "输入本次接入的 node_code（可留空）: " node_code_input

  echo "----- 新节点上线一条龙向导（最少切换）-----"
  echo "目标：管理端/节点端各操作 1 次，最后回管理端确认 1 次。"
  echo ""
  echo "A) 管理服务器（先做）"
  echo "  1. 先在 Bot 或 API 创建节点记录（node_code 必须和节点端一致）"
  if [[ -n "$node_code_input" ]]; then
    echo "     - 本次 node_code: ${node_code_input}"
  fi
  echo "  2. 记下节点对齐值："
  echo "     - controller_url: ${controller_public:-<未设置 CONTROLLER_PUBLIC_URL，需先菜单1配置>}"
  echo "     - auth_token: ${node_primary:-<未设置 NODE_AUTH_TOKEN/AUTH_TOKEN>}"
  echo "     - poll_interval: ${poll_interval}"
  echo ""
  echo "B) 节点服务器（只做一次）"
  echo "  1. 执行：cd /root/sb-bot-panel && git pull --ff-only origin main"
  echo "  2. 执行：bash scripts/install.sh --configure-quick"
  echo "  3. 按提示填写：controller_url / node_code / auth_token / tuic_domain(可选) / poll_interval"
  echo ""
  echo "C) 回管理服务器（收口确认）"
  echo "  1. 菜单 14 -> 1：同步默认参数（auth/url/poll）"
  echo "  2. 菜单 6：看“节点连接统计”，确认该节点 last_seen 已上报"
  echo "  3. 若 token 轮换过：菜单 14 -> 2 再补一次 Token 同步"
  echo ""
  echo "D) 常见异常一键处理"
  echo "  - 节点显示未连接：菜单 6 看 last_seen + 节点机看 journalctl -u sb-agent -n 80"
  echo "  - 节点提示 sing-box 缺失：节点菜单 23 安装/更新 sing-box"
  echo "  - 鉴权 401：管理菜单 16 -> 3 查看完整 token，确保节点 auth_token=NODE_PRIMARY"
}

show_node_connection_overview() {
  local controller_port auth_token
  local overview_response overview_http overview_body
  local nodes_response nodes_http nodes_body
  local node_access_response node_access_http node_access_body
  local now_ts threshold
  controller_port="$(get_controller_port)"
  auth_token="$(resolve_admin_auth_token "$controller_port")"

  if [[ -n "$auth_token" ]]; then
    overview_response="$(
      curl -sS --max-time 8 \
        "http://127.0.0.1:${controller_port}/admin/overview" \
        -H "Authorization: Bearer ${auth_token}" \
        -H "X-Actor: ${ADMIN_SCRIPT_ACTOR}" \
        -w $'\n%{http_code}' 2>/dev/null || true
    )"
    nodes_response="$(
      curl -sS --max-time 8 \
        "http://127.0.0.1:${controller_port}/nodes" \
        -H "Authorization: Bearer ${auth_token}" \
        -H "X-Actor: ${ADMIN_SCRIPT_ACTOR}" \
        -w $'\n%{http_code}' 2>/dev/null || true
    )"
    node_access_response="$(
      curl -sS --max-time 8 \
        "http://127.0.0.1:${controller_port}/admin/node_access/status" \
        -H "Authorization: Bearer ${auth_token}" \
        -H "X-Actor: ${ADMIN_SCRIPT_ACTOR}" \
        -w $'\n%{http_code}' 2>/dev/null || true
    )"
  else
    overview_response="$(
      curl -sS --max-time 8 \
        "http://127.0.0.1:${controller_port}/admin/overview" \
        -H "X-Actor: ${ADMIN_SCRIPT_ACTOR}" \
        -w $'\n%{http_code}' 2>/dev/null || true
    )"
    nodes_response="$(
      curl -sS --max-time 8 \
        "http://127.0.0.1:${controller_port}/nodes" \
        -H "X-Actor: ${ADMIN_SCRIPT_ACTOR}" \
        -w $'\n%{http_code}' 2>/dev/null || true
    )"
    node_access_response="$(
      curl -sS --max-time 8 \
        "http://127.0.0.1:${controller_port}/admin/node_access/status" \
        -H "X-Actor: ${ADMIN_SCRIPT_ACTOR}" \
        -w $'\n%{http_code}' 2>/dev/null || true
    )"
  fi

  overview_http="${overview_response##*$'\n'}"
  overview_body="${overview_response%$'\n'*}"
  nodes_http="${nodes_response##*$'\n'}"
  nodes_body="${nodes_response%$'\n'*}"
  node_access_http="${node_access_response##*$'\n'}"
  node_access_body="${node_access_response%$'\n'*}"

  echo "----- 节点连接统计（管理视角） -----"
  if [[ "$overview_http" != "200" || "$nodes_http" != "200" ]]; then
    warn "读取节点连接统计失败：/admin/overview HTTP=${overview_http} /nodes HTTP=${nodes_http}"
    if [[ -n "$overview_body" ]]; then
      echo "$overview_body"
    fi
    return 1
  fi
  if ! command -v jq >/dev/null 2>&1; then
    warn "未安装 jq，输出原始概览："
    echo "$overview_body"
    return 0
  fi

  threshold="$(echo "$overview_body" | jq -r '.monitor.threshold_seconds // 120' 2>/dev/null || echo "120")"
  if [[ ! "$threshold" =~ ^[0-9]+$ ]]; then
    threshold="120"
  fi
  now_ts="$(date +%s)"

  echo "$nodes_body" | jq -r --argjson now "$now_ts" --argjson threshold "$threshold" '
    def last_seen_num: ((.last_seen_at // 0) | tonumber);
    def online: (last_seen_num > 0 and (($now - last_seen_num) <= $threshold));
    "总节点: \((length))",
    "启用节点: \((map(select((.enabled // 0 | tonumber)==1)) | length))",
    "在线阈值: \($threshold) 秒",
    "已连接节点(last_seen 在阈值内): \((map(select(online)) | length))",
    "启用且已连接: \((map(select((.enabled // 0 | tonumber)==1 and online)) | length))",
    "启用但未连接: \((map(select((.enabled // 0 | tonumber)==1 and (online|not))) | length))"
  '
  echo ""
  echo "节点明细（按节点编码）："
  echo "$nodes_body" | jq -r --argjson now "$now_ts" --argjson threshold "$threshold" '
    def last_seen_num: ((.last_seen_at // 0) | tonumber);
    def online: (last_seen_num > 0 and (($now - last_seen_num) <= $threshold));
    sort_by(.node_code)[] |
    . as $n |
    " - \($n.node_code) | enabled=\(($n.enabled // 0)|tonumber) | monitor=\(($n.monitor_enabled // 0)|tonumber) | " +
    (if online then "connected" else "disconnected" end) +
    " | last_seen=" +
    (if last_seen_num > 0 then (last_seen_num | strftime("%Y-%m-%d %H:%M:%S")) else "never" end)
  '

  if [[ "$node_access_http" == "200" ]]; then
    echo ""
    echo "访问收敛（/admin/node_access/status）："
    echo "$node_access_body" | jq -r '"启用总数=\(.enabled_nodes) | 已锁定启用=\(.locked_enabled_nodes) | 未锁定启用=\(.unlocked_enabled_nodes) | 白名单缺口=\(.whitelist_missing_count)"'
  fi
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

upsert_env_value_local() {
  local key="$1"
  local value="$2"
  local env_file="${PROJECT_DIR}/.env"
  local escaped
  if [[ ! -f "$env_file" ]]; then
    err "未检测到 ${env_file}，请先执行安装/配置。"
    return 1
  fi
  escaped="$(printf '%s' "$value" | sed 's/[\\/&]/\\&/g')"
  if grep -q "^${key}=" "$env_file"; then
    sed -i "s/^${key}=.*/${key}=${escaped}/" "$env_file"
  else
    printf '%s=%s\n' "$key" "$value" >>"$env_file"
  fi
}

is_secret_env_key() {
  local key="$1"
  case "$key" in
    AUTH_TOKEN|ADMIN_AUTH_TOKEN|NODE_AUTH_TOKEN|BOT_TOKEN|SUB_LINK_SIGN_KEY)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

admin_param_hint() {
  local key="$1"
  case "$key" in
    CONTROLLER_PORT) echo "controller 对外监听端口（1-65535）" ;;
    CONTROLLER_PORT_WHITELIST) echo "controller 端口白名单（逗号分隔 IP/CIDR）" ;;
    ADMIN_API_WHITELIST) echo "管理接口来源白名单（逗号分隔 IP/CIDR）" ;;
    CONTROLLER_URL) echo "bot 调 controller 地址，通常同机 http://127.0.0.1:8080" ;;
    CONTROLLER_PUBLIC_URL) echo "节点/外部访问管理端地址（建议域名）" ;;
    PANEL_BASE_URL) echo "用户订阅链接展示地址（不要 127.0.0.1）" ;;
    ENABLE_HTTPS) echo "是否启用 Caddy HTTPS（0/1）" ;;
    HTTPS_DOMAIN) echo "HTTPS 证书域名（启用 HTTPS 时必填）" ;;
    HTTPS_ACME_EMAIL) echo "证书申请邮箱（建议填写）" ;;
    BOT_TOKEN) echo "Telegram 机器人 token（占位值会导致 bot 不启动）" ;;
    BOT_MENU_TTL) echo "bot 菜单自动清理秒数" ;;
    BOT_NODE_MONITOR_INTERVAL) echo "节点监控轮询间隔秒数" ;;
    BOT_NODE_OFFLINE_THRESHOLD) echo "节点离线判定阈值秒数" ;;
    BOT_NODE_TIME_SYNC_INTERVAL) echo "节点自动对时周期秒数（0=关闭）" ;;
    *) echo "此参数可直接修改并立即应用。" ;;
  esac
}

admin_param_brief() {
  local key="$1"
  case "$key" in
    CONTROLLER_PORT) echo "监听端口" ;;
    CONTROLLER_PORT_WHITELIST) echo "8080白名单" ;;
    ADMIN_API_WHITELIST) echo "管理来源白名单" ;;
    SECURITY_BLOCK_PROTECTED_IPS) echo "封禁保护白名单" ;;
    CONTROLLER_URL) echo "控制器内网地址" ;;
    CONTROLLER_PUBLIC_URL) echo "控制器公网地址" ;;
    PANEL_BASE_URL) echo "订阅展示地址" ;;
    ENABLE_HTTPS) echo "启用HTTPS" ;;
    HTTPS_DOMAIN) echo "HTTPS域名" ;;
    HTTPS_ACME_EMAIL) echo "证书邮箱" ;;
    ADMIN_AUTH_TOKEN) echo "管理鉴权Token" ;;
    NODE_AUTH_TOKEN) echo "节点鉴权Token" ;;
    AUTH_TOKEN) echo "兼容Token" ;;
    BOT_TOKEN) echo "机器人Token" ;;
    ADMIN_CHAT_IDS) echo "管理员ID" ;;
    VIEW_ADMIN_CHAT_IDS) echo "只读管理员ID" ;;
    OPS_ADMIN_CHAT_IDS) echo "运维管理员ID" ;;
    SUPER_ADMIN_CHAT_IDS) echo "超级管理员ID" ;;
    MIGRATE_DIR) echo "迁移目录" ;;
    BACKUP_RETENTION_COUNT) echo "备份保留数" ;;
    MIGRATE_RETENTION_COUNT) echo "迁移包保留数" ;;
    LOG_ARCHIVE_WINDOW_HOURS) echo "日志归档窗口" ;;
    LOG_ARCHIVE_RETENTION_COUNT) echo "日志归档保留数" ;;
    LOG_ARCHIVE_DIR) echo "日志归档目录" ;;
    BOT_MENU_TTL) echo "菜单TTL" ;;
    BOT_NODE_MONITOR_INTERVAL) echo "节点监控间隔" ;;
    BOT_NODE_OFFLINE_THRESHOLD) echo "离线阈值" ;;
    BOT_NODE_TIME_SYNC_INTERVAL) echo "对时间隔" ;;
    BOT_MUTATION_COOLDOWN) echo "操作冷却秒数" ;;
    TRUST_X_FORWARDED_FOR) echo "信任XFF" ;;
    TRUSTED_PROXY_IPS) echo "受信代理IP" ;;
    SECURITY_EVENTS_EXCLUDE_LOCAL) echo "事件过滤本机" ;;
    SECURITY_AUTO_BLOCK_ENABLED) echo "自动封禁开关" ;;
    SECURITY_AUTO_BLOCK_INTERVAL_SECONDS) echo "自动封禁周期" ;;
    SECURITY_AUTO_BLOCK_WINDOW_SECONDS) echo "统计窗口秒数" ;;
    SECURITY_AUTO_BLOCK_THRESHOLD) echo "封禁阈值" ;;
    SECURITY_AUTO_BLOCK_DURATION_SECONDS) echo "封禁时长秒数" ;;
    SECURITY_AUTO_BLOCK_MAX_PER_INTERVAL) echo "每轮封禁上限" ;;
    CONTROLLER_HTTP_TIMEOUT) echo "HTTP超时秒数" ;;
    BOT_ACTOR_LABEL) echo "审计操作者标签" ;;
    NODE_TASK_RUNNING_TIMEOUT) echo "任务运行超时" ;;
    NODE_TASK_RETENTION_SECONDS) echo "任务保留秒数" ;;
    NODE_TASK_MAX_PENDING_PER_NODE) echo "单节点待执行上限" ;;
    SUB_LINK_SIGN_KEY) echo "订阅签名密钥" ;;
    SUB_LINK_REQUIRE_SIGNATURE) echo "强制签名" ;;
    SUB_LINK_DEFAULT_TTL_SECONDS) echo "签名默认TTL" ;;
    API_RATE_LIMIT_ENABLED) echo "限流开关" ;;
    API_RATE_LIMIT_WINDOW_SECONDS) echo "限流窗口秒数" ;;
    API_RATE_LIMIT_MAX_REQUESTS) echo "窗口最大请求" ;;
    ADMIN_OVERVIEW_CACHE_TTL_SECONDS) echo "概览缓存秒数" ;;
    ADMIN_SECURITY_STATUS_CACHE_TTL_SECONDS) echo "安全状态缓存秒数" ;;
    ADMIN_SECURITY_EVENTS_CACHE_TTL_SECONDS) echo "安全事件缓存秒数" ;;
    *) echo "参数" ;;
  esac
}

edit_single_config_param() {
  local env_file="${PROJECT_DIR}/.env"
  if [[ ! -f "$env_file" ]]; then
    err "未检测到 ${env_file}，请先执行配置。"
    return 1
  fi

  local -a keys=(
    CONTROLLER_PORT CONTROLLER_PORT_WHITELIST ADMIN_API_WHITELIST SECURITY_BLOCK_PROTECTED_IPS
    CONTROLLER_URL CONTROLLER_PUBLIC_URL PANEL_BASE_URL
    ENABLE_HTTPS HTTPS_DOMAIN HTTPS_ACME_EMAIL
    ADMIN_AUTH_TOKEN NODE_AUTH_TOKEN AUTH_TOKEN
    BOT_TOKEN ADMIN_CHAT_IDS VIEW_ADMIN_CHAT_IDS OPS_ADMIN_CHAT_IDS SUPER_ADMIN_CHAT_IDS
    MIGRATE_DIR BACKUP_RETENTION_COUNT MIGRATE_RETENTION_COUNT
    LOG_ARCHIVE_WINDOW_HOURS LOG_ARCHIVE_RETENTION_COUNT LOG_ARCHIVE_DIR
    BOT_MENU_TTL BOT_NODE_MONITOR_INTERVAL BOT_NODE_OFFLINE_THRESHOLD BOT_NODE_TIME_SYNC_INTERVAL BOT_MUTATION_COOLDOWN
    TRUST_X_FORWARDED_FOR TRUSTED_PROXY_IPS SECURITY_EVENTS_EXCLUDE_LOCAL
    SECURITY_AUTO_BLOCK_ENABLED SECURITY_AUTO_BLOCK_INTERVAL_SECONDS SECURITY_AUTO_BLOCK_WINDOW_SECONDS SECURITY_AUTO_BLOCK_THRESHOLD SECURITY_AUTO_BLOCK_DURATION_SECONDS SECURITY_AUTO_BLOCK_MAX_PER_INTERVAL
    CONTROLLER_HTTP_TIMEOUT BOT_ACTOR_LABEL
    NODE_TASK_RUNNING_TIMEOUT NODE_TASK_RETENTION_SECONDS NODE_TASK_MAX_PENDING_PER_NODE
    SUB_LINK_SIGN_KEY SUB_LINK_REQUIRE_SIGNATURE SUB_LINK_DEFAULT_TTL_SECONDS
    API_RATE_LIMIT_ENABLED API_RATE_LIMIT_WINDOW_SECONDS API_RATE_LIMIT_MAX_REQUESTS
    ADMIN_OVERVIEW_CACHE_TTL_SECONDS ADMIN_SECURITY_STATUS_CACHE_TTL_SECONDS ADMIN_SECURITY_EVENTS_CACHE_TTL_SECONDS
  )

  local choice idx key current_value display_value new_value
  while true; do
    echo "----- 管理端参数单项修改 -----"
    for idx in "${!keys[@]}"; do
      key="${keys[$idx]}"
      current_value="$(get_env_value_local "$key")"
      if [[ -z "$current_value" ]]; then
        display_value="未设置"
      elif is_secret_env_key "$key"; then
        display_value="$(mask_secret_local "$current_value")"
      else
        display_value="$current_value"
      fi
      printf "%2d) %s｜%s = %s\n" "$((idx + 1))" "$(admin_param_brief "$key")" "$key" "$display_value"
    done
    echo " q) 返回"
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
    current_value="$(get_env_value_local "$key")"
    echo "参数: ${key}"
    echo "说明: $(admin_param_hint "$key")"
    if is_secret_env_key "$key"; then
      echo "当前值: $(mask_secret_local "$current_value")"
    else
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

    if [[ "$key" == "CONTROLLER_PORT" ]]; then
      if ! [[ "$new_value" =~ ^[0-9]+$ ]] || (( new_value < 1 || new_value > 65535 )); then
        warn "端口无效，需为 1-65535。"
        continue
      fi
    fi
    if [[ "$key" =~ ^(ENABLE_HTTPS|TRUST_X_FORWARDED_FOR|SECURITY_EVENTS_EXCLUDE_LOCAL|SECURITY_AUTO_BLOCK_ENABLED|API_RATE_LIMIT_ENABLED|SUB_LINK_REQUIRE_SIGNATURE)$ ]]; then
      if [[ "$new_value" != "0" && "$new_value" != "1" ]]; then
        warn "${key} 仅支持 0 或 1。"
        continue
      fi
    fi

    if ! upsert_env_value_local "$key" "$new_value"; then
      err "写入失败：${key}"
      continue
    fi
    msg "已更新 ${key}"

    systemctl restart sb-controller >/dev/null 2>&1 || true
    if [[ "$key" == "BOT_TOKEN" && ( -z "$new_value" || "$new_value" == "__REPLACE_WITH_TELEGRAM_BOT_TOKEN__" ) ]]; then
      systemctl stop sb-bot >/dev/null 2>&1 || true
      warn "BOT_TOKEN 为空/占位，已停止 sb-bot。"
    else
      systemctl restart sb-bot >/dev/null 2>&1 || true
    fi
    msg "已尝试重启 controller/bot 使配置生效。"
    echo ""
  done
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
  local controller_port controller_url controller_public panel_base enable_https https_domain https_acme_email
  local admin_token node_token auth_token bot_token super_admin admin_whitelist port_whitelist
  local caddy_installed caddy_active
  controller_port="$(get_env_value_local CONTROLLER_PORT)"; controller_port="${controller_port:-8080}"
  controller_url="$(get_env_value_local CONTROLLER_URL)"
  controller_public="$(get_env_value_local CONTROLLER_PUBLIC_URL)"
  panel_base="$(get_env_value_local PANEL_BASE_URL)"
  enable_https="$(get_env_value_local ENABLE_HTTPS)"; enable_https="${enable_https:-0}"
  https_domain="$(get_env_value_local HTTPS_DOMAIN)"
  https_acme_email="$(get_env_value_local HTTPS_ACME_EMAIL)"
  admin_token="$(get_env_value_local ADMIN_AUTH_TOKEN)"
  node_token="$(get_env_value_local NODE_AUTH_TOKEN)"
  auth_token="$(get_env_value_local AUTH_TOKEN)"
  bot_token="$(get_env_value_local BOT_TOKEN)"
  super_admin="$(get_env_value_local SUPER_ADMIN_CHAT_IDS)"
  admin_whitelist="$(get_env_value_local ADMIN_API_WHITELIST)"
  port_whitelist="$(get_env_value_local CONTROLLER_PORT_WHITELIST)"
  if systemd_unit_exists "caddy.service"; then
    caddy_installed="是"
  else
    caddy_installed="否"
  fi
  if systemctl is-active caddy >/dev/null 2>&1; then
    caddy_active="active"
  else
    caddy_active="inactive"
  fi

  echo "----- 当前关键配置（含建议） -----"
  echo "CONTROLLER_PORT: ${controller_port}（建议保持 8080）"
  echo "CONTROLLER_URL: ${controller_url:-未设置}（建议同机部署用 http://127.0.0.1:${controller_port}）"
  echo "CONTROLLER_PUBLIC_URL: ${controller_public:-未设置}（建议填节点可访问地址，优先域名）"
  echo "PANEL_BASE_URL: ${panel_base:-未设置}（建议填用户实际访问地址，避免 127.0.0.1）"
  echo "ENABLE_HTTPS: ${enable_https}（1=启用；未切DNS前建议 0）"
  echo "HTTPS_DOMAIN: ${https_domain:-未设置}"
  echo "HTTPS_ACME_EMAIL: ${https_acme_email:-未设置}"
  echo "Caddy组件: 已安装=${caddy_installed} 运行状态=${caddy_active}"
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
  echo "提示：如需查看完整 token，请使用菜单 16 -> 3（有二次确认）。"
}

show_menu() {
  clear
  if [[ "$MENU_VIEW_MODE" == "advanced" ]]; then
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
6. 状态查看（controller/bot + 节点连接统计）
7. 查看日志（controller/bot/归档/运维审计）
8. HTTPS 证书状态（Caddy）
9. HTTPS 证书刷新（重载 Caddy）
10. 迁移：导出迁移包
11. 迁移：导入迁移包
12. 一键验收自检（语法/单测/API）
13. 数据库一致性校验（迁移前建议）
14. 节点同步（默认参数 / Token / 时间）
15. 安全加固向导（token轮换 + 8080收敛）
16. Token 工具（收敛 token / 拆分迁移 / 显示完整）
17. 手动安全清理（过期封禁 + 审计日志）
18. SSH 安全状态总览（只读）
19. SSH 一键安全修复（半自动）
20. 运维快照（导出关键状态）
21. AI诊断包导出（可粘贴给任意AI）
22. 组件自检与自动修复（controller/bot/caddy）
23. 部署参数自检与修复向导（循环到通过）

【系统级操作（谨慎）】
24. 安装/重装（交互配置 + 依赖 + venv + 重启）
25. 更新（git pull + 复用现有配置 + 重启）
26. 深度卸载
27. 退出
========================================
EOF
    echo "视图切换：输入 B 返回简化视图（仅常用项）"
    return
  fi

  cat <<'EOF'
========================================
 sb-bot-panel 管理服务器菜单（简化视图）
========================================
【常用项】
1. 配置（快速默认 / 高级变量向导）
6. 状态查看（含节点连接统计）
7. 查看日志（controller/bot/归档/运维审计）
12. 一键验收自检（语法/单测/API）
14. 节点同步（默认参数 / Token / 时间 / 对齐参数）
15. 安全加固向导（token轮换 + 8080收敛）
16. Token 工具（收敛 token / 拆分迁移 / 显示完整）
20. 运维快照（导出关键状态）
21. AI诊断包导出（可粘贴给任意AI）
22. 组件自检与自动修复（controller/bot/caddy）
23. 部署参数自检与修复向导（循环到通过）
25. 更新（git pull + 复用现有配置 + 重启）
27. 退出
========================================
EOF
  echo "视图切换：输入 A 查看全部功能（高级视图）"
}

do_install() {
  if [[ -f "$INSTALL_SCRIPT" ]]; then
    bash "$INSTALL_SCRIPT"
  else
    err "未找到安装脚本: $INSTALL_SCRIPT"
  fi
}

do_update_reuse_config() {
  if ! update_repo_from_origin_main "$PROJECT_DIR"; then
    err "更新已中止：代码拉取失败。"
    return 1
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
    echo "  4) 参数单项修改（点选一项直接改）"
    local cfg_mode
    read -r -p "请选择 [1/2/3/4]（默认 1）: " cfg_mode
    cfg_mode="${cfg_mode:-1}"
    if [[ "$cfg_mode" == "3" ]]; then
      show_current_config_overview
    elif [[ "$cfg_mode" == "4" ]]; then
      edit_single_config_param
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
  echo ""
  show_node_connection_overview || true
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
  local enable_https https_domain https_email caddy_installed caddy_active
  local cert_file cert_end raw_end days_left cert_status
  local -a issues
  local summary

  enable_https="$(get_env_value_local ENABLE_HTTPS)"; enable_https="${enable_https:-0}"
  https_domain="$(get_env_value_local HTTPS_DOMAIN)"
  https_email="$(get_env_value_local HTTPS_ACME_EMAIL)"

  if systemd_unit_exists "caddy.service"; then
    caddy_installed="是"
  else
    caddy_installed="否"
  fi
  if systemctl is-active caddy >/dev/null 2>&1; then
    caddy_active="运行中"
  else
    caddy_active="未运行"
  fi

  cert_file=""
  cert_end=""
  days_left=""
  cert_status="未知"
  if [[ -n "$https_domain" ]]; then
    cert_file="$(find /var/lib/caddy -type f \( -name "*.crt" -o -name "*.pem" \) 2>/dev/null | grep -F "$https_domain" | head -n1 || true)"
    if [[ -n "$cert_file" && -f "$cert_file" ]]; then
      raw_end="$(openssl x509 -in "$cert_file" -noout -enddate 2>/dev/null | sed 's/^notAfter=//')"
      if [[ -n "$raw_end" ]]; then
        cert_end="$(date -d "$raw_end" '+%Y-%m-%d %H:%M:%S' 2>/dev/null || echo "$raw_end")"
        if date -d "$raw_end" +%s >/dev/null 2>&1; then
          days_left="$(( ( $(date -d "$raw_end" +%s) - $(date +%s) ) / 86400 ))"
          if (( days_left < 0 )); then
            cert_status="已过期"
          elif (( days_left <= 7 )); then
            cert_status="即将过期"
          else
            cert_status="正常"
          fi
        fi
      fi
    fi
  fi

  issues=()
  if [[ "$enable_https" != "1" ]]; then
    issues+=("HTTPS 未启用")
  fi
  if [[ -z "$https_domain" ]]; then
    issues+=("未配置 HTTPS_DOMAIN")
  fi
  if [[ "$caddy_installed" != "是" ]]; then
    issues+=("未安装 caddy")
  fi
  if [[ "$caddy_active" != "运行中" ]]; then
    issues+=("caddy 未运行")
  fi
  if [[ -n "$https_domain" && -z "$cert_file" ]]; then
    issues+=("未检测到证书文件")
  fi
  if [[ -n "$days_left" && "$cert_status" != "正常" ]]; then
    issues+=("证书${cert_status}")
  fi

  if (( ${#issues[@]} == 0 )); then
    summary="正常"
  else
    summary="异常（${#issues[@]} 项）"
  fi

  echo "----- HTTPS 证书状态（强判定）-----"
  echo "结论: ${summary}"
  echo "ENABLE_HTTPS: ${enable_https}（1=启用）"
  echo "HTTPS_DOMAIN: ${https_domain:-未设置}"
  echo "HTTPS_ACME_EMAIL: ${https_email:-未设置}"
  echo "Caddy 组件: 已安装=${caddy_installed} 运行状态=${caddy_active}"
  if [[ -n "$https_domain" ]]; then
    if [[ -n "$cert_file" ]]; then
      echo "证书到期: ${cert_end:-未知}"
      if [[ -n "$days_left" ]]; then
        echo "剩余天数: ${days_left} 天（${cert_status}）"
      else
        echo "证书状态: ${cert_status}"
      fi
    else
      echo "证书状态: 未找到"
    fi
  fi

  if (( ${#issues[@]} > 0 )); then
    echo ""
    echo "问题清单："
    for issue in "${issues[@]}"; do
      echo "  - ${issue}"
    done
  fi

  echo ""
  echo "建议处理："
  echo "  1) ENABLE_HTTPS=1 且 HTTPS_DOMAIN 设置正确"
  echo "  2) 确认域名解析指向本机公网 IP，且 443 端口放行"
  echo "  3) 执行菜单 22（组件自检与自动修复）"

  echo ""
  echo "提示：如需详细日志，请手动执行："
  echo "  journalctl -u caddy -n 120 --no-pager"
}

reload_https_cert() {
  if ! systemd_unit_exists "caddy.service"; then
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
    warn "未检测到 ${env_file}，建议先执行菜单 24（安装/重装）完成初始化。"
    return 1
  fi

  if [[ ! -f "${PROJECT_DIR}/venv/bin/python3" ]]; then
    warn "未检测到 venv Python：${PROJECT_DIR}/venv/bin/python3"
    need_repair=1
  else
    msg "venv Python 已存在。"
  fi

  if ! systemd_unit_exists "sb-controller.service"; then
    warn "未检测到 sb-controller.service"
    need_repair=1
  else
    msg "sb-controller.service 已安装。"
  fi

  if ! systemd_unit_exists "sb-bot.service"; then
    warn "未检测到 sb-bot.service"
    need_repair=1
  else
    msg "sb-bot.service 已安装。"
  fi

  if [[ "$enable_https" == "1" ]]; then
    if ! command -v caddy >/dev/null 2>&1 || ! systemd_unit_exists "caddy.service"; then
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
  local answer remove_common_pkgs migrate_dir log_archive_dir s_ui_path
  local caddy_pkg_installed

  warn "将执行深度卸载（管理服务器）：controller/bot/caddy/快捷命令/项目目录/备份目录。"
  warn "该操作不可逆，建议先确认迁移包与备份已保存到外部位置。"
  read -r -p "确认继续深度卸载？[y/N]: " answer
  answer="${answer:-N}"
  if [[ ! "$answer" =~ ^[Yy]$ ]]; then
    warn "已取消。"
    return
  fi

  migrate_dir="$(get_env_value_local MIGRATE_DIR)"
  migrate_dir="${migrate_dir:-/var/backups/sb-migrate}"
  log_archive_dir="$(get_env_value_local LOG_ARCHIVE_DIR)"
  log_archive_dir="${log_archive_dir:-/var/backups/sb-controller/logs}"

  systemctl stop sb-bot 2>/dev/null || true
  systemctl stop sb-controller 2>/dev/null || true
  systemctl disable sb-bot 2>/dev/null || true
  systemctl disable sb-controller 2>/dev/null || true
  if systemd_unit_exists "caddy.service"; then
    systemctl stop caddy 2>/dev/null || true
    systemctl disable caddy 2>/dev/null || true
  fi
  rm -f /etc/systemd/system/sb-bot.service
  rm -f /etc/systemd/system/sb-controller.service
  rm -f /etc/caddy/Caddyfile
  systemctl daemon-reload
  msg "服务与 systemd 单元已移除。"

  rm -f /usr/local/bin/sb-admin
  if [[ -f /usr/local/bin/sb-bot-panel ]] && grep -q "sb-bot-panel-main-shortcut" /usr/local/bin/sb-bot-panel 2>/dev/null; then
    rm -f /usr/local/bin/sb-bot-panel
  fi
  s_ui_path="$(command -v s-ui || true)"
  if [[ "$s_ui_path" == "/usr/local/bin/s-ui" ]] && grep -q "sb-bot-panel-admin-shortcut" /usr/local/bin/s-ui 2>/dev/null; then
    rm -f /usr/local/bin/s-ui
  fi
  msg "菜单快捷命令已清理。"

  if [[ -d "$PROJECT_DIR" && "$PROJECT_DIR" == */sb-bot-panel ]]; then
    rm -rf "$PROJECT_DIR"
  else
    warn "已跳过删除非常规项目目录：${PROJECT_DIR}"
  fi
  rm -rf /var/backups/sb-controller
  if [[ -n "$migrate_dir" && "$migrate_dir" == /var/backups/* ]]; then
    rm -rf "$migrate_dir"
  else
    warn "已跳过删除非常规 MIGRATE_DIR：${migrate_dir}"
  fi
  if [[ -n "$log_archive_dir" && "$log_archive_dir" == /var/backups/* ]]; then
    rm -rf "$log_archive_dir"
  else
    warn "已跳过删除非常规 LOG_ARCHIVE_DIR：${log_archive_dir}"
  fi
  msg "项目目录与备份归档目录已清理。"

  caddy_pkg_installed=0
  if dpkg -s caddy >/dev/null 2>&1; then
    caddy_pkg_installed=1
  fi
  if (( caddy_pkg_installed == 1 )); then
    if confirm_action "检测到 caddy 包，是否一并卸载？" "Y"; then
      export DEBIAN_FRONTEND=noninteractive
      apt-get purge -y caddy >/dev/null 2>&1 || true
      apt-get autoremove -y >/dev/null 2>&1 || true
      msg "caddy 包已卸载。"
    else
      warn "已保留 caddy 包。"
    fi
  fi

  if confirm_action "是否额外卸载通用系统依赖（python3.11/python3.11-venv/git/curl/jq/ufw/fail2ban）？" "N"; then
    export DEBIAN_FRONTEND=noninteractive
    apt-get purge -y python3.11 python3.11-venv git curl jq ufw fail2ban >/dev/null 2>&1 || true
    apt-get autoremove -y >/dev/null 2>&1 || true
    msg "通用依赖卸载命令已执行。"
  fi

  msg "管理服务器深度卸载完成。"
}

main() {
  require_root

  while true; do
    show_menu
    local action_prompt
    if [[ "$MENU_VIEW_MODE" == "advanced" ]]; then
      action_prompt="请输入选项 [1-27/b]: "
    else
      action_prompt="请输入选项 [常用编号/a]: "
    fi
    read -r -p "$action_prompt" action
    case "$action" in
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
        echo "  3) 查看完整 token（高风险，二次确认）"
        read -r -p "请输入 [1/2/3]（默认 1）: " token_action
        token_action="${token_action:-1}"
        if [[ "$token_action" == "3" ]]; then
          show_full_auth_tokens
        elif [[ "$token_action" == "2" ]]; then
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
        if [[ -x "$RUNTIME_SELF_CHECK_SCRIPT" ]]; then
          bash "$RUNTIME_SELF_CHECK_SCRIPT"
        else
          err "未找到部署参数自检脚本: $RUNTIME_SELF_CHECK_SCRIPT"
        fi
        pause
        ;;
      24)
        if confirm_action "确认执行安装/重装？（会进入交互配置）" "N"; then
          do_install
        else
          warn "已取消安装/重装。"
        fi
        pause
        ;;
      25)
        if confirm_action "确认执行更新？（自动复用现有 .env 参数）" "Y"; then
          if ! do_update_reuse_config; then
            warn "更新失败，请先处理上述报错后重试。"
          fi
        else
          warn "已取消更新。"
        fi
        pause
        ;;
      26)
        do_uninstall
        pause
        ;;
      27)
        msg "已退出。"
        exit 0
        ;;
      *)
        if [[ "$MENU_VIEW_MODE" == "advanced" ]]; then
          warn "无效选项，请输入 1-27 或 b。"
        else
          warn "无效选项，请输入简化视图中的编号，或输入 a 切换高级视图。"
        fi
        pause
        ;;
    esac
  done
}

main "$@"
