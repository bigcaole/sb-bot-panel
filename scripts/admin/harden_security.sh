#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-/root/sb-bot-panel}"
ENV_FILE="${PROJECT_DIR}/.env"
SCRIPT_ACTOR="harden-security"
AI_CONTEXT_SCRIPT="${PROJECT_DIR}/scripts/admin/ai_context_export.sh"
AI_CONTEXT_ON_FAIL="${HARDEN_EXPORT_AI_CONTEXT_ON_FAIL:-1}"
AI_CONTEXT_EXPORTED=0

msg() { echo -e "\033[1;32m[信息]\033[0m $*"; }
warn() { echo -e "\033[1;33m[警告]\033[0m $*"; }
err() { echo -e "\033[1;31m[错误]\033[0m $*" >&2; }

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
  ai_context_path="/tmp/sb-harden-security-ai-context-on-fail-$(date +%Y%m%d-%H%M%S).md"
  if bash "$AI_CONTEXT_SCRIPT" --output "$ai_context_path" >/tmp/sb_harden_ai_export.log 2>&1; then
    echo "失败辅助诊断包：${ai_context_path}"
    echo "提示：可将该文件整体粘贴给任意 AI 做继续定位。"
  else
    warn "自动导出 AI 诊断包失败（不影响原始失败结论），可手动执行: bash scripts/admin/ai_context_export.sh"
    cat /tmp/sb_harden_ai_export.log || true
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

get_env_value() {
  local key="$1"
  if [[ ! -f "$ENV_FILE" ]]; then
    echo ""
    return
  fi
  grep -E "^${key}=" "$ENV_FILE" | head -n1 | cut -d= -f2- || true
}

set_env_value() {
  local key="$1"
  local value="$2"
  local escaped
  escaped="$(printf '%s' "$value" | sed 's/[&|]/\\&/g')"
  if grep -qE "^${key}=" "$ENV_FILE"; then
    sed -i "s|^${key}=.*|${key}=${escaped}|" "$ENV_FILE"
  else
    echo "${key}=${value}" >>"$ENV_FILE"
  fi
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
    token="${token:0:48}"
  fi
  if [[ -z "$token" ]]; then
    token="token$(date +%s)"
  fi
  echo "$token"
}

wait_for_controller_ready() {
  local controller_port="$1"
  local timeout_seconds="${2:-30}"
  local i
  for i in $(seq 1 "$timeout_seconds"); do
    if curl -fsSL --max-time 3 "http://127.0.0.1:${controller_port}/health" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
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

normalize_whitelist_csv() {
  local raw="$1"
  local result=""
  IFS=',' read -r -a items <<<"$raw"
  for item in "${items[@]}"; do
    local ip
    ip="$(echo "$item" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
    [[ -z "$ip" ]] && continue
    if [[ -z "$result" ]]; then
      result="$ip"
    else
      result="${result},${ip}"
    fi
  done
  echo "$result"
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

csv_contains_item() {
  local csv="$1"
  local target="$2"
  local normalized_target
  normalized_target="$(echo "$target" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
  [[ -z "$normalized_target" ]] && return 1
  IFS=',' read -r -a items <<<"$csv"
  local item
  for item in "${items[@]}"; do
    item="$(echo "$item" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
    if [[ "$item" == "$normalized_target" ]]; then
      return 0
    fi
  done
  return 1
}

append_csv_item() {
  local csv="$1"
  local item="$2"
  local normalized_csv
  local normalized_item
  normalized_csv="$(normalize_whitelist_csv "$csv")"
  normalized_item="$(echo "$item" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
  if [[ -z "$normalized_item" ]]; then
    echo "$normalized_csv"
    return
  fi
  if csv_contains_item "$normalized_csv" "$normalized_item"; then
    echo "$normalized_csv"
    return
  fi
  if [[ -z "$normalized_csv" ]]; then
    echo "$normalized_item"
  else
    echo "${normalized_csv},${normalized_item}"
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

post_ops_audit_event() {
  local controller_port="$1"
  local token="$2"
  local action="$3"
  local resource_type="$4"
  local resource_id="$5"
  local detail_json="$6"
  local payload

  if command -v jq >/dev/null 2>&1; then
    payload="$(jq -nc \
      --arg action "$action" \
      --arg resource_type "$resource_type" \
      --arg resource_id "$resource_id" \
      --argjson detail "$detail_json" \
      '{action:$action, resource_type:$resource_type, resource_id:$resource_id, detail:$detail}')"
  else
    payload="{\"action\":\"${action}\",\"resource_type\":\"${resource_type}\",\"resource_id\":\"${resource_id}\"}"
  fi

  if [[ -n "$token" ]]; then
    curl -fsSL --max-time 8 -X POST \
      "http://127.0.0.1:${controller_port}/admin/audit/event" \
      -H "Authorization: Bearer ${token}" \
      -H "X-Actor: ${SCRIPT_ACTOR}" \
      -H "Content-Type: application/json" \
      -d "$payload" >/dev/null 2>&1 || true
  else
    curl -fsSL --max-time 8 -X POST \
      "http://127.0.0.1:${controller_port}/admin/audit/event" \
      -H "X-Actor: ${SCRIPT_ACTOR}" \
      -H "Content-Type: application/json" \
      -d "$payload" >/dev/null 2>&1 || true
  fi
}

sync_node_tokens_after_rotation() {
  local controller_port="$1"
  local admin_token_raw="$2"
  local token=""
  local response=""
  local body=""
  local http_code=""
  local selected=""
  local created=""
  local deduplicated=""
  local failed=""

  token="$(pick_working_auth_token "$controller_port" "$admin_token_raw")" || {
    warn "管理 token 多值模式下未探测到可用 token，回退使用第一个 token 执行节点同步。"
  }
  if [[ -z "$token" ]]; then
    token="$(first_auth_token "$admin_token_raw")"
  fi
  if [[ -z "$token" ]]; then
    warn "自动同步节点 token 已跳过：管理 token 为空。"
    return
  fi

  response="$(
    curl -sS --max-time 15 -X POST \
      "http://127.0.0.1:${controller_port}/admin/auth/sync_node_tokens?include_disabled=1&force_new=1" \
      -H "Authorization: Bearer ${token}" \
      -H "X-Actor: ${SCRIPT_ACTOR}" \
      -H "Content-Type: application/json" \
      -w $'\n%{http_code}' 2>/dev/null || true
  )"
  http_code="${response##*$'\n'}"
  body="${response%$'\n'*}"
  if [[ "$http_code" != "200" ]]; then
    warn "自动同步节点 token 失败（HTTP ${http_code:-unknown}）。可在 sb-admin 菜单手动执行。"
    if [[ -n "$body" ]]; then
      warn "返回：${body}"
    fi
    return
  fi

  if command -v jq >/dev/null 2>&1; then
    selected="$(echo "$body" | jq -r '.selected // 0' 2>/dev/null || echo "0")"
    created="$(echo "$body" | jq -r '.created // 0' 2>/dev/null || echo "0")"
    deduplicated="$(echo "$body" | jq -r '.deduplicated // 0' 2>/dev/null || echo "0")"
    failed="$(echo "$body" | jq -r '.failed // 0' 2>/dev/null || echo "0")"
    msg "已自动同步节点 token：目标=${selected} 新建=${created} 去重=${deduplicated} 失败=${failed}"
  else
    msg "已自动触发节点 token 同步。"
  fi
}

apply_ufw_rules() {
  local controller_port="$1"
  local mode="$2"
  local whitelist_csv="$3"
  local enable_https="$4"

  if ! command -v ufw >/dev/null 2>&1; then
    warn "未检测到 ufw，跳过防火墙规则更新。"
    return
  fi

  if ! ufw status 2>/dev/null | grep -q "Status: active"; then
    read -r -p "UFW 当前未启用，是否立即启用？[Y/n]: " answer
    answer="${answer:-Y}"
    if [[ "$answer" =~ ^[Yy]$ ]]; then
      ufw --force enable >/dev/null
      msg "UFW 已启用。"
    else
      warn "已跳过 UFW 启用。"
    fi
  fi

  ufw allow 22/tcp >/dev/null 2>&1 || true
  if [[ "$enable_https" == "1" ]]; then
    ufw allow 80/tcp >/dev/null 2>&1 || true
    ufw allow 443/tcp >/dev/null 2>&1 || true
  fi

  delete_controller_port_rules "$controller_port"

  if [[ "$mode" == "public" ]]; then
    ufw allow "${controller_port}/tcp" >/dev/null 2>&1 || true
    msg "已放开 ${controller_port}/tcp 公网访问。"
    return
  fi

  if [[ "$mode" == "whitelist" ]]; then
    IFS=',' read -r -a items <<<"$whitelist_csv"
    for ip in "${items[@]}"; do
      [[ -z "${ip:-}" ]] && continue
      ufw allow from "$ip" to any port "$controller_port" proto tcp >/dev/null 2>&1 || true
    done
    msg "已按白名单放行 ${controller_port}/tcp。"
    return
  fi

  msg "已关闭 ${controller_port}/tcp 的公网放行规则。"
}

main() {
  require_root
  if [[ ! -f "$ENV_FILE" ]]; then
    err "未找到环境文件：$ENV_FILE"
    exit 1
  fi

  local controller_port
  local enable_https
  local auth_token_raw
  local admin_auth_raw
  local node_auth_raw
  local whitelist_current
  local admin_api_whitelist_current
  local protect_csv_current
  local current_client_ip
  controller_port="$(get_env_value CONTROLLER_PORT)"
  controller_port="${controller_port:-8080}"
  enable_https="$(get_env_value ENABLE_HTTPS)"
  enable_https="${enable_https:-0}"
  auth_token_raw="$(get_env_value AUTH_TOKEN)"
  admin_auth_raw="$(get_env_value ADMIN_AUTH_TOKEN)"
  node_auth_raw="$(get_env_value NODE_AUTH_TOKEN)"
  whitelist_current="$(get_env_value CONTROLLER_PORT_WHITELIST)"
  admin_api_whitelist_current="$(get_env_value ADMIN_API_WHITELIST)"
  protect_csv_current="$(get_env_value SECURITY_BLOCK_PROTECTED_IPS)"
  current_client_ip="$(detect_current_ssh_client_ip)"

  msg "开始执行管理面安全加固向导。"
  echo "当前端口: ${controller_port}"
  echo "当前白名单: ${whitelist_current:-（空）}"
  echo "管理接口来源白名单: ${admin_api_whitelist_current:-（空）}"
  echo "封禁保护白名单: ${protect_csv_current:-（空）}"
  echo "当前 SSH 来源 IP: ${current_client_ip:-未知}"
  if [[ -n "${admin_auth_raw//[[:space:]]/}" || -n "${node_auth_raw//[[:space:]]/}" ]]; then
    echo "鉴权模式: 拆分优先（ADMIN/NODE）"
  else
    echo "鉴权模式: 兼容模式（AUTH_TOKEN）"
  fi
  echo ""

  local rotate_admin_token="Y"
  local rotate_node_token="Y"
  local rotate_auth_token="N"
  local new_admin_token=""
  local new_node_token=""
  local new_auth_token=""
  local final_admin_auth="$admin_auth_raw"
  local final_node_auth="$node_auth_raw"
  local final_auth_token="$auth_token_raw"
  local fallback_for_admin="$admin_auth_raw"
  local fallback_for_node="$node_auth_raw"
  if [[ -z "${fallback_for_admin//[[:space:]]/}" ]]; then
    fallback_for_admin="$auth_token_raw"
  fi
  if [[ -z "${fallback_for_node//[[:space:]]/}" ]]; then
    fallback_for_node="$auth_token_raw"
  fi

  read -r -p "是否轮换 ADMIN_AUTH_TOKEN（推荐）？[Y/n]: " rotate_admin_token
  rotate_admin_token="${rotate_admin_token:-Y}"
  if [[ "$rotate_admin_token" =~ ^[Yy]$ ]]; then
    new_admin_token="$(generate_auth_token)"
    if [[ -n "${fallback_for_admin//[[:space:]]/}" ]]; then
      final_admin_auth="${new_admin_token},${fallback_for_admin}"
    else
      final_admin_auth="$new_admin_token"
    fi
    set_env_value "ADMIN_AUTH_TOKEN" "$final_admin_auth"
    msg "已写入新 ADMIN_AUTH_TOKEN（过渡模式保留旧 token）。"
  fi

  read -r -p "是否轮换 NODE_AUTH_TOKEN（推荐）？[Y/n]: " rotate_node_token
  rotate_node_token="${rotate_node_token:-Y}"
  if [[ "$rotate_node_token" =~ ^[Yy]$ ]]; then
    new_node_token="$(generate_auth_token)"
    if [[ -n "${fallback_for_node//[[:space:]]/}" ]]; then
      final_node_auth="${new_node_token},${fallback_for_node}"
    else
      final_node_auth="$new_node_token"
    fi
    set_env_value "NODE_AUTH_TOKEN" "$final_node_auth"
    msg "已写入新 NODE_AUTH_TOKEN（过渡模式保留旧 token）。"
  fi

  read -r -p "是否同时轮换兼容 AUTH_TOKEN（可选）？[y/N]: " rotate_auth_token
  rotate_auth_token="${rotate_auth_token:-N}"
  if [[ "$rotate_auth_token" =~ ^[Yy]$ ]]; then
    new_auth_token="$(generate_auth_token)"
    if [[ -n "${auth_token_raw//[[:space:]]/}" ]]; then
      final_auth_token="${new_auth_token},${auth_token_raw}"
    else
      final_auth_token="$new_auth_token"
    fi
    set_env_value "AUTH_TOKEN" "$final_auth_token"
    msg "已写入新 AUTH_TOKEN（过渡模式保留旧 token）。"
  else
    final_auth_token="$(get_env_value AUTH_TOKEN)"
  fi

  local mode_choice="1"
  local mode="whitelist"
  local whitelist_input=""
  local whitelist_final=""
  local admin_api_whitelist_toggle="Y"
  local admin_api_whitelist_input=""
  local admin_api_whitelist_final=""
  echo "请选择 8080 访问策略："
  echo "  1) 白名单放行（推荐）"
  echo "  2) 公网放行（不推荐）"
  echo "  3) 完全关闭（节点需走 443/反代）"
  read -r -p "请输入选项 [1/2/3]（默认 1）: " mode_choice
  mode_choice="${mode_choice:-1}"
  case "$mode_choice" in
    1)
      mode="whitelist"
      read -r -p "请输入允许访问 ${controller_port} 的 IP/CIDR（逗号分隔） [${whitelist_current}]: " whitelist_input
      whitelist_input="${whitelist_input:-$whitelist_current}"
      whitelist_final="$(normalize_whitelist_csv "$whitelist_input")"
      if [[ -n "$current_client_ip" ]] && ! csv_contains_item "$whitelist_final" "$current_client_ip"; then
        warn "当前 SSH 来源 IP(${current_client_ip}) 不在 controller 白名单中。"
        read -r -p "是否自动追加当前来源 IP 到白名单？[Y/n]: " answer
        answer="${answer:-Y}"
        if [[ "$answer" =~ ^[Yy]$ ]]; then
          whitelist_final="$(append_csv_item "$whitelist_final" "$current_client_ip")"
          msg "已追加当前来源 IP 到白名单。"
        fi
      fi
      set_env_value "CONTROLLER_PORT_WHITELIST" "$whitelist_final"
      ;;
    2)
      mode="public"
      whitelist_final=""
      set_env_value "CONTROLLER_PORT_WHITELIST" ""
      ;;
    3)
      mode="closed"
      whitelist_final=""
      set_env_value "CONTROLLER_PORT_WHITELIST" ""
      ;;
    *)
      warn "无效选项，按白名单模式处理。"
      mode="whitelist"
      whitelist_final="$(normalize_whitelist_csv "$whitelist_current")"
      set_env_value "CONTROLLER_PORT_WHITELIST" "$whitelist_final"
      ;;
  esac

  admin_api_whitelist_final="$(normalize_whitelist_csv "$admin_api_whitelist_current")"
  read -r -p "是否启用管理接口来源白名单 ADMIN_API_WHITELIST（推荐）？[Y/n]: " admin_api_whitelist_toggle
  admin_api_whitelist_toggle="${admin_api_whitelist_toggle:-Y}"
  if [[ "$admin_api_whitelist_toggle" =~ ^[Yy]$ ]]; then
    local default_admin_api_whitelist
    default_admin_api_whitelist="$admin_api_whitelist_final"
    if [[ -z "${default_admin_api_whitelist//[[:space:]]/}" && -n "$current_client_ip" ]]; then
      default_admin_api_whitelist="$current_client_ip"
    fi
    read -r -p "请输入允许访问管理接口的 IP/CIDR（逗号分隔） [${default_admin_api_whitelist}]: " admin_api_whitelist_input
    admin_api_whitelist_input="${admin_api_whitelist_input:-$default_admin_api_whitelist}"
    admin_api_whitelist_final="$(normalize_whitelist_csv "$admin_api_whitelist_input")"
    if [[ -n "$current_client_ip" ]] && ! csv_contains_item "$admin_api_whitelist_final" "$current_client_ip"; then
      warn "当前 SSH 来源 IP(${current_client_ip}) 不在 ADMIN_API_WHITELIST 中。"
      read -r -p "是否自动追加当前来源 IP 到 ADMIN_API_WHITELIST？[Y/n]: " answer
      answer="${answer:-Y}"
      if [[ "$answer" =~ ^[Yy]$ ]]; then
        admin_api_whitelist_final="$(append_csv_item "$admin_api_whitelist_final" "$current_client_ip")"
        msg "已追加当前来源 IP 到 ADMIN_API_WHITELIST。"
      fi
    fi
    set_env_value "ADMIN_API_WHITELIST" "$admin_api_whitelist_final"
  else
    admin_api_whitelist_final=""
    set_env_value "ADMIN_API_WHITELIST" ""
    warn "已关闭 ADMIN_API_WHITELIST（管理接口仅依赖鉴权 token）。"
  fi

  apply_ufw_rules "$controller_port" "$mode" "$whitelist_final" "$enable_https"

  if [[ -n "$current_client_ip" ]]; then
    if ! csv_contains_item "$protect_csv_current" "$current_client_ip"; then
      protect_csv_current="$(append_csv_item "$protect_csv_current" "$current_client_ip")"
      set_env_value "SECURITY_BLOCK_PROTECTED_IPS" "$protect_csv_current"
      msg "已将当前 SSH 来源 IP(${current_client_ip})加入封禁保护白名单。"
    fi
  fi

  systemctl restart sb-controller
  systemctl restart sb-bot
  if wait_for_controller_ready "$controller_port" 30; then
    msg "controller 已就绪：http://127.0.0.1:${controller_port}/health"
  else
    err "controller 启动超时，请检查日志：journalctl -u sb-controller -n 120 --no-pager"
    exit 1
  fi

  echo ""
  msg "安全加固完成。"
  echo "ADMIN_AUTH_TOKEN: $(if [[ -n "$final_admin_auth" ]]; then echo '已维护'; else echo '未设置'; fi)"
  echo "NODE_AUTH_TOKEN: $(if [[ -n "$final_node_auth" ]]; then echo '已维护'; else echo '未设置'; fi)"
  echo "AUTH_TOKEN: $(if [[ -n "$new_auth_token" ]]; then echo '已轮换'; elif [[ -n "$final_auth_token" ]]; then echo '未轮换'; else echo '未设置'; fi)"
  echo "8080 策略: ${mode}"
  echo "8080 白名单: ${whitelist_final:-（空）}"
  echo "ADMIN_API_WHITELIST: ${admin_api_whitelist_final:-（空）}"
  local sync_admin_raw="$final_admin_auth"
  if [[ -z "${sync_admin_raw//[[:space:]]/}" ]]; then
    sync_admin_raw="$final_auth_token"
  fi
  if [[ -z "${sync_admin_raw//[[:space:]]/}" ]]; then
    sync_admin_raw="$final_node_auth"
  fi
  if [[ -n "$new_admin_token" || -n "$new_node_token" || -n "$new_auth_token" ]]; then
    sync_node_tokens_after_rotation "$controller_port" "$sync_admin_raw"
    echo ""
    warn "当前为 token 过渡模式（新,旧）。节点完成更新后，建议执行收敛脚本转为单值。"
    echo "建议后续执行："
    echo "  bash /root/sb-bot-panel/scripts/admin/auth_token_collapse.sh --yes"
    if [[ -n "$new_admin_token" ]]; then
      echo "  ADMIN_AUTH_TOKEN=${new_admin_token}"
    fi
    if [[ -n "$new_node_token" ]]; then
      echo "  NODE_AUTH_TOKEN=${new_node_token}"
    fi
    if [[ -n "$new_auth_token" ]]; then
      echo "  AUTH_TOKEN=${new_auth_token}"
    fi
  fi

  local ops_token
  local detail_json
  ops_token="$(pick_working_auth_token "$controller_port" "$sync_admin_raw")" || true
  if [[ -z "$ops_token" ]]; then
    ops_token="$(first_auth_token "$sync_admin_raw")"
  fi
  if command -v jq >/dev/null 2>&1; then
    detail_json="$(jq -nc \
      --arg mode "$mode" \
      --arg whitelist "$whitelist_final" \
      --arg admin_api_whitelist "$admin_api_whitelist_final" \
      --arg rotate_admin "$([[ -n "$new_admin_token" ]] && echo true || echo false)" \
      --arg rotate_node "$([[ -n "$new_node_token" ]] && echo true || echo false)" \
      --arg rotate_auth "$([[ -n "$new_auth_token" ]] && echo true || echo false)" \
      '{mode:$mode, whitelist:$whitelist, admin_api_whitelist:$admin_api_whitelist, rotate_admin:($rotate_admin=="true"), rotate_node:($rotate_node=="true"), rotate_auth:($rotate_auth=="true")}')"
  else
    detail_json="{}"
  fi
  post_ops_audit_event "$controller_port" "$ops_token" "ops.harden_security.apply" "security" "controller" "$detail_json"
}

main "$@"
