#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-/root/sb-bot-panel}"
ENV_FILE="${PROJECT_DIR}/.env"
SCRIPT_ACTOR="harden-security"

msg() { echo -e "\033[1;32m[信息]\033[0m $*"; }
warn() { echo -e "\033[1;33m[警告]\033[0m $*"; }
err() { echo -e "\033[1;31m[错误]\033[0m $*" >&2; }

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

sync_node_tokens_after_rotation() {
  local controller_port="$1"
  local auth_token_raw="$2"
  local token=""
  local response=""
  local body=""
  local http_code=""
  local selected=""
  local created=""
  local deduplicated=""
  local failed=""

  token="$(pick_working_auth_token "$controller_port" "$auth_token_raw")" || {
    warn "AUTH_TOKEN 多值模式下未探测到可用 token，回退使用第一个 token 执行节点同步。"
  }
  if [[ -z "$token" ]]; then
    token="$(first_auth_token "$auth_token_raw")"
  fi
  if [[ -z "$token" ]]; then
    warn "自动同步节点 token 已跳过：AUTH_TOKEN 为空。"
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
  local whitelist_current
  controller_port="$(get_env_value CONTROLLER_PORT)"
  controller_port="${controller_port:-8080}"
  enable_https="$(get_env_value ENABLE_HTTPS)"
  enable_https="${enable_https:-0}"
  auth_token_raw="$(get_env_value AUTH_TOKEN)"
  whitelist_current="$(get_env_value CONTROLLER_PORT_WHITELIST)"

  msg "开始执行管理面安全加固向导。"
  echo "当前端口: ${controller_port}"
  echo "当前白名单: ${whitelist_current:-（空）}"
  echo ""

  local rotate_token="Y"
  local new_token=""
  local final_auth_token="$auth_token_raw"
  read -r -p "是否轮换 AUTH_TOKEN（推荐）？[Y/n]: " rotate_token
  rotate_token="${rotate_token:-Y}"
  if [[ "$rotate_token" =~ ^[Yy]$ ]]; then
    new_token="$(generate_auth_token)"
    if [[ -n "$auth_token_raw" ]]; then
      final_auth_token="${new_token},${auth_token_raw}"
    else
      final_auth_token="$new_token"
    fi
    set_env_value "AUTH_TOKEN" "$final_auth_token"
    msg "已写入新 AUTH_TOKEN（过渡模式保留旧 token）。"
  fi

  local mode_choice="1"
  local mode="whitelist"
  local whitelist_input=""
  local whitelist_final=""
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

  apply_ufw_rules "$controller_port" "$mode" "$whitelist_final" "$enable_https"

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
  echo "AUTH_TOKEN: $(if [[ -n "$final_auth_token" ]]; then echo '已更新'; else echo '未变更'; fi)"
  echo "8080 策略: ${mode}"
  echo "8080 白名单: ${whitelist_final:-（空）}"
  if [[ -n "$new_token" ]]; then
    sync_node_tokens_after_rotation "$controller_port" "$final_auth_token"
    echo ""
    warn "当前为 token 过渡模式（新,旧）。节点完成更新后，请将 AUTH_TOKEN 收敛为新 token。"
    echo "建议后续收敛为："
    echo "  AUTH_TOKEN=${new_token}"
  fi
}

main "$@"
