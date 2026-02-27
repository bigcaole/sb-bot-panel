#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-/root/sb-bot-panel}"
ENV_FILE="${PROJECT_DIR}/.env"
AUTO_YES=0
SCRIPT_ACTOR="token-collapse"

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

parse_primary_token() {
  local raw="$1"
  local item token
  IFS=',' read -r -a items <<<"$raw"
  for item in "${items[@]}"; do
    token="$(echo "$item" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
    if [[ -n "$token" ]]; then
      echo "$token"
      return
    fi
  done
  echo ""
}

post_ops_audit_event() {
  local controller_port="$1"
  local token="$2"
  local action="$3"
  local detail_json="$4"
  local payload

  if command -v jq >/dev/null 2>&1; then
    payload="$(jq -nc \
      --arg action "$action" \
      --argjson detail "$detail_json" \
      '{action:$action, resource_type:"security", resource_id:"controller", detail:$detail}')"
  else
    payload="{\"action\":\"${action}\",\"resource_type\":\"security\",\"resource_id\":\"controller\"}"
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

sync_node_tokens_after_collapse() {
  local controller_port="$1"
  local token="$2"
  local response=""
  local body=""
  local http_code=""
  local selected=""
  local created=""
  local deduplicated=""
  local failed=""

  if [[ -z "$token" ]]; then
    warn "自动同步节点 token 已跳过：收敛后 token 为空。"
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
    warn "收敛后自动同步节点 token 失败（HTTP ${http_code:-unknown}）。可在 sb-admin 菜单手动同步。"
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
    msg "收敛后已自动同步节点 token：目标=${selected} 新建=${created} 去重=${deduplicated} 失败=${failed}"
  else
    msg "收敛后已自动触发节点 token 同步。"
  fi
}

main() {
  local arg
  for arg in "$@"; do
    case "$arg" in
      --yes|-y)
        AUTO_YES=1
        ;;
      *)
        err "未知参数：$arg"
        err "用法：bash scripts/admin/auth_token_collapse.sh [--yes]"
        exit 1
        ;;
    esac
  done

  require_root
  if [[ ! -f "$ENV_FILE" ]]; then
    err "未找到环境文件：$ENV_FILE"
    exit 1
  fi

  local auth_raw controller_port primary_token
  auth_raw="$(get_env_value AUTH_TOKEN)"
  controller_port="$(get_env_value CONTROLLER_PORT)"
  controller_port="${controller_port:-8080}"
  primary_token="$(parse_primary_token "$auth_raw")"

  if [[ -z "$primary_token" ]]; then
    warn "AUTH_TOKEN 为空，当前无需收敛。"
    exit 0
  fi

  if [[ "$auth_raw" != *","* ]]; then
    msg "AUTH_TOKEN 已是单值，无需收敛。"
    if wait_for_controller_ready "$controller_port" 15; then
      sync_node_tokens_after_collapse "$controller_port" "$primary_token"
      post_ops_audit_event "$controller_port" "$primary_token" "ops.auth_token_collapse.noop" '{"mode":"single_token_noop"}'
    else
      warn "controller 未就绪，已跳过节点 token 对齐同步。"
    fi
    exit 0
  fi

  if [[ "$AUTO_YES" -ne 1 ]]; then
    echo "当前 AUTH_TOKEN 为多值过渡模式。"
    echo "即将收敛为首个 token（其余旧 token 会移除）。"
    read -r -p "确认继续？[y/N]: " confirm
    confirm="${confirm:-N}"
    if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
      warn "已取消。"
      exit 0
    fi
  fi

  local backup_file
  backup_file="${ENV_FILE}.bak-$(date +%Y%m%d-%H%M%S)"
  cp "$ENV_FILE" "$backup_file"
  set_env_value "AUTH_TOKEN" "$primary_token"

  msg "已更新 AUTH_TOKEN 为单值，并备份旧 .env：$backup_file"
  systemctl restart sb-controller
  systemctl restart sb-bot

  if wait_for_controller_ready "$controller_port" 30; then
    msg "收敛完成：controller 已就绪（127.0.0.1:${controller_port}）。"
    sync_node_tokens_after_collapse "$controller_port" "$primary_token"
    post_ops_audit_event "$controller_port" "$primary_token" "ops.auth_token_collapse.apply" '{"mode":"collapsed"}'
  else
    err "controller 启动超时，请检查日志：journalctl -u sb-controller -n 120 --no-pager"
    exit 1
  fi
}

main "$@"
