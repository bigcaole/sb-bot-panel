#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR_DEFAULT="/root/sb-bot-panel"
PROJECT_DIR="${PROJECT_DIR:-$PROJECT_DIR_DEFAULT}"
ENV_FILE="${PROJECT_DIR}/.env"
SCRIPT_ACTOR="db-check"
AI_CONTEXT_SCRIPT="${PROJECT_DIR}/scripts/admin/ai_context_export.sh"
AI_CONTEXT_ON_FAIL="${DB_CHECK_EXPORT_AI_CONTEXT_ON_FAIL:-1}"
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
  ai_context_path="/tmp/sb-db-check-ai-context-on-fail-$(date +%Y%m%d-%H%M%S).md"
  if bash "$AI_CONTEXT_SCRIPT" --output "$ai_context_path" >/tmp/sb_db_check_ai_export.log 2>&1; then
    echo "失败辅助诊断包：${ai_context_path}"
    echo "提示：可将该文件整体粘贴给任意 AI 做继续定位。"
  else
    warn "自动导出 AI 诊断包失败（不影响原始失败结论），可手动执行: bash scripts/admin/ai_context_export.sh"
    cat /tmp/sb_db_check_ai_export.log || true
  fi
}

on_script_exit() {
  local code=$?
  if (( code != 0 )); then
    emit_ai_context_on_failure
  fi
}

trap on_script_exit EXIT

require_tool() {
  local tool="$1"
  if ! command -v "$tool" >/dev/null 2>&1; then
    err "缺少命令: $tool"
    exit 1
  fi
}

load_env() {
  if [[ -f "$ENV_FILE" ]]; then
    # shellcheck disable=SC1090
    set -a; . "$ENV_FILE"; set +a
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
  local api_base_url="$1"
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
      "${api_base_url%/}/admin/security/status" || true)"
    if [[ "$code" == "200" ]]; then
      echo "$item"
      return 0
    fi
  done

  echo "${candidates[0]}"
  return 1
}

build_auth_header() {
  local token_raw token
  token_raw="${ADMIN_AUTH_TOKEN:-${AUTH_TOKEN:-${NODE_AUTH_TOKEN:-}}}"
  token="$(pick_working_auth_token "${CONTROLLER_URL:-http://127.0.0.1:8080}" "$token_raw")" || {
    warn "管理 token 多值模式下未探测到可用 token，回退使用第一个 token。"
  }
  if [[ -z "$token" ]]; then
    token="$(first_auth_token "$token_raw")"
  fi
  if [[ -n "$token" ]]; then
    echo "Authorization: Bearer ${token}"
  fi
}

api_post() {
  local endpoint="$1"
  local payload="$2"
  local url="${CONTROLLER_URL%/}${endpoint}"
  local auth_header
  auth_header="$(build_auth_header)"
  if [[ -n "$auth_header" ]]; then
    curl -fsSL -X POST "$url" \
      -H "$auth_header" \
      -H "X-Actor: ${SCRIPT_ACTOR}" \
      -H "Content-Type: application/json" \
      -d "$payload"
  else
    curl -fsSL -X POST "$url" \
      -H "X-Actor: ${SCRIPT_ACTOR}" \
      -H "Content-Type: application/json" \
      -d "$payload"
  fi
}

api_get() {
  local endpoint="$1"
  local url="${CONTROLLER_URL%/}${endpoint}"
  local auth_header
  auth_header="$(build_auth_header)"
  if [[ -n "$auth_header" ]]; then
    curl -fsSL "$url" -H "$auth_header" -H "X-Actor: ${SCRIPT_ACTOR}"
  else
    curl -fsSL "$url" -H "X-Actor: ${SCRIPT_ACTOR}"
  fi
}

write_ops_audit_event() {
  local action="$1"
  local detail_json="$2"
  local payload
  if command -v jq >/dev/null 2>&1; then
    payload="$(jq -nc \
      --arg action "$action" \
      --argjson detail "$detail_json" \
      '{action:$action, resource_type:"db", resource_id:"consistency", detail:$detail}')"
  else
    payload="{\"action\":\"${action}\",\"resource_type\":\"db\",\"resource_id\":\"consistency\"}"
  fi
  api_post "/admin/audit/event" "$payload" >/dev/null 2>&1 || true
}

main() {
  require_tool curl
  require_tool jq
  load_env

  CONTROLLER_URL="${CONTROLLER_URL:-http://127.0.0.1:8080}"
  local token_raw
  token_raw="${ADMIN_AUTH_TOKEN:-${AUTH_TOKEN:-${NODE_AUTH_TOKEN:-}}}"
  if [[ -n "$token_raw" && "$token_raw" == *","* ]]; then
    warn "检测到管理 token 多值模式，脚本会自动优先选取可用 token。"
  fi
  msg "controller: ${CONTROLLER_URL}"

  msg "1) 创建逻辑导出快照..."
  local export_json export_path
  export_json="$(api_post "/admin/db/export" "{}")"
  export_path="$(echo "$export_json" | jq -r '.path // ""')"
  if [[ -z "$export_path" || "$export_path" == "null" ]]; then
    err "导出失败：未返回 path"
    echo "$export_json"
    exit 1
  fi
  msg "导出完成: ${export_path}"

  msg "2) 校验导出快照并与当前数据库比对..."
  local verify_json verify_ok mismatch_count
  verify_json="$(api_post "/admin/db/verify_export" "{\"path\":\"${export_path}\",\"compare_live\":true}")"
  verify_ok="$(echo "$verify_json" | jq -r '.ok')"
  mismatch_count="$(echo "$verify_json" | jq -r '(.mismatches // []) | length')"
  if [[ "$verify_ok" != "true" ]]; then
    err "一致性校验失败（mismatches=${mismatch_count}）"
    echo "$verify_json" | jq
    exit 2
  fi
  msg "一致性校验通过。"

  msg "3) 执行 SQLite 完整性检查..."
  local integrity_json integrity_ok
  integrity_json="$(api_get "/admin/db/integrity")"
  integrity_ok="$(echo "$integrity_json" | jq -r '.ok')"
  if [[ "$integrity_ok" != "true" ]]; then
    err "SQLite 完整性检查失败。"
    echo "$integrity_json" | jq
    exit 3
  fi
  msg "SQLite 完整性检查通过。"

  write_ops_audit_event "ops.db_consistency.check" "$(jq -nc \
    --arg path "$export_path" \
    --arg compare_live "true" \
    '{path:$path, compare_live:($compare_live=="true"), result:"ok"}')"

  echo ""
  msg "数据库迁移前置校验完成。"
  echo "$verify_json" | jq '{ok, path, snapshot_valid, compare_live, live_match, mismatch_count: ((.mismatches // []) | length)}'
}

main "$@"
