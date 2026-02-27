#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR_DEFAULT="/root/sb-bot-panel"
PROJECT_DIR="${PROJECT_DIR:-$PROJECT_DIR_DEFAULT}"
ENV_FILE="${PROJECT_DIR}/.env"
SCRIPT_ACTOR="db-check"

msg() { echo -e "\033[1;32m[信息]\033[0m $*"; }
warn() { echo -e "\033[1;33m[警告]\033[0m $*"; }
err() { echo -e "\033[1;31m[错误]\033[0m $*" >&2; }

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

build_auth_header() {
  local token
  token="$(first_auth_token "${AUTH_TOKEN:-}")"
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

main() {
  require_tool curl
  require_tool jq
  load_env

  CONTROLLER_URL="${CONTROLLER_URL:-http://127.0.0.1:8080}"
  if [[ -n "${AUTH_TOKEN:-}" && "${AUTH_TOKEN:-}" == *","* ]]; then
    warn "检测到 AUTH_TOKEN 多 token 过渡模式，脚本将使用第一个 token。"
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

  echo ""
  msg "数据库迁移前置校验完成。"
  echo "$verify_json" | jq '{ok, path, snapshot_valid, compare_live, live_match, mismatch_count: ((.mismatches // []) | length)}'
}

main "$@"
