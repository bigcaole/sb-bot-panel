#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
ENV_FILE="${PROJECT_DIR}/.env"

SNAPSHOT_DIR_DEFAULT="/var/backups/sb-controller/ops-snapshots"
SNAPSHOT_DIR="${SNAPSHOT_DIR:-$SNAPSHOT_DIR_DEFAULT}"
OUTPUT_PATH=""

msg() { echo -e "\033[1;32m[信息]\033[0m $*"; }
warn() { echo -e "\033[1;33m[警告]\033[0m $*"; }
err() { echo -e "\033[1;31m[错误]\033[0m $*" >&2; }

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

single_line() {
  local text="${1:-}"
  text="$(echo "$text" | tr '\r\n' ' ' | tr -s ' ')"
  echo "${text:0:500}"
}

api_code=""
api_body=""
api_get() {
  local path="$1"
  local need_auth="${2:-0}"
  local url="http://127.0.0.1:${CONTROLLER_PORT:-8080}${path}"
  local tmp_file
  tmp_file="$(mktemp)"

  local -a args=("-sS" "--max-time" "8" "-o" "$tmp_file" "-w" "%{http_code}")
  if [[ "$need_auth" == "1" && -n "${AUTH_TOKEN_FIRST:-}" ]]; then
    args+=("-H" "Authorization: Bearer ${AUTH_TOKEN_FIRST}")
  fi
  args+=("$url")

  api_code="$(curl "${args[@]}" 2>/dev/null || true)"
  api_body="$(cat "$tmp_file" 2>/dev/null || true)"
  rm -f "$tmp_file"
  if [[ -z "$api_code" ]]; then
    api_code="000"
  fi
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --output)
        shift
        OUTPUT_PATH="${1:-}"
        ;;
      *)
        err "未知参数: $1"
        err "用法: bash scripts/admin/ops_snapshot.sh [--output /path/to/file]"
        exit 1
        ;;
    esac
    shift || true
  done
}

load_env() {
  if [[ -f "$ENV_FILE" ]]; then
    set -a
    # shellcheck disable=SC1090
    . "$ENV_FILE"
    set +a
  else
    warn "未找到 ${ENV_FILE}，将使用默认端口和空鉴权进行快照。"
  fi
  CONTROLLER_PORT="${CONTROLLER_PORT:-8080}"
  AUTH_TOKEN_FIRST="$(first_auth_token "${AUTH_TOKEN:-}")"
}

main() {
  parse_args "$@"
  load_env

  local now_ts now_human host_name
  now_ts="$(date +%s)"
  now_human="$(date '+%F %T %Z')"
  host_name="$(hostname)"

  local sb_controller_state sb_bot_state caddy_state
  sb_controller_state="$(systemctl is-active sb-controller 2>/dev/null || true)"
  sb_bot_state="$(systemctl is-active sb-bot 2>/dev/null || true)"
  caddy_state="$(systemctl is-active caddy 2>/dev/null || true)"
  [[ -z "$sb_controller_state" ]] && sb_controller_state="unknown"
  [[ -z "$sb_bot_state" ]] && sb_bot_state="unknown"
  [[ -z "$caddy_state" ]] && caddy_state="unknown"

  api_get "/health" 0
  local health_code health_body
  health_code="$api_code"
  health_body="$(single_line "$api_body")"

  api_get "/admin/security/status" 1
  local sec_code sec_body sec_summary
  sec_code="$api_code"
  sec_body="$api_body"
  sec_summary="$(single_line "$sec_body")"
  if [[ "$sec_code" == "200" ]] && command -v jq >/dev/null 2>&1; then
    sec_summary="$(echo "$sec_body" | jq -c '{auth_enabled,auth_token_count,api_rate_limit_enabled,sub_link_require_signature,node_task_max_pending_per_node,warnings}' 2>/dev/null || echo "$sec_summary")"
  fi

  api_get "/admin/overview" 1
  local overview_code overview_body overview_summary
  overview_code="$api_code"
  overview_body="$api_body"
  overview_summary="$(single_line "$overview_body")"
  if [[ "$overview_code" == "200" ]] && command -v jq >/dev/null 2>&1; then
    overview_summary="$(echo "$overview_body" | jq -c '{totals,monitor,task_queue,security_events,warnings}' 2>/dev/null || echo "$overview_summary")"
  fi

  api_get "/admin/node_access/status" 1
  local node_access_code node_access_body node_access_summary
  node_access_code="$api_code"
  node_access_body="$api_body"
  node_access_summary="$(single_line "$node_access_body")"
  if [[ "$node_access_code" == "200" ]] && command -v jq >/dev/null 2>&1; then
    node_access_summary="$(echo "$node_access_body" | jq -c '{enabled_nodes,locked_enabled_nodes,unlocked_enabled_nodes,unlocked_disabled_nodes,whitelist_missing_count}' 2>/dev/null || echo "$node_access_summary")"
  fi

  if [[ -z "$OUTPUT_PATH" ]]; then
    mkdir -p "$SNAPSHOT_DIR"
    OUTPUT_PATH="${SNAPSHOT_DIR}/ops-snapshot-$(date +%Y%m%d-%H%M%S).txt"
  else
    mkdir -p "$(dirname "$OUTPUT_PATH")"
  fi

  local auth_configured="no"
  local token_length="0"
  if [[ -n "${AUTH_TOKEN:-}" ]]; then
    auth_configured="yes"
  fi
  if [[ -n "${AUTH_TOKEN_FIRST:-}" ]]; then
    token_length="${#AUTH_TOKEN_FIRST}"
  fi

  cat >"$OUTPUT_PATH" <<EOF
sb-bot-panel 运维快照
生成时间: ${now_human}
生成时间戳: ${now_ts}
主机: ${host_name}
项目目录: ${PROJECT_DIR}

[服务状态]
sb-controller: ${sb_controller_state}
sb-bot: ${sb_bot_state}
caddy: ${caddy_state}

[API 检查]
controller 端口: ${CONTROLLER_PORT}
/health: HTTP ${health_code} | ${health_body}
/admin/security/status: HTTP ${sec_code} | ${sec_summary}
/admin/overview: HTTP ${overview_code} | ${overview_summary}
/admin/node_access/status: HTTP ${node_access_code} | ${node_access_summary}

[鉴权信息]
AUTH_TOKEN 已配置: ${auth_configured}
AUTH_TOKEN 首个可用token长度: ${token_length}
EOF

  msg "运维快照已生成: ${OUTPUT_PATH}"
  msg "建议在反馈问题时附上该文件内容（可先脱敏 token）。"
}

main "$@"
