#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
ENV_FILE="${PROJECT_DIR}/.env"
OUTPUT_DIR_DEFAULT="/var/backups/sb-controller/ai-context"
OUTPUT_DIR="${OUTPUT_DIR:-$OUTPUT_DIR_DEFAULT}"
OUTPUT_PATH=""

msg() { echo -e "\033[1;32m[信息]\033[0m $*"; }
warn() { echo -e "\033[1;33m[警告]\033[0m $*"; }
err() { echo -e "\033[1;31m[错误]\033[0m $*" >&2; }

single_line() {
  local text="${1:-}"
  text="$(echo "$text" | tr '\r\n' ' ' | tr -s ' ')"
  echo "${text:0:500}"
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

env_value() {
  local key="$1"
  if [[ -f "$ENV_FILE" ]]; then
    grep -E "^${key}=" "$ENV_FILE" | tail -n1 | cut -d= -f2- || true
  fi
}

curl_code=""
curl_body=""
api_get() {
  local url="$1"
  local token="${2:-}"
  local tmp
  tmp="$(mktemp)"
  local -a args=("-sS" "--max-time" "8" "-o" "$tmp" "-w" "%{http_code}")
  if [[ -n "$token" ]]; then
    args+=("-H" "Authorization: Bearer ${token}")
  fi
  args+=("$url")
  curl_code="$(curl "${args[@]}" 2>/dev/null || true)"
  curl_body="$(cat "$tmp" 2>/dev/null || true)"
  rm -f "$tmp"
  if [[ -z "$curl_code" ]]; then
    curl_code="000"
  fi
}

service_state() {
  local svc="$1"
  if command -v systemctl >/dev/null 2>&1; then
    systemctl is-active "$svc" 2>/dev/null || true
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
        err "用法: bash scripts/admin/ai_context_export.sh [--output /path/to/file.md]"
        exit 1
        ;;
    esac
    shift || true
  done
}

main() {
  parse_args "$@"

  local controller_port auth_raw auth_first auth_count
  controller_port="$(env_value CONTROLLER_PORT)"
  controller_port="${controller_port:-8080}"
  auth_raw="$(env_value AUTH_TOKEN)"
  auth_first="$(first_auth_token "$auth_raw")"

  auth_count=0
  if [[ -n "$auth_raw" ]]; then
    IFS=',' read -r -a _tokens <<<"$auth_raw"
    auth_count="${#_tokens[@]}"
  fi

  local git_head host_name now_human now_ts
  git_head="$(git -C "$PROJECT_DIR" rev-parse --short HEAD 2>/dev/null || echo "unknown")"
  host_name="$(hostname 2>/dev/null || echo "unknown")"
  now_human="$(date '+%F %T %Z')"
  now_ts="$(date +%s)"

  local os_name kernel
  os_name="$( (source /etc/os-release >/dev/null 2>&1 && echo "${PRETTY_NAME:-unknown}") || echo "unknown")"
  kernel="$(uname -sr 2>/dev/null || echo "unknown")"

  local st_controller st_bot st_caddy
  st_controller="$(service_state sb-controller)"; st_controller="${st_controller:-unknown}"
  st_bot="$(service_state sb-bot)"; st_bot="${st_bot:-unknown}"
  st_caddy="$(service_state caddy)"; st_caddy="${st_caddy:-unknown}"

  local health_code health_body sec_code sec_body overview_code overview_body node_access_code node_access_body
  api_get "http://127.0.0.1:${controller_port}/health" ""
  health_code="$curl_code"
  health_body="$(single_line "$curl_body")"

  api_get "http://127.0.0.1:${controller_port}/admin/security/status" "$auth_first"
  sec_code="$curl_code"
  sec_body="$curl_body"
  if [[ "$sec_code" == "200" ]] && command -v jq >/dev/null 2>&1; then
    sec_body="$(echo "$sec_body" | jq -c '{auth_enabled,auth_token_count,api_rate_limit_enabled,sub_link_require_signature,node_task_max_pending_per_node,warnings}' 2>/dev/null || echo "$sec_body")"
  fi
  sec_body="$(single_line "$sec_body")"

  api_get "http://127.0.0.1:${controller_port}/admin/overview" "$auth_first"
  overview_code="$curl_code"
  overview_body="$curl_body"
  if [[ "$overview_code" == "200" ]] && command -v jq >/dev/null 2>&1; then
    overview_body="$(echo "$overview_body" | jq -c '{totals,monitor,task_queue,security_events,warnings}' 2>/dev/null || echo "$overview_body")"
  fi
  overview_body="$(single_line "$overview_body")"

  api_get "http://127.0.0.1:${controller_port}/admin/node_access/status" "$auth_first"
  node_access_code="$curl_code"
  node_access_body="$curl_body"
  if [[ "$node_access_code" == "200" ]] && command -v jq >/dev/null 2>&1; then
    node_access_body="$(echo "$node_access_body" | jq -c '{enabled_nodes,locked_enabled_nodes,unlocked_enabled_nodes,unlocked_disabled_nodes,whitelist_missing_count}' 2>/dev/null || echo "$node_access_body")"
  fi
  node_access_body="$(single_line "$node_access_body")"

  local log_controller log_bot log_caddy
  if command -v journalctl >/dev/null 2>&1; then
    log_controller="$(journalctl -u sb-controller -n 120 --no-pager 2>/dev/null | grep -E 'ERROR|Traceback|Exception|unauthorized|403|401|failed' | tail -n 30 || true)"
    log_bot="$(journalctl -u sb-bot -n 120 --no-pager 2>/dev/null | grep -E 'ERROR|Traceback|Exception|unauthorized|403|401|failed' | tail -n 30 || true)"
    log_caddy="$(journalctl -u caddy -n 120 --no-pager 2>/dev/null | grep -E 'ERROR|level=error|failed|certificate|acme' | tail -n 30 || true)"
  else
    log_controller=""
    log_bot=""
    log_caddy=""
  fi

  if [[ -z "$OUTPUT_PATH" ]]; then
    mkdir -p "$OUTPUT_DIR"
    OUTPUT_PATH="${OUTPUT_DIR}/ai-context-admin-$(date +%Y%m%d-%H%M%S).md"
  else
    mkdir -p "$(dirname "$OUTPUT_PATH")"
  fi

  cat >"$OUTPUT_PATH" <<EOF
# sb-bot-panel 管理端 AI 诊断包

生成时间: ${now_human}  
时间戳: ${now_ts}  
主机: ${host_name}  
系统: ${os_name} (${kernel})  
代码版本: ${git_head}

## 1) 目标与症状（请人工补充）
- 你希望达成的目标:
- 实际异常现象:
- 异常开始时间:
- 最近一次变更（命令/菜单操作）:

## 2) 服务状态
- sb-controller: ${st_controller}
- sb-bot: ${st_bot}
- caddy: ${st_caddy}

## 3) API 检查
- controller_port: ${controller_port}
- /health: HTTP ${health_code} | ${health_body}
- /admin/security/status: HTTP ${sec_code} | ${sec_body}
- /admin/overview: HTTP ${overview_code} | ${overview_body}
- /admin/node_access/status: HTTP ${node_access_code} | ${node_access_body}

## 4) 鉴权与关键配置（脱敏）
- AUTH_TOKEN 已配置: $( [[ -n "$auth_raw" ]] && echo "yes" || echo "no" )
- AUTH_TOKEN 数量: ${auth_count}
- 首个 token 长度: $( [[ -n "$auth_first" ]] && echo "${#auth_first}" || echo "0" )
- ENABLE_HTTPS: $(env_value ENABLE_HTTPS)
- HTTPS_DOMAIN: $(env_value HTTPS_DOMAIN)
- CONTROLLER_PORT_WHITELIST: $(env_value CONTROLLER_PORT_WHITELIST)
- SECURITY_BLOCK_PROTECTED_IPS: $(env_value SECURITY_BLOCK_PROTECTED_IPS)

## 5) 最近错误线索（最多 30 行）
### controller
\`\`\`
${log_controller:-<empty>}
\`\`\`

### bot
\`\`\`
${log_bot:-<empty>}
\`\`\`

### caddy
\`\`\`
${log_caddy:-<empty>}
\`\`\`

## 6) 建议提问模板（贴给任意 AI）
请按以下要求分析：
1. 先判断故障属于：鉴权/网络/证书/节点同步/任务队列/配置错误中的哪一类。
2. 给出“最小变更”的修复步骤（按先后顺序，含命令）。
3. 每一步都要给“成功判定标准”。
4. 若存在回滚风险，给出回滚命令。

---
备注：本文件可直接整体复制给其他 AI。已做 token 脱敏（仅保留是否存在与长度）。
EOF

  msg "AI 诊断包已生成: ${OUTPUT_PATH}"
}

main "$@"
