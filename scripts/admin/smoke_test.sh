#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
ENV_FILE="${PROJECT_DIR}/.env"
PYTHON_BIN=""

API_MODE="auto" # auto | require | skip
API_BASE_URL=""
FAIL_ITEMS=()
WARN_ITEMS=()
FAIL_PY=0
FAIL_API=0
SMOKE_ACTOR="smoke-test"
AI_CONTEXT_SCRIPT="${PROJECT_DIR}/scripts/admin/ai_context_export.sh"
AI_CONTEXT_ON_FAIL="${SMOKE_EXPORT_AI_CONTEXT_ON_FAIL:-1}"
REQUIRE_TOKEN_SPLIT="${SMOKE_REQUIRE_TOKEN_SPLIT:-0}"

msg() { echo -e "\033[1;32m[信息]\033[0m $*"; }
warn() { echo -e "\033[1;33m[警告]\033[0m $*"; }
err() { echo -e "\033[1;31m[错误]\033[0m $*" >&2; }

record_warn() {
  local item="$1"
  WARN_ITEMS+=("$item")
  warn "$item"
}

record_fail() {
  local item="$1"
  FAIL_ITEMS+=("$item")
  err "$item"
}

emit_ai_context_on_failure() {
  if [[ "${AI_CONTEXT_ON_FAIL}" != "1" ]]; then
    return
  fi
  if [[ ! -x "$AI_CONTEXT_SCRIPT" ]]; then
    warn "未找到 AI 诊断包脚本: ${AI_CONTEXT_SCRIPT}"
    return
  fi
  local ai_context_path
  ai_context_path="/tmp/sb-admin-ai-context-on-fail-$(date +%Y%m%d-%H%M%S).md"
  if bash "$AI_CONTEXT_SCRIPT" --output "$ai_context_path" >/tmp/sb_smoke_ai_export.log 2>&1; then
    echo "失败辅助诊断包：${ai_context_path}"
    echo "提示：可将该文件整体粘贴给任意 AI 做继续定位。"
  else
    warn "自动导出 AI 诊断包失败（不影响退出码），可手动执行: bash scripts/admin/ai_context_export.sh"
    cat /tmp/sb_smoke_ai_export.log || true
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
  local api_url="$1"
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
    code="$(curl -sS -o /dev/null -w "%{http_code}" \
      -H "Authorization: Bearer ${item}" \
      "${api_url}/admin/security/status" || true)"
    if [[ "$code" == "200" ]]; then
      echo "$item"
      return 0
    fi
  done

  echo "${candidates[0]}"
  return 1
}

usage() {
  cat <<'EOF'
用法：
  bash scripts/admin/smoke_test.sh
  bash scripts/admin/smoke_test.sh --require-api
  bash scripts/admin/smoke_test.sh --skip-api
  bash scripts/admin/smoke_test.sh --api-base-url http://127.0.0.1:8080
  bash scripts/admin/smoke_test.sh --require-token-split

说明：
  - 默认执行：Python 语法检查 + unittest + API 冒烟（auto）
  - auto 模式下：若 API 不可达，仅给警告，不判失败
  - require-api 模式下：API 不可达会直接失败
  - require-token-split 模式下：要求管理/节点 token 必须拆分，否则判失败
  - 验收失败时默认自动导出 AI 诊断包（可用环境变量 SMOKE_EXPORT_AI_CONTEXT_ON_FAIL=0 关闭）
EOF
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --require-api)
        API_MODE="require"
        shift
        ;;
      --skip-api)
        API_MODE="skip"
        shift
        ;;
      --api-base-url)
        if [[ $# -lt 2 ]]; then
          err "--api-base-url 需要参数"
          exit 1
        fi
        API_BASE_URL="${2%/}"
        shift 2
        ;;
      --require-token-split)
        REQUIRE_TOKEN_SPLIT="1"
        shift
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        err "未知参数：$1"
        usage
        exit 1
        ;;
    esac
  done
}

load_env() {
  if [[ -f "$ENV_FILE" ]]; then
    # shellcheck disable=SC1090
    set -a; . "$ENV_FILE"; set +a
  fi
}

select_python_bin() {
  if [[ -x "${PROJECT_DIR}/venv/bin/python3" ]]; then
    PYTHON_BIN="${PROJECT_DIR}/venv/bin/python3"
  elif [[ -x "${PROJECT_DIR}/venv/bin/python" ]]; then
    PYTHON_BIN="${PROJECT_DIR}/venv/bin/python"
  elif command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="$(command -v python3)"
  else
    err "未找到可用的 Python 解释器。"
    emit_ai_context_on_failure
    exit 1
  fi
  if ! "$PYTHON_BIN" - <<'PY'
import sys
raise SystemExit(0 if sys.version_info >= (3, 11) else 1)
PY
  then
    err "当前 Python 版本低于 3.11：${PYTHON_BIN}。请先执行安装/更新脚本升级运行环境。"
    emit_ai_context_on_failure
    exit 10
  fi
  msg "使用 Python: ${PYTHON_BIN}"
}

run_py_checks() {
  msg "1/3 运行 Python 语法检查..."
  if ! PYTHONPYCACHEPREFIX=/tmp/pycache_sb_panel "$PYTHON_BIN" -m py_compile \
      "${PROJECT_DIR}"/controller/*.py \
      "${PROJECT_DIR}"/bot/bot.py \
      "${PROJECT_DIR}"/tests/*.py; then
    FAIL_PY=1
    record_fail "Python 语法检查失败"
  fi

  msg "2/3 运行 unittest..."
  local py_warnings
  py_warnings="ignore:Unclosed <MemoryObjectSendStream:ResourceWarning,ignore:Unclosed <MemoryObjectReceiveStream:ResourceWarning"
  if ! PYTHONPYCACHEPREFIX=/tmp/pycache_sb_panel PYTHONWARNINGS="$py_warnings" \
      "$PYTHON_BIN" -m unittest discover \
      -s "${PROJECT_DIR}/tests" \
      -p 'test_*.py' \
      -v; then
    FAIL_PY=1
    record_fail "unittest 失败"
  fi
}

http_code() {
  local url="$1"
  shift || true
  curl -sS -o /tmp/sb_smoke_resp.txt -w "%{http_code}" -H "X-Actor: ${SMOKE_ACTOR}" "$@" "$url" || true
}

wait_api_ready() {
  local api_url="$1"
  local timeout_seconds="${2:-20}"
  local i
  for i in $(seq 1 "$timeout_seconds"); do
    if curl -fsS --max-time 2 "${api_url}/health" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

run_api_checks() {
  if [[ "$API_MODE" == "skip" ]]; then
    msg "3/3 已跳过 API 冒烟检查（--skip-api）"
    return
  fi

  local controller_port="${CONTROLLER_PORT:-8080}"
  local api_url="${API_BASE_URL:-http://127.0.0.1:${controller_port}}"
  local auth_token_raw="${ADMIN_AUTH_TOKEN:-${AUTH_TOKEN:-${NODE_AUTH_TOKEN:-}}}"
  local node_token_raw="${NODE_AUTH_TOKEN:-${AUTH_TOKEN:-${ADMIN_AUTH_TOKEN:-}}}"
  local auth_token
  local node_token
  auth_token="$(pick_working_auth_token "$api_url" "$auth_token_raw")" || {
    record_warn "检测到管理 token 多值模式，但未探测到可用 token，已回退使用第一个 token。"
  }
  if [[ -z "$auth_token" ]]; then
    auth_token="$(first_auth_token "$auth_token_raw")"
  fi
  auth_token="${auth_token#"${auth_token%%[![:space:]]*}"}"
  auth_token="${auth_token%"${auth_token##*[![:space:]]}"}"
  if [[ -n "$auth_token_raw" && "$auth_token_raw" == *","* ]]; then
    record_warn "检测到管理 token 多值模式，验收会自动优先选取可用 token。"
  fi
  node_token="$(first_auth_token "$node_token_raw")"
  node_token="${node_token#"${node_token%%[![:space:]]*}"}"
  node_token="${node_token%"${node_token##*[![:space:]]}"}"
  local require_node_lock="${SMOKE_REQUIRE_NODE_LOCK:-0}"
  local code
  local node_access_status
  local node_access_metrics
  local enabled_nodes
  local unlocked_enabled_nodes
  local whitelist_missing_count
  local first_node_code=""
  local admin_auth_source=""
  local node_auth_source=""
  local auth_token_split_active=0

  msg "3/3 运行 API 冒烟检查（${api_url}）..."

  if ! wait_api_ready "$api_url" 20; then
    code="$(http_code "${api_url}/health")"
  else
    code="200"
  fi
  if [[ "$code" != "200" ]]; then
    if [[ "$API_MODE" == "require" ]]; then
      FAIL_API=1
      record_fail "/health 不可用（require 模式），HTTP=${code}"
      return
    fi
    record_warn "/health 不可用，已跳过 API 检查（auto 模式），HTTP=${code}"
    return
  fi

  if [[ -n "$auth_token" ]]; then
    code="$(http_code "${api_url}/nodes")"
    if [[ "$code" != "401" ]]; then
      FAIL_API=1
      record_fail "鉴权校验异常：未带 token 访问 /nodes 期望 401，实际 ${code}"
    fi
    code="$(http_code "${api_url}/nodes" -H "Authorization: Bearer ${auth_token}")"
    if [[ "$code" != "200" ]]; then
      FAIL_API=1
      record_fail "鉴权校验异常：带 token 访问 /nodes 期望 200，实际 ${code}"
    else
      first_node_code="$("$PYTHON_BIN" - <<'PY'
import json
from pathlib import Path

try:
    payload = json.loads(Path("/tmp/sb_smoke_resp.txt").read_text(encoding="utf-8"))
except Exception:
    payload = []
if isinstance(payload, list) and payload:
    item = payload[0] if isinstance(payload[0], dict) else {}
    print(str(item.get("node_code", "")).strip())
PY
      )"
    fi
    code="$(http_code "${api_url}/admin/security/status" -H "Authorization: Bearer ${auth_token}")"
    if [[ "$code" != "200" ]]; then
      FAIL_API=1
      record_fail "管理接口校验异常：带 token 访问 /admin/security/status 期望 200，实际 ${code}"
    else
      local sec_fields
      sec_fields="$("$PYTHON_BIN" - <<'PY'
import json
from pathlib import Path

payload = json.loads(Path("/tmp/sb_smoke_resp.txt").read_text(encoding="utf-8"))
admin_source = str(payload.get("admin_auth_source", "")).strip()
node_source = str(payload.get("node_auth_source", "")).strip()
split_active = 1 if bool(payload.get("auth_token_split_active")) else 0
print(f"{admin_source},{node_source},{split_active}")
PY
      )"
      IFS=',' read -r admin_auth_source node_auth_source auth_token_split_active <<<"$sec_fields"
      if [[ "$admin_auth_source" != "admin_auth_token" ]]; then
        record_warn "管理鉴权来源为 ${admin_auth_source:-unknown}（建议使用 ADMIN_AUTH_TOKEN）"
      fi
      if [[ "$node_auth_source" != "node_auth_token" ]]; then
        record_warn "节点鉴权来源为 ${node_auth_source:-unknown}（建议使用 NODE_AUTH_TOKEN）"
      fi
      if [[ "${auth_token_split_active}" != "1" ]]; then
        if [[ "${REQUIRE_TOKEN_SPLIT}" == "1" ]]; then
          FAIL_API=1
          record_fail "token 拆分检查失败：当前仍是兼容模式（auth_token_split_active=false）。可执行：bash /root/sb-bot-panel/scripts/admin/auth_token_split_migrate.sh --yes"
        else
          record_warn "token 拆分未启用（兼容模式）；建议拆分 ADMIN_AUTH_TOKEN 与 NODE_AUTH_TOKEN"
        fi
      fi
    fi
    code="$(http_code "${api_url}/admin/overview" -H "Authorization: Bearer ${auth_token}")"
    if [[ "$code" != "200" ]]; then
      FAIL_API=1
      record_fail "管理接口校验异常：带 token 访问 /admin/overview 期望 200，实际 ${code}"
    fi
    code="$(http_code "${api_url}/admin/node_tasks/idempotency" -H "Authorization: Bearer ${auth_token}")"
    if [[ "$code" != "200" ]]; then
      FAIL_API=1
      record_fail "管理接口校验异常：带 token 访问 /admin/node_tasks/idempotency 期望 200，实际 ${code}"
    fi
    code="$(http_code "${api_url}/admin/security/maintenance_cleanup" -X POST -H "Authorization: Bearer ${auth_token}")"
    if [[ "$code" != "200" ]]; then
      FAIL_API=1
      record_fail "管理接口校验异常：带 token 调用 /admin/security/maintenance_cleanup 期望 200，实际 ${code}"
    fi
    code="$(http_code "${api_url}/admin/security/auto_block/run" -X POST -H "Authorization: Bearer ${auth_token}")"
    if [[ "$code" != "200" ]]; then
      FAIL_API=1
      record_fail "管理接口校验异常：带 token 调用 /admin/security/auto_block/run 期望 200，实际 ${code}"
    fi
    code="$(http_code "${api_url}/admin/db/integrity" -H "Authorization: Bearer ${auth_token}")"
    if [[ "$code" != "200" ]]; then
      FAIL_API=1
      record_fail "数据库完整性接口校验异常：带 token 访问 /admin/db/integrity 期望 200，实际 ${code}"
    fi
    if [[ -n "$first_node_code" ]]; then
      code="$(http_code "${api_url}/nodes/${first_node_code}/tasks/next" -X POST -H "Authorization: Bearer ${auth_token}")"
      if [[ "$code" == "200" ]]; then
        FAIL_API=1
        record_fail "鉴权隔离异常：管理 token 访问节点任务接口被允许（/nodes/${first_node_code}/tasks/next=200）"
      elif [[ "$code" != "401" && "$code" != "403" ]]; then
        record_warn "管理 token 访问节点任务接口返回非常规状态（HTTP=${code}）"
      fi
    fi
    if [[ -n "$node_token" ]]; then
      code="$(http_code "${api_url}/admin/security/status" -H "Authorization: Bearer ${node_token}")"
      if [[ "$code" == "200" ]]; then
        if [[ "${auth_token_split_active}" == "1" ]]; then
          FAIL_API=1
          record_fail "鉴权隔离异常：节点 token 可访问管理接口（/admin/security/status=200）"
        else
          record_warn "节点 token 仍可访问管理接口（兼容模式未拆分）"
        fi
      elif [[ "$code" != "401" && "$code" != "403" ]]; then
        record_warn "节点 token 访问管理接口返回非常规状态（HTTP=${code}）"
      fi
    fi
  else
    code="$(http_code "${api_url}/nodes")"
    if [[ "$code" != "200" ]]; then
      FAIL_API=1
      record_fail "管理 token 为空时 /nodes 应可访问，期望 200，实际 ${code}"
    fi
    code="$(http_code "${api_url}/admin/db/integrity")"
    if [[ "$code" != "200" ]]; then
      FAIL_API=1
      record_fail "管理 token 为空时 /admin/db/integrity 应可访问，期望 200，实际 ${code}"
    fi
    code="$(http_code "${api_url}/admin/overview")"
    if [[ "$code" != "200" ]]; then
      FAIL_API=1
      record_fail "管理 token 为空时 /admin/overview 应可访问，期望 200，实际 ${code}"
    fi
    code="$(http_code "${api_url}/admin/node_tasks/idempotency")"
    if [[ "$code" != "200" ]]; then
      FAIL_API=1
      record_fail "管理 token 为空时 /admin/node_tasks/idempotency 应可访问，期望 200，实际 ${code}"
    fi
    code="$(http_code "${api_url}/admin/security/maintenance_cleanup" -X POST)"
    if [[ "$code" != "200" ]]; then
      FAIL_API=1
      record_fail "管理 token 为空时 /admin/security/maintenance_cleanup 应可访问，期望 200，实际 ${code}"
    fi
    code="$(http_code "${api_url}/admin/security/auto_block/run" -X POST)"
    if [[ "$code" != "200" ]]; then
      FAIL_API=1
      record_fail "管理 token 为空时 /admin/security/auto_block/run 应可访问，期望 200，实际 ${code}"
    fi
  fi

  if [[ -n "$auth_token" ]]; then
    node_access_status="$(http_code "${api_url}/admin/node_access/status" -H "Authorization: Bearer ${auth_token}")"
  else
    node_access_status="$(http_code "${api_url}/admin/node_access/status")"
  fi
  if [[ "$node_access_status" == "200" ]]; then
    if node_access_metrics="$("$PYTHON_BIN" - <<'PY'
import json
from pathlib import Path

payload = json.loads(Path("/tmp/sb_smoke_resp.txt").read_text(encoding="utf-8"))
enabled = int(payload.get("enabled_nodes", 0) or 0)
unlocked_enabled = int(payload.get("unlocked_enabled_nodes", 0) or 0)
missing = int(payload.get("whitelist_missing_count", 0) or 0)
print(f"{enabled},{unlocked_enabled},{missing}")
PY
    )"; then
      IFS=',' read -r enabled_nodes unlocked_enabled_nodes whitelist_missing_count <<<"$node_access_metrics"
      if [[ "${unlocked_enabled_nodes:-0}" =~ ^[0-9]+$ ]] && (( unlocked_enabled_nodes > 0 )); then
        if [[ "${require_node_lock}" == "1" ]]; then
          FAIL_API=1
          record_fail "访问收敛检查失败：启用节点未锁定来源IP=${unlocked_enabled_nodes}（enabled_nodes=${enabled_nodes}，whitelist_missing=${whitelist_missing_count}）"
        else
          record_warn "访问收敛检查告警：启用节点未锁定来源IP=${unlocked_enabled_nodes}（enabled_nodes=${enabled_nodes}，whitelist_missing=${whitelist_missing_count}）"
        fi
      fi
    else
      record_warn "访问收敛检查告警：/admin/node_access/status 响应解析失败"
    fi
  else
    record_warn "访问收敛检查告警：/admin/node_access/status HTTP=${node_access_status}"
  fi

  if (( FAIL_API == 0 )); then
    msg "API 冒烟检查通过。"
  fi
}

print_summary_and_exit() {
  local exit_code=0
  if (( FAIL_PY == 1 )); then
    exit_code=$((exit_code + 10))
  fi
  if (( FAIL_API == 1 )); then
    exit_code=$((exit_code + 20))
  fi

  echo ""
  echo "========== 验收汇总 =========="
  if (( ${#WARN_ITEMS[@]} > 0 )); then
    echo "警告项（${#WARN_ITEMS[@]}）："
    for item in "${WARN_ITEMS[@]}"; do
      echo "  - ${item}"
    done
  else
    echo "警告项：0"
  fi
  if (( ${#FAIL_ITEMS[@]} > 0 )); then
    echo "失败项（${#FAIL_ITEMS[@]}）："
    for item in "${FAIL_ITEMS[@]}"; do
      echo "  - ${item}"
    done
  else
    echo "失败项：0"
  fi
  echo "退出码：${exit_code}"
  echo "退出码说明：0=通过，10=代码检查失败，20=API检查失败，30=代码+API均失败"
  echo "============================="

  if (( exit_code == 0 )); then
    msg "验收完成：全部检查通过。"
  else
    err "验收失败，请按失败项处理。"
    emit_ai_context_on_failure
  fi
  exit "${exit_code}"
}

main() {
  parse_args "$@"
  load_env
  select_python_bin
  run_py_checks
  run_api_checks
  print_summary_and_exit
}

main "$@"
