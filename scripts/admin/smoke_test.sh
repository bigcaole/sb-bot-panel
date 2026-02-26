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

usage() {
  cat <<'EOF'
用法：
  bash scripts/admin/smoke_test.sh
  bash scripts/admin/smoke_test.sh --require-api
  bash scripts/admin/smoke_test.sh --skip-api
  bash scripts/admin/smoke_test.sh --api-base-url http://127.0.0.1:8080

说明：
  - 默认执行：Python 语法检查 + unittest + API 冒烟（auto）
  - auto 模式下：若 API 不可达，仅给警告，不判失败
  - require-api 模式下：API 不可达会直接失败
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
    exit 1
  fi
  if ! "$PYTHON_BIN" - <<'PY'
import sys
raise SystemExit(0 if sys.version_info >= (3, 11) else 1)
PY
  then
    err "当前 Python 版本低于 3.11：${PYTHON_BIN}。请先执行安装/更新脚本升级运行环境。"
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
  curl -sS -o /tmp/sb_smoke_resp.txt -w "%{http_code}" "$@" "$url" || true
}

run_api_checks() {
  if [[ "$API_MODE" == "skip" ]]; then
    msg "3/3 已跳过 API 冒烟检查（--skip-api）"
    return
  fi

  local controller_port="${CONTROLLER_PORT:-8080}"
  local api_url="${API_BASE_URL:-http://127.0.0.1:${controller_port}}"
  local auth_token="${AUTH_TOKEN:-}"
  local code

  msg "3/3 运行 API 冒烟检查（${api_url}）..."

  code="$(http_code "${api_url}/health")"
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
    fi
    code="$(http_code "${api_url}/admin/security/status" -H "Authorization: Bearer ${auth_token}")"
    if [[ "$code" != "200" ]]; then
      FAIL_API=1
      record_fail "管理接口校验异常：带 token 访问 /admin/security/status 期望 200，实际 ${code}"
    fi
  else
    code="$(http_code "${api_url}/nodes")"
    if [[ "$code" != "200" ]]; then
      FAIL_API=1
      record_fail "AUTH_TOKEN 为空时 /nodes 应可访问，期望 200，实际 ${code}"
    fi
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
