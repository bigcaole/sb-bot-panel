#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
ENV_FILE="${PROJECT_DIR}/.env"

API_MODE="auto" # auto | require | skip
API_BASE_URL=""

msg() { echo -e "\033[1;32m[信息]\033[0m $*"; }
warn() { echo -e "\033[1;33m[警告]\033[0m $*"; }
err() { echo -e "\033[1;31m[错误]\033[0m $*" >&2; }

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

run_py_checks() {
  msg "1/3 运行 Python 语法检查..."
  PYTHONPYCACHEPREFIX=/tmp/pycache_sb_panel python3 -m py_compile \
    "${PROJECT_DIR}"/controller/*.py \
    "${PROJECT_DIR}"/bot/bot.py \
    "${PROJECT_DIR}"/tests/*.py

  msg "2/3 运行 unittest..."
  PYTHONPYCACHEPREFIX=/tmp/pycache_sb_panel python3 -m unittest discover \
    -s "${PROJECT_DIR}/tests" \
    -p 'test_*.py' \
    -v
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
      err "/health 不可用，HTTP=${code}"
      exit 1
    fi
    warn "/health 不可用，已跳过 API 检查（auto 模式）。"
    return
  fi

  if [[ -n "$auth_token" ]]; then
    code="$(http_code "${api_url}/nodes")"
    if [[ "$code" != "401" ]]; then
      err "鉴权校验异常：未带 token 访问 /nodes 期望 401，实际 ${code}"
      exit 1
    fi
    code="$(http_code "${api_url}/nodes" -H "Authorization: Bearer ${auth_token}")"
    if [[ "$code" != "200" ]]; then
      err "鉴权校验异常：带 token 访问 /nodes 期望 200，实际 ${code}"
      exit 1
    fi
    code="$(http_code "${api_url}/admin/security/status" -H "Authorization: Bearer ${auth_token}")"
    if [[ "$code" != "200" ]]; then
      err "管理接口校验异常：带 token 访问 /admin/security/status 期望 200，实际 ${code}"
      exit 1
    fi
  else
    code="$(http_code "${api_url}/nodes")"
    if [[ "$code" != "200" ]]; then
      err "AUTH_TOKEN 为空时 /nodes 应可访问，期望 200，实际 ${code}"
      exit 1
    fi
  fi

  msg "API 冒烟检查通过。"
}

main() {
  parse_args "$@"
  load_env
  run_py_checks
  run_api_checks
  msg "验收完成：全部检查通过。"
}

main "$@"
