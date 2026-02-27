#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
PY_BIN="${PY_BIN:-${PROJECT_DIR}/venv/bin/python3}"

if [[ ! -x "$PY_BIN" ]]; then
  PY_BIN="$(command -v python3 || true)"
fi
if [[ -z "${PY_BIN:-}" ]]; then
  echo "[错误] 未找到 python3。"
  exit 1
fi

cd "$PROJECT_DIR"
echo "[信息] 使用 Python: $PY_BIN"
echo "[信息] 运行 bot 回归测试（关键命令 + callback 分发）..."
"$PY_BIN" -m unittest \
  tests.test_bot_callback_coverage \
  tests.test_bot_dispatch_contract \
  -v
