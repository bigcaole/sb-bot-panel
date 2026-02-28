#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-/root/sb-bot-panel}"
ENV_FILE="${PROJECT_DIR}/.env"
AUTO_YES=0
SCRIPT_ACTOR="token-split-migrate"
AI_CONTEXT_SCRIPT="${PROJECT_DIR}/scripts/admin/ai_context_export.sh"
AI_CONTEXT_ON_FAIL="${TOKEN_SPLIT_EXPORT_AI_CONTEXT_ON_FAIL:-1}"
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
  ai_context_path="/tmp/sb-token-split-ai-context-on-fail-$(date +%Y%m%d-%H%M%S).md"
  if bash "$AI_CONTEXT_SCRIPT" --output "$ai_context_path" >/tmp/sb_token_split_ai_export.log 2>&1; then
    echo "失败辅助诊断包：${ai_context_path}"
    echo "提示：可将该文件整体粘贴给任意 AI 做继续定位。"
  else
    warn "自动导出 AI 诊断包失败（不影响原始失败结论），可手动执行: bash scripts/admin/ai_context_export.sh"
    cat /tmp/sb_token_split_ai_export.log || true
  fi
}

on_script_exit() {
  local code=$?
  if (( code != 0 )); then
    emit_ai_context_on_failure
  fi
}

trap on_script_exit EXIT

usage() {
  cat <<'EOF'
用法：
  bash scripts/admin/auth_token_split_migrate.sh [--yes]

说明：
  - 将兼容模式（AUTH_TOKEN）迁移到拆分模式（ADMIN_AUTH_TOKEN + NODE_AUTH_TOKEN）
  - 自动重启 controller/bot，并触发一次节点 token 同步（include_disabled=1, force_new=1）
  - 迁移后保留旧 token 为过渡值（new,old），避免立即中断
EOF
}

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

first_auth_token() {
  local raw="${1:-}"
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

join_new_old_token() {
  local new_token="$1"
  local old_raw="$2"
  local old_trimmed
  old_trimmed="$(echo "${old_raw:-}" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
  if [[ -n "$old_trimmed" ]]; then
    echo "${new_token},${old_trimmed}"
  else
    echo "${new_token}"
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

pick_working_admin_token() {
  local controller_port="$1"
  local raw="$2"
  local item token code
  IFS=',' read -r -a items <<<"$raw"
  for item in "${items[@]}"; do
    token="$(echo "$item" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
    [[ -z "$token" ]] && continue
    code="$(curl -sS -o /dev/null -w "%{http_code}" --max-time 3 \
      -H "Authorization: Bearer ${token}" \
      "http://127.0.0.1:${controller_port}/admin/security/status" || true)"
    if [[ "$code" == "200" ]]; then
      echo "$token"
      return 0
    fi
  done
  echo "$(first_auth_token "$raw")"
  return 1
}

sync_node_tokens_after_split() {
  local controller_port="$1"
  local admin_auth_raw="$2"
  local token response body http_code selected created deduplicated failed
  token="$(pick_working_admin_token "$controller_port" "$admin_auth_raw")" || true
  token="$(echo "${token:-}" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
  if [[ -z "$token" ]]; then
    warn "自动同步节点 token 已跳过：无法获取可用管理 token。"
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
    [[ -n "$body" ]] && warn "返回：${body}"
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

main() {
  local arg
  for arg in "$@"; do
    case "$arg" in
      --yes|-y)
        AUTO_YES=1
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        err "未知参数：$arg"
        usage
        exit 1
        ;;
    esac
  done

  require_root
  if [[ ! -f "$ENV_FILE" ]]; then
    err "未找到环境文件：$ENV_FILE"
    exit 1
  fi

  local controller_port auth_raw admin_raw node_raw
  local old_admin_raw old_node_raw old_auth_primary
  local new_admin_token new_node_token
  local final_admin_raw final_node_raw backup_file

  controller_port="$(get_env_value CONTROLLER_PORT)"
  controller_port="${controller_port:-8080}"
  auth_raw="$(get_env_value AUTH_TOKEN)"
  admin_raw="$(get_env_value ADMIN_AUTH_TOKEN)"
  node_raw="$(get_env_value NODE_AUTH_TOKEN)"

  old_admin_raw="$admin_raw"
  if [[ -z "$(first_auth_token "$old_admin_raw")" ]]; then
    old_admin_raw="$auth_raw"
  fi
  old_node_raw="$node_raw"
  if [[ -z "$(first_auth_token "$old_node_raw")" ]]; then
    old_node_raw="$auth_raw"
  fi
  old_auth_primary="$(first_auth_token "$auth_raw")"

  if [[ "$AUTO_YES" -ne 1 ]]; then
    echo "将执行 token 拆分迁移："
    echo "  - ADMIN_AUTH_TOKEN: 写入 new,old"
    echo "  - NODE_AUTH_TOKEN:  写入 new,old"
    echo "  - AUTH_TOKEN 保留不变（兼容字段）"
    read -r -p "确认继续？[y/N]: " confirm
    confirm="${confirm:-N}"
    if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
      warn "已取消。"
      exit 0
    fi
  fi

  new_admin_token="$(generate_auth_token)"
  new_node_token="$(generate_auth_token)"
  while [[ "$new_node_token" == "$new_admin_token" ]]; do
    new_node_token="$(generate_auth_token)"
  done

  final_admin_raw="$(join_new_old_token "$new_admin_token" "$old_admin_raw")"
  final_node_raw="$(join_new_old_token "$new_node_token" "$old_node_raw")"

  backup_file="${ENV_FILE}.bak-$(date +%Y%m%d-%H%M%S)"
  cp "$ENV_FILE" "$backup_file"
  set_env_value "ADMIN_AUTH_TOKEN" "$final_admin_raw"
  set_env_value "NODE_AUTH_TOKEN" "$final_node_raw"
  if [[ -z "$old_auth_primary" ]]; then
    set_env_value "AUTH_TOKEN" "$new_admin_token"
  fi

  msg "已写入拆分 token，并备份旧 .env：$backup_file"

  systemctl restart sb-controller
  systemctl restart sb-bot

  if ! wait_for_controller_ready "$controller_port" 30; then
    err "controller 启动超时，请检查日志：journalctl -u sb-controller -n 120 --no-pager"
    exit 1
  fi

  sync_node_tokens_after_split "$controller_port" "$final_admin_raw"

  echo ""
  msg "迁移完成：已进入 token 拆分过渡模式。"
  echo "ADMIN_AUTH_TOKEN: ${new_admin_token},..."
  echo "NODE_AUTH_TOKEN:  ${new_node_token},..."
  echo "建议下一步：运行严格验收确认拆分生效。"
  echo "  bash /root/sb-bot-panel/scripts/admin/smoke_test.sh --require-api --require-token-split"
}

main "$@"
