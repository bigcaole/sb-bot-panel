#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="${PROJECT_DIR:-/root/sb-bot-panel}"
ENV_FILE="${PROJECT_DIR}/.env"
API_BASE=""

declare -a ISSUE_ORDER=()
declare -A ISSUE_SEVERITY=()
declare -A ISSUE_MSG=()
declare -A ISSUE_FIX=()

msg() { echo -e "\033[1;32m[信息]\033[0m $*"; }
warn() { echo -e "\033[1;33m[警告]\033[0m $*"; }
err() { echo -e "\033[1;31m[错误]\033[0m $*" >&2; }

require_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    err "请用 root 执行。"
    exit 1
  fi
}

is_bot_token_configured() {
  local token="${1:-}"
  [[ -n "$token" && "$token" != "__REPLACE_WITH_TELEGRAM_BOT_TOKEN__" ]]
}

normalize_input_url() {
  local raw="$1"
  local default_scheme="${2:-http}"
  raw="${raw//$'\r'/}"
  raw="${raw//$'\n'/}"
  raw="$(echo "$raw" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')"
  if [[ -z "$raw" ]]; then
    echo ""
    return
  fi
  if [[ "$raw" =~ ^https?:// ]]; then
    echo "${raw%/}"
    return
  fi
  if [[ "$default_scheme" != "https" ]]; then
    default_scheme="http"
  fi
  echo "${default_scheme}://${raw%/}"
}

extract_url_host() {
  local raw="$1"
  raw="${raw#*://}"
  raw="${raw%%/*}"
  raw="${raw%%:*}"
  echo "$raw"
}

get_public_ipv4() {
  curl -4 -fsSL ifconfig.me 2>/dev/null \
    || curl -4 -fsSL https://api.ipify.org 2>/dev/null \
    || true
}

upsert_env() {
  local key="$1"
  local value="$2"
  local escaped
  escaped="$(printf '%s' "$value" | sed 's/[\\/&]/\\&/g')"
  if grep -q "^${key}=" "$ENV_FILE" 2>/dev/null; then
    sed -i "s/^${key}=.*/${key}=${escaped}/" "$ENV_FILE"
  else
    printf '%s=%s\n' "$key" "$value" >>"$ENV_FILE"
  fi
}

generate_auth_token() {
  local token=""
  if command -v openssl >/dev/null 2>&1; then
    token="$(openssl rand -hex 24 2>/dev/null || true)"
  fi
  if [[ -z "$token" ]]; then
    token="$( (cat /proc/sys/kernel/random/uuid 2>/dev/null || true) | tr -d '-' )"
    token="${token}$(date +%s)"
    token="${token:0:40}"
  fi
  if [[ -z "$token" ]]; then
    token="token$(date +%s)"
  fi
  echo "$token"
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
  local raw="$1"
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
      "${API_BASE}/admin/security/status" || true)"
    if [[ "$code" == "200" ]]; then
      echo "$item"
      return 0
    fi
  done
  echo "${candidates[0]}"
  return 1
}

add_issue() {
  local id="$1"
  local severity="$2"
  local text="$3"
  local fix="$4"
  ISSUE_ORDER+=("$id")
  ISSUE_SEVERITY["$id"]="$severity"
  ISSUE_MSG["$id"]="$text"
  ISSUE_FIX["$id"]="$fix"
}

clear_issues() {
  ISSUE_ORDER=()
  ISSUE_SEVERITY=()
  ISSUE_MSG=()
  ISSUE_FIX=()
}

run_checks() {
  clear_issues
  if [[ ! -f "$ENV_FILE" ]]; then
    add_issue "env_missing" "required_missing" ".env 不存在" "运行：bash scripts/admin/install_admin.sh"
    return
  fi

  # shellcheck disable=SC1090
  set -a; . "$ENV_FILE"; set +a

  local controller_port="${CONTROLLER_PORT:-8080}"
  if ! [[ "$controller_port" =~ ^[0-9]+$ ]] || (( controller_port < 1 || controller_port > 65535 )); then
    add_issue "port_invalid" "config_error" "CONTROLLER_PORT 非法：${controller_port}" "建议改为 8080"
    controller_port="8080"
  fi
  API_BASE="http://127.0.0.1:${controller_port}"

  if [[ -z "${CONTROLLER_URL:-}" ]]; then
    add_issue "controller_url_missing" "required_missing" "CONTROLLER_URL 未配置" "建议填 http://127.0.0.1:${controller_port}"
  fi
  if [[ -z "${CONTROLLER_PUBLIC_URL:-}" ]]; then
    add_issue "controller_public_missing" "required_missing" "CONTROLLER_PUBLIC_URL 未配置" "建议填节点可访问地址（优先域名）"
  fi
  if [[ -z "${PANEL_BASE_URL:-}" ]]; then
    add_issue "panel_base_missing" "required_missing" "PANEL_BASE_URL 未配置" "建议填用户实际访问地址（不要 127.0.0.1）"
  fi

  if [[ -z "${ADMIN_AUTH_TOKEN:-}" && -z "${AUTH_TOKEN:-}" ]]; then
    add_issue "admin_token_missing" "required_missing" "ADMIN_AUTH_TOKEN/AUTH_TOKEN 均为空" "至少配置一个管理鉴权 token"
  fi
  if [[ -z "${NODE_AUTH_TOKEN:-}" && -z "${AUTH_TOKEN:-}" ]]; then
    add_issue "node_token_missing" "required_missing" "NODE_AUTH_TOKEN/AUTH_TOKEN 均为空" "至少配置一个节点鉴权 token"
  fi

  if [[ -z "${CONTROLLER_PORT_WHITELIST:-}" ]]; then
    add_issue "controller_whitelist_empty" "optional_missing" "CONTROLLER_PORT_WHITELIST 未配置" "生产建议配置节点来源白名单"
  fi
  if [[ -z "${ADMIN_API_WHITELIST:-}" ]]; then
    add_issue "admin_whitelist_empty" "optional_missing" "ADMIN_API_WHITELIST 未配置" "建议至少限制到运维来源 IP"
  fi
  if ! is_bot_token_configured "${BOT_TOKEN:-}"; then
    add_issue "bot_token_placeholder" "optional_missing" "BOT_TOKEN 未配置或为占位值" "若需 bot，请在配置向导补填"
  fi

  if ! systemctl is-active sb-controller >/dev/null 2>&1; then
    add_issue "controller_inactive" "config_error" "sb-controller 未运行" "尝试重启 controller"
  fi
  if is_bot_token_configured "${BOT_TOKEN:-}" && ! systemctl is-active sb-bot >/dev/null 2>&1; then
    add_issue "bot_inactive" "config_error" "sb-bot 未运行（且 BOT_TOKEN 已配置）" "尝试重启 sb-bot"
  fi

  local health_code
  health_code="$(curl -sS -o /dev/null -w "%{http_code}" --max-time 3 "${API_BASE}/health" || true)"
  if [[ "$health_code" != "200" ]]; then
    add_issue "health_fail" "config_error" "本地 /health 异常（HTTP=${health_code}）" "检查 controller 端口/服务状态"
  fi

  local admin_raw admin_token admin_code
  admin_raw="${ADMIN_AUTH_TOKEN:-${AUTH_TOKEN:-}}"
  admin_token="$(pick_working_auth_token "$admin_raw")" || true
  if [[ -z "$admin_token" ]]; then
    admin_token="$(first_auth_token "$admin_raw")"
  fi
  if [[ -n "$admin_token" ]]; then
    admin_code="$(curl -sS -o /dev/null -w "%{http_code}" --max-time 3 \
      -H "Authorization: Bearer ${admin_token}" \
      "${API_BASE}/admin/security/status" || true)"
    if [[ "$admin_code" != "200" ]]; then
      add_issue "admin_api_auth_fail" "config_error" "管理 token 调用 /admin/security/status 失败（HTTP=${admin_code}）" "检查 token 是否生效，必要时重启 controller"
    fi
  fi

  if [[ "${ENABLE_HTTPS:-0}" == "1" ]]; then
    if ! command -v caddy >/dev/null 2>&1; then
      add_issue "caddy_missing" "config_error" "ENABLE_HTTPS=1 但未安装 caddy" "安装 caddy 并启动服务"
    elif ! systemctl is-active caddy >/dev/null 2>&1; then
      add_issue "caddy_inactive" "config_error" "ENABLE_HTTPS=1 但 caddy 未运行" "重启 caddy 并检查 443 占用"
    fi
  fi
}

apply_fix() {
  local id="$1"
  # shellcheck disable=SC1090
  [[ -f "$ENV_FILE" ]] && { set -a; . "$ENV_FILE"; set +a; }
  local controller_port="${CONTROLLER_PORT:-8080}"
  local changed_env=0

  case "$id" in
    env_missing)
      err "未检测到 .env，无法自动修复。请先执行安装脚本。"
      return 1
      ;;
    port_invalid)
      upsert_env "CONTROLLER_PORT" "8080"
      changed_env=1
      ;;
    controller_url_missing)
      upsert_env "CONTROLLER_URL" "http://127.0.0.1:${controller_port}"
      changed_env=1
      ;;
    controller_public_missing)
      local public_ip fallback
      public_ip="$(get_public_ipv4)"
      fallback="${PANEL_BASE_URL:-}"
      if [[ -z "$fallback" ]]; then
        if [[ -n "$public_ip" ]]; then
          fallback="http://${public_ip}:${controller_port}"
        else
          fallback="http://127.0.0.1:${controller_port}"
        fi
      fi
      upsert_env "CONTROLLER_PUBLIC_URL" "$fallback"
      changed_env=1
      ;;
    panel_base_missing)
      local fallback_panel
      fallback_panel="${CONTROLLER_PUBLIC_URL:-http://127.0.0.1:${controller_port}}"
      upsert_env "PANEL_BASE_URL" "$fallback_panel"
      changed_env=1
      ;;
    admin_token_missing)
      upsert_env "ADMIN_AUTH_TOKEN" "$(generate_auth_token)"
      changed_env=1
      ;;
    node_token_missing)
      upsert_env "NODE_AUTH_TOKEN" "$(generate_auth_token)"
      changed_env=1
      ;;
    controller_inactive|health_fail|admin_api_auth_fail)
      systemctl restart sb-controller || true
      ;;
    bot_inactive)
      systemctl restart sb-bot || true
      ;;
    caddy_missing)
      export DEBIAN_FRONTEND=noninteractive
      apt-get update -y >/dev/null 2>&1 || true
      apt-get install -y caddy >/dev/null 2>&1 || true
      systemctl enable --now caddy >/dev/null 2>&1 || true
      ;;
    caddy_inactive)
      systemctl restart caddy || true
      ;;
    controller_whitelist_empty|admin_whitelist_empty|bot_token_placeholder)
      warn "该项为可选缺失，允许跳过。"
      ;;
    *)
      warn "未支持自动修复项: $id"
      return 1
      ;;
  esac

  if (( changed_env == 1 )); then
    systemctl restart sb-controller >/dev/null 2>&1 || true
  fi
  return 0
}

print_issues() {
  local idx=1
  local id sev
  for id in "${ISSUE_ORDER[@]}"; do
    sev="${ISSUE_SEVERITY[$id]}"
    case "$sev" in
      required_missing) echo "[${idx}] [必需未配置] ${ISSUE_MSG[$id]}" ;;
      optional_missing) echo "[${idx}] [可选未配置] ${ISSUE_MSG[$id]}" ;;
      config_error) echo "[${idx}] [配置错误] ${ISSUE_MSG[$id]}" ;;
      *) echo "[${idx}] [未知] ${ISSUE_MSG[$id]}" ;;
    esac
    echo "     处理建议: ${ISSUE_FIX[$id]}"
    idx=$((idx + 1))
  done
}

main_loop() {
  require_root
  msg "开始管理服务器部署参数自检（支持循环修复）。"
  while true; do
    run_checks

    local required_count=0 optional_count=0 error_count=0
    local id sev
    for id in "${ISSUE_ORDER[@]}"; do
      sev="${ISSUE_SEVERITY[$id]}"
      case "$sev" in
        required_missing) required_count=$((required_count + 1)) ;;
        optional_missing) optional_count=$((optional_count + 1)) ;;
        config_error) error_count=$((error_count + 1)) ;;
      esac
    done

    echo "----------------------------------------"
    msg "自检汇总：必需未配置=${required_count} 可选未配置=${optional_count} 配置错误=${error_count}"
    if (( ${#ISSUE_ORDER[@]} > 0 )); then
      print_issues
    fi
    echo "----------------------------------------"

    if (( required_count == 0 && error_count == 0 )); then
      msg "自检通过：无阻断问题。"
      if (( optional_count > 0 )); then
        warn "仍有可选项未配置，可按需稍后处理。"
      fi
      break
    fi

    local choice
    read -r -p "输入 a=自动修复可修项, 编号=修复单项, r=仅重检, q=退出: " choice
    case "$choice" in
      a|A)
        for id in "${ISSUE_ORDER[@]}"; do
          if [[ "${ISSUE_SEVERITY[$id]}" != "optional_missing" ]]; then
            apply_fix "$id" || true
          fi
        done
        ;;
      r|R|"")
        ;;
      q|Q)
        warn "已退出自检。"
        exit 1
        ;;
      *)
        if [[ "$choice" =~ ^[0-9]+$ ]]; then
          local idx=$((choice - 1))
          if (( idx >= 0 && idx < ${#ISSUE_ORDER[@]} )); then
            apply_fix "${ISSUE_ORDER[$idx]}" || true
          else
            warn "编号无效。"
          fi
        else
          warn "输入无效。"
        fi
        ;;
    esac
  done
}

main_loop "$@"
