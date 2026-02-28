#!/usr/bin/env bash
set -euo pipefail

CONFIG_PATH="/etc/sb-agent/config.json"
OUTPUT_DIR_DEFAULT="/var/backups/sb-agent/ai-context"
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

controller_url=""
node_code=""
auth_token=""
poll_interval=""
tuic_domain=""
tuic_listen_port=""
acme_email=""

curl_code=""
curl_body=""
curl_get() {
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
        err "用法: bash scripts/ai_context_export.sh [--output /path/to/file.md]"
        exit 1
        ;;
    esac
    shift || true
  done
}

load_config() {
  if [[ ! -f "$CONFIG_PATH" ]]; then
    warn "未找到 ${CONFIG_PATH}，将按空配置输出。"
    return
  fi
  if ! command -v jq >/dev/null 2>&1; then
    warn "未安装 jq，无法解析配置字段，将按空配置输出。"
    return
  fi
  controller_url="$(jq -r '.controller_url // ""' "$CONFIG_PATH" 2>/dev/null || true)"
  node_code="$(jq -r '.node_code // ""' "$CONFIG_PATH" 2>/dev/null || true)"
  auth_token="$(jq -r '.auth_token // ""' "$CONFIG_PATH" 2>/dev/null || true)"
  poll_interval="$(jq -r '.poll_interval // ""' "$CONFIG_PATH" 2>/dev/null || true)"
  tuic_domain="$(jq -r '.tuic_domain // ""' "$CONFIG_PATH" 2>/dev/null || true)"
  tuic_listen_port="$(jq -r '.tuic_listen_port // ""' "$CONFIG_PATH" 2>/dev/null || true)"
  acme_email="$(jq -r '.acme_email // ""' "$CONFIG_PATH" 2>/dev/null || true)"
}

main() {
  parse_args "$@"
  load_config

  local now_human now_ts host_name
  now_human="$(date '+%F %T %Z')"
  now_ts="$(date +%s)"
  host_name="$(hostname 2>/dev/null || echo "unknown")"

  local os_name kernel
  os_name="$( (source /etc/os-release >/dev/null 2>&1 && echo "${PRETTY_NAME:-unknown}") || echo "unknown")"
  kernel="$(uname -sr 2>/dev/null || echo "unknown")"

  local st_agent st_singbox st_fail2ban st_ufw
  st_agent="$(service_state sb-agent)"; st_agent="${st_agent:-unknown}"
  st_singbox="$(service_state sing-box)"; st_singbox="${st_singbox:-unknown}"
  st_fail2ban="$(service_state fail2ban)"; st_fail2ban="${st_fail2ban:-unknown}"
  st_ufw="$(ufw status 2>/dev/null | head -n1 || true)"; st_ufw="${st_ufw:-unknown}"

  local ntp_sync
  ntp_sync="$(timedatectl show -p NTPSynchronized --value 2>/dev/null || true)"
  ntp_sync="${ntp_sync:-unknown}"

  local health_code="000" health_body="" sync_code="000" sync_body=""
  if [[ -n "$controller_url" ]]; then
    local base_url="${controller_url%/}"
    curl_get "${base_url}/health" ""
    health_code="$curl_code"
    health_body="$(single_line "$curl_body")"
    if [[ -n "$node_code" ]]; then
      curl_get "${base_url}/nodes/${node_code}/sync" "$auth_token"
      sync_code="$curl_code"
      sync_body="$(single_line "$curl_body")"
    fi
  fi

  local token_set token_len
  token_set="no"
  token_len="0"
  if [[ -n "$auth_token" ]]; then
    token_set="yes"
    token_len="${#auth_token}"
  fi

  local log_agent log_singbox
  if command -v journalctl >/dev/null 2>&1; then
    log_agent="$(journalctl -u sb-agent -n 120 --no-pager 2>/dev/null | grep -E 'ERROR|Traceback|Exception|failed|401|403|拉取同步失败|同步循环异常' | tail -n 30 || true)"
    log_singbox="$(journalctl -u sing-box -n 120 --no-pager 2>/dev/null | grep -E 'ERROR|error|failed|certificate|acme' | tail -n 30 || true)"
  else
    log_agent=""
    log_singbox=""
  fi

  local tuic_rule="n/a"
  if [[ -n "$tuic_listen_port" ]] && [[ "$tuic_listen_port" =~ ^[0-9]+$ ]] && command -v ufw >/dev/null 2>&1; then
    if ufw status 2>/dev/null | grep -E "^${tuic_listen_port}/udp[[:space:]]+ALLOW" >/dev/null 2>&1; then
      tuic_rule="allow"
    else
      tuic_rule="missing"
    fi
  fi

  if [[ -z "$OUTPUT_PATH" ]]; then
    mkdir -p "$OUTPUT_DIR"
    OUTPUT_PATH="${OUTPUT_DIR}/ai-context-node-$(date +%Y%m%d-%H%M%S).md"
  else
    mkdir -p "$(dirname "$OUTPUT_PATH")"
  fi

  cat >"$OUTPUT_PATH" <<EOF
# sb-agent 节点端 AI 诊断包

生成时间: ${now_human}  
时间戳: ${now_ts}  
主机: ${host_name}  
系统: ${os_name} (${kernel})

## 1) 目标与症状（请人工补充）
- 你希望达成的目标:
- 实际异常现象:
- 异常开始时间:
- 最近一次变更（命令/菜单操作）:

## 2) 配置摘要（脱敏）
- config_path: ${CONFIG_PATH}
- controller_url: ${controller_url:-<empty>}
- node_code: ${node_code:-<empty>}
- poll_interval: ${poll_interval:-<empty>}
- tuic_domain: ${tuic_domain:-<empty>}
- tuic_listen_port: ${tuic_listen_port:-<empty>}
- acme_email_set: $( [[ -n "$acme_email" ]] && echo "yes" || echo "no" )
- auth_token_set: ${token_set}
- auth_token_length: ${token_len}

## 3) 服务状态
- sb-agent: ${st_agent}
- sing-box: ${st_singbox}
- fail2ban: ${st_fail2ban}
- ufw: ${st_ufw}
- ntp_synchronized: ${ntp_sync}

## 4) 联通检查
- controller /health: HTTP ${health_code} | ${health_body}
- node sync: HTTP ${sync_code} | ${sync_body}
- tuic_udp_rule(${tuic_listen_port:-n/a}): ${tuic_rule}

## 5) 最近错误线索（最多 30 行）
### sb-agent
\`\`\`
${log_agent:-<empty>}
\`\`\`

### sing-box
\`\`\`
${log_singbox:-<empty>}
\`\`\`

## 6) 建议提问模板（贴给任意 AI）
请按以下要求分析：
1. 先判断故障属于：鉴权/网络/证书/时间同步/配置错误中的哪一类。
2. 给出“最小变更”的修复步骤（按先后顺序，含命令）。
3. 每一步都要给“成功判定标准”。
4. 若存在回滚风险，给出回滚命令。

---
备注：本文件可直接整体复制给其他 AI。已做 token 脱敏（仅保留是否存在与长度）。
EOF

  msg "AI 诊断包已生成: ${OUTPUT_PATH}"
}

main "$@"
