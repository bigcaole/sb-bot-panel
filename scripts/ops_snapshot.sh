#!/usr/bin/env bash
set -euo pipefail

CONFIG_PATH="/etc/sb-agent/config.json"
SNAPSHOT_DIR_DEFAULT="/var/backups/sb-agent/ops-snapshots"
SNAPSHOT_DIR="${SNAPSHOT_DIR:-$SNAPSHOT_DIR_DEFAULT}"
OUTPUT_PATH=""

msg() { echo -e "\033[1;32m[信息]\033[0m $*"; }
warn() { echo -e "\033[1;33m[警告]\033[0m $*"; }
err() { echo -e "\033[1;31m[错误]\033[0m $*" >&2; }

controller_url=""
node_code=""
auth_token=""
poll_interval=""
tuic_domain=""
tuic_listen_port=""
acme_email=""

curl_code=""
curl_body=""

single_line() {
  local text="${1:-}"
  text="$(echo "$text" | tr '\r\n' ' ' | tr -s ' ')"
  echo "${text:0:500}"
}

curl_get() {
  local url="$1"
  local auth="${2:-}"
  local tmp_file
  tmp_file="$(mktemp)"

  local -a args=("-sS" "--max-time" "8" "-o" "$tmp_file" "-w" "%{http_code}")
  if [[ -n "$auth" ]]; then
    args+=("-H" "Authorization: Bearer ${auth}")
  fi
  args+=("$url")

  curl_code="$(curl "${args[@]}" 2>/dev/null || true)"
  curl_body="$(cat "$tmp_file" 2>/dev/null || true)"
  rm -f "$tmp_file"
  if [[ -z "$curl_code" ]]; then
    curl_code="000"
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
        err "用法: bash scripts/ops_snapshot.sh [--output /path/to/file]"
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
  host_name="$(hostname)"

  local agent_state singbox_state fail2ban_state ufw_state
  agent_state="$(systemctl is-active sb-agent 2>/dev/null || true)"
  singbox_state="$(systemctl is-active sing-box 2>/dev/null || true)"
  fail2ban_state="$(systemctl is-active fail2ban 2>/dev/null || true)"
  ufw_state="$(ufw status 2>/dev/null | head -n1 || true)"
  [[ -z "$agent_state" ]] && agent_state="unknown"
  [[ -z "$singbox_state" ]] && singbox_state="unknown"
  [[ -z "$fail2ban_state" ]] && fail2ban_state="unknown"
  [[ -z "$ufw_state" ]] && ufw_state="unknown"

  local ntp_sync=""
  if command -v timedatectl >/dev/null 2>&1; then
    ntp_sync="$(timedatectl show -p NTPSynchronized --value 2>/dev/null || true)"
  fi
  [[ -z "$ntp_sync" ]] && ntp_sync="unknown"

  local health_code="000" health_body=""
  local sync_code="000" sync_body=""
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

  local auth_token_set="no"
  local auth_token_len="0"
  if [[ -n "$auth_token" ]]; then
    auth_token_set="yes"
    auth_token_len="${#auth_token}"
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
    mkdir -p "$SNAPSHOT_DIR"
    OUTPUT_PATH="${SNAPSHOT_DIR}/node-ops-snapshot-$(date +%Y%m%d-%H%M%S).txt"
  else
    mkdir -p "$(dirname "$OUTPUT_PATH")"
  fi

  cat >"$OUTPUT_PATH" <<EOF
sb-agent 节点运维快照
生成时间: ${now_human}
生成时间戳: ${now_ts}
主机: ${host_name}

[配置摘要]
config_path: ${CONFIG_PATH}
controller_url: ${controller_url:-<empty>}
node_code: ${node_code:-<empty>}
poll_interval: ${poll_interval:-<empty>}
tuic_domain: ${tuic_domain:-<empty>}
tuic_listen_port: ${tuic_listen_port:-<empty>}
acme_email_set: $( [[ -n "$acme_email" ]] && echo "yes" || echo "no" )
auth_token_set: ${auth_token_set}
auth_token_length: ${auth_token_len}

[服务状态]
sb-agent: ${agent_state}
sing-box: ${singbox_state}
fail2ban: ${fail2ban_state}
ufw: ${ufw_state}
ntp_synchronized: ${ntp_sync}

[联通检查]
controller_health: HTTP ${health_code} | ${health_body}
node_sync: HTTP ${sync_code} | ${sync_body}

[端口/防火墙]
tuic_udp_rule(${tuic_listen_port:-n/a}): ${tuic_rule}
EOF

  msg "节点运维快照已生成: ${OUTPUT_PATH}"
  msg "建议排障时附上该文件内容（token 已仅保留长度信息）。"
}

main "$@"
