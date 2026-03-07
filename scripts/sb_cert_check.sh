#!/usr/bin/env bash
set -euo pipefail

CONFIG_PATH="/etc/sb-agent/config.json"
CERTMAGIC_DIR="/var/lib/sing-box/certmagic"

msg() { echo "[信息] $*"; }
warn() { echo "[警告] $*"; }
err() { echo "[错误] $*"; }

get_public_ipv4() {
  curl -4 -fsSL ifconfig.me 2>/dev/null \
    || curl -4 -fsSL https://api.ipify.org 2>/dev/null \
    || true
}

resolve_domain_ipv4() {
  local domain="$1"
  local ip=""
  if command -v dig >/dev/null 2>&1; then
    ip="$(dig +short A "$domain" | grep -E '^[0-9.]+$' | head -n1 || true)"
  elif command -v host >/dev/null 2>&1; then
    ip="$(host -t A "$domain" 2>/dev/null | awk '/has address/{print $NF; exit}')"
  fi
  echo "$ip"
}

find_domain_cert() {
  local domain="$1"
  find "$CERTMAGIC_DIR" -type f \( -name "*.crt" -o -name "*.pem" \) 2>/dev/null \
    | grep -F "$domain" \
    | head -n1 \
    || true
}

read_runtime_tuic_summary() {
  local runtime_cfg="/etc/sing-box/config.json"
  if [[ ! -f "$runtime_cfg" ]]; then
    echo "0,0,0"
    return
  fi
  if ! command -v jq >/dev/null 2>&1; then
    echo "0,0,0"
    return
  fi
  jq -r --arg d "${1:-}" '
    ( [ .inbounds[]? | select(.type=="tuic") ] | length ) as $tuic_total
    | ( [ .inbounds[]? | select(.type=="tuic" and (.tls.acme // null) != null) ] | length ) as $tuic_with_acme
    | ( [ .inbounds[]? | select(.type=="tuic" and ((.tls.acme.domain // []) | index($d)) != null and ((.tls.acme.email // "") | length > 0)) ] | length ) as $domain_match
    | "\($tuic_total),\($tuic_with_acme),\($domain_match)"
  ' "$runtime_cfg" 2>/dev/null || echo "0,0,0"
}

main() {
  if [[ ! -f "$CONFIG_PATH" ]]; then
    err "未找到配置文件: $CONFIG_PATH"
    exit 1
  fi
  if ! command -v jq >/dev/null 2>&1; then
    err "缺少 jq，请先安装。"
    exit 1
  fi

  local tuic_domain
  local acme_email
  tuic_domain="$(jq -r '.tuic_domain // ""' "$CONFIG_PATH")"
  tuic_domain="${tuic_domain//[$'\r\n']}"
  acme_email="$(jq -r '.acme_email // ""' "$CONFIG_PATH")"
  acme_email="${acme_email//[$'\r\n']}"
  if [[ -z "$tuic_domain" ]]; then
    warn "当前未启用 TUIC（tuic_domain 为空），无需检查证书。"
    exit 0
  fi

  msg "TUIC 域名: $tuic_domain"
  if [[ -z "$acme_email" ]]; then
    warn "acme_email 为空：sb-agent 会跳过 TUIC 入站生成，证书不会签发。"
    warn "修复建议：节点菜单 1（高级变量向导）或执行 bash scripts/install.sh --configure-only 补齐 acme_email。"
  else
    msg "ACME 邮箱: $acme_email"
  fi
  msg "说明：节点 TUIC 证书由 sing-box 内置 ACME 处理，不依赖 Caddy。"
  warn "不建议在节点安装 Caddy 占用 443，可能与 sing-box 入站冲突。"

  if systemctl is-active sing-box >/dev/null 2>&1; then
    msg "sing-box 服务状态: active"
  else
    warn "sing-box 服务状态: inactive（先执行菜单 8/23 排查组件与服务）"
  fi

  local tuic_total tuic_with_acme domain_match runtime_summary
  runtime_summary="$(read_runtime_tuic_summary "$tuic_domain")"
  tuic_total="$(echo "$runtime_summary" | cut -d, -f1)"
  tuic_with_acme="$(echo "$runtime_summary" | cut -d, -f2)"
  domain_match="$(echo "$runtime_summary" | cut -d, -f3)"
  msg "运行时配置: TUIC入站=${tuic_total} 含ACME=${tuic_with_acme} 当前域名匹配=${domain_match}"
  if [[ "$tuic_total" == "0" ]]; then
    warn "当前 sing-box 运行配置没有 TUIC 入站（证书无法签发）。"
  elif [[ "$domain_match" == "0" ]]; then
    warn "TUIC 入站未匹配当前域名/邮箱（可能配置未生效）。建议先重启 sb-agent 与 sing-box。"
  fi
  local public_ip dns_ip
  public_ip="$(get_public_ipv4)"
  dns_ip="$(resolve_domain_ipv4 "$tuic_domain")"

  if [[ -n "$public_ip" ]]; then
    msg "本机公网 IPv4: $public_ip"
  else
    warn "无法获取本机公网 IPv4。"
  fi
  if [[ -n "$dns_ip" ]]; then
    msg "域名 A 记录 IPv4: $dns_ip"
  else
    warn "未解析到域名 A 记录 IPv4。"
  fi

  if [[ -n "$public_ip" && -n "$dns_ip" ]]; then
    if [[ "$public_ip" == "$dns_ip" ]]; then
      msg "解析检查: 通过"
    else
      warn "解析检查: 不一致（请确认 Cloudflare 关闭代理、小黄云置灰，A 记录指向本机 IP）"
    fi
  fi

  local cert_file
  cert_file="$(find_domain_cert "$tuic_domain")"
  if [[ -z "$cert_file" ]]; then
    warn "未找到证书文件（certmagic 目录中暂无该域名证书）。"
  else
    msg "证书文件: $cert_file"
    local end_date end_epoch now_epoch days_left
    end_date="$(openssl x509 -in "$cert_file" -noout -enddate 2>/dev/null | cut -d= -f2- || true)"
    if [[ -n "$end_date" ]]; then
      end_epoch="$(date -d "$end_date" +%s 2>/dev/null || true)"
      now_epoch="$(date +%s)"
      if [[ -n "$end_epoch" ]]; then
        days_left="$(( (end_epoch - now_epoch) / 86400 ))"
        msg "证书到期时间: $end_date"
        msg "剩余天数: ${days_left} 天"
        if (( days_left < 7 )); then
          warn "证书即将过期，建议尽快检查续期。"
        fi
      fi
    fi
  fi

  echo ""
  msg "最近 ACME 相关日志（sing-box）："
  local acme_logs
  acme_logs="$(journalctl -u sing-box --since "3 days ago" --no-pager 2>/dev/null \
    | grep -Ei 'acme|certificate|certmagic|challenge|letsencrypt|tls' \
    | tail -n 20 || true)"
  if [[ -n "$acme_logs" ]]; then
    echo "$acme_logs"
  else
    echo "（最近 3 天未发现明显 ACME 关键字日志）"
  fi
  echo ""
  msg "最近 TUIC/证书相关日志（sb-agent）："
  journalctl -u sb-agent --since "3 days ago" --no-pager 2>/dev/null \
    | grep -Ei 'tuic|acme|证书|跳过 TUIC 入站|config_set|配置变更' \
    | tail -n 30 || echo "（最近 3 天未发现明显 TUIC/证书关键字日志）"

  echo ""
  if [[ -n "$cert_file" ]]; then
    msg "近期签发判断: 已检测到证书文件，视为近期有成功签发/续期基础。"
  else
    warn "近期签发判断: 未检测到证书文件，请检查域名解析、端口与 ACME 日志。"
  fi
}

main "$@"
