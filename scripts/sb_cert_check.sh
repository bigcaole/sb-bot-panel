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
  tuic_domain="$(jq -r '.tuic_domain // ""' "$CONFIG_PATH")"
  tuic_domain="${tuic_domain//[$'\r\n']}"
  if [[ -z "$tuic_domain" ]]; then
    warn "当前未启用 TUIC（tuic_domain 为空），无需检查证书。"
    exit 0
  fi

  msg "TUIC 域名: $tuic_domain"
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
  if [[ -n "$cert_file" ]]; then
    msg "近期签发判断: 已检测到证书文件，视为近期有成功签发/续期基础。"
  else
    warn "近期签发判断: 未检测到证书文件，请检查域名解析、端口与 ACME 日志。"
  fi
}

main "$@"
