#!/usr/bin/env bash
set -euo pipefail

CONFIG_PATH="/etc/sb-agent/config.json"
CERTMAGIC_DIR="/var/lib/sing-box/certmagic"

msg() { echo "[信息] $*"; }
warn() { echo "[警告] $*"; }
err() { echo "[错误] $*"; }

service_active() {
  local svc="$1"
  if command -v systemctl >/dev/null 2>&1; then
    systemctl is-active "$svc" >/dev/null 2>&1
    return $?
  fi
  if command -v rc-service >/dev/null 2>&1; then
    rc-service "$svc" status >/dev/null 2>&1
    return $?
  fi
  return 1
}

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

  if service_active sing-box; then
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

  local cert_file
  cert_file="$(find_domain_cert "$tuic_domain")"
  local end_date end_epoch now_epoch days_left cert_status
  cert_status="未知"
  if [[ -n "$cert_file" ]]; then
    end_date="$(openssl x509 -in "$cert_file" -noout -enddate 2>/dev/null | cut -d= -f2- || true)"
    if [[ -n "$end_date" ]]; then
      end_epoch="$(date -d "$end_date" +%s 2>/dev/null || true)"
      now_epoch="$(date +%s)"
      if [[ -n "$end_epoch" ]]; then
        days_left="$(( (end_epoch - now_epoch) / 86400 ))"
        if (( days_left < 0 )); then
          cert_status="已过期"
        elif (( days_left <= 7 )); then
          cert_status="即将过期"
        else
          cert_status="正常"
        fi
      fi
    fi
  fi

  local -a issues
  local summary
  issues=()
  if [[ -z "$acme_email" ]]; then
    issues+=("acme_email 未设置")
  fi
  if ! service_active sing-box; then
    issues+=("sing-box 未运行")
  fi
  if [[ "$tuic_total" == "0" ]]; then
    issues+=("运行配置未包含 TUIC 入站")
  elif [[ "$domain_match" == "0" ]]; then
    issues+=("TUIC 入站未匹配当前域名/邮箱")
  fi
  if [[ -z "$public_ip" || -z "$dns_ip" || "$public_ip" != "$dns_ip" ]]; then
    issues+=("域名解析未指向本机公网 IP")
  fi
  if [[ -z "$cert_file" ]]; then
    issues+=("未检测到证书文件")
  fi
  if [[ -n "${days_left:-}" && "$cert_status" != "正常" ]]; then
    issues+=("证书${cert_status}")
  fi

  if (( ${#issues[@]} == 0 )); then
    summary="正常"
  else
    summary="异常（${#issues[@]} 项）"
  fi

  echo ""
  msg "----- TUIC 证书状态（强判定）-----"
  echo "结论: ${summary}"
  echo "TUIC 域名: ${tuic_domain}"
  echo "ACME 邮箱: ${acme_email:-未设置}"
  echo "sing-box: $(service_active sing-box && echo active || echo inactive)"
  echo "证书到期: ${end_date:-未知}"
  if [[ -n "${days_left:-}" ]]; then
    echo "剩余天数: ${days_left} 天（${cert_status}）"
  else
    echo "证书状态: ${cert_status}"
  fi
  if (( ${#issues[@]} > 0 )); then
    echo "问题清单："
    for issue in "${issues[@]}"; do
      echo "  - ${issue}"
    done
  fi
  echo "建议处理："
  echo "  1) 确认域名 A 记录指向本机公网 IP，且 UDP 端口放行"
  echo "  2) 补齐 acme_email 后重启 sb-agent"
  echo "  3) 需要日志时手动执行："
  echo "     journalctl -u sing-box -n 120 --no-pager"
  echo "     journalctl -u sb-agent -n 120 --no-pager"
}

main "$@"
