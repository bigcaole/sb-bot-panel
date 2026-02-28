#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
INSTALL_SCRIPT="$ROOT_DIR/scripts/install.sh"
LOCAL_CERT_CHECK_SCRIPT="$ROOT_DIR/scripts/sb_cert_check.sh"
OPS_SNAPSHOT_SCRIPT="$ROOT_DIR/scripts/ops_snapshot.sh"
SYSTEM_CERT_CHECK_SCRIPT="/usr/local/bin/sb-cert-check.sh"

AGENT_SERVICE="sb-agent"
SINGBOX_SERVICE="sing-box"
CERT_TIMER="sb-cert-check.timer"
CERT_SERVICE="sb-cert-check.service"

CONFIG_PATH="/etc/sb-agent/config.json"
CERTMAGIC_DIR="/var/lib/sing-box/certmagic"
BACKUP_DIR="/var/backups/sb-agent"
SSH_HARDEN_FILE="/etc/ssh/sshd_config.d/99-sb-agent-hardening.conf"
FAIL2BAN_JAIL_FILE="/etc/fail2ban/jail.d/sb-agent-sshd.local"

msg() { echo -e "\033[1;32m[信息]\033[0m $*"; }
warn() { echo -e "\033[1;33m[警告]\033[0m $*"; }
err() { echo -e "\033[1;31m[错误]\033[0m $*" >&2; }

pause() {
  echo ""
  read -r -p "按回车继续..." _
}

confirm_action() {
  local prompt="$1"
  local default="${2:-N}"
  local answer
  local hint="[y/N]"
  if [[ "$default" == "Y" ]]; then
    hint="[Y/n]"
  fi
  read -r -p "${prompt} ${hint}: " answer
  answer="${answer:-$default}"
  [[ "$answer" =~ ^[Yy]$ ]]
}

require_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    err "请使用 root 权限运行菜单，例如：sudo bash scripts/menu.sh"
    exit 1
  fi
}

detect_ssh_service() {
  if systemctl list-unit-files 2>/dev/null | grep -q '^sshd.service'; then
    echo "sshd"
    return
  fi
  echo "ssh"
}

has_authorized_keys_for_user() {
  local user_name="${1:-root}"
  local user_home auth_file
  user_home="$(getent passwd "$user_name" | awk -F: '{print $6}' || true)"
  if [[ -z "$user_home" ]]; then
    return 1
  fi
  auth_file="${user_home}/.ssh/authorized_keys"
  [[ -s "$auth_file" ]]
}

get_authorized_keys_path_for_user() {
  local user_name="${1:-root}"
  local user_home
  user_home="$(getent passwd "$user_name" | awk -F: '{print $6}' || true)"
  if [[ -z "$user_home" ]]; then
    echo ""
    return
  fi
  echo "${user_home}/.ssh/authorized_keys"
}

detect_sshd_port() {
  local port
  port=""
  if command -v sshd >/dev/null 2>&1; then
    port="$(sshd -T 2>/dev/null | awk '/^port /{print $2; exit}' || true)"
  fi
  if [[ "$port" =~ ^[0-9]+$ ]]; then
    echo "$port"
  else
    echo "22"
  fi
}

detect_current_ssh_client_ip() {
  local ip
  ip=""
  if [[ -n "${SSH_CONNECTION:-}" ]]; then
    ip="$(echo "$SSH_CONNECTION" | awk '{print $1}')"
  elif [[ -n "${SSH_CLIENT:-}" ]]; then
    ip="$(echo "$SSH_CLIENT" | awk '{print $1}')"
  else
    ip="$(who -m 2>/dev/null | sed -n 's/.*(\([0-9.]*\)).*/\1/p' | head -n1 || true)"
  fi
  echo "$ip"
}

is_fail2ban_banned_ip() {
  local ip="$1"
  if ! command -v fail2ban-client >/dev/null 2>&1; then
    return 1
  fi
  if ! systemctl is-active fail2ban >/dev/null 2>&1; then
    return 1
  fi
  local banned_line
  banned_line="$(fail2ban-client status sshd 2>/dev/null | awk -F: '/Banned IP list/{print $2; exit}' || true)"
  [[ " ${banned_line} " == *" ${ip} "* ]]
}

ufw_allows_ssh_for_ip() {
  local ip="$1"
  local ssh_port="$2"
  if ! command -v ufw >/dev/null 2>&1; then
    return 0
  fi
  local status_line
  status_line="$(ufw status 2>/dev/null | head -n1 || true)"
  if [[ "$status_line" == *"inactive"* ]]; then
    return 0
  fi
  ufw status 2>/dev/null | awk -v p="$ssh_port" -v ip="$ip" '
    BEGIN { ok=0 }
    $0 ~ ("^ *" p "(/tcp)?([[:space:]]|$)") && $0 ~ /ALLOW/ {
      if ($0 ~ /Anywhere/ || $0 ~ ("(^|[[:space:]])" ip "($|[[:space:]])") || $0 ~ (ip "/32")) {
        ok=1
      }
    }
    END { exit(ok ? 0 : 1) }
  '
}

ufw_has_allow_for_port() {
  local ssh_port="$1"
  if ! command -v ufw >/dev/null 2>&1; then
    return 1
  fi
  ufw status 2>/dev/null | grep -E "^ *${ssh_port}(/tcp)?[[:space:]]+ALLOW" >/dev/null 2>&1
}

remove_ufw_allow_rules_for_port() {
  local ssh_port="$1"
  local removed_count=0
  if ! command -v ufw >/dev/null 2>&1; then
    echo "0"
    return
  fi
  local -a delete_nums=()
  local line num rule
  while IFS= read -r line; do
    num="$(echo "$line" | sed -n 's/^\[ *\([0-9][0-9]*\)\].*/\1/p')"
    rule="$(echo "$line" | sed -n 's/^\[ *[0-9][0-9]*\] *//p')"
    if [[ -z "$num" || -z "$rule" ]]; then
      continue
    fi
    if echo "$rule" | grep -E "^${ssh_port}(/tcp)?[[:space:]]+ALLOW" >/dev/null 2>&1; then
      delete_nums+=("$num")
    fi
  done < <(ufw status numbered 2>/dev/null || true)

  if (( ${#delete_nums[@]} > 0 )); then
    local sorted_num
    while IFS= read -r sorted_num; do
      if [[ -n "$sorted_num" ]]; then
        ufw --force delete "$sorted_num" >/dev/null 2>&1 || true
        removed_count=$((removed_count + 1))
      fi
    done < <(printf '%s\n' "${delete_nums[@]}" | sort -rn)
  fi
  echo "$removed_count"
}

precheck_ssh_lockout_risk() {
  local client_ip ssh_port
  client_ip="$(detect_current_ssh_client_ip)"
  ssh_port="$(detect_sshd_port)"

  if [[ -n "$client_ip" ]]; then
    msg "当前 SSH 会话来源 IP: ${client_ip}，sshd 端口: ${ssh_port}"
    if is_fail2ban_banned_ip "$client_ip"; then
      err "当前来源 IP(${client_ip}) 已在 fail2ban 封禁列表，请先解封后再启用仅密钥登录。"
      return 1
    fi
    if ! ufw_allows_ssh_for_ip "$client_ip" "$ssh_port"; then
      warn "UFW 未明确放行当前来源 IP(${client_ip}) 到 SSH 端口 ${ssh_port}。"
      if ! confirm_action "仍继续启用仅密钥登录？（可能导致失联）" "N"; then
        warn "已取消启用。"
        return 1
      fi
    fi
  else
    warn "未检测到当前 SSH 会话来源 IP（可能是本机控制台）。"
    if ! confirm_action "仍继续启用仅密钥登录？" "N"; then
      warn "已取消启用。"
      return 1
    fi
  fi

  if ! confirm_action "是否已在另一个终端验证公钥可登录？" "N"; then
    warn "未确认公钥可登录，已取消启用。"
    return 1
  fi
  return 0
}

install_or_enable_fail2ban() {
  msg "安装并启用 fail2ban（SSH 防爆破）..."
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -y
  apt-get install -y fail2ban

  mkdir -p /etc/fail2ban/jail.d
  cat >"$FAIL2BAN_JAIL_FILE" <<'EOF'
[sshd]
enabled = true
mode = normal
port = ssh
filter = sshd
logpath = %(sshd_log)s
backend = systemd
maxretry = 5
findtime = 10m
bantime = 1h
EOF

  systemctl enable --now fail2ban >/dev/null
  msg "fail2ban 已启用。"
}

show_fail2ban_status() {
  systemctl status fail2ban --no-pager || true
  echo ""
  if command -v fail2ban-client >/dev/null 2>&1; then
    msg "fail2ban 总状态："
    fail2ban-client status || true
    echo ""
    msg "sshd jail 状态："
    fail2ban-client status sshd || true
  else
    warn "未检测到 fail2ban-client。"
  fi
}

show_ssh_security_status() {
  local ssh_service ssh_port client_ip pass_auth permit_root pubkey_auth
  local fail2ban_bans_24h
  local risk_score risk_level
  local root_has_keys=0
  local fail2ban_ok=0
  local ufw_ok=1
  local ufw_installed=1
  local ssh_active=0
  local client_ip_allowed=1
  local need_fix_ssh_service=0
  local need_root_keys=0
  local need_enable_pubkey=0
  local need_disable_password=0
  local need_fix_permit_root=0
  local need_start_fail2ban=0
  local need_install_fail2ban=0
  local need_open_ssh_port=0
  local need_allow_current_ip=0
  local need_install_ufw=0
  local need_reduce_attack_surface=0
  local need_close_legacy_ssh_port=0
  ssh_service="$(detect_ssh_service)"
  ssh_port="$(detect_sshd_port)"
  client_ip="$(detect_current_ssh_client_ip)"
  pass_auth="unknown"
  permit_root="unknown"
  pubkey_auth="unknown"
  fail2ban_bans_24h="-1"
  risk_score=0
  risk_level="低"

  echo "----- SSH 安全状态总览 -----"
  echo "sshd 服务名: ${ssh_service}"
  echo "sshd 端口: ${ssh_port}"
  echo "当前会话来源 IP: ${client_ip:-未知}"

  if systemctl is-active "$ssh_service" >/dev/null 2>&1; then
    ssh_active=1
    msg "SSH 服务状态：运行中"
  else
    risk_score=$((risk_score + 3))
    need_fix_ssh_service=1
    warn "SSH 服务状态：未运行"
  fi

  if command -v sshd >/dev/null 2>&1; then
    pass_auth="$(sshd -T 2>/dev/null | awk '/^passwordauthentication /{print $2; exit}' || true)"
    permit_root="$(sshd -T 2>/dev/null | awk '/^permitrootlogin /{print $2; exit}' || true)"
    pubkey_auth="$(sshd -T 2>/dev/null | awk '/^pubkeyauthentication /{print $2; exit}' || true)"
  fi

  echo ""
  echo "----- SSH 策略（生效值）-----"
  echo "PubkeyAuthentication: ${pubkey_auth}"
  echo "PasswordAuthentication: ${pass_auth}"
  echo "PermitRootLogin: ${permit_root}"
  if [[ "$pubkey_auth" == "yes" && "$pass_auth" == "no" ]]; then
    msg "当前策略符合“仅密钥登录”基本要求。"
  else
    if [[ "$pubkey_auth" != "yes" ]]; then
      risk_score=$((risk_score + 2))
      need_enable_pubkey=1
    fi
    if [[ "$pass_auth" != "no" ]]; then
      risk_score=$((risk_score + 2))
      need_disable_password=1
    fi
    warn "当前策略不是严格仅密钥登录（建议 PasswordAuthentication=no 且 PubkeyAuthentication=yes）。"
  fi
  if [[ "$permit_root" == "yes" ]]; then
    risk_score=$((risk_score + 1))
    need_fix_permit_root=1
  fi

  echo ""
  echo "----- authorized_keys -----"
  echo "公钥存放路径提示："
  echo "  - root: /root/.ssh/authorized_keys"
  echo "  - 普通用户: /home/<用户名>/.ssh/authorized_keys"
  if has_authorized_keys_for_user root; then
    root_has_keys=1
    msg "root 用户已检测到 authorized_keys。"
  else
    risk_score=$((risk_score + 3))
    need_root_keys=1
    warn "root 用户未检测到 authorized_keys。"
  fi

  echo ""
  echo "----- fail2ban（sshd）-----"
  if command -v fail2ban-client >/dev/null 2>&1; then
    if systemctl is-active fail2ban >/dev/null 2>&1; then
      fail2ban_ok=1
      fail2ban-client status sshd 2>/dev/null || warn "未检测到 sshd jail（可能未启用）。"
      fail2ban_bans_24h="$(get_fail2ban_ban_count_24h)"
      if [[ "$fail2ban_bans_24h" =~ ^[0-9]+$ ]]; then
        echo "近24小时封禁次数: ${fail2ban_bans_24h}"
        if (( fail2ban_bans_24h >= 30 )); then
          risk_score=$((risk_score + 1))
          need_reduce_attack_surface=1
          warn "近24小时封禁次数较高，建议收敛 SSH 暴露面（来源IP白名单/变更端口）。"
        fi
      else
        warn "未能统计近24小时封禁次数（journalctl 可能不可用）。"
      fi
    else
      risk_score=$((risk_score + 1))
      need_start_fail2ban=1
      warn "fail2ban 服务未运行。"
    fi
  else
    risk_score=$((risk_score + 1))
    need_install_fail2ban=1
    warn "系统未安装 fail2ban。"
  fi

  echo ""
  echo "----- UFW SSH 放行 -----"
  if command -v ufw >/dev/null 2>&1; then
    local ufw_state
    ufw_state="$(ufw status 2>/dev/null | head -n1 || true)"
    echo "UFW 状态: ${ufw_state:-未知}"
    if ! ufw status 2>/dev/null | grep -E "^ *${ssh_port}(/tcp)?[[:space:]]" >/dev/null; then
      ufw_ok=0
      risk_score=$((risk_score + 2))
      need_open_ssh_port=1
      warn "未发现 SSH 端口(${ssh_port})放行规则。"
    else
      ufw status 2>/dev/null | grep -E "^ *${ssh_port}(/tcp)?[[:space:]]" || true
    fi
    if [[ -n "$client_ip" ]]; then
      if ufw_allows_ssh_for_ip "$client_ip" "$ssh_port"; then
        msg "当前来源 IP(${client_ip}) 对 SSH 端口放行状态：允许。"
      else
        client_ip_allowed=0
        risk_score=$((risk_score + 2))
        need_allow_current_ip=1
        warn "当前来源 IP(${client_ip}) 对 SSH 端口放行状态：不明确允许。"
      fi
    fi
    if [[ "$ssh_port" != "22" ]] && ufw_has_allow_for_port "22"; then
      need_close_legacy_ssh_port=1
      risk_score=$((risk_score + 1))
      warn "检测到 22/tcp 仍放行，当前 SSH 端口为 ${ssh_port}，建议清理遗留 22 规则。"
    fi
  else
    ufw_installed=0
    risk_score=$((risk_score + 1))
    need_install_ufw=1
    warn "系统未安装 UFW。"
  fi

  echo ""
  echo "----- 风险评估 -----"
  if (( risk_score >= 6 )); then
    risk_level="高"
  elif (( risk_score >= 3 )); then
    risk_level="中"
  else
    risk_level="低"
  fi
  echo "风险等级: ${risk_level}（评分=${risk_score}）"
  if [[ "$risk_level" == "低" ]]; then
    msg "当前 SSH 安全基线较好，可按流程执行仅密钥切换。"
  elif [[ "$risk_level" == "中" ]]; then
    warn "存在中等风险，建议先修复后再做仅密钥切换。"
  else
    warn "存在高风险，暂不建议切换仅密钥登录。"
  fi

  if (( need_root_keys + need_enable_pubkey + need_disable_password + need_fix_permit_root + need_fix_ssh_service + need_install_ufw + need_open_ssh_port + need_allow_current_ip + need_install_fail2ban + need_start_fail2ban + need_reduce_attack_surface + need_close_legacy_ssh_port > 0 )); then
    echo ""
    echo "----- 修复建议（按顺序）-----"
    local i=1
    if (( need_root_keys == 1 )); then
      echo "${i}) 先为 root 写入公钥再启用仅密钥登录：mkdir -p /root/.ssh && chmod 700 /root/.ssh && 编辑 /root/.ssh/authorized_keys"
      i=$((i + 1))
    fi
    if (( need_enable_pubkey == 1 )); then
      echo "${i}) 启用公钥认证：菜单 17（启用仅密钥登录）或在 sshd 配置中设置 PubkeyAuthentication yes"
      i=$((i + 1))
    fi
    if (( need_disable_password == 1 )); then
      echo "${i}) 禁用密码登录：菜单 17（启用仅密钥登录）后验证 PasswordAuthentication=no"
      i=$((i + 1))
    fi
    if (( need_fix_permit_root == 1 )); then
      echo "${i}) 建议将 PermitRootLogin 调整为 prohibit-password（菜单 17 会自动处理）"
      i=$((i + 1))
    fi
    if (( need_fix_ssh_service == 1 )); then
      echo "${i}) 检查 SSH 服务并恢复：systemctl status ${ssh_service} && systemctl restart ${ssh_service}"
      i=$((i + 1))
    fi
    if (( need_install_ufw == 1 )); then
      echo "${i}) 安装并启用 UFW 后仅放行必要端口（SSH/443/TUIC）"
      i=$((i + 1))
    fi
    if (( need_open_ssh_port == 1 )); then
      echo "${i}) 放行 SSH 端口：ufw allow ${ssh_port}/tcp"
      i=$((i + 1))
    fi
    if (( need_allow_current_ip == 1 )) && [[ -n "$client_ip" ]]; then
      echo "${i}) 放行当前运维来源 IP：ufw allow from ${client_ip} to any port ${ssh_port} proto tcp"
      i=$((i + 1))
    fi
    if (( need_install_fail2ban == 1 )); then
      echo "${i}) 安装 fail2ban：菜单 11（安装/启用 fail2ban）"
      i=$((i + 1))
    fi
    if (( need_start_fail2ban == 1 )); then
      echo "${i}) 启动 fail2ban：菜单 11（安装/启用 fail2ban）"
      i=$((i + 1))
    fi
    if (( need_reduce_attack_surface == 1 )); then
      echo "${i}) 收敛 SSH 暴露面：仅放行管理来源IP，必要时变更 SSH 端口并启用自动封禁。"
      i=$((i + 1))
    fi
    if (( need_close_legacy_ssh_port == 1 )); then
      echo "${i}) 如确认已迁移到 ${ssh_port}，请清理遗留 22 规则：ufw status numbered 后删除 22/tcp 项。"
      i=$((i + 1))
    fi
  fi

  # Keep variables referenced for shellcheck clarity.
  : "${ssh_active}" "${root_has_keys}" "${fail2ban_ok}" "${ufw_ok}" "${ufw_installed}" "${client_ip_allowed}"
}

run_ssh_security_quick_fix() {
  local ssh_port client_ip ufw_state removed_ufw_rules removed_legacy_22
  ssh_port="$(detect_sshd_port)"
  client_ip="$(detect_current_ssh_client_ip)"

  msg "开始执行半自动安全修复（不包含仅密钥切换）..."
  echo "目标：修复 fail2ban 运行状态、确保 SSH 防火墙放行。"
  echo ""

  if command -v ufw >/dev/null 2>&1; then
    msg "同步 UFW SSH 规则..."
    ufw allow "${ssh_port}/tcp" >/dev/null || true
    if [[ -n "$client_ip" ]]; then
      ufw allow from "$client_ip" to any port "$ssh_port" proto tcp >/dev/null || true
      msg "已尝试放行当前来源 IP(${client_ip}) 到 SSH 端口 ${ssh_port}。"
    fi
    removed_ufw_rules="$(cleanup_ufw_duplicate_ssh_rules "$ssh_port")"
    if [[ "$removed_ufw_rules" =~ ^[0-9]+$ ]] && (( removed_ufw_rules > 0 )); then
      msg "已清理重复 SSH 防火墙规则：${removed_ufw_rules} 条。"
    fi
    if [[ "$ssh_port" != "22" ]] && ufw_has_allow_for_port "22"; then
      if confirm_action "检测到遗留 22/tcp 放行规则，是否清理？" "N"; then
        removed_legacy_22="$(remove_ufw_allow_rules_for_port "22")"
        if [[ "$removed_legacy_22" =~ ^[0-9]+$ ]] && (( removed_legacy_22 > 0 )); then
          msg "已清理遗留 22 端口放行规则：${removed_legacy_22} 条。"
        else
          warn "未清理到 22 端口规则（可能不存在或删除失败）。"
        fi
      fi
    fi
    ufw_state="$(ufw status 2>/dev/null | head -n1 || true)"
    if [[ "$ufw_state" == *"inactive"* ]]; then
      if confirm_action "检测到 UFW 未启用，是否立即启用？" "Y"; then
        ufw --force enable >/dev/null
        msg "UFW 已启用。"
      else
        warn "你选择不启用 UFW。"
      fi
    else
      msg "UFW 已启用，SSH 规则已同步。"
    fi
  else
    warn "系统未安装 UFW，跳过防火墙修复。"
  fi

  if command -v fail2ban-client >/dev/null 2>&1 && systemctl is-active fail2ban >/dev/null 2>&1; then
    msg "fail2ban 已运行。"
  else
    if confirm_action "fail2ban 未就绪，是否安装/启用？" "Y"; then
      install_or_enable_fail2ban
    else
      warn "你选择跳过 fail2ban 安装/启用。"
    fi
  fi

  echo ""
  msg "半自动修复执行完成，建议立即查看菜单 15（SSH 安全状态总览）确认结果。"
}

unban_fail2ban_ip() {
  if ! command -v fail2ban-client >/dev/null 2>&1; then
    err "未检测到 fail2ban-client，请先安装 fail2ban。"
    return
  fi
  local ip
  read -r -p "请输入要解封的 IP: " ip
  ip="$(echo "$ip" | tr -d '[:space:]')"
  if [[ -z "$ip" ]]; then
    warn "IP 不能为空。"
    return
  fi
  fail2ban-client set sshd unbanip "$ip"
  msg "已尝试从 sshd jail 解封: $ip"
}

get_fail2ban_ban_count_24h() {
  if ! command -v journalctl >/dev/null 2>&1; then
    echo "-1"
    return
  fi
  journalctl -u fail2ban --since "24 hours ago" --no-pager 2>/dev/null | awk '
    / Ban / {count++}
    END {print count + 0}
  '
}

cleanup_ufw_duplicate_ssh_rules() {
  local ssh_port removed_count
  ssh_port="${1:-22}"
  removed_count=0

  if ! command -v ufw >/dev/null 2>&1; then
    echo "0"
    return
  fi

  local line num rule normalized
  local -a delete_nums=()
  local -A seen_rules=()

  while IFS= read -r line; do
    num="$(echo "$line" | sed -n 's/^\[ *\([0-9][0-9]*\)\].*/\1/p')"
    rule="$(echo "$line" | sed -n 's/^\[ *[0-9][0-9]*\] *//p')"
    if [[ -z "$num" || -z "$rule" ]]; then
      continue
    fi
    if ! echo "$rule" | grep -E "^${ssh_port}(/tcp)?[[:space:]]+ALLOW" >/dev/null 2>&1; then
      continue
    fi
    normalized="$(echo "$rule" | tr -s ' ' ' ' | sed 's/^ //; s/ $//')"
    if [[ -n "${seen_rules[$normalized]+x}" ]]; then
      delete_nums+=("$num")
    else
      seen_rules["$normalized"]=1
    fi
  done < <(ufw status numbered 2>/dev/null || true)

  if (( ${#delete_nums[@]} > 0 )); then
    local sorted_num
    while IFS= read -r sorted_num; do
      if [[ -n "$sorted_num" ]]; then
        ufw --force delete "$sorted_num" >/dev/null 2>&1 || true
        removed_count=$((removed_count + 1))
      fi
    done < <(printf '%s\n' "${delete_nums[@]}" | sort -rn)
  fi

  echo "$removed_count"
}

generate_ssh_keypair() {
  local user_name user_home key_path passphrase comment overwrite auth_file
  read -r -p "请输入要生成密钥的用户名 [root]: " user_name
  user_name="${user_name:-root}"
  user_home="$(getent passwd "$user_name" | awk -F: '{print $6}' || true)"
  if [[ -z "$user_home" ]]; then
    err "用户不存在: $user_name"
    return
  fi
  key_path="${user_home}/.ssh/id_ed25519"
  read -r -p "请输入私钥保存路径 [${key_path}]: " key_path
  key_path="${key_path:-${user_home}/.ssh/id_ed25519}"

  if [[ -f "$key_path" ]]; then
    read -r -p "密钥已存在，是否覆盖？[y/N]: " overwrite
    overwrite="${overwrite:-N}"
    if [[ ! "$overwrite" =~ ^[Yy]$ ]]; then
      warn "已取消生成密钥。"
      return
    fi
  fi

  read -r -p "请输入密钥口令（留空=无口令）: " passphrase
  comment="${user_name}@$(hostname)-sb-agent"
  mkdir -p "$(dirname "$key_path")"
  chmod 700 "$(dirname "$key_path")"
  ssh-keygen -t ed25519 -a 100 -f "$key_path" -N "$passphrase" -C "$comment"
  chown -R "$user_name":"$user_name" "$(dirname "$key_path")"
  chmod 600 "$key_path"
  chmod 644 "${key_path}.pub"

  auth_file="$(get_authorized_keys_path_for_user "$user_name")"
  msg "公钥如下（请追加到服务器文件：${auth_file}）："
  cat "${key_path}.pub"
  echo ""
  echo "推荐在目标节点服务器执行："
  echo "  mkdir -p $(dirname "$auth_file") && chmod 700 $(dirname "$auth_file")"
  echo "  # 将上方公钥追加到 ${auth_file}"
  echo "  chmod 600 ${auth_file}"
}

enable_ssh_key_only_login() {
  local user_name ssh_service auth_file
  read -r -p "请输入用于校验 authorized_keys 的用户名 [root]: " user_name
  user_name="${user_name:-root}"
  auth_file="$(get_authorized_keys_path_for_user "$user_name")"

  if ! has_authorized_keys_for_user "$user_name"; then
    warn "用户 ${user_name} 没有可用 authorized_keys（${auth_file}），拒绝启用（避免锁死 SSH）。"
    return
  fi
  if ! precheck_ssh_lockout_risk; then
    return
  fi

  mkdir -p /etc/ssh/sshd_config.d
  cat >"$SSH_HARDEN_FILE" <<'EOF'
# Managed by sb-agent menu
PubkeyAuthentication yes
PasswordAuthentication no
KbdInteractiveAuthentication no
ChallengeResponseAuthentication no
UsePAM yes
PermitRootLogin prohibit-password
EOF

  if command -v sshd >/dev/null 2>&1 && ! sshd -t; then
    rm -f "$SSH_HARDEN_FILE"
    err "sshd 配置校验失败，已回滚。"
    return
  fi

  ssh_service="$(detect_ssh_service)"
  systemctl restart "$ssh_service" >/dev/null 2>&1 || true
  msg "SSH 已切换为仅密钥登录（密码登录已禁用）。"
}

disable_ssh_key_only_login() {
  local ssh_service
  read -r -p "确认恢复 SSH 密码登录（应急用途）？[y/N]: " answer
  answer="${answer:-N}"
  if [[ ! "$answer" =~ ^[Yy]$ ]]; then
    warn "已取消恢复密码登录。"
    return
  fi

  rm -f "$SSH_HARDEN_FILE"
  if command -v sshd >/dev/null 2>&1 && ! sshd -t; then
    err "sshd 配置校验失败，请手动检查 /etc/ssh/sshd_config*"
    return
  fi
  ssh_service="$(detect_ssh_service)"
  systemctl restart "$ssh_service" >/dev/null 2>&1 || true
  msg "已移除仅密钥策略，SSH 密码登录恢复。"
}

run_install() {
  if [[ ! -f "$INSTALL_SCRIPT" ]]; then
    err "未找到 install.sh: $INSTALL_SCRIPT"
    return
  fi
  if [[ -d "${ROOT_DIR}/.git" ]] && command -v git >/dev/null 2>&1; then
    msg "检测到 Git 仓库，尝试拉取最新代码..."
    git -C "$ROOT_DIR" pull --ff-only origin main || warn "git pull 失败，请手动处理分支后重试。"
  fi

  if [[ -f "$CONFIG_PATH" ]]; then
    msg "检测到现有配置，执行无交互更新（复用原参数）..."
    bash "$INSTALL_SCRIPT" --sync-only
  else
    msg "未检测到现有配置，执行首次安装流程..."
    bash "$INSTALL_SCRIPT"
  fi
}

run_reconfigure() {
  if [[ ! -f "$INSTALL_SCRIPT" ]]; then
    err "未找到 install.sh: $INSTALL_SCRIPT"
    return
  fi
  msg "配置模式选择："
  echo "  1) 快速配置（推荐默认值，最少提问）"
  echo "  2) 高级变量设置向导（逐项说明，全部可调）"
  local cfg_mode
  read -r -p "请选择 [1/2]（默认 1）: " cfg_mode
  cfg_mode="${cfg_mode:-1}"
  if [[ "$cfg_mode" == "2" ]]; then
    bash "$INSTALL_SCRIPT" --configure-only
  else
    bash "$INSTALL_SCRIPT" --configure-quick
  fi
}

show_agent_status() {
  systemctl status "$AGENT_SERVICE" --no-pager || true
}

show_singbox_status_logs() {
  systemctl status "$SINGBOX_SERVICE" --no-pager || true
  echo ""
  msg "最近 80 行 sing-box 日志："
  journalctl -u "$SINGBOX_SERVICE" -n 80 --no-pager || true
}

tail_agent_log() {
  if [[ -f /var/log/sb-agent/agent.log ]]; then
    tail -f /var/log/sb-agent/agent.log
  else
    warn "未找到 /var/log/sb-agent/agent.log，改用 journalctl 跟随输出。"
    journalctl -u "$AGENT_SERVICE" -f
  fi
}

run_cert_check() {
  if [[ -x "$SYSTEM_CERT_CHECK_SCRIPT" ]]; then
    "$SYSTEM_CERT_CHECK_SCRIPT"
    return
  fi
  if [[ -x "$LOCAL_CERT_CHECK_SCRIPT" ]]; then
    "$LOCAL_CERT_CHECK_SCRIPT"
    return
  fi
  err "未找到证书检查脚本。"
}

refresh_certificate() {
  msg "开始执行证书重新申请/刷新流程（安全模式：先备份）..."
  mkdir -p "$BACKUP_DIR"
  local ts backup_tar
  ts="$(date +%Y%m%d-%H%M%S)"
  backup_tar="$BACKUP_DIR/certmagic-${ts}.tar.gz"

  if [[ -d "$CERTMAGIC_DIR" ]]; then
    tar -czf "$backup_tar" -C "$(dirname "$CERTMAGIC_DIR")" "$(basename "$CERTMAGIC_DIR")"
    msg "已备份 certmagic: $backup_tar"
  else
    warn "未检测到 certmagic 目录，跳过备份。"
  fi

  if [[ -f /etc/sing-box/config.json ]]; then
    cp /etc/sing-box/config.json "$BACKUP_DIR/config-${ts}.json.bak"
    msg "已备份配置: $BACKUP_DIR/config-${ts}.json.bak"
  fi

  read -r -p "确认清理 ACME 缓存并重启 sing-box？[Y/n]: " answer
  answer="${answer:-Y}"
  if [[ ! "$answer" =~ ^[Yy]$ ]]; then
    warn "已取消证书刷新。"
    return
  fi

  if [[ -d "$CERTMAGIC_DIR/acme" ]]; then
    rm -rf "$CERTMAGIC_DIR/acme"
    msg "已清理: $CERTMAGIC_DIR/acme"
  else
    warn "未发现 $CERTMAGIC_DIR/acme，无需清理。"
  fi

  systemctl restart "$SINGBOX_SERVICE" || true
  msg "已触发 sing-box 重启。可使用菜单 9/8 查看证书与日志状态。"
}

uninstall_all() {
  warn "将执行卸载：停止并移除 sb-agent、证书检查服务与配置文件。"
  read -r -p "确认继续？[y/N]: " answer
  answer="${answer:-N}"
  if [[ ! "$answer" =~ ^[Yy]$ ]]; then
    warn "已取消卸载。"
    return
  fi

  systemctl stop "$AGENT_SERVICE" 2>/dev/null || true
  systemctl disable "$AGENT_SERVICE" 2>/dev/null || true
  systemctl stop "$CERT_TIMER" 2>/dev/null || true
  systemctl disable "$CERT_TIMER" 2>/dev/null || true

  rm -f /etc/systemd/system/sb-agent.service
  rm -f /etc/systemd/system/sb-cert-check.service
  rm -f /etc/systemd/system/sb-cert-check.timer
  rm -f "$SYSTEM_CERT_CHECK_SCRIPT"

  rm -rf /opt/sb-agent
  rm -rf /etc/sb-agent
  rm -rf /var/log/sb-agent

  systemctl daemon-reload
  msg "sb-agent 相关文件已移除。"

  read -r -p "是否一并卸载 sing-box（仅二进制/服务，不删除证书数据）？[y/N]: " rm_sb
  rm_sb="${rm_sb:-N}"
  if [[ "$rm_sb" =~ ^[Yy]$ ]]; then
    systemctl stop "$SINGBOX_SERVICE" 2>/dev/null || true
    systemctl disable "$SINGBOX_SERVICE" 2>/dev/null || true
    rm -f /etc/systemd/system/sing-box.service
    rm -f /usr/local/bin/sing-box
    systemctl daemon-reload
    msg "sing-box 已卸载（如通过其他方式安装，请自行检查残留）。"
  fi

  msg "卸载完成。"
}

show_menu() {
  clear
  echo "========================================"
  echo " sb-agent 中文管理菜单"
  echo "========================================"
  echo "【运行与配置】"
  echo " 1) 配置（快速默认 / 高级变量向导）"
  echo " 2) 启动 sb-agent"
  echo " 3) 停止 sb-agent"
  echo " 4) 重启 sb-agent"
  echo " 5) 查看 sb-agent 状态"
  echo " 6) 查看 sb-agent 日志（tail -f）"
  echo " 7) 重启 sing-box"
  echo " 8) 查看 sing-box 状态与最近日志"
  echo " 9) 证书状态检查"
  echo "10) 触发证书重新申请/刷新（先备份）"
  echo ""
  echo "【安全工具】"
  echo "11) 安装/启用 fail2ban（SSH 防爆破）"
  echo "12) 查看 fail2ban 状态与封禁列表"
  echo "13) 解封 fail2ban 封禁 IP"
  echo "14) 生成 SSH 密钥（ed25519）"
  echo "15) SSH 安全状态总览（只读）"
  echo "16) 一键安全修复（半自动）"
  echo "17) 启用 SSH 仅密钥登录（禁用密码）"
  echo "18) 恢复 SSH 密码登录（应急）"
  echo ""
  echo "【系统级操作（谨慎）】"
  echo "19) 节点运维快照（导出关键状态）"
  echo "20) 更新同步（保留原配置，自动 git pull）"
  echo "21) 卸载"
  echo "22) 退出"
  echo "========================================"
}

main() {
  require_root
  while true; do
    show_menu
    read -r -p "请选择操作 [1-22]: " choice
    case "$choice" in
      1)
        run_reconfigure
        pause
        ;;
      2)
        systemctl start "$AGENT_SERVICE" || true
        msg "已执行启动。"
        pause
        ;;
      3)
        systemctl stop "$AGENT_SERVICE" || true
        msg "已执行停止。"
        pause
        ;;
      4)
        systemctl restart "$AGENT_SERVICE" || true
        msg "已执行重启。"
        pause
        ;;
      5)
        show_agent_status
        pause
        ;;
      6)
        tail_agent_log
        ;;
      7)
        systemctl restart "$SINGBOX_SERVICE" || true
        msg "已执行 sing-box 重启。"
        pause
        ;;
      8)
        show_singbox_status_logs
        pause
        ;;
      9)
        run_cert_check
        pause
        ;;
      10)
        refresh_certificate
        pause
        ;;
      11)
        install_or_enable_fail2ban
        pause
        ;;
      12)
        show_fail2ban_status
        pause
        ;;
      13)
        unban_fail2ban_ip
        pause
        ;;
      14)
        generate_ssh_keypair
        pause
        ;;
      15)
        show_ssh_security_status
        pause
        ;;
      16)
        run_ssh_security_quick_fix
        pause
        ;;
      17)
        enable_ssh_key_only_login
        pause
        ;;
      18)
        disable_ssh_key_only_login
        pause
        ;;
      19)
        if [[ -f "$OPS_SNAPSHOT_SCRIPT" ]]; then
          bash "$OPS_SNAPSHOT_SCRIPT"
        else
          err "未找到节点运维快照脚本: $OPS_SNAPSHOT_SCRIPT"
        fi
        pause
        ;;
      20)
        if confirm_action "确认执行更新同步？" "N"; then
          run_install
        else
          warn "已取消更新同步。"
        fi
        pause
        ;;
      21)
        uninstall_all
        pause
        ;;
      22)
        msg "已退出。"
        exit 0
        ;;
      *)
        warn "无效选项，请输入 1-22。"
        pause
        ;;
    esac
  done
}

main "$@"
