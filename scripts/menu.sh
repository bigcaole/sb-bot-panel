#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
INSTALL_SCRIPT="$ROOT_DIR/scripts/install.sh"
LOCAL_CERT_CHECK_SCRIPT="$ROOT_DIR/scripts/sb_cert_check.sh"
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

generate_ssh_keypair() {
  local user_name user_home key_path passphrase comment overwrite
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

  msg "公钥如下（请加入服务器 ~/.ssh/authorized_keys）："
  cat "${key_path}.pub"
}

enable_ssh_key_only_login() {
  local user_name ssh_service
  read -r -p "请输入用于校验 authorized_keys 的用户名 [root]: " user_name
  user_name="${user_name:-root}"

  if ! has_authorized_keys_for_user "$user_name"; then
    warn "用户 ${user_name} 没有可用 authorized_keys，拒绝启用（避免锁死 SSH）。"
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
  bash "$INSTALL_SCRIPT" --configure-only
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
  msg "已触发 sing-box 重启。可使用菜单 10/9 查看证书与日志状态。"
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
  echo " 1) 配置（修改 /etc/sb-agent/config.json）"
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
  echo "15) 启用 SSH 仅密钥登录（禁用密码）"
  echo "16) 恢复 SSH 密码登录（应急）"
  echo ""
  echo "【系统级操作（谨慎）】"
  echo "17) 更新同步（保留原配置，自动 git pull）"
  echo "18) 卸载"
  echo "19) 退出"
  echo "========================================"
}

main() {
  require_root
  while true; do
    show_menu
    read -r -p "请选择操作 [1-19]: " choice
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
        enable_ssh_key_only_login
        pause
        ;;
      16)
        disable_ssh_key_only_login
        pause
        ;;
      17)
        if confirm_action "确认执行更新同步？" "N"; then
          run_install
        else
          warn "已取消更新同步。"
        fi
        pause
        ;;
      18)
        uninstall_all
        pause
        ;;
      19)
        if confirm_action "确认退出菜单？" "Y"; then
          msg "已退出。"
          exit 0
        fi
        ;;
      *)
        warn "无效选项，请输入 1-19。"
        pause
        ;;
    esac
  done
}

main "$@"
