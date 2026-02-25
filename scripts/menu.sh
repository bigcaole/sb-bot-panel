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

msg() { echo -e "\033[1;32m[信息]\033[0m $*"; }
warn() { echo -e "\033[1;33m[警告]\033[0m $*"; }
err() { echo -e "\033[1;31m[错误]\033[0m $*" >&2; }

pause() {
  echo ""
  read -r -p "按回车继续..." _
}

require_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    err "请使用 root 权限运行菜单，例如：sudo bash scripts/menu.sh"
    exit 1
  fi
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
  echo " 1) 更新同步（保留原配置，自动 git pull）"
  echo " 2) 配置（修改 /etc/sb-agent/config.json）"
  echo " 3) 启动 sb-agent"
  echo " 4) 停止 sb-agent"
  echo " 5) 重启 sb-agent"
  echo " 6) 查看 sb-agent 状态"
  echo " 7) 查看 sb-agent 日志（tail -f）"
  echo " 8) 重启 sing-box"
  echo " 9) 查看 sing-box 状态与最近日志"
  echo "10) 证书状态检查"
  echo "11) 触发证书重新申请/刷新（先备份）"
  echo "12) 卸载"
  echo " 0) 退出"
  echo "========================================"
}

main() {
  require_root
  while true; do
    show_menu
    read -r -p "请选择操作 [0-12]: " choice
    case "$choice" in
      1)
        run_install
        pause
        ;;
      2)
        run_reconfigure
        pause
        ;;
      3)
        systemctl start "$AGENT_SERVICE" || true
        msg "已执行启动。"
        pause
        ;;
      4)
        systemctl stop "$AGENT_SERVICE" || true
        msg "已执行停止。"
        pause
        ;;
      5)
        systemctl restart "$AGENT_SERVICE" || true
        msg "已执行重启。"
        pause
        ;;
      6)
        show_agent_status
        pause
        ;;
      7)
        tail_agent_log
        ;;
      8)
        systemctl restart "$SINGBOX_SERVICE" || true
        msg "已执行 sing-box 重启。"
        pause
        ;;
      9)
        show_singbox_status_logs
        pause
        ;;
      10)
        run_cert_check
        pause
        ;;
      11)
        refresh_certificate
        pause
        ;;
      12)
        uninstall_all
        pause
        ;;
      0)
        msg "已退出。"
        exit 0
        ;;
      *)
        warn "无效选项，请输入 0-12。"
        pause
        ;;
    esac
  done
}

main "$@"
