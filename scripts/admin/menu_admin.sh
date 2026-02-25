#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
PROJECT_DIR_DEFAULT="/root/sb-bot-panel"
PROJECT_DIR="${PROJECT_DIR:-$PROJECT_DIR_DEFAULT}"

if [[ ! -f "${PROJECT_DIR}/scripts/admin/install_admin.sh" && -f "${SCRIPT_PROJECT_DIR}/scripts/admin/install_admin.sh" ]]; then
  PROJECT_DIR="$SCRIPT_PROJECT_DIR"
fi

INSTALL_SCRIPT="${PROJECT_DIR}/scripts/admin/install_admin.sh"
EXPORT_SCRIPT="${PROJECT_DIR}/scripts/admin/sb_migrate_export.sh"
IMPORT_SCRIPT="${PROJECT_DIR}/scripts/admin/sb_migrate_import.sh"

msg() { echo -e "\033[1;32m[信息]\033[0m $*"; }
warn() { echo -e "\033[1;33m[警告]\033[0m $*"; }
err() { echo -e "\033[1;31m[错误]\033[0m $*" >&2; }

require_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    err "请使用 root 权限运行（sudo）。"
    exit 1
  fi
}

pause() {
  echo ""
  read -r -p "按回车继续..." _
}

show_config_guide() {
  echo "配置项用途说明："
  echo "  - CONTROLLER_PORT（controller 对外监听端口；节点 agent 需要访问）"
  echo "  - CONTROLLER_PUBLIC_URL（可选，给节点/外部访问的完整 URL）"
  echo "  - AUTH_TOKEN（可选；用于保护 /admin/*；也给 bot/agent 调用时使用）"
  echo "  - BOT_TOKEN（必填；Telegram 机器人 token）"
  echo "  - ADMIN_CHAT_IDS（可选；限制谁能使用 bot）"
  echo "  - MIGRATE_DIR（迁移包/备份包输出目录）"
  echo "  - BOT_MENU_TTL（bot 菜单按钮自动清理秒数）"
  echo "  - BOT_NODE_MONITOR_INTERVAL（节点在线检测周期秒数）"
  echo "  - BOT_NODE_OFFLINE_THRESHOLD（节点离线判定阈值秒数）"
  echo "  - UFW/端口放行（按需开放 controller 端口，并限制来源）"
  echo ""
}

show_menu() {
  clear
  cat <<'EOF'
========================================
 sb-bot-panel 管理服务器菜单
========================================
1. 安装/更新（git pull + 依赖 + venv + 重启）
2. 配置向导（仅写 .env 并重启）
3. 启动 controller
4. 停止 controller
5. 启动 bot
6. 停止 bot
7. 状态查看（controller/bot）
8. 查看日志（controller/bot）
9. 迁移：导出迁移包
10. 迁移：导入迁移包
11. 卸载
12. 退出
========================================
EOF
}

install_or_update() {
  if [[ -d "${PROJECT_DIR}/.git" ]]; then
    msg "检测到 Git 仓库，执行 git pull..."
    git -C "$PROJECT_DIR" pull --ff-only || warn "git pull 失败，请手动处理。"
  else
    warn "未检测到 .git，跳过 git pull。"
  fi

  if [[ -d "${PROJECT_DIR}/venv" ]]; then
    read -r -p "是否重建 venv（推荐）？[Y/n]: " answer
    answer="${answer:-Y}"
    if [[ "$answer" =~ ^[Yy]$ ]]; then
      rm -rf "${PROJECT_DIR}/venv"
      msg "已删除旧 venv。"
    fi
  fi

  if [[ -f "$INSTALL_SCRIPT" ]]; then
    bash "$INSTALL_SCRIPT"
  else
    err "未找到安装脚本: $INSTALL_SCRIPT"
  fi
}

configure_only() {
  if [[ -f "$INSTALL_SCRIPT" ]]; then
    msg "即将进入配置向导（仅写 .env 并重启服务）。"
    show_config_guide
    bash "$INSTALL_SCRIPT" --configure-only
  else
    err "未找到安装脚本: $INSTALL_SCRIPT"
  fi
}

show_status() {
  echo "----- sb-controller -----"
  systemctl status sb-controller --no-pager || true
  echo ""
  echo "----- sb-bot -----"
  systemctl status sb-bot --no-pager || true
}

show_logs() {
  local choice
  read -r -p "查看哪个服务日志？1=controller 2=bot [1]: " choice
  choice="${choice:-1}"
  if [[ "$choice" == "2" ]]; then
    journalctl -u sb-bot -n 200 --no-pager || true
  else
    journalctl -u sb-controller -n 200 --no-pager || true
  fi
}

do_uninstall() {
  read -r -p "确认卸载服务？[y/N]: " answer
  answer="${answer:-N}"
  if [[ ! "$answer" =~ ^[Yy]$ ]]; then
    warn "已取消。"
    return
  fi

  systemctl stop sb-bot 2>/dev/null || true
  systemctl stop sb-controller 2>/dev/null || true
  systemctl disable sb-bot 2>/dev/null || true
  systemctl disable sb-controller 2>/dev/null || true
  rm -f /etc/systemd/system/sb-bot.service
  rm -f /etc/systemd/system/sb-controller.service
  systemctl daemon-reload
  msg "服务已卸载。"

  read -r -p "是否删除项目目录 ${PROJECT_DIR}？[y/N]: " remove_proj
  remove_proj="${remove_proj:-N}"
  if [[ "$remove_proj" =~ ^[Yy]$ ]]; then
    rm -rf "$PROJECT_DIR"
    msg "项目目录已删除。"
  fi
}

main() {
  require_root

  while true; do
    show_menu
    read -r -p "请输入选项 [1-12]: " action
    case "$action" in
      1)
        install_or_update
        pause
        ;;
      2)
        configure_only
        pause
        ;;
      3)
        systemctl start sb-controller || true
        msg "已执行启动 controller。"
        pause
        ;;
      4)
        systemctl stop sb-controller || true
        msg "已执行停止 controller。"
        pause
        ;;
      5)
        systemctl start sb-bot || true
        msg "已执行启动 bot。"
        pause
        ;;
      6)
        systemctl stop sb-bot || true
        msg "已执行停止 bot。"
        pause
        ;;
      7)
        show_status
        pause
        ;;
      8)
        show_logs
        pause
        ;;
      9)
        if [[ -f "$EXPORT_SCRIPT" ]]; then
          bash "$EXPORT_SCRIPT"
        else
          err "未找到导出脚本: $EXPORT_SCRIPT"
        fi
        pause
        ;;
      10)
        if [[ -f "$IMPORT_SCRIPT" ]]; then
          read -r -p "请输入迁移包路径: " pkg_path
          bash "$IMPORT_SCRIPT" "$pkg_path"
        else
          err "未找到导入脚本: $IMPORT_SCRIPT"
        fi
        pause
        ;;
      11)
        do_uninstall
        pause
        ;;
      12)
        msg "已退出。"
        exit 0
        ;;
      *)
        warn "无效选项，请输入 1-12。"
        pause
        ;;
    esac
  done
}

main "$@"
