import os
import logging
import re
import time
import ipaddress
import asyncio
import shlex
import subprocess
import json
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse

import httpx
from telegram import BotCommand, BotCommandScopeChat, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import BadRequest, Forbidden
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)


logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


def get_primary_auth_token(raw: str) -> str:
    for item in str(raw or "").split(","):
        token = str(item or "").strip()
        if token:
            return token
    return ""


CONTROLLER_URL = os.getenv("CONTROLLER_URL", "http://127.0.0.1:8080").rstrip("/")
if not CONTROLLER_URL:
    CONTROLLER_URL = "http://127.0.0.1:8080"
CONTROLLER_AUTH_TOKEN = get_primary_auth_token(os.getenv("AUTH_TOKEN", "").strip())
BOT_ACTOR_LABEL = os.getenv("BOT_ACTOR_LABEL", "sb-bot").strip() or "sb-bot"
try:
    CONTROLLER_HTTP_TIMEOUT_SECONDS = float(
        os.getenv("CONTROLLER_HTTP_TIMEOUT", "10").strip() or "10"
    )
except ValueError:
    CONTROLLER_HTTP_TIMEOUT_SECONDS = 10.0
if CONTROLLER_HTTP_TIMEOUT_SECONDS <= 0:
    CONTROLLER_HTTP_TIMEOUT_SECONDS = 10.0
_parsed_controller_url = urlparse(CONTROLLER_URL)
if _parsed_controller_url.port:
    CONTROLLER_PORT_HINT = int(_parsed_controller_url.port)
elif _parsed_controller_url.scheme == "https":
    CONTROLLER_PORT_HINT = 443
else:
    CONTROLLER_PORT_HINT = 8080
raw_controller_port = os.getenv("CONTROLLER_PORT", "").strip()
if raw_controller_port.isdigit():
    CONTROLLER_PORT_HINT = int(raw_controller_port)

PANEL_BASE_URL = os.getenv("PANEL_BASE_URL", CONTROLLER_URL).rstrip("/")
if not PANEL_BASE_URL:
    PANEL_BASE_URL = CONTROLLER_URL
try:
    MENU_AUTO_CLEAR_SECONDS = int(os.getenv("BOT_MENU_TTL", "60").strip() or "60")
except ValueError:
    MENU_AUTO_CLEAR_SECONDS = 60
if MENU_AUTO_CLEAR_SECONDS <= 0:
    MENU_AUTO_CLEAR_SECONDS = 60
try:
    LOG_VIEW_COOLDOWN_SECONDS = float(
        os.getenv("BOT_LOG_VIEW_COOLDOWN", "1").strip() or "1"
    )
except ValueError:
    LOG_VIEW_COOLDOWN_SECONDS = 1.0
if LOG_VIEW_COOLDOWN_SECONDS < 0:
    LOG_VIEW_COOLDOWN_SECONDS = 0.0
try:
    MUTATION_COOLDOWN_SECONDS = float(
        os.getenv("BOT_MUTATION_COOLDOWN", "1").strip() or "1"
    )
except ValueError:
    MUTATION_COOLDOWN_SECONDS = 1.0
if MUTATION_COOLDOWN_SECONDS < 0:
    MUTATION_COOLDOWN_SECONDS = 0.0
try:
    LOG_VIEW_MAX_PAGES = int(os.getenv("BOT_LOG_VIEW_MAX_PAGES", "100").strip() or "100")
except ValueError:
    LOG_VIEW_MAX_PAGES = 100
if LOG_VIEW_MAX_PAGES < 1:
    LOG_VIEW_MAX_PAGES = 1
try:
    OPS_AUDIT_WINDOW_SECONDS = int(
        os.getenv("BOT_OPS_AUDIT_WINDOW_SECONDS", "604800").strip() or "604800"
    )
except ValueError:
    OPS_AUDIT_WINDOW_SECONDS = 604800
if OPS_AUDIT_WINDOW_SECONDS < 3600:
    OPS_AUDIT_WINDOW_SECONDS = 3600
if OPS_AUDIT_WINDOW_SECONDS > 30 * 86400:
    OPS_AUDIT_WINDOW_SECONDS = 30 * 86400

MENU_AUTO_CLEAR_JOBS_KEY = "menu_auto_clear_jobs"
NODE_MONITOR_STATE_KEY = "node_monitor_state"
KNOWN_CHAT_IDS_KEY = "known_chat_ids"
MAIN_MENU_MESSAGE_IDS_KEY = "main_menu_message_ids"
LOG_VIEW_LAST_ACTION_AT_KEY = "maintain_log_last_action_at"
MUTATION_LAST_ACTION_MAP_KEY = "mutation_last_action_map"
try:
    NODE_MONITOR_INTERVAL_SECONDS = int(
        os.getenv("BOT_NODE_MONITOR_INTERVAL", "60").strip() or "60"
    )
except ValueError:
    NODE_MONITOR_INTERVAL_SECONDS = 60
if NODE_MONITOR_INTERVAL_SECONDS <= 0:
    NODE_MONITOR_INTERVAL_SECONDS = 60
try:
    NODE_OFFLINE_THRESHOLD_SECONDS = int(
        os.getenv("BOT_NODE_OFFLINE_THRESHOLD", "120").strip() or "120"
    )
except ValueError:
    NODE_OFFLINE_THRESHOLD_SECONDS = 120
if NODE_OFFLINE_THRESHOLD_SECONDS <= 0:
    NODE_OFFLINE_THRESHOLD_SECONDS = 120
try:
    NODE_TIME_SYNC_INTERVAL_SECONDS = int(
        os.getenv("BOT_NODE_TIME_SYNC_INTERVAL", "86400").strip() or "86400"
    )
except ValueError:
    NODE_TIME_SYNC_INTERVAL_SECONDS = 86400
if NODE_TIME_SYNC_INTERVAL_SECONDS < 0:
    NODE_TIME_SYNC_INTERVAL_SECONDS = 0
if NODE_TIME_SYNC_INTERVAL_SECONDS > 30 * 86400:
    NODE_TIME_SYNC_INTERVAL_SECONDS = 30 * 86400

WIZARD_KEY = "create_user_wizard"
CREATE_DISPLAY_NAME, CREATE_TUIC_PORT, CREATE_SPEED_MBPS, CREATE_VALID_DAYS, CREATE_CONFIRM = range(5)
NODES_WIZARD_KEY = "nodes_create_wizard"
(
    NODE_CREATE_NODE_CODE,
    NODE_CREATE_REGION,
    NODE_CREATE_HOST,
    NODE_CREATE_AGENT_IP,
    NODE_CREATE_REALITY_SERVER_NAME,
    NODE_CREATE_TUIC_PORT_START,
    NODE_CREATE_TUIC_PORT_END,
    NODE_CREATE_NOTE,
    NODE_CREATE_CONFIRM,
) = range(100, 109)
NODE_EDIT_KEY = "node_edit_wizard"
(
    NODE_EDIT_HOST,
    NODE_EDIT_SNI,
    NODE_EDIT_POOL,
    NODE_EDIT_CONFIRM,
    NODE_EDIT_TUIC_SNI,
    NODE_EDIT_AGENT_IP,
) = range(200, 206)
NODE_REALITY_KEY = "node_reality_setup_wizard"
NODE_REALITY_PASTE, NODE_REALITY_CONFIRM = 500, 501
USER_NODES_WIZARD_KEY = "user_nodes_wizard"
USER_NODES_INPUT = 300
USER_SPEED_PENDING_KEY = "user_speed_pending"
USER_SPEED_ACTIVE_KEY = "user_speed_active"
USER_SPEED_INPUT, USER_SPEED_CONFIRM = 400, 401
MAINTAIN_CONFIG_INPUT = 700
MAINTAIN_IMPORT_INPUT = 701
NODE_OPS_CONFIG_KEY = "node_ops_config_wizard"
NODE_OPS_CONFIG_INPUT = 900
NODE_OPS_ALLOWED_KEYS = {
    "poll_interval",
    "tuic_domain",
    "tuic_listen_port",
    "acme_email",
    "controller_url",
    "auth_token",
    "node_code",
}

_controller_http_client: Optional[httpx.AsyncClient] = None

ADMIN_PROJECT_DIR = os.getenv("SB_PANEL_DIR", "/root/sb-bot-panel").strip() or "/root/sb-bot-panel"
ADMIN_ENV_FILE = os.path.join(ADMIN_PROJECT_DIR, ".env")
ADMIN_UPDATE_SCRIPT = os.path.join(ADMIN_PROJECT_DIR, "scripts/admin/install_admin.sh")
ADMIN_IMPORT_SCRIPT = os.path.join(ADMIN_PROJECT_DIR, "scripts/admin/sb_migrate_import.sh")
ADMIN_SMOKE_SCRIPT = os.path.join(ADMIN_PROJECT_DIR, "scripts/admin/smoke_test.sh")
ADMIN_LOG_ARCHIVE_SCRIPT = os.path.join(ADMIN_PROJECT_DIR, "scripts/admin/log_archive.sh")
ADMIN_TOKEN_COLLAPSE_SCRIPT = os.path.join(
    ADMIN_PROJECT_DIR, "scripts/admin/auth_token_collapse.sh"
)


SUBMENUS = {
    "user": {
        "title": "用户管理",
        "buttons": [
            ("👤 创建用户", "action:user_create"),
            ("🔁 禁用/启用", "action:user_toggle"),
            ("🗑 删除用户", "action:user_delete"),
            ("🚦 修改限速", "action:user_speed"),
            ("🧩 节点分配", "action:user_nodes"),
            ("⬅️ 返回主菜单", "menu:main"),
        ],
    },
    "speed": {
        "title": "限速管理",
        "buttons": [
            ("🚦 设置限速", "action:user_speed"),
            ("🔀 切换限速模式", "action:speed_switch"),
            ("⬅️ 返回主菜单", "menu:main"),
        ],
    },
    "query": {
        "title": "查询",
        "buttons": [
            ("🔎 用户信息", "action:query_user_info"),
            ("⏳ 即将到期", "action:query_expiring"),
            ("📊 流量排行", "action:query_traffic"),
            ("⬅️ 返回主菜单", "menu:main"),
        ],
    },
    "backup": {
        "title": "备份与维护",
        "buttons": [
            ("💾 立即备份", "action:backup_now"),
            ("🧾 操作日志", "action:backup_audit"),
            ("🛑 紧急停止", "action:backup_stop"),
            ("⬅️ 返回主菜单", "menu:main"),
        ],
    },
    "nodes": {
        "title": "节点管理",
        "buttons": [
            ("🗂 列表与详情", "menu:nodes_listing"),
            ("🧱 参数与创建", "menu:nodes_edit"),
            ("🛠 远程运维", "menu:nodes_ops"),
            ("⬅️ 返回主菜单", "menu:main"),
        ],
    },
    "nodes_listing": {
        "title": "节点管理 / 列表与详情",
        "buttons": [
            ("🗂 查看节点列表", "action:nodes_list"),
            ("⬅️ 返回节点管理", "menu:nodes"),
        ],
    },
    "nodes_edit": {
        "title": "节点管理 / 参数与创建",
        "buttons": [
            ("➕ 新增节点", "action:nodes_create"),
            ("⬅️ 返回节点管理", "menu:nodes"),
        ],
    },
    "nodes_ops": {
        "title": "节点管理 / 远程运维",
        "buttons": [
            ("🛠 节点远程运维", "action:node_ops"),
            ("⬅️ 返回节点管理", "menu:nodes"),
        ],
    },
    "maintain": {
        "title": "管理服务器",
        "buttons": [
            ("🧭 服务运维", "menu:maintain_ops"),
            ("🛡 安全访问", "menu:maintain_security"),
            ("🔐 证书与备份", "menu:maintain_cert"),
            ("📦 迁移与配置", "menu:maintain_data"),
            ("⬅️ 返回主菜单", "menu:main"),
        ],
    },
    "maintain_ops": {
        "title": "管理服务器 / 服务运维",
        "buttons": [
            ("📈 状态查看", "action:maintain_status"),
            ("🧾 运维审计", "action:maintain_ops_audit"),
            ("📜 查看日志", "action:maintain_logs"),
            ("🗂 日志归档", "action:maintain_log_archive"),
            ("✅ 一键验收自检", "action:maintain_smoke"),
            ("▶️ 启动controller", "action:maintain_controller_start"),
            ("⏹ 停止controller", "action:maintain_controller_stop"),
            ("⬅️ 返回管理服务器", "menu:maintain"),
        ],
    },
    "maintain_security": {
        "title": "管理服务器 / 安全访问",
        "buttons": [
            ("🛡 安全事件(1h)", "action:maintain_security_events"),
            ("🧩 订阅安全预设", "action:maintain_sub_policy"),
            ("🛑 紧急停用用户", "action:backup_stop"),
            ("🔄 节点默认参数同步", "action:maintain_sync_node_defaults"),
            ("🔁 同步节点Token", "action:maintain_sync_node_tokens"),
            ("🕒 同步节点时间", "action:maintain_sync_node_time"),
            ("🧱 访问安全", "action:maintain_acl_status"),
            ("🔑 收敛AUTH_TOKEN", "action:maintain_token_collapse"),
            ("⬅️ 返回管理服务器", "menu:maintain"),
        ],
    },
    "maintain_cert": {
        "title": "管理服务器 / 证书与备份",
        "buttons": [
            ("🔐 HTTPS证书状态", "action:maintain_https_status"),
            ("♻️ HTTPS证书刷新", "action:maintain_https_reload"),
            ("💾 立即备份", "action:maintain_backup"),
            ("⬅️ 返回管理服务器", "menu:maintain"),
        ],
    },
    "maintain_data": {
        "title": "管理服务器 / 迁移与配置",
        "buttons": [
            ("⬆️ 安装/更新（本管理服务器）", "action:maintain_update"),
            ("⚙️ 配置向导", "action:maintain_config"),
            ("📦 生成迁移包", "action:maintain_migrate_export"),
            ("📥 迁移导入", "action:maintain_migrate_import"),
            ("⬅️ 返回管理服务器", "menu:maintain"),
        ],
    },
}


ACTION_LABELS = {}
ACTION_PARENT = {}
for submenu_key, submenu in SUBMENUS.items():
    for label, callback_data in submenu["buttons"]:
        if callback_data.startswith("action:"):
            ACTION_LABELS[callback_data] = label
            ACTION_PARENT[callback_data] = submenu_key


def parse_chat_id_list(raw_value: str) -> list:
    chat_ids = []
    for part in raw_value.split(","):
        token = part.strip()
        if not token:
            continue
        try:
            chat_ids.append(int(token))
        except ValueError:
            logger.warning("invalid ADMIN_CHAT_IDS item ignored: %s", token)
    return chat_ids


ADMIN_CHAT_ID_LIST = parse_chat_id_list(os.getenv("ADMIN_CHAT_IDS", ""))
VIEW_ADMIN_CHAT_ID_LIST = parse_chat_id_list(os.getenv("VIEW_ADMIN_CHAT_IDS", ""))
OPS_ADMIN_CHAT_ID_LIST = parse_chat_id_list(os.getenv("OPS_ADMIN_CHAT_IDS", ""))
SUPER_ADMIN_CHAT_ID_LIST = parse_chat_id_list(os.getenv("SUPER_ADMIN_CHAT_IDS", ""))

ROLE_VIEWER = 1
ROLE_OPERATOR = 2
ROLE_SUPER = 3

ROLE_SPLIT_ENABLED = bool(
    VIEW_ADMIN_CHAT_ID_LIST or OPS_ADMIN_CHAT_ID_LIST or SUPER_ADMIN_CHAT_ID_LIST
)
SUPER_ADMIN_CHAT_ID_SET = set(ADMIN_CHAT_ID_LIST)
SUPER_ADMIN_CHAT_ID_SET.update(SUPER_ADMIN_CHAT_ID_LIST)
OPS_ADMIN_CHAT_ID_SET = set(OPS_ADMIN_CHAT_ID_LIST)
OPS_ADMIN_CHAT_ID_SET.update(SUPER_ADMIN_CHAT_ID_SET)
VIEW_ADMIN_CHAT_ID_SET = set(VIEW_ADMIN_CHAT_ID_LIST)
VIEW_ADMIN_CHAT_ID_SET.update(OPS_ADMIN_CHAT_ID_SET)

PRIVILEGED_CALLBACK_EXACT = {
    "action:user_create": ROLE_OPERATOR,
    "action:user_toggle": ROLE_OPERATOR,
    "action:user_speed": ROLE_OPERATOR,
    "action:user_nodes": ROLE_OPERATOR,
    "action:speed_switch": ROLE_OPERATOR,
    "action:backup_now": ROLE_OPERATOR,
    "action:backup_stop": ROLE_SUPER,
    "action:nodes_create": ROLE_OPERATOR,
    "action:maintain_backup": ROLE_OPERATOR,
    "action:maintain_smoke": ROLE_OPERATOR,
    "action:maintain_log_archive": ROLE_OPERATOR,
    "action:maintain_ops_audit": ROLE_OPERATOR,
    "action:maintain_sub_policy": ROLE_OPERATOR,
    "action:maintain_controller_start": ROLE_OPERATOR,
    "action:maintain_https_reload": ROLE_OPERATOR,
    "action:maintain_sync_node_defaults": ROLE_SUPER,
    "action:maintain_sync_node_tokens": ROLE_SUPER,
    "action:maintain_sync_node_time": ROLE_SUPER,
    "action:user_delete": ROLE_SUPER,
    "action:node_ops": ROLE_SUPER,
    "action:maintain_update": ROLE_SUPER,
    "action:maintain_config": ROLE_SUPER,
    "action:maintain_controller_stop": ROLE_SUPER,
    "action:maintain_migrate_export": ROLE_SUPER,
    "action:maintain_migrate_import": ROLE_SUPER,
    "action:maintain_token_collapse": ROLE_SUPER,
    "maintain:token_collapse:confirm": ROLE_SUPER,
    "backup:stop:confirm": ROLE_SUPER,
    "usernodes:manual_input": ROLE_OPERATOR,
    "wizard:create_confirm": ROLE_OPERATOR,
    "wizard:nodes_create_confirm": ROLE_OPERATOR,
}

PRIVILEGED_CALLBACK_PREFIXES = {
    "userdelete:": ROLE_SUPER,
    "usertoggle:": ROLE_OPERATOR,
    "userspeed:": ROLE_OPERATOR,
    "usermode:": ROLE_OPERATOR,
    "usernodes:": ROLE_OPERATOR,
    "node:toggle:": ROLE_OPERATOR,
    "node:monitor_toggle:": ROLE_OPERATOR,
    "node:sync_preview:": ROLE_OPERATOR,
    "node:delete_confirm:": ROLE_SUPER,
    "node:delete:": ROLE_SUPER,
    "node:edit_": ROLE_OPERATOR,
    "node:apply_edit:": ROLE_OPERATOR,
    "node:reality_paste:": ROLE_OPERATOR,
    "node:reality_apply:": ROLE_OPERATOR,
    "nodeops:": ROLE_SUPER,
    "maintain:logs:": ROLE_OPERATOR,
    "maintain:logsdate:": ROLE_OPERATOR,
    "maintain:subpolicy:": ROLE_SUPER,
    "sb:bl:": ROLE_OPERATOR,
    "sb:bi:": ROLE_SUPER,
    "sb:bd:": ROLE_SUPER,
    "sb:ba:": ROLE_SUPER,
    "sb:bu:": ROLE_SUPER,
    "sb:mc:": ROLE_SUPER,
    "sb:ab:": ROLE_SUPER,
}

MUTATION_CALLBACK_EXACT = {
    "wizard:create_confirm",
    "wizard:nodes_create_confirm",
    "action:backup_now",
    "backup:stop:confirm",
    "action:maintain_backup",
    "action:maintain_smoke",
    "action:maintain_log_archive",
    "action:maintain_update",
    "action:maintain_controller_start",
    "action:maintain_controller_stop",
    "action:maintain_https_reload",
    "action:maintain_migrate_export",
    "action:maintain_migrate_import",
    "maintain:token_collapse:confirm",
    "action:maintain_sync_node_defaults",
    "action:maintain_sync_node_tokens",
    "action:maintain_sync_node_time",
}

MUTATION_CALLBACK_PREFIXES = (
    "userdelete:apply:",
    "usertoggle:apply:",
    "userspeed:apply:",
    "usermode:apply:",
    "usernodes:assign_apply:",
    "usernodes:unassign_apply:",
    "node:toggle:",
    "node:monitor_toggle:",
    "node:delete:",
    "node:apply_edit:",
    "node:reality_apply:",
    "nodeops:run:",
    "sb:ba:",
    "sb:bu:",
    "sb:mc:",
    "sb:ab:",
    "maintain:subpolicy:",
)
MAINTAIN_ALLOWED_ENV_KEYS = [
    "CONTROLLER_PORT",
    "CONTROLLER_PORT_WHITELIST",
    "CONTROLLER_URL",
    "CONTROLLER_PUBLIC_URL",
    "PANEL_BASE_URL",
    "AUTH_TOKEN",
    "BOT_TOKEN",
    "ADMIN_CHAT_IDS",
    "VIEW_ADMIN_CHAT_IDS",
    "OPS_ADMIN_CHAT_IDS",
    "SUPER_ADMIN_CHAT_IDS",
    "ENABLE_HTTPS",
    "HTTPS_DOMAIN",
    "HTTPS_ACME_EMAIL",
    "MIGRATE_DIR",
    "LOG_ARCHIVE_WINDOW_HOURS",
    "LOG_ARCHIVE_RETENTION_COUNT",
    "LOG_ARCHIVE_DIR",
    "BOT_MENU_TTL",
    "BOT_NODE_MONITOR_INTERVAL",
    "BOT_NODE_OFFLINE_THRESHOLD",
    "BOT_NODE_TIME_SYNC_INTERVAL",
    "BOT_LOG_VIEW_COOLDOWN",
    "BOT_MUTATION_COOLDOWN",
    "BOT_LOG_VIEW_MAX_PAGES",
    "BOT_OPS_AUDIT_WINDOW_SECONDS",
    "CONTROLLER_HTTP_TIMEOUT",
    "BOT_ACTOR_LABEL",
    "SUB_LINK_SIGN_KEY",
    "SUB_LINK_REQUIRE_SIGNATURE",
    "SUB_LINK_DEFAULT_TTL_SECONDS",
    "API_RATE_LIMIT_ENABLED",
    "API_RATE_LIMIT_WINDOW_SECONDS",
    "API_RATE_LIMIT_MAX_REQUESTS",
    "SECURITY_EVENTS_EXCLUDE_LOCAL",
    "UNAUTHORIZED_AUDIT_SAMPLE_SECONDS",
    "SECURITY_BLOCK_CLEANUP_INTERVAL_SECONDS",
    "SECURITY_AUTO_BLOCK_ENABLED",
    "SECURITY_AUTO_BLOCK_INTERVAL_SECONDS",
    "SECURITY_AUTO_BLOCK_WINDOW_SECONDS",
    "SECURITY_AUTO_BLOCK_THRESHOLD",
    "SECURITY_AUTO_BLOCK_DURATION_SECONDS",
    "SECURITY_AUTO_BLOCK_MAX_PER_INTERVAL",
    "AUDIT_LOG_RETENTION_DAYS",
    "AUDIT_LOG_CLEANUP_INTERVAL_SECONDS",
    "AUDIT_LOG_CLEANUP_BATCH_SIZE",
]


def truncate_output(text: str, limit: int = 3200) -> str:
    raw = str(text or "").strip()
    if not raw:
        return "(空)"
    if len(raw) <= limit:
        return raw
    return raw[:limit] + "\n...（输出已截断）"


def format_recent_log_output(raw_text: str, line_limit: int = 50, char_limit: int = 3200) -> str:
    raw = str(raw_text or "").replace("\r\n", "\n").strip()
    if not raw:
        return "(空)"
    lines = raw.splitlines()
    if len(lines) > line_limit:
        lines = lines[-line_limit:]
    text = "\n".join(lines).strip()
    if len(text) <= char_limit:
        return text
    return "…（仅显示最近日志片段）\n" + text[-char_limit:]


async def run_local_shell(command: str, timeout: int = 30) -> tuple:
    proc = await asyncio.create_subprocess_shell(
        command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout_bytes, stderr_bytes = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.communicate()
        return -1, "", "命令执行超时"
    stdout = (stdout_bytes or b"").decode("utf-8", errors="replace")
    stderr = (stderr_bytes or b"").decode("utf-8", errors="replace")
    return int(proc.returncode or 0), stdout, stderr


def launch_background_job(shell_command: str, job_tag: str) -> str:
    timestamp = int(time.time())
    safe_tag = re.sub(r"[^a-zA-Z0-9_-]+", "-", job_tag).strip("-") or "job"
    log_path = "/tmp/sb-bot-{0}-{1}.log".format(safe_tag, timestamp)
    full_command = "{0} > {1} 2>&1".format(shell_command, shlex.quote(log_path))
    try:
        process = subprocess.Popen(  # nosec B603
            ["bash", "-lc", full_command],
            start_new_session=True,
        )
        logger.info("background_job tag=%s pid=%s cmd=%s", safe_tag, process.pid, shell_command)
    except Exception as exc:
        logger.error("launch background job failed: %s", exc)
    return log_path


def load_env_map(env_path: str = ADMIN_ENV_FILE) -> dict:
    result = {}
    if not os.path.exists(env_path):
        return result
    try:
        with open(env_path, "r", encoding="utf-8") as file_obj:
            for raw_line in file_obj:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                result[key.strip()] = value.strip()
    except OSError:
        return {}
    return result


def write_env_updates(updates: dict, env_path: str = ADMIN_ENV_FILE) -> tuple:
    if not updates:
        return False, "没有可更新项"

    lines = []
    existing_keys = set()
    if os.path.exists(env_path):
        try:
            with open(env_path, "r", encoding="utf-8") as file_obj:
                lines = file_obj.readlines()
        except OSError as exc:
            return False, "读取 .env 失败: {0}".format(exc)

    output_lines = []
    for raw_line in lines:
        stripped = raw_line.strip()
        if stripped.startswith("#") or "=" not in stripped:
            output_lines.append(raw_line)
            continue
        key = stripped.split("=", 1)[0].strip()
        if key in updates:
            output_lines.append("{0}={1}\n".format(key, updates[key]))
            existing_keys.add(key)
        else:
            output_lines.append(raw_line)

    for key, value in updates.items():
        if key not in existing_keys:
            output_lines.append("{0}={1}\n".format(key, value))

    try:
        os.makedirs(os.path.dirname(env_path), exist_ok=True)
        with open(env_path, "w", encoding="utf-8") as file_obj:
            file_obj.writelines(output_lines)
    except OSError as exc:
        return False, "写入 .env 失败: {0}".format(exc)
    return True, ""


def backup_env_file(prefix: str = "env-backup", env_path: str = ADMIN_ENV_FILE) -> tuple:
    if not os.path.exists(env_path):
        return "", ".env 不存在：{0}".format(env_path)
    timestamp = int(time.time())
    safe_prefix = re.sub(r"[^a-zA-Z0-9_-]+", "-", str(prefix or "env-backup")).strip("-")
    if not safe_prefix:
        safe_prefix = "env-backup"
    backup_path = "/tmp/sb-bot-{0}-{1}.env.bak".format(safe_prefix, timestamp)
    try:
        with open(env_path, "rb") as src:
            data = src.read()
        with open(backup_path, "wb") as dst:
            dst.write(data)
    except OSError as exc:
        return "", "备份 .env 失败: {0}".format(exc)
    return backup_path, ""


def restore_env_file(backup_path: str, env_path: str = ADMIN_ENV_FILE) -> tuple:
    if not backup_path:
        return False, "未提供备份路径"
    if not os.path.exists(backup_path):
        return False, "备份文件不存在：{0}".format(backup_path)
    try:
        with open(backup_path, "rb") as src:
            data = src.read()
        with open(env_path, "wb") as dst:
            dst.write(data)
    except OSError as exc:
        return False, "回滚 .env 失败: {0}".format(exc)
    return True, ""


def normalize_simple_url(raw_value: str, default_scheme: str = "http") -> str:
    value = str(raw_value or "").strip()
    if not value:
        return ""
    if value.startswith("http://") or value.startswith("https://"):
        return value.rstrip("/")
    scheme = "https" if default_scheme == "https" else "http"
    return "{0}://{1}".format(scheme, value.rstrip("/"))


def build_maintain_logs_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("controller日志", callback_data="maintain:logs:controller"),
                InlineKeyboardButton("bot日志", callback_data="maintain:logs:bot"),
            ],
            [
                InlineKeyboardButton("caddy日志", callback_data="maintain:logs:caddy"),
            ],
            [InlineKeyboardButton("返回", callback_data="menu:maintain")],
        ]
    )


def build_maintain_log_date_keyboard(target: str, date_keys: list) -> InlineKeyboardMarkup:
    rows = []
    for date_key in date_keys[:7]:
        day_text = "{0}-{1}-{2}".format(date_key[0:4], date_key[4:6], date_key[6:8])
        rows.append(
            [
                InlineKeyboardButton(
                    day_text, callback_data=f"maintain:logsdate:{target}:{date_key}"
                )
            ]
        )
    rows.append([InlineKeyboardButton("刷新日期列表", callback_data=f"maintain:logs:{target}")])
    rows.append([InlineKeyboardButton("返回服务选择", callback_data="action:maintain_logs")])
    rows.append([InlineKeyboardButton("返回维护菜单", callback_data="menu:maintain")])
    return InlineKeyboardMarkup(rows)


def build_maintain_log_result_keyboard(
    target: str, date_key: str, current_page: int, total_pages: int
) -> InlineKeyboardMarkup:
    safe_page = 1 if current_page < 1 else current_page
    safe_total = 1 if total_pages < 1 else total_pages
    if safe_page > safe_total:
        safe_page = safe_total
    prev_page = safe_page - 1 if safe_page > 1 else 1
    next_page = safe_page + 1 if safe_page < safe_total else safe_total

    rows = []
    if safe_total > 1:
        rows.append(
            [
                InlineKeyboardButton(
                    "上一页",
                    callback_data=f"maintain:logsdate:{target}:{date_key}:{prev_page}",
                ),
                InlineKeyboardButton(
                    "下一页",
                    callback_data=f"maintain:logsdate:{target}:{date_key}:{next_page}",
                ),
            ]
        )
    rows.append(
        [
            InlineKeyboardButton(
                f"第 {safe_page}/{safe_total} 页（50条/页）",
                callback_data=f"maintain:logsdate:{target}:{date_key}:{safe_page}",
            )
        ]
    )
    rows.append(
        [
            InlineKeyboardButton(
                "同日刷新", callback_data=f"maintain:logsdate:{target}:{date_key}:{safe_page}"
            ),
            InlineKeyboardButton("切换日期", callback_data=f"maintain:logs:{target}"),
        ]
    )
    rows.append([InlineKeyboardButton("返回服务选择", callback_data="action:maintain_logs")])
    rows.append([InlineKeyboardButton("返回维护菜单", callback_data="menu:maintain")])
    return InlineKeyboardMarkup(rows)


def build_maintain_token_collapse_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "确认收敛为单token",
                    callback_data="maintain:token_collapse:confirm",
                ),
                InlineKeyboardButton("取消", callback_data="menu:maintain"),
            ]
        ]
    )


def build_node_ops_picker_keyboard(nodes: list) -> InlineKeyboardMarkup:
    rows = []
    for node in nodes or []:
        node_code = str(node.get("node_code", "")).strip()
        if not node_code:
            continue
        region = str(node.get("region", "")).strip() or "-"
        host = str(node.get("host", "")).strip() or "-"
        enabled_text = "启用" if int(node.get("enabled", 0) or 0) == 1 else "禁用"
        rows.append(
            [
                InlineKeyboardButton(
                    f"{node_code} | {region} | {enabled_text} | {host}",
                    callback_data=f"nodeops:panel:{node_code}",
                )
            ]
        )
    rows.append([InlineKeyboardButton("返回", callback_data="menu:nodes")])
    return InlineKeyboardMarkup(rows)


def build_node_ops_panel_keyboard(node_code: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("同步更新（仅当前节点）", callback_data=f"nodeops:run:{node_code}:update")],
            [InlineKeyboardButton("重启 sing-box", callback_data=f"nodeops:run:{node_code}:restart")],
            [
                InlineKeyboardButton("查看 sing-box 状态", callback_data=f"nodeops:run:{node_code}:status_sb"),
                InlineKeyboardButton("查看 sb-agent 状态", callback_data=f"nodeops:run:{node_code}:status_ag"),
            ],
            [
                InlineKeyboardButton("查看 sing-box 日志", callback_data=f"nodeops:run:{node_code}:logs_sb"),
                InlineKeyboardButton("查看 sb-agent 日志", callback_data=f"nodeops:run:{node_code}:logs_ag"),
            ],
            [InlineKeyboardButton("修改节点参数", callback_data=f"nodeops:config:{node_code}")],
            [InlineKeyboardButton("查看任务记录", callback_data=f"nodeops:tasks:{node_code}")],
            [InlineKeyboardButton("返回节点远程运维列表", callback_data="action:node_ops")],
        ]
    )


def build_node_ops_task_done_keyboard(node_code: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("刷新任务记录", callback_data=f"nodeops:tasks:{node_code}"),
                InlineKeyboardButton("返回运维面板", callback_data=f"nodeops:panel:{node_code}"),
            ]
        ]
    )


def build_node_ops_task_list_keyboard(node_code: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("刷新", callback_data=f"nodeops:tasks:{node_code}"),
                InlineKeyboardButton("返回运维面板", callback_data=f"nodeops:panel:{node_code}"),
            ]
        ]
    )


def is_admin_chat(update: Update) -> bool:
    return get_chat_role_level(update) >= ROLE_VIEWER


def get_chat_role_level(update: Update) -> int:
    chat = update.effective_chat
    if not chat:
        return 0
    chat_id = int(chat.id)

    # 兼容旧行为：未配置任何管理员列表时默认不限制。
    if (
        not ROLE_SPLIT_ENABLED
        and not ADMIN_CHAT_ID_LIST
        and not VIEW_ADMIN_CHAT_ID_LIST
        and not OPS_ADMIN_CHAT_ID_LIST
        and not SUPER_ADMIN_CHAT_ID_LIST
    ):
        return ROLE_SUPER

    if chat_id in SUPER_ADMIN_CHAT_ID_SET:
        return ROLE_SUPER
    if chat_id in OPS_ADMIN_CHAT_ID_SET:
        return ROLE_OPERATOR
    if chat_id in VIEW_ADMIN_CHAT_ID_SET:
        return ROLE_VIEWER
    return 0


def role_name(role_level: int) -> str:
    if role_level >= ROLE_SUPER:
        return "超级管理员"
    if role_level >= ROLE_OPERATOR:
        return "运维管理员"
    if role_level >= ROLE_VIEWER:
        return "只读管理员"
    return "未授权"


def get_required_role_for_callback(callback_data: str) -> int:
    if callback_data in PRIVILEGED_CALLBACK_EXACT:
        return int(PRIVILEGED_CALLBACK_EXACT[callback_data])
    for prefix, level in PRIVILEGED_CALLBACK_PREFIXES.items():
        if callback_data.startswith(prefix):
            return int(level)

    if callback_data == "wizard:cancel":
        return ROLE_OPERATOR
    return ROLE_VIEWER


def get_no_permission_text() -> str:
    return (
        "当前账号无权限使用管理功能。\n"
        "请联系管理员将你的 chat_id 加入 "
        "VIEW_ADMIN_CHAT_IDS / OPS_ADMIN_CHAT_IDS / SUPER_ADMIN_CHAT_IDS。\n"
        "可先发送 /whoami 获取自己的 chat_id。"
    )


async def deny_non_admin(update: Update) -> None:
    query = update.callback_query
    if query:
        try:
            await query.answer("无权限", show_alert=True)
        except BadRequest:
            pass
        return
    if update.message:
        await update.message.reply_text(get_no_permission_text())


async def deny_insufficient_role(update: Update, required_level: int) -> None:
    required = role_name(required_level)
    query = update.callback_query
    if query:
        try:
            await query.answer("权限不足，需要：{0}".format(required), show_alert=True)
        except BadRequest:
            pass
        return
    if update.message:
        current = role_name(get_chat_role_level(update))
        await update.message.reply_text(
            "当前权限不足。\n"
            "当前角色：{0}\n"
            "所需角色：{1}\n"
            "请联系超级管理员调整权限。".format(current, required)
        )


async def ensure_admin_callback(update: Update, required_level: int = ROLE_VIEWER) -> bool:
    query = update.callback_query
    if not query:
        return False
    current_level = get_chat_role_level(update)
    if current_level >= required_level:
        return True
    if current_level <= 0:
        await deny_non_admin(update)
        return False
    await deny_insufficient_role(update, required_level)
    return False


async def _menu_auto_clear_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    data = context.job.data if context.job else {}
    chat_id = data.get("chat_id")
    message_id = data.get("message_id")
    if chat_id is None or message_id is None:
        return

    key = f"{chat_id}:{message_id}"
    try:
        await context.bot.edit_message_reply_markup(
            chat_id=chat_id,
            message_id=message_id,
            reply_markup=None,
        )
    except (BadRequest, Forbidden):
        pass
    finally:
        jobs_map = context.application.bot_data.get(MENU_AUTO_CLEAR_JOBS_KEY, {})
        if isinstance(jobs_map, dict):
            jobs_map.pop(key, None)


def schedule_menu_auto_clear(
    context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int
) -> None:
    if not context.job_queue:
        return

    jobs_map = context.application.bot_data.setdefault(MENU_AUTO_CLEAR_JOBS_KEY, {})
    if not isinstance(jobs_map, dict):
        jobs_map = {}
        context.application.bot_data[MENU_AUTO_CLEAR_JOBS_KEY] = jobs_map

    key = f"{chat_id}:{message_id}"
    old_job = jobs_map.get(key)
    if old_job:
        old_job.schedule_removal()

    jobs_map[key] = context.job_queue.run_once(
        _menu_auto_clear_job,
        MENU_AUTO_CLEAR_SECONDS,
        data={"chat_id": chat_id, "message_id": message_id},
        name=f"menu_auto_clear:{key}",
    )


async def reply_text_with_auto_clear(
    message, context: ContextTypes.DEFAULT_TYPE, text: str, reply_markup=None
):
    sent = await message.reply_text(text, reply_markup=reply_markup)
    if isinstance(reply_markup, InlineKeyboardMarkup):
        schedule_menu_auto_clear(context, sent.chat_id, sent.message_id)
    return sent


async def edit_or_reply_with_auto_clear(
    query,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
    reply_markup=None,
) -> None:
    try:
        await query.edit_message_text(text, reply_markup=reply_markup)
        if query.message and isinstance(reply_markup, InlineKeyboardMarkup):
            schedule_menu_auto_clear(context, query.message.chat_id, query.message.message_id)
        return
    except (BadRequest, Forbidden):
        pass

    if query.message:
        try:
            await reply_text_with_auto_clear(
                query.message, context, text, reply_markup=reply_markup
            )
            return
        except (BadRequest, Forbidden):
            pass

    await query.answer("操作完成，请重新打开菜单查看结果。", show_alert=True)


async def refresh_callback_menu_ttl(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    query = update.callback_query
    if not query or not query.message:
        return
    schedule_menu_auto_clear(context, query.message.chat_id, query.message.message_id)


def get_main_menu_message_map(context: ContextTypes.DEFAULT_TYPE) -> dict:
    message_map = context.application.bot_data.setdefault(MAIN_MENU_MESSAGE_IDS_KEY, {})
    if not isinstance(message_map, dict):
        message_map = {}
        context.application.bot_data[MAIN_MENU_MESSAGE_IDS_KEY] = message_map
    return message_map


def register_main_menu_message(
    context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int
) -> None:
    message_map = get_main_menu_message_map(context)
    existing = message_map.get(chat_id, [])
    if not isinstance(existing, list):
        existing = []
    if message_id not in existing:
        existing.append(message_id)
    # 保留最近 20 条记录，避免异常情况下无限增长
    message_map[chat_id] = existing[-20:]


def set_main_menu_message(
    context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int
) -> None:
    message_map = get_main_menu_message_map(context)
    message_map[chat_id] = [message_id]


async def purge_main_menu_messages(
    context: ContextTypes.DEFAULT_TYPE, chat_id: int, keep_message_id: Optional[int] = None
) -> None:
    message_map = get_main_menu_message_map(context)
    existing = message_map.get(chat_id, [])
    if not isinstance(existing, list):
        existing = []
    kept = []
    for old_message_id in existing:
        if keep_message_id is not None and old_message_id == keep_message_id:
            kept.append(old_message_id)
            continue
        try:
            await context.bot.edit_message_reply_markup(
                chat_id=chat_id,
                message_id=old_message_id,
                reply_markup=None,
            )
            continue
        except (BadRequest, Forbidden):
            pass
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=old_message_id)
        except (BadRequest, Forbidden):
            pass
    message_map[chat_id] = kept


def remember_known_chat(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    if not chat:
        return
    known_ids = context.application.bot_data.setdefault(KNOWN_CHAT_IDS_KEY, set())
    if not isinstance(known_ids, set):
        try:
            known_ids = set(known_ids)
        except TypeError:
            known_ids = set()
    known_ids.add(chat.id)
    context.application.bot_data[KNOWN_CHAT_IDS_KEY] = known_ids


def get_monitor_target_chat_ids(context: ContextTypes.DEFAULT_TYPE) -> list:
    if VIEW_ADMIN_CHAT_ID_SET:
        return sorted(list(VIEW_ADMIN_CHAT_ID_SET))
    if ADMIN_CHAT_ID_LIST:
        return list(dict.fromkeys(ADMIN_CHAT_ID_LIST))
    known_ids = context.application.bot_data.get(KNOWN_CHAT_IDS_KEY, set())
    if isinstance(known_ids, set):
        return sorted(list(known_ids))
    if isinstance(known_ids, list):
        return [chat_id for chat_id in known_ids if isinstance(chat_id, int)]
    return []


def format_last_seen_text(last_seen_at: int) -> str:
    if last_seen_at <= 0:
        return "暂无"
    return datetime.fromtimestamp(last_seen_at).strftime("%Y-%m-%d %H:%M:%S")


async def send_node_monitor_alert(
    context: ContextTypes.DEFAULT_TYPE, node: dict, is_online: bool, last_seen_at: int
) -> None:
    chat_ids = get_monitor_target_chat_ids(context)
    if not chat_ids:
        return
    title = "【节点恢复】" if is_online else "【节点掉线】"
    online_text = "在线" if is_online else "离线"
    text = (
        f"{title}\n"
        f"节点：{node.get('node_code', '')}\n"
        f"地区：{node.get('region', '')}\n"
        f"入口：{node.get('host', '')}\n"
        f"状态：{online_text}\n"
        f"最后心跳：{format_last_seen_text(last_seen_at)}"
    )
    if not is_online:
        text += f"\n说明：超过 {NODE_OFFLINE_THRESHOLD_SECONDS} 秒未收到心跳。"

    for chat_id in chat_ids:
        try:
            await context.bot.send_message(chat_id=chat_id, text=text)
        except (BadRequest, Forbidden):
            continue


async def run_node_monitor_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    nodes, error_message, _ = await controller_request("GET", "/nodes")
    if error_message or not isinstance(nodes, list):
        if error_message:
            logger.warning("node monitor fetch failed: %s", error_message)
        return

    state_map = context.application.bot_data.setdefault(NODE_MONITOR_STATE_KEY, {})
    if not isinstance(state_map, dict):
        state_map = {}
    now = int(time.time())
    monitored_codes = set()

    for node in nodes:
        node_code = str(node.get("node_code", "")).strip()
        if not node_code:
            continue
        if int(node.get("monitor_enabled", 0) or 0) != 1:
            continue
        monitored_codes.add(node_code)
        try:
            last_seen_at = int(node.get("last_seen_at", 0) or 0)
        except (TypeError, ValueError):
            last_seen_at = 0

        is_online = (
            last_seen_at > 0
            and (now - last_seen_at) <= NODE_OFFLINE_THRESHOLD_SECONDS
        )
        previous = state_map.get(node_code)
        state_map[node_code] = is_online

        if previous is None:
            if not is_online:
                await send_node_monitor_alert(context, node, False, last_seen_at)
            continue
        if previous != is_online:
            await send_node_monitor_alert(context, node, is_online, last_seen_at)

    for node_code in list(state_map.keys()):
        if node_code not in monitored_codes:
            state_map.pop(node_code, None)

    context.application.bot_data[NODE_MONITOR_STATE_KEY] = state_map


async def run_node_time_sync_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    if NODE_TIME_SYNC_INTERVAL_SECONDS <= 0:
        return
    result, error_message, status_code = await controller_request(
        "POST",
        "/admin/nodes/sync_time?include_disabled=0&force_new=0",
    )
    if error_message:
        if status_code in (401, 403):
            logger.warning("node time sync unauthorized: %s", error_message)
        else:
            logger.warning("node time sync failed: %s", error_message)
        return
    if not isinstance(result, dict):
        logger.warning("node time sync invalid response")
        return
    logger.info(
        "node time sync dispatched: selected=%s created=%s deduplicated=%s failed=%s server_unix=%s",
        int(result.get("selected", 0) or 0),
        int(result.get("created", 0) or 0),
        int(result.get("deduplicated", 0) or 0),
        int(result.get("failed", 0) or 0),
        int(result.get("server_unix", 0) or 0),
    )


def build_main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("👤 用户与订阅管理", callback_data="menu:user")],
            [InlineKeyboardButton("🛰 节点与线路管理", callback_data="menu:nodes")],
            [InlineKeyboardButton("🔎 查询与告警中心", callback_data="menu:query")],
            [InlineKeyboardButton("🚦 限速策略管理", callback_data="menu:speed")],
            [InlineKeyboardButton("🛠 管理服务器运维", callback_data="menu:maintain")],
        ]
    )


def build_submenu(submenu_key: str) -> InlineKeyboardMarkup:
    buttons = SUBMENUS[submenu_key]["buttons"]
    # 按钮较多的子菜单改为双列，减少纵向滚动；返回按钮保持单独一行。
    if len(buttons) >= 6:
        rows = []
        action_row = []
        back_rows = []
        for text, data in buttons:
            button = InlineKeyboardButton(text, callback_data=data)
            if str(data).startswith("menu:"):
                back_rows.append([button])
                continue
            action_row.append(button)
            if len(action_row) == 2:
                rows.append(action_row)
                action_row = []
        if action_row:
            rows.append(action_row)
        rows.extend(back_rows)
    else:
        rows = [[InlineKeyboardButton(text, callback_data=data)] for text, data in buttons]
    return InlineKeyboardMarkup(rows)


def build_create_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("确认创建", callback_data="wizard:create_confirm"),
                InlineKeyboardButton("取消", callback_data="wizard:cancel"),
            ]
        ]
    )


def build_nodes_create_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("确认创建", callback_data="wizard:nodes_create_confirm"),
                InlineKeyboardButton("取消", callback_data="wizard:cancel"),
            ]
        ]
    )


def build_nodes_list_keyboard(nodes: list) -> InlineKeyboardMarkup:
    rows = []
    for node in nodes:
        node_code = node.get("node_code", "")
        region = node.get("region", "")
        host = node.get("host", "")
        tags = format_node_tags(node)
        enabled_text = "启用" if int(node.get("enabled", 0)) == 1 else "禁用"
        rows.append(
            [
                InlineKeyboardButton(
                    f"{node_code} | {region} | {tags} | {host} | {enabled_text}",
                    callback_data=f"node:detail:{node_code}",
                )
            ]
        )
    rows.append(
        [
            InlineKeyboardButton("新增节点", callback_data="action:nodes_create"),
            InlineKeyboardButton("返回", callback_data="menu:nodes"),
        ]
    )
    return InlineKeyboardMarkup(rows)


def build_node_detail_keyboard(
    node_code: str, monitor_enabled: int = 0
) -> InlineKeyboardMarkup:
    monitor_text = "关闭节点监控" if int(monitor_enabled or 0) == 1 else "开启节点监控"
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("启用/禁用", callback_data=f"node:toggle:{node_code}")],
            [InlineKeyboardButton(monitor_text, callback_data=f"node:monitor_toggle:{node_code}")],
            [InlineKeyboardButton("同步预览（排障）", callback_data=f"node:sync_preview:{node_code}")],
            [InlineKeyboardButton("设置节点来源IP白名单", callback_data=f"node:edit_agent_ip:{node_code}")],
            [InlineKeyboardButton("修改入口（影响两种协议）", callback_data=f"node:edit_host:{node_code}")],
            [InlineKeyboardButton("修改REALITY伪装域名（R）", callback_data=f"node:edit_sni:{node_code}")],
            [InlineKeyboardButton("修改TUIC证书域名（T）", callback_data=f"node:edit_tuic_sni:{node_code}")],
            [InlineKeyboardButton("配置REALITY参数（生成/录入）", callback_data=f"node:reality_setup:{node_code}")],
            [InlineKeyboardButton("修改TUIC端口池（仅TUIC）", callback_data=f"node:edit_pool:{node_code}")],
            [InlineKeyboardButton("删除节点", callback_data=f"node:delete_confirm:{node_code}")],
            [InlineKeyboardButton("返回列表", callback_data="action:nodes_list")],
        ]
    )


def build_node_delete_confirm_keyboard(node_code: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("确认删除", callback_data=f"node:delete:{node_code}"),
                InlineKeyboardButton("取消", callback_data=f"node:detail:{node_code}"),
            ]
        ]
    )


def build_user_nodes_manage_keyboard(user_code: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("➕ 分配节点", callback_data=f"usernodes:assign:{user_code}"),
                InlineKeyboardButton("➖ 解绑节点", callback_data=f"usernodes:unassign:{user_code}"),
            ],
            [
                InlineKeyboardButton("📎 订阅链接（明文）", callback_data=f"sub:links:{user_code}"),
                InlineKeyboardButton("📎 订阅链接（Base64）", callback_data=f"sub:base64:{user_code}"),
            ],
            [InlineKeyboardButton("返回", callback_data="menu:user")],
        ]
    )


def build_user_nodes_picker_keyboard(users: list) -> InlineKeyboardMarkup:
    rows = []
    for user in users:
        user_code = str(user.get("user_code", ""))
        display_name = str(user.get("display_name") or "").strip()
        button_text = f"{display_name}（{user_code}）" if display_name else user_code
        rows.append(
            [
                InlineKeyboardButton(
                    button_text,
                    callback_data=f"usernodes:manage:{user_code}",
                )
            ]
        )
    rows.append(
        [
            InlineKeyboardButton("手动输入用户编号", callback_data="usernodes:manual_input"),
            InlineKeyboardButton("返回", callback_data="menu:user"),
        ]
    )
    return InlineKeyboardMarkup(rows)


def build_user_nodes_empty_users_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("返回", callback_data="menu:user")]]
    )


def build_user_speed_picker_keyboard(users: list) -> InlineKeyboardMarkup:
    rows = []
    for user in users:
        user_code = str(user.get("user_code", ""))
        display_name = str(user.get("display_name") or "").strip()
        button_text = f"{display_name}（{user_code}）" if display_name else user_code
        rows.append(
            [
                InlineKeyboardButton(
                    button_text,
                    callback_data=f"userspeed:pick:{user_code}",
                )
            ]
        )
    rows.append([InlineKeyboardButton("返回", callback_data="menu:user")])
    return InlineKeyboardMarkup(rows)


def build_user_speed_confirm_keyboard(user_code: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("确认修改", callback_data=f"userspeed:apply:{user_code}"),
                InlineKeyboardButton("取消", callback_data="menu:user"),
            ]
        ]
    )


def build_user_limit_mode_picker_keyboard(users: list) -> InlineKeyboardMarkup:
    rows = []
    for user in users:
        user_code = str(user.get("user_code", ""))
        display_name = str(user.get("display_name") or "").strip()
        raw_mode = str(user.get("limit_mode") or "tc").strip().lower() or "tc"
        mode_text = "tc" if raw_mode == "tc" else "off"
        button_text = (
            f"{display_name}（{user_code}）| {mode_text}"
            if display_name
            else f"{user_code} | {mode_text}"
        )
        rows.append(
            [
                InlineKeyboardButton(
                    button_text,
                    callback_data=f"usermode:pick:{user_code}",
                )
            ]
        )
    rows.append([InlineKeyboardButton("返回", callback_data="menu:speed")])
    return InlineKeyboardMarkup(rows)


def build_user_limit_mode_confirm_keyboard(user_code: str, target_mode: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "确认切换",
                    callback_data=f"usermode:apply:{user_code}:{target_mode}",
                ),
                InlineKeyboardButton("取消", callback_data="menu:speed"),
            ]
        ]
    )


def build_user_delete_picker_keyboard(users: list) -> InlineKeyboardMarkup:
    rows = []
    for user in users:
        user_code = str(user.get("user_code", ""))
        display_name = str(user.get("display_name") or "").strip()
        button_text = f"{display_name}（{user_code}）" if display_name else user_code
        rows.append([InlineKeyboardButton(button_text, callback_data=f"userdelete:pick:{user_code}")])
    rows.append([InlineKeyboardButton("返回", callback_data="menu:user")])
    return InlineKeyboardMarkup(rows)


def build_user_delete_confirm_keyboard(user_code: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("确认删除", callback_data=f"userdelete:apply:{user_code}"),
                InlineKeyboardButton("取消", callback_data="menu:user"),
            ]
        ]
    )


def build_user_toggle_picker_keyboard(users: list) -> InlineKeyboardMarkup:
    rows = []
    for user in users:
        user_code = str(user.get("user_code", ""))
        display_name = str(user.get("display_name") or "").strip()
        status_text = str(user.get("status", "") or "-")
        button_text = (
            f"{display_name}（{user_code}）| {status_text}" if display_name else f"{user_code} | {status_text}"
        )
        rows.append([InlineKeyboardButton(button_text, callback_data=f"usertoggle:pick:{user_code}")])
    rows.append([InlineKeyboardButton("返回", callback_data="menu:user")])
    return InlineKeyboardMarkup(rows)


def build_user_toggle_confirm_keyboard(user_code: str, target_status: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "确认修改",
                    callback_data=f"usertoggle:apply:{user_code}:{target_status}",
                ),
                InlineKeyboardButton("取消", callback_data="menu:user"),
            ]
        ]
    )


def build_user_nodes_assign_list_keyboard(
    user_code: str, nodes: list
) -> InlineKeyboardMarkup:
    rows = []
    for node in nodes:
        node_code = node.get("node_code", "")
        region = node.get("region", "")
        host = node.get("host", "")
        tags = format_node_tags(node)
        rows.append(
            [
                InlineKeyboardButton(
                    f"{node_code} | {region} | {tags} | {host}",
                    callback_data=f"usernodes:assign_pick:{user_code}:{node_code}",
                )
            ]
        )
    rows.append([InlineKeyboardButton("返回", callback_data=f"usernodes:manage:{user_code}")])
    return InlineKeyboardMarkup(rows)


def build_user_nodes_unassign_list_keyboard(
    user_code: str, user_nodes: list
) -> InlineKeyboardMarkup:
    rows = []
    for item in user_nodes:
        node_code = item.get("node_code", "")
        region = item.get("region", "")
        host = item.get("host", "")
        tuic_port = item.get("tuic_port", "")
        rows.append(
            [
                InlineKeyboardButton(
                    f"{node_code} | {region} | {host} | TUIC:{tuic_port}",
                    callback_data=f"usernodes:unassign_pick:{user_code}:{node_code}",
                )
            ]
        )
    rows.append([InlineKeyboardButton("返回", callback_data=f"usernodes:manage:{user_code}")])
    return InlineKeyboardMarkup(rows)


def build_user_nodes_assign_confirm_keyboard(
    user_code: str, node_code: str
) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "确认分配",
                    callback_data=f"usernodes:assign_apply:{user_code}:{node_code}",
                ),
                InlineKeyboardButton("取消", callback_data=f"usernodes:manage:{user_code}"),
            ]
        ]
    )


def build_user_nodes_unassign_confirm_keyboard(
    user_code: str, node_code: str
) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "确认解绑",
                    callback_data=f"usernodes:unassign_apply:{user_code}:{node_code}",
                ),
                InlineKeyboardButton("取消", callback_data=f"usernodes:manage:{user_code}"),
            ]
        ]
    )


def build_node_edit_confirm_keyboard(field: str, node_code: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("确认修改", callback_data=f"node:apply_edit:{field}:{node_code}"),
                InlineKeyboardButton("取消", callback_data=f"node:detail:{node_code}"),
            ]
        ]
    )


def build_sub_links_info_keyboard(user_code: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("返回用户节点管理", callback_data=f"usernodes:manage:{user_code}")],
            [InlineKeyboardButton("返回用户管理", callback_data="menu:user")],
        ]
    )


def build_node_reality_setup_keyboard(node_code: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("我已生成，开始粘贴", callback_data=f"node:reality_paste:{node_code}")],
            [InlineKeyboardButton("返回节点详情", callback_data=f"node:detail:{node_code}")],
        ]
    )


def build_node_reality_confirm_keyboard(node_code: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("确认保存", callback_data=f"node:reality_apply:{node_code}"),
                InlineKeyboardButton("取消", callback_data=f"node:detail:{node_code}"),
            ]
        ]
    )


def build_node_back_to_detail_keyboard(node_code: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("返回节点详情", callback_data=f"node:detail:{node_code}")]]
    )


def build_node_sync_preview_keyboard(node_code: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("刷新预览", callback_data=f"node:sync_preview:{node_code}")],
            [InlineKeyboardButton("返回节点详情", callback_data=f"node:detail:{node_code}")],
        ]
    )


def build_back_keyboard(callback_data: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("返回", callback_data=callback_data)]]
    )


def security_events_mode_callback(include_local: bool) -> str:
    if include_local:
        return "action:maintain_security_events_local"
    return "action:maintain_security_events"


def encode_ip_token(source_ip: str) -> str:
    return str(source_ip or "").strip().replace(":", "_")


def decode_ip_token(token: str) -> str:
    raw_value = str(token or "").strip().replace("_", ":")
    if not raw_value:
        return ""
    try:
        return str(ipaddress.ip_address(raw_value))
    except ValueError:
        return ""


def shorten_ip_for_button(source_ip: str, limit: int = 18) -> str:
    text = str(source_ip or "").strip()
    if len(text) <= limit:
        return text
    if limit <= 6:
        return text[:limit]
    return text[: limit - 3] + "..."


def build_security_events_keyboard(include_local: bool, top_ips: list) -> InlineKeyboardMarkup:
    mode_flag = "1" if include_local else "0"
    rows = [
        [
            InlineKeyboardButton(
                "🧪 过滤本机视角",
                callback_data="action:maintain_security_events",
            ),
            InlineKeyboardButton(
                "🌐 包含本机视角",
                callback_data="action:maintain_security_events_local",
            ),
        ],
    ]
    for source_ip in top_ips[:3]:
        token = encode_ip_token(str(source_ip))
        button_ip = shorten_ip_for_button(str(source_ip))
        rows.append(
            [
                InlineKeyboardButton(
                    f"⛔ 封禁 {button_ip}",
                    callback_data=f"sb:bi:{mode_flag}:{token}",
                ),
            ]
        )
    rows.append([InlineKeyboardButton("🧹 手动安全清理", callback_data=f"sb:mc:{mode_flag}")])
    rows.append([InlineKeyboardButton("🛡 执行自动封禁检查", callback_data=f"sb:ab:{mode_flag}")])
    rows.append([InlineKeyboardButton("📄 查看封禁列表", callback_data=f"sb:bl:{mode_flag}:1")])
    rows.append([InlineKeyboardButton("⬅️ 返回维护菜单", callback_data="menu:maintain")])
    return InlineKeyboardMarkup(rows)


def build_security_duration_keyboard(include_local: bool, source_ip: str) -> InlineKeyboardMarkup:
    mode_flag = "1" if include_local else "0"
    token = encode_ip_token(source_ip)
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("1小时", callback_data=f"sb:bd:3600:{mode_flag}:{token}"),
                InlineKeyboardButton("24小时", callback_data=f"sb:bd:86400:{mode_flag}:{token}"),
            ],
            [
                InlineKeyboardButton("7天", callback_data=f"sb:bd:604800:{mode_flag}:{token}"),
                InlineKeyboardButton("永久", callback_data=f"sb:bd:0:{mode_flag}:{token}"),
            ],
            [
                InlineKeyboardButton(
                    "❌ 取消", callback_data=security_events_mode_callback(include_local)
                ),
            ],
        ]
    )


def build_security_blocklist_keyboard(
    include_local: bool, blocked_ips: list, page: int, total_pages: int
) -> InlineKeyboardMarkup:
    mode_flag = "1" if include_local else "0"
    rows = []
    for source_ip in blocked_ips[:8]:
        token = encode_ip_token(str(source_ip))
        rows.append(
            [
                InlineKeyboardButton(
                    "解封 {0}".format(shorten_ip_for_button(str(source_ip), limit=28)),
                    callback_data=f"sb:bu:{mode_flag}:{token}",
                )
            ]
        )
    nav_row = []
    if page > 1:
        nav_row.append(
            InlineKeyboardButton("上一页", callback_data=f"sb:bl:{mode_flag}:{page - 1}")
        )
    if page < total_pages:
        nav_row.append(
            InlineKeyboardButton("下一页", callback_data=f"sb:bl:{mode_flag}:{page + 1}")
        )
    if nav_row:
        rows.append(nav_row)
    rows.append(
        [
            InlineKeyboardButton("刷新列表", callback_data=f"sb:bl:{mode_flag}:{page}"),
            InlineKeyboardButton("返回安全事件", callback_data=security_events_mode_callback(include_local)),
        ]
    )
    rows.append(
        [
            InlineKeyboardButton(
                f"第 {page}/{total_pages} 页",
                callback_data=f"sb:bl:{mode_flag}:{page}",
            )
        ]
    )
    rows.append([InlineKeyboardButton("返回维护菜单", callback_data="menu:maintain")])
    return InlineKeyboardMarkup(rows)


def build_security_block_confirm_keyboard(include_local: bool, duration_seconds: int, source_ip: str) -> InlineKeyboardMarkup:
    mode_flag = "1" if include_local else "0"
    token = encode_ip_token(source_ip)
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "确认封禁",
                    callback_data=f"sb:ba:{int(duration_seconds)}:{mode_flag}:{token}",
                ),
                InlineKeyboardButton(
                    "取消",
                    callback_data=f"sb:bi:{mode_flag}:{token}",
                ),
            ]
        ]
    )


def build_sub_policy_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🔒 推荐：签名+限流", callback_data="maintain:subpolicy:strict"),
            ],
            [
                InlineKeyboardButton("🧾 仅签名", callback_data="maintain:subpolicy:signed"),
                InlineKeyboardButton("🧪 开放测试", callback_data="maintain:subpolicy:open"),
            ],
            [
                InlineKeyboardButton("返回维护菜单", callback_data="menu:maintain"),
            ],
        ]
    )


def build_backup_audit_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("刷新", callback_data="action:backup_audit"),
                InlineKeyboardButton("返回", callback_data="menu:backup"),
            ]
        ]
    )


def build_backup_stop_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("确认禁用全部活跃用户", callback_data="backup:stop:confirm"),
                InlineKeyboardButton("取消", callback_data="backup:stop:cancel"),
            ],
            [InlineKeyboardButton("返回备份菜单", callback_data="menu:backup")],
        ]
    )


def build_maintain_ops_audit_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("刷新", callback_data="action:maintain_ops_audit"),
                InlineKeyboardButton("返回服务运维", callback_data="menu:maintain_ops"),
            ],
            [InlineKeyboardButton("返回维护菜单", callback_data="menu:maintain")],
        ]
    )


def build_query_user_picker_keyboard(users: list) -> InlineKeyboardMarkup:
    rows = []
    for user in users:
        user_code = str(user.get("user_code", ""))
        if not user_code:
            continue
        display_name = str(user.get("display_name") or "").strip()
        button_text = f"{display_name}（{user_code}）" if display_name else user_code
        rows.append(
            [InlineKeyboardButton(button_text, callback_data=f"query:user:{user_code}")]
        )
    rows.append([InlineKeyboardButton("返回", callback_data="menu:query")])
    return InlineKeyboardMarkup(rows)


def build_query_user_detail_keyboard(user_code: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("订阅链接", callback_data=f"sub:links:{user_code}"),
                InlineKeyboardButton("Base64 订阅", callback_data=f"sub:base64:{user_code}"),
            ],
            [InlineKeyboardButton("返回", callback_data="menu:query")],
        ]
    )


def get_node_edit_scope_text(field: str) -> str:
    mapping = {
        "host": "VLESS+REALITY & TUIC（两种协议）",
        "sni": "仅 VLESS+REALITY",
        "pool": "仅 TUIC",
        "agent_ip": "仅控制面通信（agent -> controller）",
    }
    return mapping.get(field, "未知范围")


def localize_controller_error(error_message: str) -> str:
    if error_message.startswith("node source ip not allowed for"):
        return "节点来源IP不在白名单中"
    mapping = {
        "unauthorized": "未授权（请检查 AUTH_TOKEN）",
        "User not found": "用户不存在",
        "Node not found": "节点不存在",
        "Task not found": "任务不存在",
        "unsupported task_type": "不支持的任务类型",
        "payload not allowed for task_type": "该任务不接受自定义参数",
        "unsupported payload keys": "任务参数包含不支持的字段",
        "config_set payload required": "配置任务参数不能为空",
        "payload too large (max 2048 bytes)": "任务参数过大（超过限制）",
        "too many pending tasks for node": "该节点待执行任务过多，请稍后再试",
        "lines must be 20-300": "日志行数必须在 20-300 之间",
        "Node is disabled": "节点已禁用",
        "User already assigned to this node": "该用户已绑定该节点",
        "No available TUIC port in node pool": "该节点端口池已满，暂无可用TUIC端口",
        "User-node binding not found": "该用户未绑定该节点",
        "该用户仍有节点绑定，请先解绑后再删除": "该用户仍有节点绑定，请先解绑后再删除",
        "status must be active or disabled": "状态值无效，仅支持 active/disabled",
        "agent_ip must be a valid IPv4/IPv6 address": "节点来源IP格式无效，请填写正确的IP地址",
    }
    return mapping.get(error_message, error_message)


def format_admin_audit_text(items: list, line_limit: int = 50, char_limit: int = 3600) -> str:
    if not items:
        return "操作日志（最近记录）：\n（暂无记录）"
    lines = ["操作日志（最近记录）："]
    count = 0
    for item in items:
        if count >= line_limit:
            break
        try:
            created_at = int(item.get("created_at", 0) or 0)
        except (TypeError, ValueError):
            created_at = 0
        time_text = (
            datetime.fromtimestamp(created_at).strftime("%Y-%m-%d %H:%M:%S")
            if created_at > 0
            else "-"
        )
        action = str(item.get("action", "-"))
        resource_type = str(item.get("resource_type", "")).strip()
        resource_id = str(item.get("resource_id", "")).strip()
        target_text = resource_type if resource_type else "-"
        if resource_id:
            target_text = f"{target_text}:{resource_id}"
        actor = str(item.get("actor", "")).strip() or "unknown"
        source_ip = str(item.get("source_ip", "")).strip() or "-"
        detail = str(item.get("detail", "")).strip()
        if len(detail) > 180:
            detail = detail[:180] + "..."
        lines.append(
            f"{time_text} | {action}\n"
            f"对象：{target_text}\n"
            f"操作者：{actor} | 来源IP：{source_ip}\n"
            f"详情：{detail if detail else '-'}"
        )
        count += 1
    text = "\n\n".join(lines)
    if len(text) > char_limit:
        text = text[:char_limit] + "\n\n...（日志较多，已截断）"
    return text


def format_ops_audit_text(items: list, line_limit: int = 20, char_limit: int = 3600) -> str:
    if not items:
        return "运维审计（ops.*）：\n（暂无记录）"
    lines = ["运维审计（ops.* 最近记录）："]
    count = 0
    for item in items:
        if count >= line_limit:
            break
        action = str(item.get("action", "")).strip()
        if not action.startswith("ops."):
            continue
        try:
            created_at = int(item.get("created_at", 0) or 0)
        except (TypeError, ValueError):
            created_at = 0
        time_text = (
            datetime.fromtimestamp(created_at).strftime("%Y-%m-%d %H:%M:%S")
            if created_at > 0
            else "-"
        )
        actor = str(item.get("actor", "")).strip() or "unknown"
        source_ip = str(item.get("source_ip", "")).strip() or "-"
        detail = str(item.get("detail", "")).strip()
        if len(detail) > 180:
            detail = detail[:180] + "..."
        lines.append(
            f"{time_text} | {action}\n"
            f"操作者：{actor} | 来源IP：{source_ip}\n"
            f"详情：{detail if detail else '-'}"
        )
        count += 1
    if count == 0:
        lines.append("（暂无记录）")
    text = "\n\n".join(lines)
    if len(text) > char_limit:
        text = text[:char_limit] + "\n\n...（日志较多，已截断）"
    return text


def format_admin_overview_text(overview: dict) -> str:
    if not isinstance(overview, dict):
        return "控制面概览：数据格式异常"

    generated_at = 0
    try:
        generated_at = int(overview.get("generated_at", 0) or 0)
    except (TypeError, ValueError):
        generated_at = 0
    generated_text = (
        datetime.fromtimestamp(generated_at).strftime("%Y-%m-%d %H:%M:%S")
        if generated_at > 0
        else "-"
    )

    totals = overview.get("totals", {})
    if not isinstance(totals, dict):
        totals = {}
    monitor = overview.get("monitor", {})
    if not isinstance(monitor, dict):
        monitor = {}
    tasks = overview.get("tasks", {})
    if not isinstance(tasks, dict):
        tasks = {}
    security = overview.get("security", {})
    if not isinstance(security, dict):
        security = {}
    security_events = overview.get("security_events", {})
    if not isinstance(security_events, dict):
        security_events = {}

    offline_items = monitor.get("offline_items", [])
    if not isinstance(offline_items, list):
        offline_items = []
    offline_codes = []
    for item in offline_items[:5]:
        if not isinstance(item, dict):
            continue
        node_code = str(item.get("node_code", "")).strip()
        if node_code:
            offline_codes.append(node_code)

    pending_by_node = tasks.get("pending_by_node", [])
    if not isinstance(pending_by_node, list):
        pending_by_node = []
    pending_parts = []
    for item in pending_by_node[:5]:
        if not isinstance(item, dict):
            continue
        node_code = str(item.get("node_code", "")).strip()
        try:
            pending_count = int(item.get("pending", 0) or 0)
        except (TypeError, ValueError):
            pending_count = 0
        if node_code:
            pending_parts.append(f"{node_code}({pending_count})")

    near_cap_nodes = tasks.get("near_cap_nodes", [])
    if not isinstance(near_cap_nodes, list):
        near_cap_nodes = []
    near_cap_parts = []
    for item in near_cap_nodes[:5]:
        if not isinstance(item, dict):
            continue
        node_code = str(item.get("node_code", "")).strip()
        try:
            pending_count = int(item.get("pending", 0) or 0)
        except (TypeError, ValueError):
            pending_count = 0
        if node_code:
            near_cap_parts.append(f"{node_code}({pending_count})")

    idempotency = tasks.get("idempotency_24h", {})
    if not isinstance(idempotency, dict):
        idempotency = {}
    idempotency_created = int(idempotency.get("created", 0) or 0)
    idempotency_deduplicated = int(idempotency.get("deduplicated", 0) or 0)
    idempotency_ratio = float(idempotency.get("dedup_ratio", 0.0) or 0.0)
    idempotency_parts = []
    top_nodes = idempotency.get("top_nodes", [])
    if isinstance(top_nodes, list):
        for item in top_nodes[:3]:
            if not isinstance(item, dict):
                continue
            node_code = str(item.get("node_code", "")).strip()
            dedup_count = int(item.get("deduplicated", 0) or 0)
            incoming_total = int(item.get("incoming_total", 0) or 0)
            if node_code:
                idempotency_parts.append(f"{node_code}({dedup_count}/{incoming_total})")

    warnings = security.get("warnings", [])
    if not isinstance(warnings, list):
        warnings = []
    unauthorized_1h = int(security_events.get("unauthorized_1h", 0) or 0)
    unauthorized_24h = int(security_events.get("unauthorized_24h", 0) or 0)
    top_unauthorized_ips = security_events.get("top_unauthorized_ips", [])
    if not isinstance(top_unauthorized_ips, list):
        top_unauthorized_ips = []
    top_unauthorized_parts = []
    for item in top_unauthorized_ips[:3]:
        if not isinstance(item, dict):
            continue
        source_ip = str(item.get("source_ip", "")).strip()
        try:
            count = int(item.get("count", 0) or 0)
        except (TypeError, ValueError):
            count = 0
        if source_ip:
            top_unauthorized_parts.append("{0}({1})".format(source_ip, count))
    queue_cap_per_node = int(tasks.get("queue_cap_per_node", 0) or 0)
    near_cap_threshold = int(tasks.get("near_cap_threshold", 0) or 0)
    events_exclude_local = bool(security.get("security_events_exclude_local", True))

    lines = [
        "控制面概览：",
        f"生成时间：{generated_text}",
        "用户(活跃/禁用)/节点(启用)：{0}({1}/{2})/{3}({4})".format(
            int(totals.get("users", 0) or 0),
            int(totals.get("active_users", 0) or 0),
            int(totals.get("disabled_users", 0) or 0),
            int(totals.get("nodes", 0) or 0),
            int(totals.get("enabled_nodes", 0) or 0),
        ),
        "绑定关系：{0}".format(int(totals.get("bindings", 0) or 0)),
        "节点监控：启用 {0}，在线 {1}，离线 {2}，未上报 {3}（阈值 {4} 秒）".format(
            int(monitor.get("enabled_nodes", 0) or 0),
            int(monitor.get("online_nodes", 0) or 0),
            int(monitor.get("offline_nodes", 0) or 0),
            int(monitor.get("never_seen_nodes", 0) or 0),
            int(monitor.get("threshold_seconds", 0) or 0),
        ),
        "任务队列：pending {0} / running {1} / failed {2} / timeout {3}".format(
            int(tasks.get("pending", 0) or 0),
            int(tasks.get("running", 0) or 0),
            int(tasks.get("failed", 0) or 0),
            int(tasks.get("timeout", 0) or 0),
        ),
        "队列阈值：单节点上限 {0}，预警阈值 {1}".format(
            queue_cap_per_node,
            near_cap_threshold,
        ),
        "幂等统计(24h)：创建 {0} / 去重命中 {1} / 命中率 {2:.1%}".format(
            idempotency_created,
            idempotency_deduplicated,
            idempotency_ratio,
        ),
        "安全告警：{0} 条".format(len(warnings)),
        "未授权访问(1h/24h)：{0}/{1}".format(unauthorized_1h, unauthorized_24h),
        "事件统计过滤本机：{0}".format("是" if events_exclude_local else "否"),
    ]

    if offline_codes:
        lines.append("离线节点（最多 5 个）：{0}".format(", ".join(offline_codes)))
    if pending_parts:
        lines.append("待执行节点（最多 5 个）：{0}".format(", ".join(pending_parts)))
    if near_cap_parts:
        lines.append("队列接近上限（最多 5 个）：{0}".format(", ".join(near_cap_parts)))
    if idempotency_parts:
        lines.append("去重热点节点（命中/总请求）：{0}".format(", ".join(idempotency_parts)))
    if top_unauthorized_parts:
        lines.append("未授权来源TOP（最多 3 个）：{0}".format(", ".join(top_unauthorized_parts)))
    lines.append("说明：24h 为滚动统计，加固后会随时间自然下降。")
    if warnings:
        lines.append("告警摘要：")
        for warning in warnings[:3]:
            lines.append("- {0}".format(str(warning)))

    return "\n".join(lines)


def get_maintain_log_unit(target: str) -> str:
    unit_map = {
        "controller": "sb-controller",
        "bot": "sb-bot",
        "caddy": "caddy",
    }
    return unit_map.get(target, "")


def extract_log_date_keys(raw_text: str, max_dates: int = 7) -> list:
    matched_dates = []
    seen = set()
    for raw_line in str(raw_text or "").splitlines():
        line = raw_line.strip()
        match = re.match(r"^(\d{4})-(\d{2})-(\d{2})", line)
        if not match:
            continue
        date_key = "{0}{1}{2}".format(match.group(1), match.group(2), match.group(3))
        if date_key in seen:
            continue
        seen.add(date_key)
        matched_dates.append(date_key)
    matched_dates.sort(reverse=True)
    return matched_dates[:max_dates]


def get_log_importance_level(line: str) -> int:
    text = str(line or "").lower()
    level_1_keywords = [
        "panic",
        "fatal",
        "critical",
        "error",
        "failed",
        "failure",
        "exception",
        "traceback",
        "unauthorized",
        "denied",
        "timeout",
        "refused",
        "segfault",
        "invalid",
        "no such",
        "not found",
        "expired",
        "错误",
        "失败",
        "异常",
        "拒绝",
        "超时",
        "崩溃",
        "未授权",
        "无效",
        "中断",
    ]
    level_2_keywords = [
        "warn",
        "warning",
        "degraded",
        "reload",
        "restart",
        "start",
        "stop",
        "stopped",
        "changed",
        "update",
        "notice",
        "retry",
        "证书",
        "续期",
        "重载",
        "重启",
        "更新",
        "告警",
        "警告",
    ]
    for keyword in level_1_keywords:
        if keyword in text:
            return 1
    for keyword in level_2_keywords:
        if keyword in text:
            return 2
    return 3


def build_priority_log_entries(raw_text: str, line_char_limit: int = 0) -> tuple:
    raw_lines = [str(item or "").strip() for item in str(raw_text or "").splitlines()]
    raw_lines = [item for item in raw_lines if item]
    if not raw_lines:
        return [], 0, 0, 0

    buckets = {1: [], 2: [], 3: []}
    for line in reversed(raw_lines):
        level = get_log_importance_level(line)
        compact = " ".join(line.replace("\r", "").split())
        if line_char_limit > 0 and len(compact) > line_char_limit:
            compact = compact[:line_char_limit] + "..."
        buckets[level].append(compact)

    labels = {1: "[一级]", 2: "[二级]", 3: "[三级]"}
    selected_lines = []
    for level in (1, 2, 3):
        for line in buckets[level]:
            selected_lines.append("{0} {1}".format(labels[level], line))
    return selected_lines, len(buckets[1]), len(buckets[2]), len(buckets[3])


def split_log_entry(entry: str, max_piece_chars: int = 900) -> list:
    text = str(entry or "")
    if len(text) <= max_piece_chars:
        return [text]
    parts = []
    total = (len(text) + max_piece_chars - 1) // max_piece_chars
    for index in range(total):
        start = index * max_piece_chars
        end = start + max_piece_chars
        piece = text[start:end]
        parts.append("（分段 {0}/{1}）{2}".format(index + 1, total, piece))
    return parts


def build_log_pages(entries: list, max_lines_per_page: int = 50, max_chars_per_page: int = 3200) -> list:
    if not entries:
        return [["(当日无日志)"]]

    normalized_entries = []
    for entry in entries:
        normalized_entries.extend(split_log_entry(entry, max_piece_chars=900))

    pages = []
    current_page = []
    current_chars = 0
    for entry in normalized_entries:
        entry_len = len(entry) + 1
        need_new_page = False
        if current_page and len(current_page) >= max_lines_per_page:
            need_new_page = True
        if current_page and current_chars + entry_len > max_chars_per_page:
            need_new_page = True
        if need_new_page:
            pages.append(current_page)
            current_page = []
            current_chars = 0
        current_page.append(entry)
        current_chars += entry_len

    if current_page:
        pages.append(current_page)
    if not pages:
        return [["(当日无日志)"]]
    return pages


def get_log_view_cooldown_remaining(
    context: ContextTypes.DEFAULT_TYPE, now_ts: float = 0.0
) -> float:
    if LOG_VIEW_COOLDOWN_SECONDS <= 0:
        return 0.0
    if now_ts <= 0:
        now_ts = time.time()
    try:
        last_ts = float(context.user_data.get(LOG_VIEW_LAST_ACTION_AT_KEY, 0.0) or 0.0)
    except (TypeError, ValueError):
        last_ts = 0.0
    delta = now_ts - last_ts
    remaining = LOG_VIEW_COOLDOWN_SECONDS - delta
    return remaining if remaining > 0 else 0.0


def mark_log_view_action(context: ContextTypes.DEFAULT_TYPE, now_ts: float = 0.0) -> None:
    if now_ts <= 0:
        now_ts = time.time()
    context.user_data[LOG_VIEW_LAST_ACTION_AT_KEY] = now_ts


def is_mutation_callback(callback_data: str) -> bool:
    if callback_data in MUTATION_CALLBACK_EXACT:
        return True
    for prefix in MUTATION_CALLBACK_PREFIXES:
        if callback_data.startswith(prefix):
            return True
    return False


def build_mutation_action_key(update: Update, callback_data: str) -> str:
    chat_id = 0
    user_id = 0
    if update.effective_chat:
        chat_id = int(update.effective_chat.id)
    if update.effective_user:
        user_id = int(update.effective_user.id)
    return "{0}:{1}:{2}".format(chat_id, user_id, callback_data)


def get_mutation_cooldown_remaining(
    context: ContextTypes.DEFAULT_TYPE,
    action_key: str,
    now_ts: float = 0.0,
) -> float:
    if MUTATION_COOLDOWN_SECONDS <= 0:
        return 0.0
    if now_ts <= 0:
        now_ts = time.time()
    state_map = context.application.bot_data.get(MUTATION_LAST_ACTION_MAP_KEY, {})
    if not isinstance(state_map, dict):
        state_map = {}
    try:
        last_ts = float(state_map.get(action_key, 0.0) or 0.0)
    except (TypeError, ValueError):
        last_ts = 0.0
    delta = now_ts - last_ts
    remaining = MUTATION_COOLDOWN_SECONDS - delta
    return remaining if remaining > 0 else 0.0


def mark_mutation_action(
    context: ContextTypes.DEFAULT_TYPE,
    action_key: str,
    now_ts: float = 0.0,
) -> None:
    if now_ts <= 0:
        now_ts = time.time()
    state_map = context.application.bot_data.setdefault(MUTATION_LAST_ACTION_MAP_KEY, {})
    if not isinstance(state_map, dict):
        state_map = {}
        context.application.bot_data[MUTATION_LAST_ACTION_MAP_KEY] = state_map
    state_map[action_key] = now_ts

    if len(state_map) > 2000:
        expire_before = now_ts - max(MUTATION_COOLDOWN_SECONDS * 5, 60.0)
        stale_keys = []
        for key, value in state_map.items():
            try:
                ts = float(value)
            except (TypeError, ValueError):
                ts = 0.0
            if ts <= expire_before:
                stale_keys.append(key)
        for key in stale_keys:
            state_map.pop(key, None)


async def enforce_mutation_cooldown(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    callback_data: str,
) -> bool:
    if not is_mutation_callback(callback_data):
        return True
    now_ts = time.time()
    action_key = build_mutation_action_key(update, callback_data)
    remaining = get_mutation_cooldown_remaining(context, action_key, now_ts=now_ts)
    if remaining <= 0:
        mark_mutation_action(context, action_key, now_ts=now_ts)
        return True

    query = update.callback_query
    if query:
        wait_seconds = "{0:.1f}".format(remaining)
        try:
            await query.answer(f"操作过快，请 {wait_seconds} 秒后重试", show_alert=False)
        except BadRequest:
            pass
    return False


def pop_user_speed_pending(
    context: ContextTypes.DEFAULT_TYPE, user_code: str = ""
) -> None:
    pending_map = context.user_data.get(USER_SPEED_PENDING_KEY, {})
    if isinstance(pending_map, dict):
        if user_code:
            pending_map.pop(user_code, None)
        else:
            pending_map.clear()
    context.user_data.pop(USER_SPEED_ACTIVE_KEY, None)


def get_node_edit_old_new_values(field: str, node: dict, pending_edit: dict) -> tuple:
    if field == "host":
        old_value = str(node.get("host", ""))
        new_value = str(pending_edit.get("new_value", ""))
        return old_value, new_value
    if field == "sni":
        old_value = str(node.get("reality_server_name") or "未设置")
        raw_new_value = str(pending_edit.get("new_value", ""))
        new_value = raw_new_value if raw_new_value else "未设置"
        return old_value, new_value
    if field == "pool":
        old_value = f"{node.get('tuic_port_start', '')}-{node.get('tuic_port_end', '')}"
        new_value = (
            f"{pending_edit.get('new_pool_start', '')}-"
            f"{pending_edit.get('new_pool_end', '')}"
        )
        return old_value, new_value
    if field == "agent_ip":
        old_value = str(node.get("agent_ip") or "未设置")
        new_value = str(pending_edit.get("new_value", "")) or "未设置"
        return old_value, new_value
    return "", ""


def format_create_summary(display_name: str, tuic_port: int, speed_mbps: int, valid_days: int) -> str:
    speed_text = "不限速（0 Mbps）" if speed_mbps == 0 else f"{speed_mbps} Mbps"
    return (
        "请确认创建信息：\n"
        f"备注/用户名：{display_name}\n"
        f"TUIC 端口：{tuic_port}\n"
        f"限速：{speed_text}\n"
        f"有效天数：{valid_days} 天"
    )


def format_nodes_create_summary(
    node_code: str,
    region: str,
    host: str,
    agent_ip: str,
    reality_server_name: str,
    tuic_port_start: int,
    tuic_port_end: int,
    note: str,
) -> str:
    reality_text = reality_server_name if reality_server_name else "未设置"
    note_text = note if note else "无"
    return (
        "请确认节点信息：\n"
        f"节点代码：{node_code}\n"
        f"地区：{region}\n"
        f"主机：{host}\n"
        f"节点来源IP白名单：{agent_ip}\n"
        f"Reality域名：{reality_text}\n"
        f"TUIC端口池：{tuic_port_start}-{tuic_port_end}\n"
        f"备注：{note_text}"
    )


def format_node_tags(node: dict) -> str:
    supports_reality_value = node.get("supports_reality", 1)
    supports_tuic_value = node.get("supports_tuic", 1)
    if supports_reality_value is None:
        supports_reality_value = 1
    if supports_tuic_value is None:
        supports_tuic_value = 1
    supports_reality = int(supports_reality_value) == 1
    supports_tuic = int(supports_tuic_value) == 1
    if supports_reality and supports_tuic:
        return "[R][T]"
    if supports_reality:
        return "[R]"
    if supports_tuic:
        return "[T]"
    return "[无]"


def build_node_tags_map(nodes: list) -> dict:
    tags_map = {}
    for node in nodes or []:
        node_code = str(node.get("node_code", ""))
        if node_code:
            tags_map[node_code] = format_node_tags(node)
    return tags_map


def is_valid_ip_address(value: str) -> bool:
    try:
        ipaddress.ip_address(value)
        return True
    except ValueError:
        return False


def format_node_detail_text(node: dict) -> str:
    reality_server_name = node.get("reality_server_name") or "未设置"
    tuic_server_name = node.get("tuic_server_name") or "未设置"
    agent_ip = str(node.get("agent_ip") or "").strip() or "未设置（建议设置）"
    enabled_text = "启用" if int(node.get("enabled", 0)) == 1 else "禁用"
    monitor_text = "开启" if int(node.get("monitor_enabled", 0) or 0) == 1 else "关闭"
    try:
        last_seen_at = int(node.get("last_seen_at", 0) or 0)
    except (TypeError, ValueError):
        last_seen_at = 0
    now = int(time.time())
    is_online = (
        last_seen_at > 0 and (now - last_seen_at) <= NODE_OFFLINE_THRESHOLD_SECONDS
    )
    online_text = "在线" if is_online else "离线"
    tags = format_node_tags(node)
    return (
        f"节点：{node.get('node_code', '')}\n"
        f"地区：{node.get('region', '')}\n"
        f"节点来源IP白名单：{agent_ip}\n"
        f"支持协议：{tags}（R=VLESS+REALITY，T=TUIC）\n"
        "【VLESS+REALITY】\n"
        f"入口(用于连接)：{node.get('host', '')}\n"
        f"REALITY伪装域名（R）：{reality_server_name}\n"
        "【TUIC】\n"
        f"TUIC证书域名（T）：{tuic_server_name}\n"
        f"端口池：{node.get('tuic_port_start', '')}-{node.get('tuic_port_end', '')}\n"
        f"状态：{enabled_text}\n"
        f"监控：{monitor_text}\n"
        f"在线状态：{online_text}（最后心跳：{format_last_seen_text(last_seen_at)}）"
    )


def mask_key_preview(value: str) -> str:
    raw_value = str(value or "")
    if not raw_value:
        return "未设置"
    if len(raw_value) <= 8:
        return raw_value
    return f"{raw_value[:8]}..."


def extract_reality_public_key_short_id(raw_text: str) -> tuple:
    lines = [line.strip() for line in raw_text.splitlines() if line.strip()]
    public_key = ""
    short_id = ""

    for line in lines:
        public_key_match = re.search(
            r"(?i)public\s*key\s*[:：]\s*([^\s]+)",
            line,
        )
        if not public_key_match:
            public_key_match = re.search(
                r"(?i)publickey\s*[:：]\s*([^\s]+)",
                line,
            )
        if public_key_match and not public_key:
            public_key = public_key_match.group(1).strip().strip("`\"'")
            public_key = public_key.rstrip(",")

        short_id_match = re.search(
            r"(?i)short\s*id\s*[:：]\s*([0-9a-fA-F]{1,8})",
            line,
        )
        if not short_id_match:
            short_id_match = re.search(
                r"(?i)shortid\s*[:：]\s*([0-9a-fA-F]{1,8})",
                line,
            )
        if short_id_match and not short_id:
            short_id = short_id_match.group(1).lower()

    if not short_id:
        for line in lines:
            candidate = line.strip().strip("`\"'")
            if re.fullmatch(r"[0-9a-fA-F]{1,8}", candidate):
                short_id = candidate.lower()
                break

    if not public_key:
        return "", ""
    if not short_id or not re.fullmatch(r"[0-9a-fA-F]{1,8}", short_id):
        return "", ""
    return public_key, short_id


def format_node_reality_setup_text(node_code: str, node: dict) -> str:
    has_public_key = "已配置" if str(node.get("reality_public_key") or "").strip() else "未配置"
    has_short_id = "已配置" if str(node.get("reality_short_id") or "").strip() else "未配置"
    return (
        "REALITY 参数配置\n\n"
        "正在配置的协议范围：仅 VLESS+REALITY\n"
        f"影响范围：仅该节点 {node_code}\n"
        f"当前状态：public_key {has_public_key} / short_id {has_short_id}\n\n"
        "请在该节点服务器（Debian/Ubuntu）执行以下命令：\n"
        "```bash\n"
        "curl -fsSL https://sing-box.app/install.sh | sh\n"
        "sing-box generate reality-keypair\n"
        "sing-box generate rand 8 --hex\n"
        "# 若 rand 子命令不可用：\n"
        "openssl rand -hex 4\n"
        "```\n\n"
        "然后仅粘贴以下两项（不要粘贴 private key）：\n"
        "PublicKey: xxxx\n"
        "ShortID: xxxx"
    )


def format_node_ops_panel_text(node: dict) -> str:
    node_code = str(node.get("node_code", "")).strip()
    region = str(node.get("region", "")).strip() or "-"
    host = str(node.get("host", "")).strip() or "-"
    enabled_text = "启用" if int(node.get("enabled", 0) or 0) == 1 else "禁用"
    tags = format_node_tags(node)
    last_seen_at = 0
    try:
        last_seen_at = int(node.get("last_seen_at", 0) or 0)
    except (TypeError, ValueError):
        last_seen_at = 0
    return (
        "节点远程运维\n\n"
        f"节点：{node_code}\n"
        f"地区：{region}\n"
        f"入口：{host}\n"
        f"状态：{enabled_text}\n"
        f"支持协议：{tags}\n"
        f"最后心跳：{format_last_seen_text(last_seen_at)}\n\n"
        "说明：以下操作通过 agent 拉取任务执行，通常在 10~30 秒内生效。"
    )


def format_node_sync_preview_text(node_code: str, payload: dict) -> str:
    node = payload.get("node", {}) if isinstance(payload, dict) else {}
    users = payload.get("users", []) if isinstance(payload, dict) else []
    if not isinstance(users, list):
        users = []

    generated_at = 0
    try:
        generated_at = int(payload.get("generated_at", 0) or 0) if isinstance(payload, dict) else 0
    except (TypeError, ValueError):
        generated_at = 0
    generated_text = (
        datetime.fromtimestamp(generated_at).strftime("%Y-%m-%d %H:%M:%S")
        if generated_at > 0
        else "-"
    )
    node_enabled = "启用" if int(node.get("enabled", 0) or 0) == 1 else "禁用"
    limit = 12
    lines = [
        "节点同步预览（管理端）",
        "",
        f"节点：{node_code}",
        f"节点状态：{node_enabled}",
        f"生成时间：{generated_text}",
        f"下发用户数：{len(users)}",
        "",
        "用户预览（最多 12 条）：",
    ]
    if not users:
        lines.append("（无）")
    else:
        for idx, item in enumerate(users[:limit], start=1):
            user_code = str(item.get("user_code", "")).strip() or "-"
            limit_mode = str(item.get("limit_mode", "tc")).strip() or "tc"
            speed = int(item.get("speed_mbps", 0) or 0)
            tuic_port = int(item.get("tuic_port", 0) or 0)
            lines.append(
                f"{idx}. {user_code} | mode={limit_mode} | speed={speed}Mbps | tuic={tuic_port}"
            )
        if len(users) > limit:
            lines.append(f"... 其余 {len(users) - limit} 条已省略")
    lines.append("")
    lines.append("说明：该页面来自 /admin/nodes/{node_code}/sync_preview，不受 agent_ip 限制。")
    return "\n".join(lines)


def truncate_task_result_text(value: str, limit: int = 240) -> str:
    raw = str(value or "").strip().replace("\r\n", "\n")
    raw = " ".join(raw.split())
    if not raw:
        return "(无输出)"
    if len(raw) <= limit:
        return raw
    return raw[:limit] + "..."


def format_node_tasks_text(node_code: str, tasks: list) -> str:
    lines = [f"节点任务记录：{node_code}", ""]
    if not tasks:
        lines.append("暂无任务记录。")
        return "\n".join(lines)
    for item in tasks:
        task_id = int(item.get("id", 0) or 0)
        task_type = str(item.get("task_type", ""))
        status_text = str(item.get("status", ""))
        attempts = int(item.get("attempts", 0) or 0)
        max_attempts = int(item.get("max_attempts", 1) or 1)
        updated_at = int(item.get("updated_at", 0) or 0)
        updated_text = (
            datetime.fromtimestamp(updated_at).strftime("%Y-%m-%d %H:%M:%S")
            if updated_at > 0
            else "-"
        )
        result_text = truncate_task_result_text(item.get("result_text", ""))
        lines.append(
            f"#{task_id} | {task_type} | {status_text} | 尝试 {attempts}/{max_attempts} | {updated_text}\n"
            f"结果：{result_text}"
        )
    return "\n\n".join(lines[:11])


def parse_node_ops_config_updates(raw_text: str) -> tuple:
    updates = {}
    for line in raw_text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if "=" not in stripped:
            return False, {}, f"格式错误：{stripped}（请使用 KEY=VALUE）"
        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip()
        if value == "-":
            value = ""
        if key not in NODE_OPS_ALLOWED_KEYS:
            return False, {}, f"不支持的参数：{key}"
        updates[key] = value
    if not updates:
        return False, {}, "未检测到有效修改项"

    if "poll_interval" in updates:
        if not updates["poll_interval"].isdigit() or int(updates["poll_interval"]) < 5:
            return False, {}, "poll_interval 必须是 >=5 的整数"
        updates["poll_interval"] = int(updates["poll_interval"])
    if "tuic_listen_port" in updates:
        if not updates["tuic_listen_port"].isdigit():
            return False, {}, "tuic_listen_port 必须是 1-65535 的整数"
        port_value = int(updates["tuic_listen_port"])
        if port_value < 1 or port_value > 65535:
            return False, {}, "tuic_listen_port 必须是 1-65535 的整数"
        updates["tuic_listen_port"] = port_value
    if "controller_url" in updates:
        updates["controller_url"] = normalize_simple_url(updates["controller_url"], "http")
        if not updates["controller_url"]:
            return False, {}, "controller_url 不能为空"
    if "node_code" in updates and not str(updates["node_code"]).strip():
        return False, {}, "node_code 不能为空"
    return True, updates, ""


async def controller_request(
    method: str, path: str, payload: dict = None
) -> tuple:
    url = f"{CONTROLLER_URL}{path}"
    headers = {}
    if CONTROLLER_AUTH_TOKEN:
        headers["Authorization"] = f"Bearer {CONTROLLER_AUTH_TOKEN}"
    headers["X-Actor"] = BOT_ACTOR_LABEL
    headers["User-Agent"] = "sb-bot-panel-bot/1.0"

    global _controller_http_client
    if _controller_http_client is None or _controller_http_client.is_closed:
        _controller_http_client = httpx.AsyncClient(
            timeout=CONTROLLER_HTTP_TIMEOUT_SECONDS,
            limits=httpx.Limits(max_connections=30, max_keepalive_connections=20),
        )
    try:
        response = await _controller_http_client.request(method, url, json=payload, headers=headers)
    except httpx.HTTPError as exc:
        return None, f"无法连接控制器接口（{exc}）", 0

    if response.status_code >= 400:
        try:
            error_body = response.json()
            error_message = str(error_body.get("detail", error_body))
        except ValueError:
            error_message = response.text or f"HTTP {response.status_code}"
        return None, error_message, response.status_code

    try:
        return response.json(), "", response.status_code
    except ValueError:
        return None, "", response.status_code


async def write_admin_audit_event_best_effort(
    action: str,
    resource_type: str,
    resource_id: str,
    detail: dict,
    retry_attempts: int = 3,
    retry_delay_seconds: float = 1.0,
) -> bool:
    payload = {
        "action": str(action or "").strip(),
        "resource_type": str(resource_type or "").strip() or "bot",
        "resource_id": str(resource_id or "").strip(),
        "detail": detail if isinstance(detail, dict) else {},
    }
    attempts = retry_attempts if retry_attempts >= 1 else 1
    last_error = ""
    for index in range(attempts):
        _, error_message, status_code = await controller_request(
            "POST",
            "/admin/audit/event",
            payload=payload,
        )
        if not error_message:
            return True
        last_error = str(error_message)
        is_connection_issue = last_error.startswith("无法连接控制器接口")
        is_last = index >= attempts - 1
        if is_last or not is_connection_issue:
            break
        await asyncio.sleep(retry_delay_seconds if retry_delay_seconds > 0 else 0.5)
    logger.warning(
        "write_admin_audit_event failed action=%s status=%s error=%s",
        payload["action"],
        status_code if "status_code" in locals() else 0,
        last_error,
    )
    return False


async def close_controller_http_client(application: Application) -> None:
    del application
    global _controller_http_client
    if _controller_http_client is not None and not _controller_http_client.is_closed:
        await _controller_http_client.aclose()
    _controller_http_client = None


async def render_nodes_list(
    query, notice: str = ""
) -> None:
    nodes, error_message, _ = await controller_request("GET", "/nodes")
    if error_message:
        text = f"获取节点列表失败：{error_message}"
        await query.edit_message_text(text, reply_markup=build_submenu("nodes"))
        return

    header = "节点列表：点击进入详情"
    if not nodes:
        header = "节点列表：点击进入详情\n（暂无节点）"
    if notice:
        header = f"{notice}\n\n{header}"
    await query.edit_message_text(header, reply_markup=build_nodes_list_keyboard(nodes or []))


async def render_node_detail(
    query, node_code: str, notice: str = ""
) -> None:
    node, error_message, status_code = await controller_request(
        "GET", f"/nodes/{node_code}"
    )
    if error_message:
        if status_code == 404:
            await render_nodes_list(query, notice=f"节点不存在：{node_code}")
            return
        await query.edit_message_text(
            f"获取节点详情失败：{error_message}",
            reply_markup=build_submenu("nodes"),
        )
        return

    detail_text = format_node_detail_text(node)
    if notice:
        detail_text = f"{notice}\n\n{detail_text}"
    await query.edit_message_text(
        detail_text,
        reply_markup=build_node_detail_keyboard(
            node_code, int(node.get("monitor_enabled", 0) or 0)
        ),
    )


async def render_node_sync_preview(
    query, node_code: str, notice: str = ""
) -> None:
    payload, error_message, status_code = await controller_request(
        "GET", f"/admin/nodes/{node_code}/sync_preview"
    )
    if error_message:
        localized = localize_controller_error(error_message)
        if status_code == 404 and localized == "节点不存在":
            await render_nodes_list(query, notice=f"节点不存在：{node_code}")
            return
        await query.edit_message_text(
            f"读取同步预览失败：{localized}",
            reply_markup=build_node_sync_preview_keyboard(node_code),
        )
        return

    text = format_node_sync_preview_text(node_code, payload if isinstance(payload, dict) else {})
    if notice:
        text = f"{notice}\n\n{text}"
    await query.edit_message_text(
        text,
        reply_markup=build_node_sync_preview_keyboard(node_code),
    )


async def send_node_detail_message(
    message, context: ContextTypes.DEFAULT_TYPE, node_code: str, notice: str = ""
) -> None:
    node, error_message, status_code = await controller_request(
        "GET", f"/nodes/{node_code}"
    )
    if error_message:
        if status_code == 404:
            await reply_text_with_auto_clear(
                message,
                context,
                f"节点不存在：{node_code}",
                reply_markup=build_submenu("nodes"),
            )
            return
        await reply_text_with_auto_clear(
            message,
            context,
            f"获取节点详情失败：{error_message}",
            reply_markup=build_submenu("nodes"),
        )
        return

    detail_text = format_node_detail_text(node)
    if notice:
        detail_text = f"{notice}\n\n{detail_text}"
    await reply_text_with_auto_clear(
        message,
        context,
        detail_text,
        reply_markup=build_node_detail_keyboard(
            node_code, int(node.get("monitor_enabled", 0) or 0)
        ),
    )


async def render_node_reality_setup(query, node_code: str) -> None:
    node, error_message, status_code = await controller_request(
        "GET", f"/nodes/{node_code}"
    )
    if error_message:
        if status_code == 404:
            await render_nodes_list(query, notice=f"节点不存在：{node_code}")
            return
        await query.edit_message_text(
            f"获取节点详情失败：{error_message}",
            reply_markup=build_submenu("nodes"),
        )
        return

    await query.edit_message_text(
        format_node_reality_setup_text(node_code, node or {}),
        reply_markup=build_node_reality_setup_keyboard(node_code),
    )


async def create_node_task(
    node_code: str,
    task_type: str,
    payload: dict = None,
    max_attempts: int = 1,
) -> tuple:
    request_payload = {"task_type": task_type}
    if isinstance(payload, dict):
        request_payload["payload"] = payload
    if max_attempts >= 1:
        request_payload["max_attempts"] = int(max_attempts)
    result, error_message, status_code = await controller_request(
        "POST", f"/nodes/{node_code}/tasks/create", payload=request_payload
    )
    return result, error_message, status_code


async def render_node_ops_picker(query, notice: str = "") -> None:
    nodes, error_message, _ = await controller_request("GET", "/nodes")
    if error_message:
        await query.edit_message_text(
            f"获取节点列表失败：{localize_controller_error(error_message)}",
            reply_markup=build_submenu("nodes"),
        )
        return
    header = "节点远程运维：请选择要操作的节点"
    if notice:
        header = f"{notice}\n\n{header}"
    if not nodes:
        header = f"{header}\n（暂无节点）"
    await query.edit_message_text(
        header,
        reply_markup=build_node_ops_picker_keyboard(nodes or []),
    )


async def render_node_ops_panel(query, node_code: str, notice: str = "") -> None:
    node, error_message, status_code = await controller_request("GET", f"/nodes/{node_code}")
    if error_message:
        localized = localize_controller_error(error_message)
        if status_code == 404 and localized == "节点不存在":
            await render_node_ops_picker(query, notice=f"节点不存在：{node_code}")
            return
        await query.edit_message_text(
            f"获取节点详情失败：{localized}",
            reply_markup=build_back_keyboard("action:node_ops"),
        )
        return

    text = format_node_ops_panel_text(node or {})
    if notice:
        text = f"{notice}\n\n{text}"
    await query.edit_message_text(
        text,
        reply_markup=build_node_ops_panel_keyboard(node_code),
    )


async def render_node_ops_tasks(query, node_code: str, notice: str = "") -> None:
    tasks, error_message, status_code = await controller_request(
        "GET", f"/nodes/{node_code}/tasks?limit=10"
    )
    if error_message:
        localized = localize_controller_error(error_message)
        if status_code == 404 and localized == "节点不存在":
            await render_node_ops_picker(query, notice=f"节点不存在：{node_code}")
            return
        await query.edit_message_text(
            f"获取任务记录失败：{localized}",
            reply_markup=build_back_keyboard(f"nodeops:panel:{node_code}"),
        )
        return

    text = format_node_tasks_text(node_code, tasks if isinstance(tasks, list) else [])
    if notice:
        text = f"{notice}\n\n{text}"
    await query.edit_message_text(
        text,
        reply_markup=build_node_ops_task_list_keyboard(node_code),
    )


async def run_node_ops_action(query, node_code: str, action_key: str) -> None:
    task_mapping = {
        "restart": ("restart_singbox", None, 2, "已下发“重启 sing-box”任务"),
        "status_sb": ("status_singbox", None, 1, "已下发“查看 sing-box 状态”任务"),
        "status_ag": ("status_agent", None, 1, "已下发“查看 sb-agent 状态”任务"),
        "logs_sb": ("logs_singbox", {"lines": 120}, 1, "已下发“查看 sing-box 日志”任务"),
        "logs_ag": ("logs_agent", {"lines": 120}, 1, "已下发“查看 sb-agent 日志”任务"),
        "update": ("update_sync", None, 2, "已下发“节点同步更新”任务（仅当前节点）"),
    }
    if action_key not in task_mapping:
        await query.edit_message_text(
            "不支持的远程操作。",
            reply_markup=build_back_keyboard(f"nodeops:panel:{node_code}"),
        )
        return
    task_type, payload, max_attempts, action_text = task_mapping[action_key]
    result, error_message, status_code = await create_node_task(
        node_code, task_type, payload, max_attempts=max_attempts
    )
    if error_message:
        localized = localize_controller_error(error_message)
        if status_code == 404 and localized == "节点不存在":
            await render_node_ops_picker(query, notice=f"节点不存在：{node_code}")
            return
        await query.edit_message_text(
            f"任务下发失败：{localized}",
            reply_markup=build_back_keyboard(f"nodeops:panel:{node_code}"),
        )
        return
    task_id = int(result.get("id", 0) or 0) if isinstance(result, dict) else 0
    await query.edit_message_text(
        f"{action_text}\n任务ID：{task_id}\n\n"
        "说明：agent 轮询到任务后会执行，可点“刷新任务记录”查看结果。",
        reply_markup=build_node_ops_task_done_keyboard(node_code),
    )


def format_user_nodes_manage_text(
    user_code: str, user: dict, user_nodes: list, node_tags_map: dict, notice: str = ""
) -> str:
    status_text = str(user.get("status", "-"))
    expire_at = int(user.get("expire_at", 0) or 0)
    expire_text = (
        datetime.fromtimestamp(expire_at).strftime("%Y-%m-%d %H:%M:%S")
        if expire_at > 0
        else "-"
    )
    speed_mbps = int(user.get("speed_mbps", 0) or 0)
    speed_text = "不限速（0 Mbps）" if speed_mbps == 0 else f"{speed_mbps} Mbps"

    lines = [
        f"用户：{user_code}",
        f"状态：{status_text}",
        f"到期：{expire_text}",
        f"限速：{speed_text}",
        "",
        "已绑定节点列表：",
    ]
    if user_nodes:
        for item in user_nodes:
            node_code = item.get("node_code", "")
            region = item.get("region", "")
            host = item.get("host", "")
            tuic_port = item.get("tuic_port", "")
            tags = node_tags_map.get(str(node_code), "[?]")
            enabled_text = "启用" if int(item.get("enabled", 0) or 0) == 1 else "禁用"
            lines.append(
                f"{node_code} | {region} | {tags} | {host} | TUIC端口:{tuic_port} | 状态:{enabled_text}"
            )
    else:
        lines.append("（暂无绑定节点）")

    text = "\n".join(lines)
    if notice:
        text = f"{notice}\n\n{text}"
    return text


def normalize_limit_mode(raw_value: str) -> str:
    value = str(raw_value or "").strip().lower()
    if value == "tc":
        return "tc"
    if value == "off":
        return "off"
    return "tc"


def format_limit_mode_label(mode: str) -> str:
    normalized = normalize_limit_mode(mode)
    if normalized == "tc":
        return "tc（启用）"
    return "off（关闭）"


def format_sub_links_info_text(
    user_code: str, links_url: str = "", base64_url: str = "", signed: bool = False, expire_at: int = 0
) -> str:
    plain_url = links_url.strip() if links_url else f"{PANEL_BASE_URL}/sub/links/{user_code}"
    b64_url = base64_url.strip() if base64_url else f"{PANEL_BASE_URL}/sub/base64/{user_code}"
    signed_hint = ""
    if signed and expire_at > 0:
        signed_hint = "（已签名）\n签名到期时间：{0}\n\n".format(
            datetime.fromtimestamp(expire_at).strftime("%Y-%m-%d %H:%M:%S")
        )
    return (
        "明文订阅链接：\n"
        f"{plain_url}\n\n"
        "Base64订阅链接：\n"
        f"{b64_url}\n\n"
        f"{signed_hint}"
        "如果REALITY参数未配置，将在明文订阅中提示并跳过vless链接。"
    )


def format_query_user_detail_text(user: dict, user_nodes: list) -> str:
    user_code = str(user.get("user_code", ""))
    display_name = str(user.get("display_name") or "").strip()
    user_label = f"{display_name}（{user_code}）" if display_name else user_code
    status_text = str(user.get("status", "-"))
    expire_at = int(user.get("expire_at", 0) or 0)
    expire_text = (
        datetime.fromtimestamp(expire_at).strftime("%Y-%m-%d %H:%M:%S")
        if expire_at > 0
        else "-"
    )
    grace_days = user.get("grace_days", "-")
    speed_mbps = int(user.get("speed_mbps", 0) or 0)
    limit_mode = format_limit_mode_label(str(user.get("limit_mode", "tc")))
    speed_text = "不限速（0 Mbps）" if speed_mbps == 0 else f"{speed_mbps} Mbps"

    lines = [
        f"用户：{user_label}",
        f"状态：{status_text}",
        f"到期时间：{expire_text}",
        f"宽限天数：{grace_days}",
        f"限速：{speed_text}",
        f"限速模式：{limit_mode}",
        f"绑定节点数量：{len(user_nodes)}",
        "",
        "节点列表：",
    ]
    if user_nodes:
        for item in user_nodes:
            node_code = str(item.get("node_code", ""))
            region = str(item.get("region", ""))
            host = str(item.get("host", ""))
            enabled_text = "启用" if int(item.get("enabled", 0) or 0) == 1 else "禁用"
            lines.append(f"{node_code} | {region} | {host} | 状态:{enabled_text}")
    else:
        lines.append("（暂无绑定节点）")
    return "\n".join(lines)


def format_query_traffic_ranking_text(payload: dict) -> str:
    generated_at = int(payload.get("generated_at", 0) or 0)
    generated_text = (
        datetime.fromtimestamp(generated_at).strftime("%Y-%m-%d %H:%M:%S")
        if generated_at > 0
        else "-"
    )
    ranked_user_count = int(payload.get("ranked_user_count", 0) or 0)
    active_user_count = int(payload.get("active_user_count", 0) or 0)
    items = payload.get("items", [])
    if not isinstance(items, list):
        items = []

    lines = [
        "流量排行（估算）",
        f"生成时间：{generated_text}",
        f"活跃用户：{active_user_count}，参与排行：{ranked_user_count}",
        "规则：估算带宽 = speed_mbps × 绑定节点数（非真实计费流量）",
        "",
    ]
    if items:
        for item in items:
            rank = int(item.get("rank", 0) or 0)
            user_code = str(item.get("user_code", ""))
            display_name = str(item.get("display_name") or "").strip()
            user_label = f"{display_name}（{user_code}）" if display_name else user_code
            limit_mode = format_limit_mode_label(str(item.get("limit_mode", "tc")))
            speed_mbps = int(item.get("speed_mbps", 0) or 0)
            bindings = int(item.get("bindings", 0) or 0)
            estimated_mbps = int(item.get("estimated_mbps", 0) or 0)
            lines.append(
                f"{rank}. {user_label} | 模式:{limit_mode} | 限速:{speed_mbps}Mbps | 节点:{bindings} | 估算:{estimated_mbps}Mbps"
            )
    else:
        lines.append("（暂无可排行的活跃用户）")
    return "\n".join(lines)


async def render_user_nodes_picker(query) -> None:
    users, error_message, _ = await controller_request("GET", "/users")
    if error_message:
        await query.edit_message_text(
            f"获取用户列表失败：{localize_controller_error(error_message)}",
            reply_markup=build_submenu("user"),
        )
        return

    if not users:
        await query.edit_message_text(
            "暂无用户，请先创建用户",
            reply_markup=build_user_nodes_empty_users_keyboard(),
        )
        return

    await query.edit_message_text(
        "请选择用户：",
        reply_markup=build_user_nodes_picker_keyboard(users),
    )


async def render_user_speed_picker(query) -> None:
    users, error_message, _ = await controller_request("GET", "/users")
    if error_message:
        await query.edit_message_text(
            f"获取用户列表失败：{localize_controller_error(error_message)}",
            reply_markup=build_submenu("user"),
        )
        return

    if not users:
        await query.edit_message_text(
            "暂无用户，请先创建用户",
            reply_markup=build_user_nodes_empty_users_keyboard(),
        )
        return

    await query.edit_message_text(
        "请选择要修改限速的用户：",
        reply_markup=build_user_speed_picker_keyboard(users),
    )


async def render_user_limit_mode_picker(query) -> None:
    users, error_message, _ = await controller_request("GET", "/users")
    if error_message:
        await query.edit_message_text(
            f"获取用户列表失败：{localize_controller_error(error_message)}",
            reply_markup=build_submenu("speed"),
        )
        return

    if not users:
        await query.edit_message_text(
            "暂无用户，请先创建用户",
            reply_markup=build_back_keyboard("menu:speed"),
        )
        return

    await query.edit_message_text(
        "请选择要切换限速模式的用户：",
        reply_markup=build_user_limit_mode_picker_keyboard(users),
    )


async def render_user_delete_picker(query) -> None:
    users, error_message, _ = await controller_request("GET", "/users")
    if error_message:
        await query.edit_message_text(
            f"获取用户列表失败：{localize_controller_error(error_message)}",
            reply_markup=build_submenu("user"),
        )
        return

    if not users:
        await query.edit_message_text(
            "暂无用户，请先创建用户",
            reply_markup=build_back_keyboard("menu:user"),
        )
        return

    await query.edit_message_text(
        "请选择要删除的用户：",
        reply_markup=build_user_delete_picker_keyboard(users),
    )


async def render_user_toggle_picker(query) -> None:
    users, error_message, _ = await controller_request("GET", "/users")
    if error_message:
        await query.edit_message_text(
            f"获取用户列表失败：{localize_controller_error(error_message)}",
            reply_markup=build_submenu("user"),
        )
        return

    if not users:
        await query.edit_message_text(
            "暂无用户，请先创建用户",
            reply_markup=build_back_keyboard("menu:user"),
        )
        return

    await query.edit_message_text(
        "请选择要禁用/启用的用户：",
        reply_markup=build_user_toggle_picker_keyboard(users),
    )


async def render_query_user_picker(query) -> None:
    users, error_message, _ = await controller_request("GET", "/users")
    if error_message:
        await query.edit_message_text(
            f"获取用户列表失败：{localize_controller_error(error_message)}",
            reply_markup=build_submenu("query"),
        )
        return

    if not users:
        await query.edit_message_text(
            "暂无用户，请先创建用户",
            reply_markup=build_back_keyboard("menu:query"),
        )
        return

    await query.edit_message_text(
        "请选择要查询的用户：",
        reply_markup=build_query_user_picker_keyboard(users),
    )


async def render_query_user_detail(query, user_code: str) -> None:
    user, user_error, user_status = await controller_request("GET", f"/users/{user_code}")
    if user_error:
        localized = localize_controller_error(user_error)
        if user_status == 404 and localized == "用户不存在":
            await query.edit_message_text("用户不存在", reply_markup=build_submenu("query"))
            return
        await query.edit_message_text(
            f"获取用户信息失败：{localized}",
            reply_markup=build_submenu("query"),
        )
        return

    user_nodes, nodes_error, nodes_status = await controller_request(
        "GET", f"/users/{user_code}/nodes"
    )
    if nodes_error:
        localized = localize_controller_error(nodes_error)
        if nodes_status == 404 and localized == "用户不存在":
            await query.edit_message_text("用户不存在", reply_markup=build_submenu("query"))
            return
        await query.edit_message_text(
            f"获取用户节点失败：{localized}",
            reply_markup=build_submenu("query"),
        )
        return

    await query.edit_message_text(
        format_query_user_detail_text(user or {}, user_nodes or []),
        reply_markup=build_query_user_detail_keyboard(user_code),
    )


async def run_admin_backup_action(query, back_menu_callback: str) -> None:
    result, error_message, _ = await controller_request("POST", "/admin/backup")
    if error_message:
        await query.edit_message_text(
            f"执行备份失败：{localize_controller_error(error_message)}",
            reply_markup=build_back_keyboard(back_menu_callback),
        )
        return

    backup_path = str(result.get("path", "")) if isinstance(result, dict) else ""
    size_bytes = int(result.get("size_bytes", 0)) if isinstance(result, dict) else 0
    cleaned_files = int(result.get("cleaned_files", 0)) if isinstance(result, dict) else 0
    keep_count = int(result.get("keep_count", 0)) if isinstance(result, dict) else 0
    created_at = int(result.get("created_at", 0)) if isinstance(result, dict) else 0
    created_text = (
        datetime.fromtimestamp(created_at).strftime("%Y-%m-%d %H:%M:%S")
        if created_at > 0
        else "-"
    )
    await query.edit_message_text(
        "备份已生成\n\n"
        f"路径：{backup_path}\n"
        f"大小：{size_bytes} bytes\n"
        f"清理旧备份：{cleaned_files} 个（保留 {keep_count} 个）\n"
        f"时间：{created_text}\n\n"
        "可用以下命令拉取备份：\n"
        f"scp root@你的服务器IP:{backup_path} ./",
        reply_markup=build_back_keyboard(back_menu_callback),
    )


def format_emergency_disable_users_text(result: dict, dry_run: bool) -> str:
    active_user_count = int(result.get("active_user_count", 0) or 0)
    changed_user_count = int(result.get("changed_user_count", 0) or 0)
    affected_bindings = int(result.get("affected_bindings", 0) or 0)
    sample_user_codes = result.get("sample_user_codes", [])
    if not isinstance(sample_user_codes, list):
        sample_user_codes = []
    sample_text = "、".join(str(item) for item in sample_user_codes[:20] if str(item).strip())
    if not sample_text:
        sample_text = "-"
    if int(result.get("sample_truncated", 0) or 0) == 1:
        sample_text = f"{sample_text} ..."
    created_at = int(result.get("created_at", 0) or 0)
    created_text = (
        datetime.fromtimestamp(created_at).strftime("%Y-%m-%d %H:%M:%S")
        if created_at > 0
        else "-"
    )
    if dry_run:
        return (
            "🛑 紧急停止预演（Dry-Run）\n\n"
            f"将禁用活跃用户：{active_user_count}\n"
            f"受影响绑定关系：{affected_bindings}\n"
            f"示例用户：{sample_text}\n"
            f"时间：{created_text}\n\n"
            "确认后将执行：禁用全部 active 用户。"
        )
    return (
        "🛑 紧急停止已执行\n\n"
        f"禁用用户数：{changed_user_count}\n"
        f"执行前活跃用户：{active_user_count}\n"
        f"受影响绑定关系：{affected_bindings}\n"
        f"示例用户：{sample_text}\n"
        f"时间：{created_text}\n\n"
        "说明：节点会在下一次同步轮询后收敛生效。"
    )


async def run_emergency_disable_users_preview(query) -> None:
    result, error_message, _ = await controller_request(
        "POST", "/admin/emergency/disable_users?dry_run=1&reason=bot-preview"
    )
    if error_message:
        await query.edit_message_text(
            f"读取紧急停止预演失败：{localize_controller_error(error_message)}",
            reply_markup=build_back_keyboard("menu:backup"),
        )
        return
    if not isinstance(result, dict):
        await query.edit_message_text(
            "读取紧急停止预演失败：响应格式异常",
            reply_markup=build_back_keyboard("menu:backup"),
        )
        return
    await query.edit_message_text(
        format_emergency_disable_users_text(result, dry_run=True),
        reply_markup=build_backup_stop_confirm_keyboard(),
    )


async def run_emergency_disable_users_apply(query) -> None:
    result, error_message, _ = await controller_request(
        "POST", "/admin/emergency/disable_users?dry_run=0&reason=bot-confirm"
    )
    if error_message:
        await query.edit_message_text(
            f"执行紧急停止失败：{localize_controller_error(error_message)}",
            reply_markup=build_back_keyboard("menu:backup"),
        )
        return
    if not isinstance(result, dict):
        await query.edit_message_text(
            "执行紧急停止失败：响应格式异常",
            reply_markup=build_back_keyboard("menu:backup"),
        )
        return
    await query.edit_message_text(
        format_emergency_disable_users_text(result, dry_run=False),
        reply_markup=build_back_keyboard("menu:backup"),
    )


async def run_admin_migrate_export_action(query, back_menu_callback: str) -> None:
    result, error_message, _ = await controller_request("POST", "/admin/migrate/export")
    if error_message:
        await query.edit_message_text(
            f"生成迁移包失败：{localize_controller_error(error_message)}",
            reply_markup=build_back_keyboard(back_menu_callback),
        )
        return

    export_path = str(result.get("path", "")) if isinstance(result, dict) else ""
    size_bytes = int(result.get("size_bytes", 0)) if isinstance(result, dict) else 0
    cleaned_files = int(result.get("cleaned_files", 0)) if isinstance(result, dict) else 0
    keep_count = int(result.get("keep_count", 0)) if isinstance(result, dict) else 0
    created_at = int(result.get("created_at", 0)) if isinstance(result, dict) else 0
    created_text = (
        datetime.fromtimestamp(created_at).strftime("%Y-%m-%d %H:%M:%S")
        if created_at > 0
        else "-"
    )
    await query.edit_message_text(
        "迁移包已生成（保存在服务器本地）\n\n"
        f"路径：{export_path}\n"
        f"大小：{size_bytes} bytes\n"
        f"清理旧迁移包：{cleaned_files} 个（保留 {keep_count} 个）\n"
        f"时间：{created_text}\n\n"
        "可用以下命令拉取迁移包：\n"
        f"scp root@你的服务器IP:{export_path} ./",
        reply_markup=build_back_keyboard(back_menu_callback),
    )


async def run_admin_smoke_action(query, back_menu_callback: str) -> None:
    smoke_cmd = "bash {0} --require-api".format(shlex.quote(ADMIN_SMOKE_SCRIPT))
    code, stdout, stderr = await run_local_shell(smoke_cmd, timeout=300)
    raw_output = stdout if stdout.strip() else stderr
    recent = format_recent_log_output(raw_output, line_limit=80, char_limit=2600)
    if code == 0:
        await query.edit_message_text(
            "一键验收自检完成：通过\n\n"
            "输出摘要：\n"
            f"{recent}",
            reply_markup=build_back_keyboard(back_menu_callback),
        )
        return
    await query.edit_message_text(
        "一键验收自检失败\n\n"
        f"退出码：{code}\n"
        "输出摘要：\n"
        f"{recent}\n\n"
        "提示：退出码 10=代码检查失败，20=API检查失败，30=代码+API均失败。",
        reply_markup=build_back_keyboard(back_menu_callback),
    )


async def run_admin_log_archive_action(query, back_menu_callback: str) -> None:
    if not os.path.exists(ADMIN_LOG_ARCHIVE_SCRIPT):
        await query.edit_message_text(
            "未找到日志归档脚本：{0}".format(ADMIN_LOG_ARCHIVE_SCRIPT),
            reply_markup=build_back_keyboard(back_menu_callback),
        )
        return

    archive_cmd = "bash {0}".format(shlex.quote(ADMIN_LOG_ARCHIVE_SCRIPT))
    code, stdout, stderr = await run_local_shell(archive_cmd, timeout=180)
    raw_output = (stdout or "").strip() or (stderr or "").strip()
    parsed = {}
    for raw_line in raw_output.splitlines():
        line = str(raw_line or "").strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        parsed[key.strip()] = value.strip()

    if code != 0:
        await query.edit_message_text(
            "日志归档失败\n\n"
            f"退出码：{code}\n"
            f"输出摘要：\n{format_recent_log_output(raw_output, line_limit=60, char_limit=2600)}",
            reply_markup=build_back_keyboard(back_menu_callback),
        )
        return

    archive_path = str(parsed.get("ARCHIVE_PATH", "")).strip()
    size_bytes = int(parsed.get("SIZE_BYTES", 0) or 0)
    cleaned_files = int(parsed.get("CLEANED_FILES", 0) or 0)
    keep_count = int(parsed.get("KEEP_COUNT", 0) or 0)
    window_hours = int(parsed.get("WINDOW_HOURS", 24) or 24)
    created_at = int(parsed.get("CREATED_AT", 0) or 0)
    created_text = (
        datetime.fromtimestamp(created_at).strftime("%Y-%m-%d %H:%M:%S")
        if created_at > 0
        else "-"
    )
    await query.edit_message_text(
        "日志归档完成\n\n"
        f"范围：最近 {window_hours} 小时\n"
        f"路径：{archive_path or '-'}\n"
        f"大小：{size_bytes} bytes\n"
        f"清理旧归档：{cleaned_files} 个（保留 {keep_count} 个）\n"
        f"时间：{created_text}\n\n"
        "可用以下命令拉取：\n"
        f"scp root@你的服务器IP:{archive_path} ./",
        reply_markup=build_back_keyboard(back_menu_callback),
    )


async def get_admin_security_status_with_retry(
    retry_attempts: int = 1, retry_delay_seconds: float = 1.0
) -> tuple:
    attempts = retry_attempts if retry_attempts >= 1 else 1
    for index in range(attempts):
        status_result, error_message, status_code = await controller_request(
            "GET", "/admin/security/status"
        )
        if not error_message:
            return status_result, "", status_code

        is_connection_issue = str(error_message).startswith("无法连接控制器接口")
        is_last = index >= attempts - 1
        if is_last or not is_connection_issue:
            return status_result, error_message, status_code
        await asyncio.sleep(retry_delay_seconds if retry_delay_seconds > 0 else 0.5)
    return None, "读取订阅安全状态失败", 0


def parse_audit_detail_dict(raw_detail: object) -> dict:
    if isinstance(raw_detail, dict):
        return raw_detail
    if not isinstance(raw_detail, str):
        return {}
    text = raw_detail.strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except Exception:
        return {}
    if isinstance(parsed, dict):
        return parsed
    return {}


def format_sub_policy_state(policy: object) -> str:
    if not isinstance(policy, dict):
        return "-"
    sign_enabled = "签名开" if str(policy.get("SUB_LINK_REQUIRE_SIGNATURE", "0")) == "1" else "签名关"
    rate_enabled = "限流开" if str(policy.get("API_RATE_LIMIT_ENABLED", "0")) == "1" else "限流关"
    return f"{sign_enabled}/{rate_enabled}"


async def get_latest_sub_policy_change_summary_lines() -> list:
    audit_rows, error_message, _ = await controller_request("GET", "/admin/audit?limit=120")
    if error_message:
        return ["最近策略变更：读取失败（{0}）".format(localize_controller_error(error_message))]
    if not isinstance(audit_rows, list):
        return ["最近策略变更：暂无记录。"]

    latest = None
    for row in audit_rows:
        action = str(row.get("action", "")).strip()
        if action in ("bot.sub_policy.apply", "bot.sub_policy.apply_failed", "bot.sub_policy.noop"):
            latest = row
            break
    if latest is None:
        return ["最近策略变更：暂无记录。"]

    detail = parse_audit_detail_dict(latest.get("detail"))
    action = str(latest.get("action", "")).strip()
    if action == "bot.sub_policy.apply":
        result_text = "应用预设（成功）"
    elif action == "bot.sub_policy.noop":
        result_text = "无需变更（No-Op）"
    elif bool(detail.get("rollback_env_restored")) and bool(detail.get("rollback_restart_ok")):
        result_text = "应用预设失败（已回滚）"
    else:
        result_text = "应用预设失败（回滚未完成）"

    created_at = int(latest.get("created_at", 0) or 0)
    created_text = (
        datetime.fromtimestamp(created_at).strftime("%Y-%m-%d %H:%M:%S") if created_at > 0 else "-"
    )
    mode_text = str(detail.get("mode_label") or detail.get("mode") or "-")
    before_state = format_sub_policy_state(detail.get("before"))
    final_state = format_sub_policy_state(detail.get("final"))
    actor = str(latest.get("actor", "")).strip() or "-"
    lines = [
        "最近策略变更：",
        f"- 时间：{created_text}",
        f"- 结果：{result_text}",
        f"- 预设：{mode_text}",
        f"- 操作人：{actor}",
    ]
    if before_state != "-" or final_state != "-":
        lines.append(f"- 策略：{before_state} -> {final_state}")
    return lines


async def run_admin_sub_policy_panel_action(
    query,
    notice: str = "",
    retry_attempts: int = 1,
    retry_delay_seconds: float = 1.0,
) -> None:
    status_result, error_message, _ = await get_admin_security_status_with_retry(
        retry_attempts=retry_attempts,
        retry_delay_seconds=retry_delay_seconds,
    )
    if error_message:
        await query.edit_message_text(
            "读取订阅安全状态失败：{0}".format(localize_controller_error(error_message)),
            reply_markup=build_back_keyboard("menu:maintain"),
        )
        return

    if not isinstance(status_result, dict):
        await query.edit_message_text(
            "读取订阅安全状态失败：返回格式异常。",
            reply_markup=build_back_keyboard("menu:maintain"),
        )
        return

    auth_enabled = "已启用" if bool(status_result.get("auth_enabled")) else "未启用"
    sign_key_enabled = "已设置" if bool(status_result.get("sub_link_sign_enabled")) else "未设置"
    sign_required = "已强制" if bool(status_result.get("sub_link_require_signature")) else "未强制"
    rate_limit_enabled = "已启用" if bool(status_result.get("api_rate_limit_enabled")) else "未启用"
    rate_window = int(status_result.get("api_rate_limit_window_seconds", 0) or 0)
    rate_max = int(status_result.get("api_rate_limit_max_requests", 0) or 0)
    sub_ttl = int(status_result.get("sub_link_default_ttl_seconds", 0) or 0)

    lines = []
    if notice:
        lines.extend([notice, ""])
    lines.extend(
        [
            "订阅与访问安全预设",
            "",
            f"接口鉴权：{auth_enabled}",
            f"订阅签名密钥：{sign_key_enabled}",
            f"订阅签名强制：{sign_required}",
            f"轻量限流：{rate_limit_enabled}（窗口 {rate_window}s / 最大 {rate_max} 次）",
            f"默认签名TTL：{sub_ttl}s",
            "",
            "预设说明：",
            "1) 推荐：签名+限流（生产）",
            "2) 仅签名（低流量环境）",
            "3) 开放测试（仅临时排障，不建议长期）",
        ]
    )
    if sign_key_enabled == "未设置":
        lines.append("")
        lines.append("提示：当前未设置 SUB_LINK_SIGN_KEY，签名相关预设将被拒绝。")
    latest_change_lines = await get_latest_sub_policy_change_summary_lines()
    if latest_change_lines:
        lines.append("")
        lines.extend(latest_change_lines[:8])
    await query.edit_message_text(
        "\n".join(lines),
        reply_markup=build_sub_policy_keyboard(),
    )


async def run_admin_sync_node_tokens_action(query) -> None:
    result, error_message, _ = await controller_request(
        "POST",
        "/admin/auth/sync_node_tokens",
    )
    if error_message:
        await query.edit_message_text(
            "同步节点 Token 失败：{0}".format(localize_controller_error(error_message)),
            reply_markup=build_back_keyboard("menu:maintain"),
        )
        return
    if not isinstance(result, dict):
        await query.edit_message_text(
            "同步节点 Token 失败：返回格式异常。",
            reply_markup=build_back_keyboard("menu:maintain"),
        )
        return

    selected = int(result.get("selected", 0) or 0)
    created = int(result.get("created", 0) or 0)
    deduplicated = int(result.get("deduplicated", 0) or 0)
    failed = int(result.get("failed", 0) or 0)
    include_disabled = bool(result.get("include_disabled"))
    force_new = bool(result.get("force_new"))
    failures = result.get("failures", [])

    lines = [
        "节点 Token 同步完成",
        "",
        f"目标节点数：{selected}",
        f"新建任务：{created}",
        f"去重任务：{deduplicated}",
        f"失败数：{failed}",
        f"包含禁用节点：{'是' if include_disabled else '否'}",
        f"强制新建任务：{'是' if force_new else '否'}",
        "",
        "说明：已为节点下发 config_set(auth_token=主 token) 任务，节点在下一次轮询时会应用。",
    ]
    if isinstance(failures, list) and failures:
        lines.append("")
        lines.append("失败节点（最多 8 条）：")
        for item in failures[:8]:
            node_code = str(item.get("node_code", "")).strip() or "-"
            err = str(item.get("error", "")).strip() or "unknown"
            lines.append(f"- {node_code}: {err}")

    await query.edit_message_text(
        "\n".join(lines),
        reply_markup=build_back_keyboard("menu:maintain"),
    )


async def run_admin_sync_node_defaults_action(query) -> None:
    result, error_message, _ = await controller_request(
        "POST",
        "/admin/nodes/sync_agent_defaults",
    )
    if error_message:
        await query.edit_message_text(
            "同步节点默认参数失败：{0}".format(localize_controller_error(error_message)),
            reply_markup=build_back_keyboard("menu:maintain"),
        )
        return
    if not isinstance(result, dict):
        await query.edit_message_text(
            "同步节点默认参数失败：返回格式异常。",
            reply_markup=build_back_keyboard("menu:maintain"),
        )
        return

    selected = int(result.get("selected", 0) or 0)
    created = int(result.get("created", 0) or 0)
    deduplicated = int(result.get("deduplicated", 0) or 0)
    failed = int(result.get("failed", 0) or 0)
    payload_obj = result.get("payload", {})
    failures = result.get("failures", [])

    lines = [
        "节点默认参数同步完成",
        "",
        f"目标节点数：{selected}",
        f"新建任务：{created}",
        f"去重任务：{deduplicated}",
        f"失败数：{failed}",
        "",
        "下发参数：",
    ]
    if isinstance(payload_obj, dict) and payload_obj:
        controller_url = str(payload_obj.get("controller_url", "")).strip()
        poll_interval = str(payload_obj.get("poll_interval", "")).strip()
        has_token = bool(str(payload_obj.get("auth_token", "")).strip())
        lines.append(f"- controller_url：{controller_url or '未下发（管理端未配置 CONTROLLER_PUBLIC_URL）'}")
        lines.append(f"- poll_interval：{poll_interval or '-'}")
        lines.append(f"- auth_token：{'已下发主 token' if has_token else '未下发'}")
    else:
        lines.append("- （空）")
    lines.append("")
    lines.append("说明：节点会在下一次轮询时应用。")

    if isinstance(failures, list) and failures:
        lines.append("")
        lines.append("失败节点（最多 8 条）：")
        for item in failures[:8]:
            node_code = str(item.get("node_code", "")).strip() or "-"
            err = str(item.get("error", "")).strip() or "unknown"
            lines.append(f"- {node_code}: {err}")

    await query.edit_message_text(
        "\n".join(lines),
        reply_markup=build_back_keyboard("menu:maintain"),
    )


async def run_admin_sync_node_time_action(query) -> None:
    result, error_message, _ = await controller_request(
        "POST",
        "/admin/nodes/sync_time",
    )
    if error_message:
        await query.edit_message_text(
            "同步节点时间失败：{0}".format(localize_controller_error(error_message)),
            reply_markup=build_back_keyboard("menu:maintain"),
        )
        return
    if not isinstance(result, dict):
        await query.edit_message_text(
            "同步节点时间失败：返回格式异常。",
            reply_markup=build_back_keyboard("menu:maintain"),
        )
        return

    selected = int(result.get("selected", 0) or 0)
    created = int(result.get("created", 0) or 0)
    deduplicated = int(result.get("deduplicated", 0) or 0)
    failed = int(result.get("failed", 0) or 0)
    server_unix = int(result.get("server_unix", 0) or 0)
    include_disabled = bool(result.get("include_disabled"))
    force_new = bool(result.get("force_new"))
    failures = result.get("failures", [])
    server_time_text = (
        datetime.fromtimestamp(server_unix).strftime("%Y-%m-%d %H:%M:%S")
        if server_unix > 0
        else "-"
    )

    lines = [
        "节点时间同步任务下发完成",
        "",
        f"基准时间（管理服务器）：{server_time_text} ({server_unix})",
        f"目标节点数：{selected}",
        f"新建任务：{created}",
        f"去重任务：{deduplicated}",
        f"失败数：{failed}",
        f"包含禁用节点：{'是' if include_disabled else '否'}",
        f"强制新建任务：{'是' if force_new else '否'}",
        "",
        "说明：节点会执行 sync_time 任务，将系统时间校准到管理服务器时间。",
    ]
    if isinstance(failures, list) and failures:
        lines.append("")
        lines.append("失败节点（最多 8 条）：")
        for item in failures[:8]:
            node_code = str(item.get("node_code", "")).strip() or "-"
            err = str(item.get("error", "")).strip() or "unknown"
            lines.append(f"- {node_code}: {err}")

    await query.edit_message_text(
        "\n".join(lines),
        reply_markup=build_back_keyboard("menu:maintain"),
    )


async def apply_admin_sub_policy_action(query, mode: str) -> None:
    mode_key = str(mode or "").strip().lower()
    mode_map = {
        "strict": ("推荐：签名+限流", {"SUB_LINK_REQUIRE_SIGNATURE": "1", "API_RATE_LIMIT_ENABLED": "1"}),
        "signed": ("仅签名", {"SUB_LINK_REQUIRE_SIGNATURE": "1", "API_RATE_LIMIT_ENABLED": "0"}),
        "open": ("开放测试", {"SUB_LINK_REQUIRE_SIGNATURE": "0", "API_RATE_LIMIT_ENABLED": "0"}),
    }
    if mode_key not in mode_map:
        await query.edit_message_text(
            "无效预设，请重试。",
            reply_markup=build_back_keyboard("action:maintain_sub_policy"),
        )
        return

    mode_name, updates = mode_map[mode_key]
    env_map = load_env_map()
    sign_key = str(env_map.get("SUB_LINK_SIGN_KEY", "")).strip()
    before_policy = {
        "SUB_LINK_REQUIRE_SIGNATURE": str(env_map.get("SUB_LINK_REQUIRE_SIGNATURE", "")).strip()
        or "0",
        "API_RATE_LIMIT_ENABLED": str(env_map.get("API_RATE_LIMIT_ENABLED", "")).strip() or "0",
    }
    target_policy = {
        "SUB_LINK_REQUIRE_SIGNATURE": str(updates.get("SUB_LINK_REQUIRE_SIGNATURE", "0")),
        "API_RATE_LIMIT_ENABLED": str(updates.get("API_RATE_LIMIT_ENABLED", "0")),
    }
    if before_policy == target_policy:
        await write_admin_audit_event_best_effort(
            action="bot.sub_policy.noop",
            resource_type="security",
            resource_id="subscription",
            detail={
                "mode": mode_key,
                "mode_label": mode_name,
                "before": before_policy,
                "target": target_policy,
                "final": before_policy,
                "restart_ok": False,
                "rollback_env_restored": False,
            },
            retry_attempts=2,
            retry_delay_seconds=0.8,
        )
        await run_admin_sub_policy_panel_action(
            query,
            notice=f"无需变更：当前已是 {mode_name}（No-Op）",
            retry_attempts=1,
            retry_delay_seconds=0.5,
        )
        return

    if mode_key in ("strict", "signed") and not sign_key:
        await query.edit_message_text(
            "当前未设置 SUB_LINK_SIGN_KEY，无法启用签名预设。\n"
            "请先在“配置向导”中设置 SUB_LINK_SIGN_KEY。",
            reply_markup=build_back_keyboard("action:maintain_sub_policy"),
        )
        return

    backup_path, backup_error = backup_env_file(prefix="subpolicy")
    if backup_error:
        await query.edit_message_text(
            "切换预设前备份失败：{0}".format(backup_error),
            reply_markup=build_back_keyboard("action:maintain_sub_policy"),
        )
        return

    ok, err_text = write_env_updates(updates)
    if not ok:
        await query.edit_message_text(
            "写入配置失败：{0}".format(err_text),
            reply_markup=build_back_keyboard("action:maintain_sub_policy"),
        )
        return

    restart_code, restart_stdout, restart_stderr = await run_local_shell(
        "systemctl restart sb-controller",
        timeout=60,
    )
    if restart_code != 0:
        raw = (restart_stdout or "").strip() or (restart_stderr or "").strip()
        rollback_ok, rollback_error = restore_env_file(backup_path)
        rollback_restart_code = -1
        rollback_restart_stdout = ""
        rollback_restart_stderr = ""
        if rollback_ok:
            rollback_restart_code, rollback_restart_stdout, rollback_restart_stderr = await run_local_shell(
                "systemctl restart sb-controller",
                timeout=60,
            )
        rollback_raw = (rollback_restart_stdout or "").strip() or (rollback_restart_stderr or "").strip()
        if rollback_ok and rollback_restart_code == 0:
            await write_admin_audit_event_best_effort(
                action="bot.sub_policy.apply_failed",
                resource_type="security",
                resource_id="subscription",
                detail={
                    "mode": mode_key,
                    "mode_label": mode_name,
                    "backup_path": backup_path,
                    "before": before_policy,
                    "target": target_policy,
                    "final": before_policy,
                    "rollback_env_restored": True,
                    "rollback_restart_ok": True,
                    "restart_error": format_recent_log_output(
                        raw, line_limit=12, char_limit=500
                    ),
                },
                retry_attempts=6,
                retry_delay_seconds=1.0,
            )
            await query.edit_message_text(
                "预设切换失败，已自动回滚并恢复 controller。\n\n"
                f"预设：{mode_name}\n"
                f".env 备份：{backup_path}\n"
                "失败输出：\n"
                f"{format_recent_log_output(raw, line_limit=40, char_limit=1700)}",
                reply_markup=build_back_keyboard("action:maintain_sub_policy"),
            )
            return

        rollback_detail = rollback_error
        if rollback_ok and rollback_restart_code != 0:
            rollback_detail = "已恢复 .env，但 controller 回滚重启失败：{0}".format(
                format_recent_log_output(rollback_raw, line_limit=30, char_limit=1200)
            )
        await write_admin_audit_event_best_effort(
            action="bot.sub_policy.apply_failed",
            resource_type="security",
            resource_id="subscription",
            detail={
                "mode": mode_key,
                "mode_label": mode_name,
                "backup_path": backup_path,
                "before": before_policy,
                "target": target_policy,
                "final": target_policy if not rollback_ok else before_policy,
                "rollback_env_restored": bool(rollback_ok),
                "rollback_restart_ok": bool(rollback_ok and rollback_restart_code == 0),
                "restart_error": format_recent_log_output(raw, line_limit=12, char_limit=500),
                "rollback_error": str(rollback_detail or "")[:500],
            },
            retry_attempts=2,
            retry_delay_seconds=0.8,
        )
        await query.edit_message_text(
            "预设切换失败，且自动回滚未完全成功。\n\n"
            f"预设：{mode_name}\n"
            f".env 备份：{backup_path}\n"
            "失败输出：\n"
            f"{format_recent_log_output(raw, line_limit=35, char_limit=1400)}\n\n"
            f"回滚状态：{rollback_detail or '未知错误'}",
            reply_markup=build_back_keyboard("action:maintain_sub_policy"),
        )
        return

    await write_admin_audit_event_best_effort(
        action="bot.sub_policy.apply",
        resource_type="security",
        resource_id="subscription",
        detail={
            "mode": mode_key,
            "mode_label": mode_name,
            "backup_path": backup_path,
            "before": before_policy,
            "target": target_policy,
            "final": target_policy,
            "restart_ok": True,
            "rollback_env_restored": False,
        },
        retry_attempts=6,
        retry_delay_seconds=1.0,
    )
    await run_admin_sub_policy_panel_action(
        query,
        notice=f"已应用预设：{mode_name}（controller 已重启，备份：{backup_path}）",
        retry_attempts=10,
        retry_delay_seconds=1.0,
    )
    return


async def run_admin_node_access_status_action(query, back_menu_callback: str) -> None:
    result, error_message, _ = await controller_request("GET", "/admin/node_access/status")
    if error_message:
        await query.edit_message_text(
            f"获取访问安全状态失败：{localize_controller_error(error_message)}",
            reply_markup=build_back_keyboard(back_menu_callback),
        )
        return

    if not isinstance(result, dict):
        await query.edit_message_text(
            "访问安全状态返回异常。",
            reply_markup=build_back_keyboard(back_menu_callback),
        )
        return

    total_nodes = int(result.get("total_nodes", 0) or 0)
    enabled_nodes = int(result.get("enabled_nodes", 0) or 0)
    locked_nodes = int(result.get("locked_nodes", 0) or 0)
    unlocked_nodes = int(result.get("unlocked_nodes", 0) or 0)
    locked_enabled_nodes = int(result.get("locked_enabled_nodes", 0) or 0)
    unlocked_enabled_nodes = int(result.get("unlocked_enabled_nodes", 0) or 0)
    unlocked_disabled_nodes = int(result.get("unlocked_disabled_nodes", 0) or 0)
    locked_items = result.get("locked_items", [])
    unlocked_items = result.get("unlocked_items", [])
    unlocked_enabled_items = result.get("unlocked_enabled_items", [])
    whitelist_items = result.get("controller_port_whitelist", [])
    whitelist_invalid_items = result.get("whitelist_invalid_items", [])
    whitelist_missing_nodes = result.get("whitelist_missing_nodes", [])
    whitelist_missing_count = int(result.get("whitelist_missing_count", 0) or 0)

    lines = [
        "节点访问安全状态",
        f"总节点数：{total_nodes}",
        f"已启用节点：{enabled_nodes}",
        f"启用且已锁定来源IP：{locked_enabled_nodes}",
        f"启用但未锁定来源IP：{unlocked_enabled_nodes}",
        f"未锁定来源IP（含禁用节点）：{unlocked_nodes}（其中禁用 {unlocked_disabled_nodes}）",
        f"已锁定来源IP（全部）：{locked_nodes}",
        f"白名单缺口：{whitelist_missing_count}",
    ]
    if isinstance(whitelist_items, list) and whitelist_items:
        lines.append("controller端口白名单：{0}".format(", ".join(str(x) for x in whitelist_items[:20])))
    else:
        lines.append("controller端口白名单：（空）")
    if isinstance(whitelist_invalid_items, list) and whitelist_invalid_items:
        lines.append("白名单格式异常项：{0}".format(", ".join(str(x) for x in whitelist_invalid_items[:10])))
    lines.extend(
        [
        "",
        "已锁定节点：",
        ]
    )
    if isinstance(locked_items, list) and locked_items:
        for item in locked_items[:20]:
            node_code = str(item.get("node_code", ""))
            agent_ip = str(item.get("agent_ip", ""))
            lines.append(f"- {node_code} -> {agent_ip}")
    else:
        lines.append("- （暂无）")

    lines.append("")
    lines.append("白名单未覆盖节点（已启用且有 agent_ip）：")
    if isinstance(whitelist_missing_nodes, list) and whitelist_missing_nodes:
        for item in whitelist_missing_nodes[:20]:
            node_code = str(item.get("node_code", ""))
            agent_ip = str(item.get("agent_ip", ""))
            lines.append(f"- {node_code} -> {agent_ip}")
    else:
        lines.append("- （暂无）")

    lines.append("")
    lines.append("未锁定节点（仅启用）：")
    if isinstance(unlocked_enabled_items, list) and unlocked_enabled_items:
        for item in unlocked_enabled_items[:20]:
            node_code = str(item.get("node_code", ""))
            lines.append(f"- {node_code}")
    else:
        lines.append("- （暂无）")

    if isinstance(unlocked_items, list) and unlocked_items and unlocked_disabled_nodes > 0:
        lines.append("")
        lines.append("未锁定节点（禁用，仅供排查）：")
        for item in unlocked_items[:20]:
            if int(item.get("enabled", 0) or 0) != 0:
                continue
            node_code = str(item.get("node_code", ""))
            lines.append(f"- {node_code}")

    lines.append("")
    security_result, security_error, _ = await controller_request("GET", "/admin/security/status")
    if security_error:
        lines.append("全局安全状态：读取失败（{0}）".format(localize_controller_error(security_error)))
    elif isinstance(security_result, dict):
        auth_enabled = "已启用" if bool(security_result.get("auth_enabled")) else "未启用"
        sign_enabled = "已启用" if bool(security_result.get("sub_link_sign_enabled")) else "未启用"
        sign_required = (
            "已强制" if bool(security_result.get("sub_link_require_signature")) else "兼容模式"
        )
        rate_limit_enabled = (
            "已启用" if bool(security_result.get("api_rate_limit_enabled")) else "未启用"
        )
        lines.append("全局安全状态：")
        lines.append(f"- 接口鉴权：{auth_enabled}")
        lines.append(
            "- controller白名单数量：{0}".format(
                int(security_result.get("controller_port_whitelist_count", 0) or 0)
            )
        )
        protected_items = security_result.get("security_block_protected_ips", [])
        protected_count = int(security_result.get("security_block_protected_ips_count", 0) or 0)
        protected_effective_count = int(
            security_result.get("security_block_protected_ips_effective_count", 0) or 0
        )
        protected_invalid_items = security_result.get("security_block_protected_ips_invalid", [])
        protected_invalid_count = int(
            security_result.get("security_block_protected_ips_invalid_count", 0) or 0
        )
        lines.append(f"- 封禁保护白名单：总 {protected_count} / 有效 {protected_effective_count}")
        if isinstance(protected_items, list) and protected_items:
            lines.append("- 封禁保护白名单样例：{0}".format(", ".join(str(x) for x in protected_items[:3])))
        if protected_invalid_count > 0:
            lines.append(f"- 封禁保护白名单无效项：{protected_invalid_count}")
            if isinstance(protected_invalid_items, list) and protected_invalid_items:
                lines.append(
                    "- 无效项样例：{0}".format(", ".join(str(x) for x in protected_invalid_items[:3]))
                )
        lines.append(f"- 订阅签名：{sign_enabled}（{sign_required}）")
        lines.append(f"- 轻量限流：{rate_limit_enabled}")
        warnings = security_result.get("warnings", [])
        if isinstance(warnings, list) and warnings:
            lines.append("- 安全提示：")
            for warning in warnings[:6]:
                lines.append(f"  * {str(warning)}")
    lines.append("")
    lines.append("建议：新增节点后立即设置“节点来源IP白名单”。")
    lines.append(f"防火墙参考（controller 端口 {CONTROLLER_PORT_HINT}）：")
    if isinstance(locked_items, list) and locked_items:
        for item in locked_items[:20]:
            agent_ip = str(item.get("agent_ip", "")).strip()
            if agent_ip:
                lines.append(
                    f"ufw allow from {agent_ip} to any port {CONTROLLER_PORT_HINT} proto tcp"
                )
    lines.append(f"ufw deny {CONTROLLER_PORT_HINT}/tcp")

    text = "\n".join(lines)
    if len(text) > 3800:
        text = text[:3800] + "\n...（输出较长，已截断）"
    await query.edit_message_text(
        text,
        reply_markup=build_back_keyboard(back_menu_callback),
    )


async def run_admin_security_events_action(query, include_local: bool) -> None:
    events_path = "/admin/security/events?window_seconds=3600&top=5"
    if include_local:
        events_path += "&include_local=1"
    events_result, events_error, _ = await controller_request("GET", events_path)
    status_result, status_error, _ = await controller_request("GET", "/admin/security/status")
    access_result, access_error, _ = await controller_request("GET", "/admin/node_access/status")
    if events_error:
        await query.edit_message_text(
            "读取安全事件失败：{0}".format(localize_controller_error(events_error)),
            reply_markup=build_back_keyboard("menu:maintain"),
        )
        return

    unauthorized_count = 0
    include_local_effective = include_local
    top_lines = []
    top_source_ips = []
    if isinstance(events_result, dict):
        try:
            unauthorized_count = int(events_result.get("unauthorized", 0) or 0)
        except (TypeError, ValueError):
            unauthorized_count = 0
        include_local_effective = bool(events_result.get("include_local"))
        top_rows = events_result.get("top_unauthorized_ips", [])
        if isinstance(top_rows, list):
            for index, item in enumerate(top_rows[:5], start=1):
                if not isinstance(item, dict):
                    continue
                source_ip = str(item.get("source_ip", "")).strip() or "-"
                try:
                    hit_count = int(item.get("count", 0) or 0)
                except (TypeError, ValueError):
                    hit_count = 0
                if source_ip and source_ip != "-" and decode_ip_token(encode_ip_token(source_ip)):
                    top_source_ips.append(source_ip)
                top_lines.append(f"{index}. {source_ip} -> {hit_count} 次")

    token_count_text = "-"
    auth_enabled_text = "-"
    sample_text = "-"
    auto_block_text = "-"
    protected_ip_text = "-"
    protected_effective_text = "-"
    protected_invalid_text = "-"
    blocked_ip_count_text = "-"
    token_rotation_text = "-"
    if status_error:
        token_count_text = "读取失败"
        auth_enabled_text = "读取失败"
        sample_text = "读取失败"
        auto_block_text = "读取失败"
        protected_ip_text = "读取失败"
        protected_effective_text = "读取失败"
        protected_invalid_text = "读取失败"
        blocked_ip_count_text = "读取失败"
        token_rotation_text = "读取失败"
    elif isinstance(status_result, dict):
        auth_enabled = bool(status_result.get("auth_enabled"))
        auth_enabled_text = "已启用" if auth_enabled else "未启用"
        token_count_value = int(status_result.get("auth_token_count", 0) or 0)
        token_count_text = str(token_count_value)
        token_rotation_text = "是" if token_count_value > 1 else "否"
        blocked_ip_count_text = str(int(status_result.get("blocked_ip_count", 0) or 0))
        sample_seconds = int(status_result.get("unauthorized_audit_sample_seconds", 0) or 0)
        sample_enabled = bool(status_result.get("unauthorized_audit_sampling_enabled"))
        if sample_enabled and sample_seconds > 0:
            sample_text = f"已启用（{sample_seconds} 秒窗口）"
        elif sample_seconds <= 0:
            sample_text = "未启用（0 秒）"
        else:
            sample_text = "未启用"
        auto_block_enabled = bool(status_result.get("security_auto_block_enabled"))
        auto_block_window = int(status_result.get("security_auto_block_window_seconds", 0) or 0)
        auto_block_threshold = int(status_result.get("security_auto_block_threshold", 0) or 0)
        auto_block_duration = int(status_result.get("security_auto_block_duration_seconds", 0) or 0)
        auto_block_text = (
            "已启用（窗口 {0}s / 阈值 {1} / 封禁 {2}s）".format(
                auto_block_window, auto_block_threshold, auto_block_duration
            )
            if auto_block_enabled
            else "未启用"
        )
        protected_items = status_result.get("security_block_protected_ips", [])
        protected_count = int(status_result.get("security_block_protected_ips_count", 0) or 0)
        protected_effective_count = int(
            status_result.get("security_block_protected_ips_effective_count", 0) or 0
        )
        if isinstance(protected_items, list) and protected_items:
            protected_ip_text = "{0}（{1}）".format(
                protected_count,
                ", ".join(str(x) for x in protected_items[:3]),
            )
        else:
            protected_ip_text = str(protected_count)
        protected_effective_text = str(protected_effective_count)
        protected_invalid_count = int(
            status_result.get("security_block_protected_ips_invalid_count", 0) or 0
        )
        protected_invalid_items = status_result.get("security_block_protected_ips_invalid", [])
        if protected_invalid_count > 0 and isinstance(protected_invalid_items, list):
            protected_invalid_text = "{0}（{1}）".format(
                protected_invalid_count,
                ", ".join(str(x) for x in protected_invalid_items[:3]),
            )
        else:
            protected_invalid_text = str(protected_invalid_count)

    unlocked_enabled_nodes = 0
    whitelist_missing_count = 0
    if access_error:
        access_text = "访问收敛状态读取失败"
    elif isinstance(access_result, dict):
        unlocked_enabled_nodes = int(access_result.get("unlocked_enabled_nodes", 0) or 0)
        whitelist_missing_count = int(access_result.get("whitelist_missing_count", 0) or 0)
        access_text = "启用未锁定 {0}，白名单缺口 {1}".format(
            unlocked_enabled_nodes,
            whitelist_missing_count,
        )
    else:
        access_text = "访问收敛状态读取异常"

    advice = []
    if unauthorized_count <= 0:
        advice.append("当前 1h 无未授权来源，维持现有策略。")
    else:
        if unlocked_enabled_nodes > 0 or whitelist_missing_count > 0:
            advice.append("优先收敛节点来源IP：为启用节点设置 agent_ip 并更新 8080 白名单。")
        if unauthorized_count >= 50:
            advice.append("1h 未授权较高，建议在云防火墙/UFW 增加来源限制。")
        if auth_enabled_text == "未启用":
            advice.append("立即启用 AUTH_TOKEN 鉴权。")
    if not advice:
        advice.append("继续观察 1h 趋势。")

    lines = [
        "安全事件（近 1 小时）",
        f"未授权请求数：{unauthorized_count}",
        "统计模式：{0}".format("包含本机来源" if include_local_effective else "过滤本机测试来源"),
        f"鉴权状态：{auth_enabled_text}",
        f"AUTH_TOKEN 数量：{token_count_text}",
        f"多token过渡：{token_rotation_text}",
        f"未授权审计采样：{sample_text}",
        f"自动封禁策略：{auto_block_text}",
        f"封禁保护白名单：{protected_ip_text}",
        f"白名单有效项：{protected_effective_text}",
        f"白名单无效项：{protected_invalid_text}",
        f"当前封禁IP：{blocked_ip_count_text}",
        f"访问收敛：{access_text}",
        "",
        "来源 TOP5：",
    ]
    if top_lines:
        lines.extend(top_lines)
    else:
        lines.append("- （当前窗口无未授权来源）")
    lines.append("")
    lines.append("建议：")
    for item in advice[:3]:
        lines.append(f"- {item}")

    await query.edit_message_text(
        "\n".join(lines),
        reply_markup=build_security_events_keyboard(
            include_local_effective, top_source_ips
        ),
    )


async def run_admin_security_block_list_action(query, include_local: bool, page: int = 1) -> None:
    result, error_message, _ = await controller_request("GET", "/admin/security/blocked_ips")
    if error_message:
        await query.edit_message_text(
            "读取封禁列表失败：{0}".format(localize_controller_error(error_message)),
            reply_markup=build_back_keyboard(security_events_mode_callback(include_local)),
        )
        return
    if not isinstance(result, dict):
        await query.edit_message_text(
            "封禁列表返回异常。",
            reply_markup=build_back_keyboard(security_events_mode_callback(include_local)),
        )
        return

    items = result.get("items", [])
    if not isinstance(items, list):
        items = []
    cleanup_released = int(result.get("cleanup_released", 0) or 0)
    cleanup_failed = result.get("cleanup_failed", [])
    if not isinstance(cleanup_failed, list):
        cleanup_failed = []

    if page < 1:
        page = 1

    lines = [
        "已封禁来源IP列表",
        f"当前数量：{int(result.get('count', 0) or 0)}",
    ]
    if cleanup_released > 0:
        lines.append(f"已自动清理过期封禁：{cleanup_released}")
    if cleanup_failed:
        lines.append(
            "过期清理失败：{0}".format(", ".join(str(x) for x in cleanup_failed[:5]))
        )
    lines.append("")
    lines.append("封禁项：")
    blocked_ips = []
    if items:
        page_size = 8
        total_pages = (len(items) + page_size - 1) // page_size
        if total_pages <= 0:
            total_pages = 1
        if page > total_pages:
            page = total_pages
        start = (page - 1) * page_size
        end = start + page_size
        page_items = items[start:end]
        for item in page_items:
            source_ip = str(item.get("source_ip", "")).strip()
            if not source_ip:
                continue
            blocked_ips.append(source_ip)
            expire_at = int(item.get("expire_at", 0) or 0)
            if expire_at > 0:
                expire_text = datetime.fromtimestamp(expire_at).strftime("%Y-%m-%d %H:%M:%S")
            else:
                expire_text = "永久"
            lines.append(f"- {source_ip} | 到期：{expire_text}")
        lines.append("")
        lines.append(f"分页：第 {page}/{total_pages} 页（每页 {page_size} 条）")
    else:
        lines.append("- （暂无）")
        total_pages = 1

    await query.edit_message_text(
        "\n".join(lines),
        reply_markup=build_security_blocklist_keyboard(
            include_local, blocked_ips, page=page, total_pages=total_pages
        ),
    )


async def run_admin_security_block_ip_action(
    query, include_local: bool, source_ip: str, duration_seconds: int
) -> None:
    payload = {
        "source_ip": source_ip,
        "duration_seconds": int(duration_seconds),
        "reason": "bot-security-events",
    }
    result, error_message, status_code = await controller_request(
        "POST", "/admin/security/block_ip", payload=payload
    )
    if error_message:
        localized = localize_controller_error(error_message)
        if status_code == 400:
            localized = f"参数或保护策略拦截：{localized}"
        await query.edit_message_text(
            f"封禁失败：{localized}",
            reply_markup=build_back_keyboard(security_events_mode_callback(include_local)),
        )
        return

    if not isinstance(result, dict):
        await query.edit_message_text(
            "封禁返回异常。",
            reply_markup=build_back_keyboard(security_events_mode_callback(include_local)),
        )
        return

    if int(duration_seconds) == 0:
        duration_text = "永久"
    elif int(duration_seconds) % 86400 == 0:
        duration_text = f"{int(duration_seconds) // 86400} 天"
    elif int(duration_seconds) % 3600 == 0:
        duration_text = f"{int(duration_seconds) // 3600} 小时"
    else:
        duration_text = f"{int(duration_seconds)} 秒"

    expire_at = int(result.get("expire_at", 0) or 0)
    expire_text = (
        datetime.fromtimestamp(expire_at).strftime("%Y-%m-%d %H:%M:%S")
        if expire_at > 0
        else "永久"
    )
    already_blocked = bool(result.get("already_blocked"))
    title_text = "封禁已存在（已刷新期限）" if already_blocked else "封禁成功"
    await query.edit_message_text(
        f"{title_text}\n\n"
        f"IP：{source_ip}\n"
        f"作用端口：{int(result.get('controller_port', CONTROLLER_PORT_HINT) or CONTROLLER_PORT_HINT)}\n"
        f"封禁时长：{duration_text}\n"
        f"到期时间：{expire_text}",
        reply_markup=InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("查看封禁列表", callback_data=f"sb:bl:{1 if include_local else 0}:1")],
                [InlineKeyboardButton("返回安全事件", callback_data=security_events_mode_callback(include_local))],
            ]
        ),
    )


async def run_admin_security_unblock_ip_action(query, include_local: bool, source_ip: str) -> None:
    result, error_message, _ = await controller_request(
        "POST",
        "/admin/security/unblock_ip",
        payload={"source_ip": source_ip, "reason": "bot-security-events"},
    )
    if error_message:
        await query.edit_message_text(
            f"解封失败：{localize_controller_error(error_message)}",
            reply_markup=build_back_keyboard(f"sb:bl:{1 if include_local else 0}:1"),
        )
        return
    removed_rules = 0
    if isinstance(result, dict):
        removed_rules = int(result.get("removed_rules", 0) or 0)
    await query.edit_message_text(
        "解封成功\n\n"
        f"IP：{source_ip}\n"
        f"删除规则数：{removed_rules}",
        reply_markup=InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("返回封禁列表", callback_data=f"sb:bl:{1 if include_local else 0}:1")],
                [InlineKeyboardButton("返回安全事件", callback_data=security_events_mode_callback(include_local))],
            ]
        ),
    )


async def run_admin_security_maintenance_cleanup_action(query, include_local: bool) -> None:
    result, error_message, _ = await controller_request("POST", "/admin/security/maintenance_cleanup")
    if error_message:
        await query.edit_message_text(
            f"手动清理失败：{localize_controller_error(error_message)}",
            reply_markup=build_back_keyboard(security_events_mode_callback(include_local)),
        )
        return
    if not isinstance(result, dict):
        await query.edit_message_text(
            "手动清理返回异常。",
            reply_markup=build_back_keyboard(security_events_mode_callback(include_local)),
        )
        return

    cleaned_blocks = int(result.get("cleaned_expired_blocks", 0) or 0)
    cleaned_audit_logs = int(result.get("cleaned_audit_logs", 0) or 0)
    active_blocked_ips = int(result.get("active_blocked_ips", 0) or 0)
    retention_days = int(result.get("audit_retention_days", 0) or 0)
    batch_size = int(result.get("audit_cleanup_batch_size", 0) or 0)
    failed_items = result.get("cleanup_failed_blocks", [])
    if not isinstance(failed_items, list):
        failed_items = []
    created_at = int(result.get("created_at", 0) or 0)
    created_text = (
        datetime.fromtimestamp(created_at).strftime("%Y-%m-%d %H:%M:%S")
        if created_at > 0
        else "-"
    )

    lines = [
        "手动安全清理完成",
        "",
        f"过期封禁释放：{cleaned_blocks}",
        f"当前封禁数量：{active_blocked_ips}",
        f"审计日志清理：{cleaned_audit_logs}",
        f"审计保留策略：{retention_days} 天（单批 {batch_size} 条）",
        f"执行时间：{created_text}",
    ]
    if failed_items:
        lines.append("清理失败IP：{0}".format(", ".join(str(x) for x in failed_items[:5])))

    await query.edit_message_text(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("返回安全事件", callback_data=security_events_mode_callback(include_local))],
                [InlineKeyboardButton("查看封禁列表", callback_data=f"sb:bl:{1 if include_local else 0}:1")],
                [InlineKeyboardButton("返回维护菜单", callback_data="menu:maintain")],
            ]
        ),
    )


async def run_admin_security_auto_block_action(query, include_local: bool) -> None:
    result, error_message, _ = await controller_request("POST", "/admin/security/auto_block/run")
    if error_message:
        await query.edit_message_text(
            f"自动封禁检查失败：{localize_controller_error(error_message)}",
            reply_markup=build_back_keyboard(security_events_mode_callback(include_local)),
        )
        return
    if not isinstance(result, dict):
        await query.edit_message_text(
            "自动封禁检查返回异常。",
            reply_markup=build_back_keyboard(security_events_mode_callback(include_local)),
        )
        return

    enabled = bool(result.get("enabled"))
    blocked_count = int(result.get("blocked_count", 0) or 0)
    window_seconds = int(result.get("window_seconds", 0) or 0)
    threshold = int(result.get("threshold", 0) or 0)
    duration_seconds = int(result.get("duration_seconds", 0) or 0)
    max_per_interval = int(result.get("max_per_interval", 0) or 0)
    blocked_items = result.get("blocked_items", [])
    failed_items = result.get("failed_items", [])
    skipped_items = result.get("skipped_items", [])
    if not isinstance(blocked_items, list):
        blocked_items = []
    if not isinstance(failed_items, list):
        failed_items = []
    if not isinstance(skipped_items, list):
        skipped_items = []

    if duration_seconds == 0:
        duration_text = "永久"
    elif duration_seconds % 86400 == 0:
        duration_text = "{0} 天".format(duration_seconds // 86400)
    elif duration_seconds % 3600 == 0:
        duration_text = "{0} 小时".format(duration_seconds // 3600)
    else:
        duration_text = "{0} 秒".format(duration_seconds)

    lines = [
        "自动封禁检查完成",
        "",
        f"策略开关：{'已启用' if enabled else '未启用'}",
        f"策略参数：窗口 {window_seconds}s / 阈值 {threshold} / 封禁 {duration_text}",
        f"每轮最多封禁：{max_per_interval}",
        f"本次新增封禁：{blocked_count}",
    ]
    if blocked_items:
        lines.append("新增封禁IP：{0}".format(", ".join(str(x) for x in blocked_items[:8])))
    if skipped_items:
        skip_reason_counter = {}
        for raw_item in skipped_items:
            raw_text = str(raw_item)
            reason_key = "other"
            if ":" in raw_text:
                reason_key = raw_text.rsplit(":", 1)[-1].strip().lower() or "other"
            skip_reason_counter[reason_key] = skip_reason_counter.get(reason_key, 0) + 1

        reason_labels = {
            "protected": "受保护IP",
            "already_blocked": "已在封禁列表",
            "non_global": "非公网IP",
            "invalid": "IP格式无效",
            "other": "其他原因",
        }
        reason_order = ["protected", "already_blocked", "non_global", "invalid", "other"]
        reason_parts = []
        for reason_key in reason_order:
            count = skip_reason_counter.get(reason_key, 0)
            if count > 0:
                reason_parts.append(
                    "{0} {1}".format(reason_labels.get(reason_key, reason_key), count)
                )
        for reason_key, count in skip_reason_counter.items():
            if reason_key not in reason_order and count > 0:
                reason_parts.append("{0} {1}".format(reason_labels.get(reason_key, reason_key), count))
        if reason_parts:
            lines.append("跳过统计：{0}".format("，".join(reason_parts)))
        lines.append("跳过样例：{0}".format(", ".join(str(x) for x in skipped_items[:8])))
    if failed_items:
        lines.append("封禁失败IP：{0}".format(", ".join(str(x) for x in failed_items[:5])))
    if not enabled:
        lines.append("")
        lines.append("提示：当前 SECURITY_AUTO_BLOCK_ENABLED=0，仅执行状态检查未封禁。")

    await query.edit_message_text(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("返回安全事件", callback_data=security_events_mode_callback(include_local))],
                [InlineKeyboardButton("查看封禁列表", callback_data=f"sb:bl:{1 if include_local else 0}:1")],
                [InlineKeyboardButton("返回维护菜单", callback_data="menu:maintain")],
            ]
        ),
    )


async def start_node_ops_config_wizard(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    if not await ensure_admin_callback(update, ROLE_SUPER):
        return ConversationHandler.END
    await query.answer()

    parts = (query.data or "").split(":", maxsplit=2)
    if len(parts) != 3:
        await query.edit_message_text("请求无效，请重试。", reply_markup=build_submenu("nodes"))
        return ConversationHandler.END
    node_code = parts[2]
    context.user_data[NODE_OPS_CONFIG_KEY] = {"node_code": node_code}
    await query.edit_message_text(
        "节点参数远程修改\n\n"
        f"目标节点：{node_code}\n"
        "请输入要修改的参数（每行一个 KEY=VALUE）：\n\n"
        "可修改键：poll_interval, tuic_domain, tuic_listen_port, acme_email,\n"
        "controller_url, auth_token, node_code\n\n"
        "示例：\n"
        "poll_interval=10\n"
        "tuic_domain=jp1.cwzs.de\n"
        "tuic_listen_port=8443\n"
        "acme_email=-   （使用 - 清空可选字段）\n\n"
        "发送 /cancel 取消。",
    )
    return NODE_OPS_CONFIG_INPUT


async def node_ops_config_input(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    if not update.message or not update.message.text:
        return NODE_OPS_CONFIG_INPUT

    pending = context.user_data.get(NODE_OPS_CONFIG_KEY, {})
    node_code = str(pending.get("node_code", "")).strip()
    if not node_code:
        context.user_data.pop(NODE_OPS_CONFIG_KEY, None)
        await reply_text_with_auto_clear(
            update.message,
            context,
            "配置状态已失效，请重新进入节点远程运维。",
            reply_markup=build_submenu("nodes"),
        )
        return ConversationHandler.END

    ok, updates, err_text = parse_node_ops_config_updates(update.message.text.strip())
    if not ok:
        await update.message.reply_text(
            f"参数校验失败：{err_text}\n请重新输入，或发送 /cancel 取消。"
        )
        return NODE_OPS_CONFIG_INPUT

    result, error_message, status_code = await create_node_task(
        node_code=node_code,
        task_type="config_set",
        payload=updates,
        max_attempts=2,
    )
    context.user_data.pop(NODE_OPS_CONFIG_KEY, None)
    if error_message:
        localized = localize_controller_error(error_message)
        if status_code == 404 and localized == "节点不存在":
            await reply_text_with_auto_clear(
                update.message,
                context,
                f"节点不存在：{node_code}",
                reply_markup=build_submenu("nodes"),
            )
            return ConversationHandler.END
        await reply_text_with_auto_clear(
            update.message,
            context,
            f"任务下发失败：{localized}",
            reply_markup=build_back_keyboard(f"nodeops:panel:{node_code}"),
        )
        return ConversationHandler.END

    task_id = int(result.get("id", 0) or 0) if isinstance(result, dict) else 0
    await reply_text_with_auto_clear(
        update.message,
        context,
        "已下发节点配置更新任务。\n"
        f"节点：{node_code}\n"
        f"任务ID：{task_id}\n\n"
        "说明：agent 拉取任务后会写入 /etc/sb-agent/config.json 并在下次轮询生效。",
        reply_markup=build_node_ops_task_done_keyboard(node_code),
    )
    return ConversationHandler.END


async def cancel_node_ops_config_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    node_code = str(context.user_data.get(NODE_OPS_CONFIG_KEY, {}).get("node_code", "")).strip()
    context.user_data.pop(NODE_OPS_CONFIG_KEY, None)
    if update.message:
        if node_code:
            await reply_text_with_auto_clear(
                update.message,
                context,
                "已取消。",
                reply_markup=build_back_keyboard(f"nodeops:panel:{node_code}"),
            )
        else:
            await reply_text_with_auto_clear(
                update.message,
                context,
                "已取消。",
                reply_markup=build_submenu("nodes"),
            )
    return ConversationHandler.END


def mask_sensitive_env_value(key: str, value: str) -> str:
    if key == "BOT_TOKEN":
        if not value:
            return "(未设置)"
        if len(value) <= 10:
            return "***"
        return value[:6] + "***" + value[-4:]
    if key == "AUTH_TOKEN":
        if not value:
            return "(空=关闭鉴权)"
        if len(value) <= 8:
            return "***"
        return value[:4] + "***" + value[-2:]
    return value or "(空)"


async def start_maintain_config_wizard(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    if not await ensure_admin_callback(update, ROLE_SUPER):
        return ConversationHandler.END
    await query.answer()

    env_map = load_env_map()
    lines = ["远程配置向导", "", "当前关键配置："]
    show_keys = [
        "CONTROLLER_PORT",
        "CONTROLLER_URL",
        "CONTROLLER_PUBLIC_URL",
        "PANEL_BASE_URL",
        "AUTH_TOKEN",
        "BOT_TOKEN",
        "ADMIN_CHAT_IDS",
        "ENABLE_HTTPS",
        "HTTPS_DOMAIN",
        "MIGRATE_DIR",
        "LOG_ARCHIVE_WINDOW_HOURS",
        "LOG_ARCHIVE_RETENTION_COUNT",
        "LOG_ARCHIVE_DIR",
        "BOT_NODE_TIME_SYNC_INTERVAL",
        "SUB_LINK_REQUIRE_SIGNATURE",
        "API_RATE_LIMIT_ENABLED",
    ]
    for key in show_keys:
        lines.append("{0}={1}".format(key, mask_sensitive_env_value(key, env_map.get(key, ""))))
    lines.append("")
    lines.append("请输入要修改的项（每行一个）：")
    lines.append("KEY=VALUE")
    lines.append("示例：")
    lines.append("CONTROLLER_PORT=8080")
    lines.append("PANEL_BASE_URL=panel.example.com")
    lines.append("ENABLE_HTTPS=1")
    lines.append("")
    lines.append("发送 /cancel 取消。")

    await query.edit_message_text("\n".join(lines))
    return MAINTAIN_CONFIG_INPUT


async def maintain_config_input(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    if not update.message or not update.message.text:
        return MAINTAIN_CONFIG_INPUT

    raw_text = update.message.text.strip()
    if not raw_text:
        await update.message.reply_text("输入为空，请按 KEY=VALUE 格式重新发送。")
        return MAINTAIN_CONFIG_INPUT

    updates = {}
    for line in raw_text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if "=" not in stripped:
            await update.message.reply_text("格式错误：{0}\n请按 KEY=VALUE 输入。".format(stripped))
            return MAINTAIN_CONFIG_INPUT
        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip()
        if key not in MAINTAIN_ALLOWED_ENV_KEYS:
            await update.message.reply_text("不允许修改的键：{0}".format(key))
            return MAINTAIN_CONFIG_INPUT
        updates[key] = value

    if not updates:
        await update.message.reply_text("未检测到有效修改项，请重新输入。")
        return MAINTAIN_CONFIG_INPUT

    if "CONTROLLER_PORT" in updates:
        port_value = updates["CONTROLLER_PORT"]
        if not port_value.isdigit() or int(port_value) < 1 or int(port_value) > 65535:
            await update.message.reply_text("CONTROLLER_PORT 必须是 1-65535 的整数。")
            return MAINTAIN_CONFIG_INPUT
    for key in (
        "BOT_MENU_TTL",
        "BOT_NODE_MONITOR_INTERVAL",
        "BOT_NODE_OFFLINE_THRESHOLD",
        "BOT_NODE_TIME_SYNC_INTERVAL",
        "BOT_LOG_VIEW_MAX_PAGES",
        "LOG_ARCHIVE_WINDOW_HOURS",
        "LOG_ARCHIVE_RETENTION_COUNT",
        "SUB_LINK_DEFAULT_TTL_SECONDS",
        "API_RATE_LIMIT_WINDOW_SECONDS",
        "API_RATE_LIMIT_MAX_REQUESTS",
    ):
        if key in updates:
            if not updates[key].isdigit():
                await update.message.reply_text("{0} 必须为整数。".format(key))
                return MAINTAIN_CONFIG_INPUT
    if "BOT_NODE_TIME_SYNC_INTERVAL" in updates:
        sync_interval = int(updates["BOT_NODE_TIME_SYNC_INTERVAL"])
        if sync_interval < 0:
            await update.message.reply_text("BOT_NODE_TIME_SYNC_INTERVAL 不能小于 0。")
            return MAINTAIN_CONFIG_INPUT
        if sync_interval > 0 and sync_interval < 3600:
            await update.message.reply_text(
                "BOT_NODE_TIME_SYNC_INTERVAL 最小为 3600（或设为 0 关闭）。"
            )
            return MAINTAIN_CONFIG_INPUT
    for key in ("BOT_LOG_VIEW_COOLDOWN", "BOT_MUTATION_COOLDOWN"):
        if key not in updates:
            continue
        try:
            float(updates[key])
        except ValueError:
            await update.message.reply_text("{0} 必须为数字。".format(key))
            return MAINTAIN_CONFIG_INPUT
    if "CONTROLLER_HTTP_TIMEOUT" in updates:
        try:
            timeout_value = float(updates["CONTROLLER_HTTP_TIMEOUT"])
        except ValueError:
            await update.message.reply_text("CONTROLLER_HTTP_TIMEOUT 必须为数字。")
            return MAINTAIN_CONFIG_INPUT
        if timeout_value <= 0:
            await update.message.reply_text("CONTROLLER_HTTP_TIMEOUT 必须大于 0。")
            return MAINTAIN_CONFIG_INPUT
    if "ENABLE_HTTPS" in updates and updates["ENABLE_HTTPS"] not in ("0", "1"):
        await update.message.reply_text("ENABLE_HTTPS 仅支持 0 或 1。")
        return MAINTAIN_CONFIG_INPUT
    for key in ("SUB_LINK_REQUIRE_SIGNATURE", "API_RATE_LIMIT_ENABLED"):
        if key in updates and updates[key] not in ("0", "1"):
            await update.message.reply_text("{0} 仅支持 0 或 1。".format(key))
            return MAINTAIN_CONFIG_INPUT

    if "CONTROLLER_URL" in updates:
        updates["CONTROLLER_URL"] = normalize_simple_url(updates["CONTROLLER_URL"], "http")
    if "CONTROLLER_PUBLIC_URL" in updates:
        default_scheme = "https" if str(updates.get("ENABLE_HTTPS", "")).strip() == "1" else "http"
        updates["CONTROLLER_PUBLIC_URL"] = normalize_simple_url(updates["CONTROLLER_PUBLIC_URL"], default_scheme)
    if "PANEL_BASE_URL" in updates:
        default_scheme = "https" if str(updates.get("ENABLE_HTTPS", "")).strip() == "1" else "http"
        updates["PANEL_BASE_URL"] = normalize_simple_url(updates["PANEL_BASE_URL"], default_scheme)

    ok, err_text = write_env_updates(updates)
    if not ok:
        await update.message.reply_text("写入配置失败：{0}".format(err_text))
        return ConversationHandler.END

    update_cmd = "cd {0} && bash {1} --reuse-config".format(
        shlex.quote(ADMIN_PROJECT_DIR),
        shlex.quote(ADMIN_UPDATE_SCRIPT),
    )
    log_path = launch_background_job(update_cmd, "maintain-config-apply")
    await update.message.reply_text(
        "配置已写入，正在后台应用（复用原配置重载服务）。\n"
        "日志文件：{0}\n\n"
        "若你改了 BOT_TOKEN，机器人可能会短暂重连。".format(log_path),
        reply_markup=build_submenu("maintain"),
    )
    return ConversationHandler.END


async def cancel_maintain_config_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    del context
    if update.message:
        await update.message.reply_text("已取消。", reply_markup=build_submenu("maintain"))
    return ConversationHandler.END


async def start_maintain_import_wizard(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    if not await ensure_admin_callback(update, ROLE_SUPER):
        return ConversationHandler.END
    await query.answer()
    await query.edit_message_text(
        "迁移导入\n\n请输入迁移包路径（例如 /root/sb-migrate-20260225-120000.tar.gz）。\n发送 /cancel 取消。"
    )
    return MAINTAIN_IMPORT_INPUT


async def maintain_import_input(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    del context
    if not update.message or not update.message.text:
        return MAINTAIN_IMPORT_INPUT
    pkg_path = update.message.text.strip()
    if not pkg_path:
        await update.message.reply_text("路径不能为空，请重新输入。")
        return MAINTAIN_IMPORT_INPUT

    import_cmd = (
        "cd {project} && bash {script} --non-interactive --package {pkg}"
    ).format(
        project=shlex.quote(ADMIN_PROJECT_DIR),
        script=shlex.quote(ADMIN_IMPORT_SCRIPT),
        pkg=shlex.quote(pkg_path),
    )
    log_path = launch_background_job(import_cmd, "maintain-migrate-import")
    await update.message.reply_text(
        "迁移导入任务已启动（后台执行）。\n"
        "日志文件：{0}\n\n"
        "提示：导入过程会重启 controller/bot。".format(log_path),
        reply_markup=build_submenu("maintain"),
    )
    return ConversationHandler.END


async def cancel_maintain_import_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    del context
    if update.message:
        await update.message.reply_text("已取消。", reply_markup=build_submenu("maintain"))
    return ConversationHandler.END


async def render_user_nodes_manage(query, user_code: str, notice: str = "") -> None:
    user, user_error, user_status = await controller_request("GET", f"/users/{user_code}")
    if user_error:
        if user_status == 404:
            await query.edit_message_text("用户不存在", reply_markup=build_submenu("user"))
            return
        await query.edit_message_text(
            f"获取用户信息失败：{localize_controller_error(user_error)}",
            reply_markup=build_submenu("user"),
        )
        return

    user_nodes, nodes_error, nodes_status = await controller_request(
        "GET", f"/users/{user_code}/nodes"
    )
    if nodes_error:
        if nodes_status == 404:
            await query.edit_message_text("用户不存在", reply_markup=build_submenu("user"))
            return
        await query.edit_message_text(
            f"获取用户节点信息失败：{localize_controller_error(nodes_error)}",
            reply_markup=build_submenu("user"),
        )
        return

    nodes, _, _ = await controller_request("GET", "/nodes")
    node_tags_map = build_node_tags_map(nodes if isinstance(nodes, list) else [])

    text = format_user_nodes_manage_text(
        user_code, user or {}, user_nodes or [], node_tags_map, notice
    )
    await query.edit_message_text(text, reply_markup=build_user_nodes_manage_keyboard(user_code))


async def send_user_nodes_manage_message(
    message, context: ContextTypes.DEFAULT_TYPE, user_code: str, notice: str = ""
) -> None:
    user, user_error, user_status = await controller_request("GET", f"/users/{user_code}")
    if user_error:
        if user_status == 404:
            await reply_text_with_auto_clear(
                message, context, "用户不存在", reply_markup=build_submenu("user")
            )
            return
        await reply_text_with_auto_clear(
            message,
            context,
            f"获取用户信息失败：{localize_controller_error(user_error)}",
            reply_markup=build_submenu("user"),
        )
        return

    user_nodes, nodes_error, nodes_status = await controller_request(
        "GET", f"/users/{user_code}/nodes"
    )
    if nodes_error:
        if nodes_status == 404:
            await reply_text_with_auto_clear(
                message, context, "用户不存在", reply_markup=build_submenu("user")
            )
            return
        await reply_text_with_auto_clear(
            message,
            context,
            f"获取用户节点信息失败：{localize_controller_error(nodes_error)}",
            reply_markup=build_submenu("user"),
        )
        return

    nodes, _, _ = await controller_request("GET", "/nodes")
    node_tags_map = build_node_tags_map(nodes if isinstance(nodes, list) else [])

    text = format_user_nodes_manage_text(
        user_code, user or {}, user_nodes or [], node_tags_map, notice
    )
    await reply_text_with_auto_clear(
        message,
        context,
        text,
        reply_markup=build_user_nodes_manage_keyboard(user_code),
    )


async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message:
        chat_id = update.message.chat_id
        message_map = get_main_menu_message_map(context)
        existing = message_map.get(chat_id, [])
        if not isinstance(existing, list):
            existing = []

        # 优先复用最近一条机器人菜单消息，避免 /start /menu 反复刷出新菜单消息。
        for old_message_id in reversed(existing):
            try:
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=old_message_id,
                    text="主菜单",
                    reply_markup=build_main_menu(),
                )
                set_main_menu_message(context, chat_id, old_message_id)
                await purge_main_menu_messages(
                    context, chat_id, keep_message_id=old_message_id
                )
                schedule_menu_auto_clear(context, chat_id, old_message_id)
                return
            except BadRequest as exc:
                if "message is not modified" in str(exc).lower():
                    set_main_menu_message(context, chat_id, old_message_id)
                    await purge_main_menu_messages(
                        context, chat_id, keep_message_id=old_message_id
                    )
                    schedule_menu_auto_clear(context, chat_id, old_message_id)
                    return
                continue
            except Forbidden:
                continue

        await purge_main_menu_messages(context, chat_id)

        sent = await reply_text_with_auto_clear(
            update.message, context, "主菜单", reply_markup=build_main_menu()
        )
        set_main_menu_message(context, chat_id, sent.message_id)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    remember_known_chat(update, context)
    if not is_admin_chat(update):
        await deny_non_admin(update)
        return
    await show_main_menu(update, context)


async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    remember_known_chat(update, context)
    if not is_admin_chat(update):
        await deny_non_admin(update)
        return
    await show_main_menu(update, context)


async def whoami(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    remember_known_chat(update, context)
    if not update.message or not update.effective_chat:
        return
    chat_id = update.effective_chat.id
    await update.message.reply_text(
        f"你的 chat_id 是: {chat_id}\n"
        "可将该数字写入 ADMIN_CHAT_IDS（逗号分隔）以限制管理员权限。"
    )


def clear_all_wizard_state(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop(WIZARD_KEY, None)
    context.user_data.pop(NODES_WIZARD_KEY, None)
    context.user_data.pop(USER_NODES_WIZARD_KEY, None)
    context.user_data.pop(NODE_EDIT_KEY, None)
    context.user_data.pop(NODE_REALITY_KEY, None)
    pop_user_speed_pending(context)


async def cancel_idle(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    remember_known_chat(update, context)
    clear_all_wizard_state(context)
    if not is_admin_chat(update):
        await deny_non_admin(update)
        return
    await show_main_menu(update, context)


async def configure_command_menu(application: Application) -> None:
    admin_commands = [
        BotCommand("start", "打开主菜单"),
        BotCommand("menu", "显示主菜单"),
        BotCommand("cancel", "取消当前操作"),
        BotCommand("whoami", "查看当前 chat_id"),
    ]
    public_commands = [
        BotCommand("whoami", "查看当前 chat_id"),
    ]

    try:
        await application.bot.set_my_commands(public_commands)
        scoped_admin_ids = sorted(VIEW_ADMIN_CHAT_ID_SET)
        if scoped_admin_ids:
            for chat_id in scoped_admin_ids:
                await application.bot.set_my_commands(
                    admin_commands,
                    scope=BotCommandScopeChat(chat_id=chat_id),
                )
        else:
            await application.bot.set_my_commands(admin_commands)
    except Exception as exc:  # pragma: no cover
        logger.warning("configure command menu failed: %s", exc)


async def start_create_user_wizard(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    if not await ensure_admin_callback(update, ROLE_OPERATOR):
        return ConversationHandler.END

    await query.answer()
    callback_data = query.data or ""
    user = query.from_user.username or query.from_user.id
    logger.info("button_click user=%s data=%s", user, callback_data)

    context.user_data[WIZARD_KEY] = {}
    await query.edit_message_text("创建用户\n\n请输入备注/用户名（非空）：")
    return CREATE_DISPLAY_NAME


async def create_user_display_name(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    if not update.message or not update.message.text:
        return CREATE_DISPLAY_NAME

    display_name = update.message.text.strip()
    if not display_name:
        await update.message.reply_text("备注/用户名不能为空，请重新输入：")
        return CREATE_DISPLAY_NAME

    context.user_data.setdefault(WIZARD_KEY, {})["display_name"] = display_name
    await update.message.reply_text("请输入 TUIC 端口（1-65535）：")
    return CREATE_TUIC_PORT


async def create_user_tuic_port(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    if not update.message or not update.message.text:
        return CREATE_TUIC_PORT

    raw_value = update.message.text.strip()
    try:
        tuic_port = int(raw_value)
    except ValueError:
        await update.message.reply_text("端口必须是数字，请输入 1-65535 的整数：")
        return CREATE_TUIC_PORT

    if not 1 <= tuic_port <= 65535:
        await update.message.reply_text("端口范围无效，请输入 1-65535 的整数：")
        return CREATE_TUIC_PORT

    context.user_data.setdefault(WIZARD_KEY, {})["tuic_port"] = tuic_port
    await update.message.reply_text("请输入限速 Mbps（整数，0 表示不限速）：")
    return CREATE_SPEED_MBPS


async def create_user_speed_mbps(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    if not update.message or not update.message.text:
        return CREATE_SPEED_MBPS

    raw_value = update.message.text.strip()
    try:
        speed_mbps = int(raw_value)
    except ValueError:
        await update.message.reply_text("限速必须是整数，请输入大于等于 0 的数字：")
        return CREATE_SPEED_MBPS

    if speed_mbps < 0:
        await update.message.reply_text("限速不能小于 0，请重新输入：")
        return CREATE_SPEED_MBPS

    context.user_data.setdefault(WIZARD_KEY, {})["speed_mbps"] = speed_mbps
    await update.message.reply_text("请输入有效天数（1-3650）：")
    return CREATE_VALID_DAYS


async def create_user_valid_days(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    if not update.message or not update.message.text:
        return CREATE_VALID_DAYS

    raw_value = update.message.text.strip()
    try:
        valid_days = int(raw_value)
    except ValueError:
        await update.message.reply_text("有效天数必须是整数，请输入 1-3650：")
        return CREATE_VALID_DAYS

    if not 1 <= valid_days <= 3650:
        await update.message.reply_text("有效天数范围无效，请输入 1-3650：")
        return CREATE_VALID_DAYS

    wizard_data = context.user_data.setdefault(WIZARD_KEY, {})
    wizard_data["valid_days"] = valid_days

    await reply_text_with_auto_clear(
        update.message,
        context,
        format_create_summary(
            wizard_data["display_name"],
            wizard_data["tuic_port"],
            wizard_data["speed_mbps"],
            wizard_data["valid_days"],
        ),
        reply_markup=build_create_confirm_keyboard(),
    )
    return CREATE_CONFIRM


async def create_user_confirm(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    query = update.callback_query
    if not query:
        return CREATE_CONFIRM
    if not await ensure_admin_callback(update, ROLE_OPERATOR):
        return ConversationHandler.END
    callback_data = query.data or ""
    if not await enforce_mutation_cooldown(update, context, callback_data):
        return CREATE_CONFIRM

    await query.answer()
    user = query.from_user.username or query.from_user.id
    logger.info("button_click user=%s data=%s", user, callback_data)

    wizard_data = context.user_data.get(WIZARD_KEY, {})
    required_fields = ["display_name", "tuic_port", "speed_mbps", "valid_days"]
    if not all(field in wizard_data for field in required_fields):
        await query.edit_message_text("创建流程数据不完整，请重新点击“创建用户”。")
        context.user_data.pop(WIZARD_KEY, None)
        return ConversationHandler.END

    payload = {
        "display_name": wizard_data["display_name"],
        "tuic_port": wizard_data["tuic_port"],
        "speed_mbps": wizard_data["speed_mbps"],
        "valid_days": wizard_data["valid_days"],
        "note": "",
    }
    result, error_message, _ = await controller_request(
        "POST", "/users/create", payload=payload
    )
    if error_message:
        await edit_or_reply_with_auto_clear(
            query,
            context,
            f"创建失败：{error_message}\n\n"
            f"{format_create_summary(payload['display_name'], payload['tuic_port'], payload['speed_mbps'], payload['valid_days'])}",
            reply_markup=build_create_confirm_keyboard(),
        )
        return CREATE_CONFIRM

    if not isinstance(result, dict):
        await edit_or_reply_with_auto_clear(
            query,
            context,
            "创建失败：控制器返回格式异常。\n\n"
            f"{format_create_summary(payload['display_name'], payload['tuic_port'], payload['speed_mbps'], payload['valid_days'])}",
            reply_markup=build_create_confirm_keyboard(),
        )
        return CREATE_CONFIRM

    user_code = result.get("user_code", "")
    expire_at = int(result.get("expire_at", 0))
    expire_text = datetime.fromtimestamp(expire_at).strftime("%Y-%m-%d %H:%M:%S")
    speed_mbps = int(result.get("speed_mbps", 0))
    speed_text = "不限速（0 Mbps）" if speed_mbps == 0 else f"{speed_mbps} Mbps"
    tuic_port = result.get("tuic_port", "")
    signed_data, signed_error, _ = await controller_request(
        "GET", f"/admin/sub/sign/{user_code}"
    )
    links_url = "{0}/sub/links/{1}".format(PANEL_BASE_URL.rstrip("/"), user_code)
    base64_url = "{0}/sub/base64/{1}".format(PANEL_BASE_URL.rstrip("/"), user_code)
    signed_tip = "（默认链接）"
    if not signed_error and isinstance(signed_data, dict):
        links_url = str(signed_data.get("links_url") or links_url)
        base64_url = str(signed_data.get("base64_url") or base64_url)
        signed_tip = "（签名链接）" if bool(signed_data.get("signed")) else "（默认链接）"

    await edit_or_reply_with_auto_clear(
        query,
        context,
        "创建成功\n\n"
        f"用户代码：{user_code}\n"
        f"到期时间：{expire_text}\n"
        f"限速：{speed_text}\n"
        f"TUIC端口：{tuic_port}\n\n"
        f"订阅链接{signed_tip}：\n{links_url}\n\n"
        f"Base64订阅{signed_tip}：\n{base64_url}",
        reply_markup=build_submenu("user"),
    )
    context.user_data.pop(WIZARD_KEY, None)
    return ConversationHandler.END


async def cancel_create_user_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    query = update.callback_query
    if not query:
        return ConversationHandler.END

    await query.answer()
    user = query.from_user.username or query.from_user.id
    logger.info("button_click user=%s data=%s", user, query.data or "")
    context.user_data.pop(WIZARD_KEY, None)
    await query.edit_message_text("已取消", reply_markup=build_submenu("user"))
    return ConversationHandler.END


async def cancel_wizard_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    context.user_data.pop(WIZARD_KEY, None)
    if update.message:
        await reply_text_with_auto_clear(
            update.message, context, "已取消", reply_markup=build_submenu("user")
        )
    return ConversationHandler.END


async def start_nodes_create_wizard(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    if not await ensure_admin_callback(update, ROLE_OPERATOR):
        return ConversationHandler.END

    await query.answer()
    callback_data = query.data or ""
    user = query.from_user.username or query.from_user.id
    logger.info("button_click user=%s data=%s", user, callback_data)

    context.user_data[NODES_WIZARD_KEY] = {}
    await query.edit_message_text("新增节点\n\n请输入节点代码（例如 JP1）：")
    return NODE_CREATE_NODE_CODE


async def nodes_create_node_code(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    if not update.message or not update.message.text:
        return NODE_CREATE_NODE_CODE

    node_code = update.message.text.strip()
    if not node_code:
        await update.message.reply_text("节点代码不能为空，请重新输入：")
        return NODE_CREATE_NODE_CODE

    context.user_data.setdefault(NODES_WIZARD_KEY, {})["node_code"] = node_code
    await update.message.reply_text("请输入地区（例如 JP）：")
    return NODE_CREATE_REGION


async def nodes_create_region(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    if not update.message or not update.message.text:
        return NODE_CREATE_REGION

    region = update.message.text.strip()
    if not region:
        await update.message.reply_text("地区不能为空，请重新输入：")
        return NODE_CREATE_REGION

    context.user_data.setdefault(NODES_WIZARD_KEY, {})["region"] = region
    await update.message.reply_text("请输入主机地址（IP 或域名）：")
    return NODE_CREATE_HOST


async def nodes_create_host(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    if not update.message or not update.message.text:
        return NODE_CREATE_HOST

    host = update.message.text.strip()
    if not host:
        await update.message.reply_text("主机地址不能为空，请重新输入：")
        return NODE_CREATE_HOST

    context.user_data.setdefault(NODES_WIZARD_KEY, {})["host"] = host
    await update.message.reply_text(
        "请输入节点公网IP（用于限制 agent -> controller 来源IP，仅支持 IP 地址）："
    )
    return NODE_CREATE_AGENT_IP


async def nodes_create_agent_ip(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    if not update.message or not update.message.text:
        return NODE_CREATE_AGENT_IP

    agent_ip = update.message.text.strip()
    if not agent_ip:
        await update.message.reply_text("节点公网IP不能为空，请重新输入：")
        return NODE_CREATE_AGENT_IP
    if not is_valid_ip_address(agent_ip):
        await update.message.reply_text("IP格式无效，请输入正确的IPv4/IPv6地址：")
        return NODE_CREATE_AGENT_IP

    context.user_data.setdefault(NODES_WIZARD_KEY, {})["agent_ip"] = agent_ip
    await update.message.reply_text("请输入 Reality 域名（可选，输入 - 跳过）：")
    return NODE_CREATE_REALITY_SERVER_NAME


async def nodes_create_reality_server_name(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    if not update.message or not update.message.text:
        return NODE_CREATE_REALITY_SERVER_NAME

    raw_value = update.message.text.strip()
    reality_server_name = "" if raw_value == "-" else raw_value
    context.user_data.setdefault(NODES_WIZARD_KEY, {})[
        "reality_server_name"
    ] = reality_server_name
    await update.message.reply_text("请输入 TUIC 起始端口（1-65535）：")
    return NODE_CREATE_TUIC_PORT_START


async def nodes_create_tuic_port_start(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    if not update.message or not update.message.text:
        return NODE_CREATE_TUIC_PORT_START

    raw_value = update.message.text.strip()
    try:
        tuic_port_start = int(raw_value)
    except ValueError:
        await update.message.reply_text("起始端口必须是整数，请输入 1-65535：")
        return NODE_CREATE_TUIC_PORT_START

    if not 1 <= tuic_port_start <= 65535:
        await update.message.reply_text("起始端口范围无效，请输入 1-65535：")
        return NODE_CREATE_TUIC_PORT_START

    context.user_data.setdefault(NODES_WIZARD_KEY, {})["tuic_port_start"] = tuic_port_start
    await update.message.reply_text("请输入 TUIC 结束端口（1-65535，且不小于起始端口）：")
    return NODE_CREATE_TUIC_PORT_END


async def nodes_create_tuic_port_end(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    if not update.message or not update.message.text:
        return NODE_CREATE_TUIC_PORT_END

    raw_value = update.message.text.strip()
    try:
        tuic_port_end = int(raw_value)
    except ValueError:
        await update.message.reply_text("结束端口必须是整数，请输入 1-65535：")
        return NODE_CREATE_TUIC_PORT_END

    if not 1 <= tuic_port_end <= 65535:
        await update.message.reply_text("结束端口范围无效，请输入 1-65535：")
        return NODE_CREATE_TUIC_PORT_END

    wizard_data = context.user_data.setdefault(NODES_WIZARD_KEY, {})
    tuic_port_start = int(wizard_data["tuic_port_start"])
    if tuic_port_end < tuic_port_start:
        await update.message.reply_text("结束端口不能小于起始端口，请重新输入：")
        return NODE_CREATE_TUIC_PORT_END

    wizard_data["tuic_port_end"] = tuic_port_end
    await update.message.reply_text("请输入备注（可选，输入 - 跳过）：")
    return NODE_CREATE_NOTE


async def nodes_create_note(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    if not update.message or not update.message.text:
        return NODE_CREATE_NOTE

    raw_value = update.message.text.strip()
    note = "" if raw_value == "-" else raw_value
    wizard_data = context.user_data.setdefault(NODES_WIZARD_KEY, {})
    wizard_data["note"] = note

    await reply_text_with_auto_clear(
        update.message,
        context,
        format_nodes_create_summary(
            wizard_data["node_code"],
            wizard_data["region"],
            wizard_data["host"],
            wizard_data["agent_ip"],
            wizard_data.get("reality_server_name", ""),
            wizard_data["tuic_port_start"],
            wizard_data["tuic_port_end"],
            wizard_data["note"],
        ),
        reply_markup=build_nodes_create_confirm_keyboard(),
    )
    return NODE_CREATE_CONFIRM


async def nodes_create_confirm(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    query = update.callback_query
    if not query:
        return NODE_CREATE_CONFIRM
    if not await ensure_admin_callback(update, ROLE_OPERATOR):
        return ConversationHandler.END
    callback_data = query.data or ""
    if not await enforce_mutation_cooldown(update, context, callback_data):
        return NODE_CREATE_CONFIRM

    await query.answer()
    user = query.from_user.username or query.from_user.id
    logger.info("button_click user=%s data=%s", user, callback_data)

    wizard_data = context.user_data.get(NODES_WIZARD_KEY, {})
    required_fields = [
        "node_code",
        "region",
        "host",
        "agent_ip",
        "tuic_port_start",
        "tuic_port_end",
        "note",
    ]
    if not all(field in wizard_data for field in required_fields):
        await query.edit_message_text("节点创建数据不完整，请重新点击“新增节点”。", reply_markup=build_submenu("nodes"))
        context.user_data.pop(NODES_WIZARD_KEY, None)
        return ConversationHandler.END

    payload = {
        "node_code": wizard_data["node_code"],
        "region": wizard_data["region"],
        "host": wizard_data["host"],
        "agent_ip": wizard_data["agent_ip"],
        "tuic_port_start": wizard_data["tuic_port_start"],
        "tuic_port_end": wizard_data["tuic_port_end"],
        "note": wizard_data["note"],
        "enabled": 1,
    }
    reality_server_name = wizard_data.get("reality_server_name", "")
    if reality_server_name:
        payload["reality_server_name"] = reality_server_name
    result, error_message, _ = await controller_request(
        "POST", "/nodes/create", payload=payload
    )
    if error_message:
        await edit_or_reply_with_auto_clear(
            query,
            context,
            f"创建节点失败：{error_message}",
            reply_markup=build_submenu("nodes"),
        )
        context.user_data.pop(NODES_WIZARD_KEY, None)
        return ConversationHandler.END

    if not isinstance(result, dict):
        await edit_or_reply_with_auto_clear(
            query,
            context,
            "创建节点失败：控制器返回格式异常",
            reply_markup=build_submenu("nodes"),
        )
        context.user_data.pop(NODES_WIZARD_KEY, None)
        return ConversationHandler.END

    reality_text = result.get("reality_server_name") or "未设置"
    await edit_or_reply_with_auto_clear(
        query,
        context,
        "创建节点成功\n\n"
        f"节点代码：{result.get('node_code', payload['node_code'])}\n"
        f"地区：{result.get('region', payload['region'])}\n"
        f"主机：{result.get('host', payload['host'])}\n"
        f"节点来源IP白名单：{result.get('agent_ip', payload['agent_ip'])}\n"
        f"Reality域名：{reality_text}\n"
        f"TUIC端口池：{result.get('tuic_port_start', payload['tuic_port_start'])}-"
        f"{result.get('tuic_port_end', payload['tuic_port_end'])}\n"
        f"状态：{'启用' if int(result.get('enabled', 1)) == 1 else '禁用'}\n\n"
        f"建议在管理服务器放行该IP到controller端口：\n"
        f"ufw allow from {result.get('agent_ip', payload['agent_ip'])} "
        f"to any port {CONTROLLER_PORT_HINT} proto tcp",
        reply_markup=build_submenu("nodes"),
    )
    context.user_data.pop(NODES_WIZARD_KEY, None)
    return ConversationHandler.END


async def cancel_nodes_create_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    query = update.callback_query
    if not query:
        return ConversationHandler.END

    await query.answer()
    user = query.from_user.username or query.from_user.id
    logger.info("button_click user=%s data=%s", user, query.data or "")
    context.user_data.pop(NODES_WIZARD_KEY, None)
    await query.edit_message_text("已取消", reply_markup=build_submenu("nodes"))
    return ConversationHandler.END


async def cancel_nodes_wizard_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    context.user_data.pop(NODES_WIZARD_KEY, None)
    if update.message:
        await reply_text_with_auto_clear(
            update.message, context, "已取消", reply_markup=build_submenu("nodes")
        )
    return ConversationHandler.END


async def start_user_nodes_wizard(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    if not await ensure_admin_callback(update, ROLE_OPERATOR):
        return ConversationHandler.END

    await query.answer()
    callback_data = query.data or ""
    user = query.from_user.username or query.from_user.id
    logger.info("button_click user=%s data=%s", user, callback_data)

    context.user_data.pop(USER_NODES_WIZARD_KEY, None)
    await render_user_nodes_picker(query)
    return ConversationHandler.END


async def start_user_nodes_manual_input(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    if not await ensure_admin_callback(update, ROLE_OPERATOR):
        return ConversationHandler.END

    await query.answer()
    callback_data = query.data or ""
    user = query.from_user.username or query.from_user.id
    logger.info("button_click user=%s data=%s", user, callback_data)

    context.user_data[USER_NODES_WIZARD_KEY] = {}
    await query.edit_message_text(
        "节点分配\n\n请输入用户代码（例如 u1001），发送 /cancel 取消："
    )
    return USER_NODES_INPUT


async def user_nodes_input_user_code(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    if not update.message or not update.message.text:
        return USER_NODES_INPUT

    raw_value = update.message.text.strip()
    if not re.match(r"^u\d+$", raw_value):
        await update.message.reply_text("用户代码格式无效，请输入类似 u1001 的代码：")
        return USER_NODES_INPUT

    context.user_data[USER_NODES_WIZARD_KEY] = {"user_code": raw_value}
    await send_user_nodes_manage_message(update.message, context, raw_value)
    context.user_data.pop(USER_NODES_WIZARD_KEY, None)
    return ConversationHandler.END


async def cancel_user_nodes_wizard_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    context.user_data.pop(USER_NODES_WIZARD_KEY, None)
    if update.message:
        await reply_text_with_auto_clear(
            update.message, context, "已取消", reply_markup=build_submenu("user")
        )
    return ConversationHandler.END


async def start_user_speed_wizard(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    if not await ensure_admin_callback(update, ROLE_OPERATOR):
        return ConversationHandler.END

    await query.answer()
    callback_data = query.data or ""
    user = query.from_user.username or query.from_user.id
    logger.info("button_click user=%s data=%s", user, callback_data)

    pop_user_speed_pending(context)
    await render_user_speed_picker(query)
    return ConversationHandler.END


async def start_user_speed_input(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    if not await ensure_admin_callback(update, ROLE_OPERATOR):
        return ConversationHandler.END

    await query.answer()
    callback_data = query.data or ""
    user = query.from_user.username or query.from_user.id
    logger.info("button_click user=%s data=%s", user, callback_data)

    parts = callback_data.split(":", maxsplit=2)
    if len(parts) != 3:
        await query.edit_message_text("请求无效，请重试。", reply_markup=build_submenu("user"))
        return ConversationHandler.END

    user_code = parts[2]
    user_info, error_message, status_code = await controller_request("GET", f"/users/{user_code}")
    if error_message:
        localized = localize_controller_error(error_message)
        if status_code == 404 and localized == "用户不存在":
            await query.edit_message_text("用户不存在", reply_markup=build_submenu("user"))
        else:
            await query.edit_message_text(
                f"获取用户信息失败：{localized}",
                reply_markup=build_submenu("user"),
            )
        return ConversationHandler.END

    display_name = str(user_info.get("display_name") or "").strip()
    old_speed = int(user_info.get("speed_mbps", 0) or 0)
    pending_map = context.user_data.setdefault(USER_SPEED_PENDING_KEY, {})
    if not isinstance(pending_map, dict):
        pending_map = {}
        context.user_data[USER_SPEED_PENDING_KEY] = pending_map
    pending_map[user_code] = {
        "display_name": display_name,
        "old_speed": old_speed,
    }
    context.user_data[USER_SPEED_ACTIVE_KEY] = user_code

    user_label = f"{display_name}（{user_code}）" if display_name else user_code
    await query.edit_message_text(
        f"用户：{user_label}\n"
        f"当前限速：{old_speed} Mbps\n\n"
        "请输入新的限速 Mbps（整数，例如 10 / 30 / 100），发送 /cancel 取消："
    )
    return USER_SPEED_INPUT


async def user_speed_input_value(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    if not update.message or not update.message.text:
        return USER_SPEED_INPUT

    raw_value = update.message.text.strip()
    try:
        new_speed = int(raw_value)
    except ValueError:
        await update.message.reply_text("限速必须是整数，请输入 1-10000：")
        return USER_SPEED_INPUT

    if not 1 <= new_speed <= 10000:
        await update.message.reply_text("限速范围无效，请输入 1-10000：")
        return USER_SPEED_INPUT

    user_code = str(context.user_data.get(USER_SPEED_ACTIVE_KEY, ""))
    pending_map = context.user_data.get(USER_SPEED_PENDING_KEY, {})
    pending = pending_map.get(user_code, {}) if isinstance(pending_map, dict) else {}
    if not user_code or not pending:
        pop_user_speed_pending(context)
        await reply_text_with_auto_clear(
            update.message,
            context,
            "修改状态已丢失，请重新选择用户。",
            reply_markup=build_submenu("user"),
        )
        return ConversationHandler.END

    pending["new_speed"] = new_speed
    pending_map[user_code] = pending
    context.user_data[USER_SPEED_PENDING_KEY] = pending_map

    display_name = str(pending.get("display_name") or "").strip()
    user_label = f"{display_name}（{user_code}）" if display_name else user_code
    old_speed = int(pending.get("old_speed", 0) or 0)
    await reply_text_with_auto_clear(
        update.message,
        context,
        "请确认修改限速：\n\n"
        f"用户：{user_label}\n"
        f"当前限速：{old_speed} Mbps\n"
        f"新限速：{new_speed} Mbps\n"
        "影响范围：该用户已绑定的所有节点（统一限速）\n"
        "提示：确认后节点侧将通过轮询同步生效（无需手动登录服务器）",
        reply_markup=build_user_speed_confirm_keyboard(user_code),
    )
    return USER_SPEED_CONFIRM


async def apply_user_speed_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    if not await ensure_admin_callback(update, ROLE_OPERATOR):
        return ConversationHandler.END
    callback_data = query.data or ""
    if not await enforce_mutation_cooldown(update, context, callback_data):
        return USER_SPEED_CONFIRM

    await query.answer()
    user = query.from_user.username or query.from_user.id
    logger.info("button_click user=%s data=%s", user, callback_data)

    parts = callback_data.split(":", maxsplit=2)
    if len(parts) != 3:
        pop_user_speed_pending(context)
        await query.edit_message_text("请求无效，请重试。", reply_markup=build_submenu("user"))
        return ConversationHandler.END

    user_code = parts[2]
    pending_map = context.user_data.get(USER_SPEED_PENDING_KEY, {})
    pending = pending_map.get(user_code, {}) if isinstance(pending_map, dict) else {}
    new_speed = pending.get("new_speed")
    if not isinstance(new_speed, int):
        pop_user_speed_pending(context, user_code)
        await query.edit_message_text("待确认限速数据已失效，请重新操作。", reply_markup=build_submenu("user"))
        return ConversationHandler.END

    _, error_message, status_code = await controller_request(
        "POST",
        f"/users/{user_code}/set_speed",
        payload={"speed_mbps": new_speed},
    )
    if error_message:
        localized = localize_controller_error(error_message)
        if status_code == 404 and localized == "用户不存在":
            pop_user_speed_pending(context, user_code)
            await query.edit_message_text("用户不存在", reply_markup=build_submenu("user"))
            return ConversationHandler.END
        pop_user_speed_pending(context, user_code)
        await query.edit_message_text(f"修改限速失败：{localized}", reply_markup=build_submenu("user"))
        return ConversationHandler.END

    bindings, _, _ = await controller_request("GET", f"/users/{user_code}/nodes")
    bound_count = len(bindings) if isinstance(bindings, list) else 0
    pop_user_speed_pending(context, user_code)
    await query.edit_message_text(
        f"修改限速成功：{user_code} -> {new_speed} Mbps\n"
        f"影响节点数：{bound_count}",
        reply_markup=build_submenu("user"),
    )
    return ConversationHandler.END


async def cancel_user_speed_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    query = update.callback_query
    if not query:
        return ConversationHandler.END

    await query.answer()
    callback_data = query.data or ""
    user = query.from_user.username or query.from_user.id
    logger.info("button_click user=%s data=%s", user, callback_data)

    pop_user_speed_pending(context)
    await query.edit_message_text(SUBMENUS["user"]["title"], reply_markup=build_submenu("user"))
    return ConversationHandler.END


async def cancel_user_speed_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    pop_user_speed_pending(context)
    if update.message:
        await reply_text_with_auto_clear(
            update.message, context, "已取消", reply_markup=build_submenu("user")
        )
    return ConversationHandler.END


async def start_node_edit_host(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    if not await ensure_admin_callback(update, ROLE_OPERATOR):
        return ConversationHandler.END
    await query.answer()
    callback_data = query.data or ""
    user = query.from_user.username or query.from_user.id
    logger.info("button_click user=%s data=%s", user, callback_data)
    node_code = callback_data.split(":", maxsplit=2)[2]
    context.user_data[NODE_EDIT_KEY] = {"node_code": node_code, "field": "host"}
    await query.edit_message_text("请输入新的入口（IP 或域名），发送 /cancel 取消。")
    return NODE_EDIT_HOST


async def start_node_edit_agent_ip(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    if not await ensure_admin_callback(update, ROLE_OPERATOR):
        return ConversationHandler.END
    await query.answer()
    callback_data = query.data or ""
    user = query.from_user.username or query.from_user.id
    logger.info("button_click user=%s data=%s", user, callback_data)
    node_code = callback_data.split(":", maxsplit=2)[2]
    context.user_data[NODE_EDIT_KEY] = {"node_code": node_code, "field": "agent_ip"}
    await query.edit_message_text(
        "请输入节点公网IP（仅允许该IP访问 controller 同步接口），发送 /cancel 取消。"
    )
    return NODE_EDIT_AGENT_IP


async def start_node_edit_sni(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    if not await ensure_admin_callback(update, ROLE_OPERATOR):
        return ConversationHandler.END
    await query.answer()
    callback_data = query.data or ""
    user = query.from_user.username or query.from_user.id
    logger.info("button_click user=%s data=%s", user, callback_data)
    node_code = callback_data.split(":", maxsplit=2)[2]
    context.user_data[NODE_EDIT_KEY] = {"node_code": node_code, "field": "sni"}
    await query.edit_message_text("请输入新的伪装域名，发送 - 清空，发送 /cancel 取消。")
    return NODE_EDIT_SNI


async def start_node_edit_pool(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    if not await ensure_admin_callback(update, ROLE_OPERATOR):
        return ConversationHandler.END
    await query.answer()
    callback_data = query.data or ""
    user = query.from_user.username or query.from_user.id
    logger.info("button_click user=%s data=%s", user, callback_data)
    node_code = callback_data.split(":", maxsplit=2)[2]
    context.user_data[NODE_EDIT_KEY] = {"node_code": node_code, "field": "pool"}
    await query.edit_message_text("请输入新的端口池，格式如 20000-20009，发送 /cancel 取消。")
    return NODE_EDIT_POOL


async def start_node_edit_tuic_sni(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    if not await ensure_admin_callback(update, ROLE_OPERATOR):
        return ConversationHandler.END
    await query.answer()
    callback_data = query.data or ""
    user = query.from_user.username or query.from_user.id
    logger.info("button_click user=%s data=%s", user, callback_data)
    node_code = callback_data.split(":", maxsplit=2)[2]
    context.user_data[NODE_EDIT_KEY] = {"node_code": node_code, "field": "tuic_sni"}
    await query.edit_message_text(
        "请输入新的TUIC证书域名（例如 jp1.cwzs.de），发送 - 清空，发送 /cancel 取消。"
    )
    return NODE_EDIT_TUIC_SNI


async def node_edit_host_input(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    if not update.message or not update.message.text:
        return NODE_EDIT_HOST
    host = update.message.text.strip()
    if not host:
        await update.message.reply_text("入口不能为空，请重新输入：")
        return NODE_EDIT_HOST

    node_code = context.user_data.get(NODE_EDIT_KEY, {}).get("node_code", "")
    if not node_code:
        await reply_text_with_auto_clear(
            update.message,
            context,
            "编辑状态已丢失，请重新进入节点详情。",
            reply_markup=build_submenu("nodes"),
        )
        return ConversationHandler.END

    context.user_data[NODE_EDIT_KEY] = {
        "node_code": node_code,
        "field": "host",
        "new_value": host,
        "patch_payload": {"host": host},
    }
    return await prompt_node_edit_confirmation(update.message, context)


async def node_edit_sni_input(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    if not update.message or not update.message.text:
        return NODE_EDIT_SNI
    raw_value = update.message.text.strip()
    if not raw_value:
        await update.message.reply_text("输入不能为空，请输入伪装域名或 - 清空：")
        return NODE_EDIT_SNI

    reality_server_name = "" if raw_value == "-" else raw_value
    node_code = context.user_data.get(NODE_EDIT_KEY, {}).get("node_code", "")
    if not node_code:
        await reply_text_with_auto_clear(
            update.message,
            context,
            "编辑状态已丢失，请重新进入节点详情。",
            reply_markup=build_submenu("nodes"),
        )
        return ConversationHandler.END

    context.user_data[NODE_EDIT_KEY] = {
        "node_code": node_code,
        "field": "sni",
        "new_value": reality_server_name,
        "patch_payload": {"reality_server_name": reality_server_name},
    }
    return await prompt_node_edit_confirmation(update.message, context)


async def node_edit_pool_input(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    if not update.message or not update.message.text:
        return NODE_EDIT_POOL
    raw_value = update.message.text.strip()
    matched = re.match(r"^(\d+)\s*-\s*(\d+)$", raw_value)
    if not matched:
        await update.message.reply_text("格式不正确，请按 20000-20009 输入：")
        return NODE_EDIT_POOL

    port_start = int(matched.group(1))
    port_end = int(matched.group(2))
    if not (1 <= port_start <= 65535 and 1 <= port_end <= 65535):
        await update.message.reply_text("端口范围无效，请输入 1-65535 之间的范围：")
        return NODE_EDIT_POOL
    if port_end < port_start:
        await update.message.reply_text("结束端口不能小于起始端口，请重新输入：")
        return NODE_EDIT_POOL

    node_code = context.user_data.get(NODE_EDIT_KEY, {}).get("node_code", "")
    if not node_code:
        await reply_text_with_auto_clear(
            update.message,
            context,
            "编辑状态已丢失，请重新进入节点详情。",
            reply_markup=build_submenu("nodes"),
        )
        return ConversationHandler.END

    context.user_data[NODE_EDIT_KEY] = {
        "node_code": node_code,
        "field": "pool",
        "new_pool_start": port_start,
        "new_pool_end": port_end,
        "patch_payload": {"tuic_port_start": port_start, "tuic_port_end": port_end},
    }
    return await prompt_node_edit_confirmation(update.message, context)


async def node_edit_tuic_sni_input(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    if not update.message or not update.message.text:
        return NODE_EDIT_TUIC_SNI

    raw_value = update.message.text.strip()
    if not raw_value:
        await update.message.reply_text("输入不能为空，请输入域名或 - 清空：")
        return NODE_EDIT_TUIC_SNI

    tuic_server_name = "" if raw_value == "-" else raw_value
    node_code = context.user_data.get(NODE_EDIT_KEY, {}).get("node_code", "")
    if not node_code:
        await reply_text_with_auto_clear(
            update.message,
            context,
            "编辑状态已丢失，请重新进入节点详情。",
            reply_markup=build_submenu("nodes"),
        )
        return ConversationHandler.END

    _, error_message, status_code = await controller_request(
        "PATCH",
        f"/nodes/{node_code}",
        payload={"tuic_server_name": tuic_server_name},
    )
    if error_message:
        if status_code == 404:
            context.user_data.pop(NODE_EDIT_KEY, None)
            await reply_text_with_auto_clear(
                update.message,
                context,
                f"节点不存在：{node_code}",
                reply_markup=build_submenu("nodes"),
            )
            return ConversationHandler.END
        context.user_data.pop(NODE_EDIT_KEY, None)
        await send_node_detail_message(
            update.message, context, node_code, notice=f"修改失败：{error_message}"
        )
        return ConversationHandler.END

    context.user_data.pop(NODE_EDIT_KEY, None)
    await send_node_detail_message(update.message, context, node_code, notice="TUIC证书域名已更新")
    return ConversationHandler.END


async def node_edit_agent_ip_input(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    if not update.message or not update.message.text:
        return NODE_EDIT_AGENT_IP
    raw_value = update.message.text.strip()
    if not raw_value:
        await update.message.reply_text("IP不能为空，请重新输入：")
        return NODE_EDIT_AGENT_IP
    if not is_valid_ip_address(raw_value):
        await update.message.reply_text("IP格式无效，请输入正确的IPv4/IPv6地址：")
        return NODE_EDIT_AGENT_IP

    node_code = context.user_data.get(NODE_EDIT_KEY, {}).get("node_code", "")
    if not node_code:
        await reply_text_with_auto_clear(
            update.message,
            context,
            "编辑状态已丢失，请重新进入节点详情。",
            reply_markup=build_submenu("nodes"),
        )
        return ConversationHandler.END

    context.user_data[NODE_EDIT_KEY] = {
        "node_code": node_code,
        "field": "agent_ip",
        "new_value": raw_value,
        "patch_payload": {"agent_ip": raw_value},
    }
    return await prompt_node_edit_confirmation(update.message, context)


async def prompt_node_edit_confirmation(
    message, context: ContextTypes.DEFAULT_TYPE
) -> int:
    pending_edit = context.user_data.get(NODE_EDIT_KEY, {})
    node_code = str(pending_edit.get("node_code", ""))
    field = str(pending_edit.get("field", ""))
    patch_payload = pending_edit.get("patch_payload")

    if not node_code or field not in ("host", "sni", "pool", "agent_ip") or not isinstance(
        patch_payload, dict
    ):
        context.user_data.pop(NODE_EDIT_KEY, None)
        await reply_text_with_auto_clear(
            message,
            context,
            "待确认修改数据无效，请重新进入节点详情操作。",
            reply_markup=build_submenu("nodes"),
        )
        return ConversationHandler.END

    node, node_error, node_status_code = await controller_request(
        "GET", f"/nodes/{node_code}"
    )
    if node_error:
        context.user_data.pop(NODE_EDIT_KEY, None)
        if node_status_code == 404:
            await reply_text_with_auto_clear(
                message,
                context,
                f"节点不存在：{node_code}",
                reply_markup=build_submenu("nodes"),
            )
        else:
            await reply_text_with_auto_clear(
                message,
                context,
                f"获取节点详情失败：{node_error}",
                reply_markup=build_submenu("nodes"),
            )
        return ConversationHandler.END

    stats, stats_error, _ = await controller_request("GET", f"/nodes/{node_code}/stats")
    if stats_error:
        context.user_data.pop(NODE_EDIT_KEY, None)
        await reply_text_with_auto_clear(
            message,
            context,
            f"获取节点统计失败：{stats_error}",
            reply_markup=build_submenu("nodes"),
        )
        return ConversationHandler.END

    old_value, new_value = get_node_edit_old_new_values(field, node, pending_edit)
    scope_text = get_node_edit_scope_text(field)
    bound_users = int(stats.get("bound_users", 0)) if isinstance(stats, dict) else 0
    warning_line = "确认后将影响所有已绑定该节点的用户订阅内容"
    if field == "agent_ip":
        warning_line = "确认后仅影响该节点 agent 的同步访问来源校验"

    await reply_text_with_auto_clear(
        message,
        context,
        "请确认修改：\n\n"
        f"正在修改的协议范围：{scope_text}\n"
        f"影响范围：仅该节点 {node_code}\n"
        f"影响用户数：{bound_users}（已绑定该节点的用户）\n"
        f"旧值 -> 新值：{old_value} -> {new_value}\n\n"
        f"{warning_line}",
        reply_markup=build_node_edit_confirm_keyboard(field, node_code),
    )
    return NODE_EDIT_CONFIRM


async def apply_node_edit_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    if not await ensure_admin_callback(update, ROLE_OPERATOR):
        return ConversationHandler.END
    callback_data = query.data or ""
    if not await enforce_mutation_cooldown(update, context, callback_data):
        return NODE_EDIT_CONFIRM
    await query.answer()

    user = query.from_user.username or query.from_user.id
    logger.info("button_click user=%s data=%s", user, callback_data)

    parts = callback_data.split(":", maxsplit=3)
    if len(parts) != 4:
        context.user_data.pop(NODE_EDIT_KEY, None)
        await query.edit_message_text("修改请求无效，请重新进入节点详情。", reply_markup=build_submenu("nodes"))
        return ConversationHandler.END

    field = parts[2]
    node_code = parts[3]
    pending_edit = context.user_data.get(NODE_EDIT_KEY, {})
    if (
        str(pending_edit.get("field", "")) != field
        or str(pending_edit.get("node_code", "")) != node_code
        or not isinstance(pending_edit.get("patch_payload"), dict)
    ):
        context.user_data.pop(NODE_EDIT_KEY, None)
        await render_node_detail(query, node_code, notice="待确认修改已失效，请重新发起修改。")
        return ConversationHandler.END

    _, error_message, _ = await controller_request(
        "PATCH",
        f"/nodes/{node_code}",
        payload=pending_edit["patch_payload"],
    )
    context.user_data.pop(NODE_EDIT_KEY, None)
    if error_message:
        await render_node_detail(query, node_code, notice=f"修改失败：{error_message}")
        return ConversationHandler.END

    success_notice_map = {
        "host": "入口已更新",
        "sni": "伪装域名SNI已更新",
        "pool": "TUIC端口池已更新",
        "agent_ip": "节点来源IP白名单已更新",
    }
    await render_node_detail(query, node_code, notice=success_notice_map.get(field, "修改成功"))
    return ConversationHandler.END


async def cancel_node_edit_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    await query.answer()

    callback_data = query.data or ""
    user = query.from_user.username or query.from_user.id
    logger.info("button_click user=%s data=%s", user, callback_data)

    node_code = callback_data.split(":", maxsplit=2)[2] if callback_data.count(":") >= 2 else ""
    context.user_data.pop(NODE_EDIT_KEY, None)
    if node_code:
        await render_node_detail(query, node_code)
    else:
        await query.edit_message_text("已取消", reply_markup=build_submenu("nodes"))
    return ConversationHandler.END


async def cancel_node_edit_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    node_code = context.user_data.get(NODE_EDIT_KEY, {}).get("node_code", "")
    context.user_data.pop(NODE_EDIT_KEY, None)
    if update.message:
        if node_code:
            await send_node_detail_message(update.message, context, node_code, notice="已取消")
        else:
            await reply_text_with_auto_clear(
                update.message, context, "已取消", reply_markup=build_submenu("nodes")
            )
    return ConversationHandler.END


async def start_node_reality_paste(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    if not await ensure_admin_callback(update, ROLE_OPERATOR):
        return ConversationHandler.END
    await query.answer()

    callback_data = query.data or ""
    user = query.from_user.username or query.from_user.id
    logger.info("button_click user=%s data=%s", user, callback_data)

    parts = callback_data.split(":", maxsplit=2)
    if len(parts) != 3:
        await query.edit_message_text("请求无效，请重试。", reply_markup=build_submenu("nodes"))
        return ConversationHandler.END

    node_code = parts[2]
    context.user_data[NODE_REALITY_KEY] = {"node_code": node_code}
    await query.edit_message_text(
        "请粘贴生成结果（包含 PublicKey 和 ShortID 两行）。发送 /cancel 取消。"
    )
    return NODE_REALITY_PASTE


async def node_reality_paste_input(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    if not update.message or not update.message.text:
        return NODE_REALITY_PASTE

    pending = context.user_data.get(NODE_REALITY_KEY, {})
    node_code = str(pending.get("node_code", ""))
    if not node_code:
        context.user_data.pop(NODE_REALITY_KEY, None)
        await reply_text_with_auto_clear(
            update.message,
            context,
            "配置状态已丢失，请重新进入节点详情。",
            reply_markup=build_submenu("nodes"),
        )
        return ConversationHandler.END

    public_key, short_id = extract_reality_public_key_short_id(update.message.text)
    if not public_key or not short_id:
        await update.message.reply_text(
            "解析失败，请按如下示例粘贴：\n"
            "PublicKey: xxxxxxxxxx\n"
            "ShortID: 1a2b3c4d\n\n"
            "也支持包含 Public key: ... 的输出文本。"
        )
        return NODE_REALITY_PASTE

    node, node_error, node_status_code = await controller_request(
        "GET", f"/nodes/{node_code}"
    )
    if node_error:
        context.user_data.pop(NODE_REALITY_KEY, None)
        if node_status_code == 404:
            await reply_text_with_auto_clear(
                update.message,
                context,
                f"节点不存在：{node_code}",
                reply_markup=build_submenu("nodes"),
            )
        else:
            await reply_text_with_auto_clear(
                update.message,
                context,
                f"获取节点详情失败：{node_error}",
                reply_markup=build_submenu("nodes"),
            )
        return ConversationHandler.END

    stats, stats_error, _ = await controller_request("GET", f"/nodes/{node_code}/stats")
    if stats_error:
        context.user_data.pop(NODE_REALITY_KEY, None)
        await reply_text_with_auto_clear(
            update.message,
            context,
            f"获取节点统计失败：{stats_error}",
            reply_markup=build_submenu("nodes"),
        )
        return ConversationHandler.END

    old_public_key = str(node.get("reality_public_key") or "")
    old_short_id = str(node.get("reality_short_id") or "")
    bound_users = int(stats.get("bound_users", 0)) if isinstance(stats, dict) else 0

    context.user_data[NODE_REALITY_KEY] = {
        "node_code": node_code,
        "public_key": public_key,
        "short_id": short_id,
    }

    await reply_text_with_auto_clear(
        update.message,
        context,
        "请确认保存 REALITY 参数：\n\n"
        "正在修改的协议范围：仅 VLESS+REALITY\n"
        f"影响范围：仅该节点 {node_code}\n"
        f"已绑定用户数：{bound_users}\n"
        f"public_key：{mask_key_preview(old_public_key)} -> {mask_key_preview(public_key)}\n"
        f"short_id：{old_short_id or '未设置'} -> {short_id}",
        reply_markup=build_node_reality_confirm_keyboard(node_code),
    )
    return NODE_REALITY_CONFIRM


async def apply_node_reality_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    if not await ensure_admin_callback(update, ROLE_OPERATOR):
        return ConversationHandler.END
    callback_data = query.data or ""
    if not await enforce_mutation_cooldown(update, context, callback_data):
        return NODE_REALITY_CONFIRM
    await query.answer()

    user = query.from_user.username or query.from_user.id
    logger.info("button_click user=%s data=%s", user, callback_data)

    parts = callback_data.split(":", maxsplit=2)
    if len(parts) != 3:
        context.user_data.pop(NODE_REALITY_KEY, None)
        await query.edit_message_text("请求无效，请重试。", reply_markup=build_submenu("nodes"))
        return ConversationHandler.END

    node_code = parts[2]
    pending = context.user_data.get(NODE_REALITY_KEY, {})
    pending_code = str(pending.get("node_code", ""))
    public_key = str(pending.get("public_key", ""))
    short_id = str(pending.get("short_id", ""))
    if pending_code != node_code or not public_key or not short_id:
        context.user_data.pop(NODE_REALITY_KEY, None)
        await render_node_detail(query, node_code, notice="待确认REALITY参数已失效，请重新配置。")
        return ConversationHandler.END

    _, error_message, _ = await controller_request(
        "PATCH",
        f"/nodes/{node_code}",
        payload={"reality_public_key": public_key, "reality_short_id": short_id},
    )
    context.user_data.pop(NODE_REALITY_KEY, None)
    if error_message:
        await render_node_detail(query, node_code, notice=f"保存失败：{error_message}")
        return ConversationHandler.END

    await query.edit_message_text(
        "已保存。现在该节点可生成 vless+reality 分享链接（若该节点已配置伪装域名 server_name）。",
        reply_markup=build_node_back_to_detail_keyboard(node_code),
    )
    return ConversationHandler.END


async def cancel_node_reality_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    await query.answer()

    callback_data = query.data or ""
    user = query.from_user.username or query.from_user.id
    logger.info("button_click user=%s data=%s", user, callback_data)

    context.user_data.pop(NODE_REALITY_KEY, None)
    node_code = callback_data.split(":", maxsplit=2)[2] if callback_data.count(":") >= 2 else ""
    if node_code:
        await render_node_detail(query, node_code)
    else:
        await query.edit_message_text("已取消", reply_markup=build_submenu("nodes"))
    return ConversationHandler.END


async def cancel_node_reality_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    node_code = str(context.user_data.get(NODE_REALITY_KEY, {}).get("node_code", ""))
    context.user_data.pop(NODE_REALITY_KEY, None)
    if update.message:
        if node_code:
            await send_node_detail_message(update.message, context, node_code, notice="已取消")
        else:
            await reply_text_with_auto_clear(
                update.message, context, "已取消", reply_markup=build_submenu("nodes")
            )
    return ConversationHandler.END


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return
    remember_known_chat(update, context)
    if not is_admin_chat(update):
        await deny_non_admin(update)
        return
    if query.message:
        register_main_menu_message(context, query.message.chat_id, query.message.message_id)

    callback_data = query.data or ""
    user = query.from_user.username or query.from_user.id
    logger.info("button_click user=%s data=%s", user, callback_data)

    required_role = get_required_role_for_callback(callback_data)
    current_role = get_chat_role_level(update)
    if current_role < required_role:
        if current_role <= 0:
            await deny_non_admin(update)
        else:
            await deny_insufficient_role(update, required_role)
        return

    if not await enforce_mutation_cooldown(update, context, callback_data):
        return

    if callback_data.startswith("maintain:logsdate:"):
        now_ts = time.time()
        cooldown_remaining = get_log_view_cooldown_remaining(context, now_ts=now_ts)
        if cooldown_remaining > 0:
            wait_seconds = "{0:.1f}".format(cooldown_remaining)
            await query.answer(f"日志翻页过快，请 {wait_seconds} 秒后重试", show_alert=False)
            return
        mark_log_view_action(context, now_ts=now_ts)

    if callback_data == "usernodes:manual_input":
        await start_user_nodes_manual_input(update, context)
        return

    if callback_data == "wizard:create_confirm":
        await create_user_confirm(update, context)
        return

    if callback_data == "wizard:nodes_create_confirm":
        await nodes_create_confirm(update, context)
        return

    if callback_data == "wizard:cancel":
        if context.user_data.get(NODES_WIZARD_KEY):
            await cancel_nodes_create_callback(update, context)
            return
        if context.user_data.get(WIZARD_KEY):
            await cancel_create_user_callback(update, context)
            return

        context.user_data.pop(WIZARD_KEY, None)
        context.user_data.pop(NODES_WIZARD_KEY, None)
        await query.answer()
        await query.edit_message_text("已取消", reply_markup=build_main_menu())
        return

    await query.answer()

    if callback_data == "menu:main":
        await query.edit_message_text("主菜单", reply_markup=build_main_menu())
        return

    if callback_data.startswith("menu:"):
        submenu_key = callback_data.split(":", maxsplit=1)[1]
        submenu = SUBMENUS.get(submenu_key)
        if not submenu:
            await query.edit_message_text(
                "主菜单", reply_markup=build_main_menu()
            )
            return
        await query.edit_message_text(
            submenu["title"], reply_markup=build_submenu(submenu_key)
        )
        return

    if callback_data == "action:speed_switch":
        await render_user_limit_mode_picker(query)
        return

    if callback_data.startswith("usermode:pick:"):
        parts = callback_data.split(":", maxsplit=2)
        if len(parts) != 3:
            await query.edit_message_text("请求无效，请重试。", reply_markup=build_submenu("speed"))
            return
        user_code = parts[2]
        user_data, error_message, status_code = await controller_request("GET", f"/users/{user_code}")
        if error_message:
            localized = localize_controller_error(error_message)
            if status_code == 404 and localized == "用户不存在":
                await query.edit_message_text("用户不存在", reply_markup=build_submenu("speed"))
                return
            await query.edit_message_text(
                f"获取用户信息失败：{localized}",
                reply_markup=build_submenu("speed"),
            )
            return
        user_nodes, nodes_error, nodes_status = await controller_request(
            "GET", f"/users/{user_code}/nodes"
        )
        if nodes_error:
            localized = localize_controller_error(nodes_error)
            if nodes_status == 404 and localized == "用户不存在":
                await query.edit_message_text("用户不存在", reply_markup=build_submenu("speed"))
                return
            await query.edit_message_text(
                f"获取用户节点失败：{localized}",
                reply_markup=build_submenu("speed"),
            )
            return
        display_name = str(user_data.get("display_name") or "").strip()
        user_label = f"{display_name}（{user_code}）" if display_name else user_code
        current_mode = normalize_limit_mode(str(user_data.get("limit_mode", "tc")))
        target_mode = "off" if current_mode == "tc" else "tc"
        current_mode_label = format_limit_mode_label(current_mode)
        target_mode_label = format_limit_mode_label(target_mode)
        bound_count = len(user_nodes) if isinstance(user_nodes, list) else 0
        await query.edit_message_text(
            "请确认切换限速模式：\n\n"
            f"用户：{user_label}\n"
            f"当前模式：{current_mode_label}\n"
            f"目标模式：{target_mode_label}\n"
            f"影响节点数：{bound_count}\n"
            "说明：off 模式将停止该用户的节点侧限速生效（speed_mbps 视为 0）。\n"
            "确认后节点会在下一轮轮询收敛。",
            reply_markup=build_user_limit_mode_confirm_keyboard(user_code, target_mode),
        )
        return

    if callback_data.startswith("usermode:apply:"):
        parts = callback_data.split(":", maxsplit=3)
        if len(parts) != 4:
            await query.edit_message_text("请求无效，请重试。", reply_markup=build_submenu("speed"))
            return
        user_code = parts[2]
        target_mode = normalize_limit_mode(parts[3])
        _, error_message, status_code = await controller_request(
            "POST",
            f"/users/{user_code}/set_limit_mode",
            payload={"limit_mode": target_mode},
        )
        if error_message:
            localized = localize_controller_error(error_message)
            if status_code == 404 and localized == "用户不存在":
                await query.edit_message_text("用户不存在", reply_markup=build_submenu("speed"))
                return
            await query.edit_message_text(
                f"切换限速模式失败：{localized}",
                reply_markup=build_submenu("speed"),
            )
            return
        bindings, _, _ = await controller_request("GET", f"/users/{user_code}/nodes")
        bound_count = len(bindings) if isinstance(bindings, list) else 0
        await query.edit_message_text(
            f"切换限速模式成功：{user_code} -> {format_limit_mode_label(target_mode)}\n"
            f"影响节点数：{bound_count}",
            reply_markup=build_submenu("speed"),
        )
        return

    if callback_data == "action:query_user_info":
        await render_query_user_picker(query)
        return

    if callback_data == "action:user_delete":
        await render_user_delete_picker(query)
        return

    if callback_data.startswith("userdelete:pick:"):
        parts = callback_data.split(":", maxsplit=2)
        if len(parts) != 3:
            await query.edit_message_text("请求无效，请重试。", reply_markup=build_submenu("user"))
            return
        user_code = parts[2]
        user_data, error_message, status_code = await controller_request("GET", f"/users/{user_code}")
        if error_message:
            localized = localize_controller_error(error_message)
            if status_code == 404 and localized == "用户不存在":
                await query.edit_message_text("用户不存在", reply_markup=build_submenu("user"))
                return
            await query.edit_message_text(
                f"获取用户信息失败：{localized}",
                reply_markup=build_submenu("user"),
            )
            return
        display_name = str(user_data.get("display_name") or "").strip()
        status_text = str(user_data.get("status", "-"))
        user_label = f"{display_name}（{user_code}）" if display_name else user_code
        user_nodes, nodes_error, nodes_status = await controller_request(
            "GET", f"/users/{user_code}/nodes"
        )
        if nodes_error:
            localized = localize_controller_error(nodes_error)
            if nodes_status == 404 and localized == "用户不存在":
                await query.edit_message_text("用户不存在", reply_markup=build_submenu("user"))
                return
            await query.edit_message_text(
                f"获取用户绑定节点失败：{localized}",
                reply_markup=build_submenu("user"),
            )
            return
        bound_nodes = user_nodes if isinstance(user_nodes, list) else []
        bound_count = len(bound_nodes)
        node_lines = []
        for item in bound_nodes[:8]:
            node_code = str(item.get("node_code", ""))
            region = str(item.get("region", "-"))
            node_lines.append(f"- {node_code}（{region}）")
        bound_text = "\n".join(node_lines) if node_lines else "（无）"
        await query.edit_message_text(
            "请确认删除用户：\n\n"
            f"用户：{user_label}\n"
            f"状态：{status_text}\n"
            f"已绑定节点数：{bound_count}\n"
            f"绑定节点：\n{bound_text}\n\n"
            "注意：若该用户仍有节点绑定，删除会失败。建议先在“节点分配”中解绑。",
            reply_markup=build_user_delete_confirm_keyboard(user_code),
        )
        return

    if callback_data.startswith("userdelete:apply:"):
        parts = callback_data.split(":", maxsplit=2)
        if len(parts) != 3:
            await query.edit_message_text("请求无效，请重试。", reply_markup=build_submenu("user"))
            return
        user_code = parts[2]
        user_nodes, nodes_error, nodes_status = await controller_request(
            "GET", f"/users/{user_code}/nodes"
        )
        if nodes_error:
            localized = localize_controller_error(nodes_error)
            if nodes_status == 404 and localized == "用户不存在":
                await query.edit_message_text("用户不存在", reply_markup=build_submenu("user"))
                return
            await query.edit_message_text(
                f"删除前检查失败：{localized}",
                reply_markup=build_submenu("user"),
            )
            return
        bound_nodes = user_nodes if isinstance(user_nodes, list) else []
        if bound_nodes:
            preview = ", ".join(str(item.get("node_code", "")) for item in bound_nodes[:8])
            await query.edit_message_text(
                "删除已拦截：该用户仍有绑定节点。\n\n"
                f"用户：{user_code}\n"
                f"绑定数量：{len(bound_nodes)}\n"
                f"节点：{preview}\n\n"
                "请先在“用户管理 -> 节点分配”中解绑后再删除。",
                reply_markup=build_submenu("user"),
            )
            return
        result, error_message, status_code = await controller_request("DELETE", f"/users/{user_code}")
        if error_message:
            localized = localize_controller_error(error_message)
            if status_code == 404 and localized == "用户不存在":
                await query.edit_message_text("用户不存在", reply_markup=build_submenu("user"))
                return
            await query.edit_message_text(
                f"删除失败：{localized}",
                reply_markup=build_submenu("user"),
            )
            return
        if isinstance(result, dict) and bool(result.get("ok")):
            await query.edit_message_text(
                f"删除成功：{user_code}",
                reply_markup=build_submenu("user"),
            )
            return
        await query.edit_message_text("删除结果异常，请重试。", reply_markup=build_submenu("user"))
        return

    if callback_data == "action:user_toggle":
        await render_user_toggle_picker(query)
        return

    if callback_data.startswith("usertoggle:pick:"):
        parts = callback_data.split(":", maxsplit=2)
        if len(parts) != 3:
            await query.edit_message_text("请求无效，请重试。", reply_markup=build_submenu("user"))
            return
        user_code = parts[2]
        user_data, error_message, status_code = await controller_request("GET", f"/users/{user_code}")
        if error_message:
            localized = localize_controller_error(error_message)
            if status_code == 404 and localized == "用户不存在":
                await query.edit_message_text("用户不存在", reply_markup=build_submenu("user"))
                return
            await query.edit_message_text(
                f"获取用户信息失败：{localized}",
                reply_markup=build_submenu("user"),
            )
            return
        display_name = str(user_data.get("display_name") or "").strip()
        current_status = str(user_data.get("status", "active")).strip().lower()
        target_status = "disabled" if current_status == "active" else "active"
        target_text = "禁用" if target_status == "disabled" else "启用"
        user_label = f"{display_name}（{user_code}）" if display_name else user_code
        user_nodes, nodes_error, nodes_status = await controller_request(
            "GET", f"/users/{user_code}/nodes"
        )
        if nodes_error:
            localized = localize_controller_error(nodes_error)
            if nodes_status == 404 and localized == "用户不存在":
                await query.edit_message_text("用户不存在", reply_markup=build_submenu("user"))
                return
            await query.edit_message_text(
                f"获取用户绑定节点失败：{localized}",
                reply_markup=build_submenu("user"),
            )
            return
        bound_count = len(user_nodes if isinstance(user_nodes, list) else [])
        await query.edit_message_text(
            "请确认用户状态变更：\n\n"
            f"用户：{user_label}\n"
            f"当前状态：{current_status}\n"
            f"目标状态：{target_status}（{target_text}）\n"
            f"影响范围：该用户已绑定的 {bound_count} 个节点\n\n"
            "提示：状态变更会影响该用户的订阅可用性。",
            reply_markup=build_user_toggle_confirm_keyboard(user_code, target_status),
        )
        return

    if callback_data.startswith("usertoggle:apply:"):
        parts = callback_data.split(":", maxsplit=3)
        if len(parts) != 4:
            await query.edit_message_text("请求无效，请重试。", reply_markup=build_submenu("user"))
            return
        user_code = parts[2]
        target_status = parts[3]
        _, error_message, status_code = await controller_request(
            "POST",
            f"/users/{user_code}/set_status",
            payload={"status": target_status},
        )
        if error_message:
            localized = localize_controller_error(error_message)
            if status_code == 404 and localized == "用户不存在":
                await query.edit_message_text("用户不存在", reply_markup=build_submenu("user"))
                return
            await query.edit_message_text(
                f"状态更新失败：{localized}",
                reply_markup=build_submenu("user"),
            )
            return
        status_text = "disabled（禁用）" if target_status == "disabled" else "active（启用）"
        await query.edit_message_text(
            f"状态更新成功：{user_code} -> {status_text}",
            reply_markup=build_submenu("user"),
        )
        return

    if callback_data.startswith("query:user:"):
        parts = callback_data.split(":", maxsplit=2)
        if len(parts) != 3:
            await query.edit_message_text("请求无效，请重试。", reply_markup=build_submenu("query"))
            return
        await render_query_user_detail(query, parts[2])
        return

    if callback_data == "action:query_expiring":
        users, error_message, _ = await controller_request("GET", "/users")
        if error_message:
            await query.edit_message_text(
                f"获取用户列表失败：{localize_controller_error(error_message)}",
                reply_markup=build_submenu("query"),
            )
            return

        now_ts = int(time.time())
        deadline = now_ts + 7 * 86400
        expiring_users = []
        for item in users or []:
            status_text = str(item.get("status", "")).lower()
            try:
                expire_at = int(item.get("expire_at", 0) or 0)
            except (TypeError, ValueError):
                expire_at = 0
            if status_text == "active" and now_ts < expire_at <= deadline:
                expiring_users.append(item)

        expiring_users.sort(key=lambda x: int(x.get("expire_at", 0) or 0))
        lines = ["即将到期（未来 7 天内，最多显示 20 条）："]
        if expiring_users:
            for item in expiring_users[:20]:
                user_code = str(item.get("user_code", ""))
                display_name = str(item.get("display_name") or "").strip()
                name_text = display_name if display_name else "-"
                expire_at = int(item.get("expire_at", 0) or 0)
                expire_text = datetime.fromtimestamp(expire_at).strftime("%Y-%m-%d %H:%M:%S")
                lines.append(f"{user_code} | {name_text} | {expire_text}")
        else:
            lines.append("（未来 7 天内无即将到期的 active 用户）")

        await query.edit_message_text(
            "\n".join(lines),
            reply_markup=build_back_keyboard("menu:query"),
        )
        return

    if callback_data == "action:query_traffic":
        ranking_payload, error_message, _ = await controller_request(
            "GET",
            "/admin/traffic/ranking?limit=20",
        )
        if error_message:
            await query.edit_message_text(
                f"获取流量排行失败：{localize_controller_error(error_message)}",
                reply_markup=build_back_keyboard("menu:query"),
            )
            return
        if not isinstance(ranking_payload, dict):
            await query.edit_message_text(
                "获取流量排行失败：响应格式异常",
                reply_markup=build_back_keyboard("menu:query"),
            )
            return
        await query.edit_message_text(
            format_query_traffic_ranking_text(ranking_payload),
            reply_markup=build_back_keyboard("menu:query"),
        )
        return

    if callback_data == "action:backup_now":
        await run_admin_backup_action(query, "menu:backup")
        return

    if callback_data == "action:maintain_backup":
        await run_admin_backup_action(query, "menu:maintain")
        return

    if callback_data == "action:maintain_smoke":
        await run_admin_smoke_action(query, "menu:maintain")
        return

    if callback_data == "action:maintain_log_archive":
        await run_admin_log_archive_action(query, "menu:maintain")
        return

    if callback_data == "action:maintain_sync_node_defaults":
        await run_admin_sync_node_defaults_action(query)
        return

    if callback_data == "action:maintain_sync_node_tokens":
        await run_admin_sync_node_tokens_action(query)
        return

    if callback_data == "action:maintain_sync_node_time":
        await run_admin_sync_node_time_action(query)
        return

    if callback_data == "action:maintain_update":
        update_cmd = "cd {0} && bash {1} --reuse-config".format(
            shlex.quote(ADMIN_PROJECT_DIR),
            shlex.quote(ADMIN_UPDATE_SCRIPT),
        )
        log_path = launch_background_job(update_cmd, "maintain-update")
        await query.edit_message_text(
            "管理服务器安装/更新任务已启动（后台执行）。\n"
            "作用范围：仅当前这台运行 sb-controller/sb-bot 的管理服务器。\n"
            f"日志文件：{log_path}\n\n"
            "说明：该任务会拉取更新、校验依赖并重启服务。",
            reply_markup=build_back_keyboard("menu:maintain"),
        )
        return

    if callback_data == "action:maintain_controller_start":
        code, stdout, stderr = await run_local_shell("systemctl start sb-controller", timeout=20)
        if code == 0:
            await query.edit_message_text(
                "已执行：启动 controller。",
                reply_markup=build_back_keyboard("menu:maintain"),
            )
        else:
            await query.edit_message_text(
                "启动 controller 失败：\n{0}".format(truncate_output(stderr or stdout)),
                reply_markup=build_back_keyboard("menu:maintain"),
            )
        return

    if callback_data == "action:maintain_controller_stop":
        code, stdout, stderr = await run_local_shell("systemctl stop sb-controller", timeout=20)
        if code == 0:
            await query.edit_message_text(
                "已执行：停止 controller。\n可通过“启动controller”恢复。",
                reply_markup=build_back_keyboard("menu:maintain"),
            )
        else:
            await query.edit_message_text(
                "停止 controller 失败：\n{0}".format(truncate_output(stderr or stdout)),
                reply_markup=build_back_keyboard("menu:maintain"),
            )
        return

    if callback_data == "action:maintain_migrate_export":
        await run_admin_migrate_export_action(query, "menu:maintain")
        return

    if callback_data == "action:maintain_logs":
        await query.edit_message_text(
            "请选择要查看的服务日志：",
            reply_markup=build_maintain_logs_keyboard(),
        )
        return

    if callback_data == "action:maintain_ops_audit":
        audit_rows, error_message, _ = await controller_request(
            "GET",
            "/admin/audit?limit=120&action_prefix=ops.&window_seconds={0}".format(
                int(OPS_AUDIT_WINDOW_SECONDS)
            ),
        )
        if error_message:
            await query.edit_message_text(
                "获取运维审计失败：{0}".format(localize_controller_error(error_message)),
                reply_markup=build_back_keyboard("menu:maintain_ops"),
            )
            return
        text = (
            "运维审计窗口：最近 {0} 秒\n\n".format(int(OPS_AUDIT_WINDOW_SECONDS))
            + format_ops_audit_text(audit_rows if isinstance(audit_rows, list) else [])
        )
        await query.edit_message_text(
            text,
            reply_markup=build_maintain_ops_audit_keyboard(),
        )
        return

    if callback_data.startswith("maintain:logs:"):
        parts = callback_data.split(":", maxsplit=2)
        if len(parts) != 3:
            await query.edit_message_text("请求无效，请重试。", reply_markup=build_back_keyboard("menu:maintain"))
            return
        target = parts[2]
        unit = get_maintain_log_unit(target)
        if not unit:
            await query.edit_message_text("不支持的日志目标。", reply_markup=build_back_keyboard("menu:maintain"))
            return

        code, stdout, stderr = await run_local_shell(
            "journalctl -u {0} --since '14 days ago' --output=short-iso --no-pager".format(
                shlex.quote(unit)
            ),
            timeout=30,
        )
        if code != 0:
            await query.edit_message_text(
                "读取日志失败：\n{0}".format(truncate_output(stderr or stdout)),
                reply_markup=build_back_keyboard("action:maintain_logs"),
            )
            return

        date_keys = extract_log_date_keys(stdout, max_dates=7)
        if not date_keys:
            await query.edit_message_text(
                "{0} 近 14 天无可用日志。".format(unit),
                reply_markup=build_back_keyboard("action:maintain_logs"),
            )
            return

        await query.edit_message_text(
            "{0} 日志日期选择（服务器本地时区）：\n"
            "规则：按一级→二级→三级排序；每页 50 条，可翻页查看全部重要日志。".format(unit),
            reply_markup=build_maintain_log_date_keyboard(target, date_keys),
        )
        return

    if callback_data.startswith("maintain:logsdate:"):
        parts = callback_data.split(":")
        if len(parts) not in (4, 5):
            await query.edit_message_text("请求无效，请重试。", reply_markup=build_back_keyboard("action:maintain_logs"))
            return
        target = parts[2]
        date_key = parts[3]
        page = 1
        if len(parts) == 5:
            if not str(parts[4]).isdigit():
                await query.edit_message_text(
                    "分页参数无效，请重新选择。",
                    reply_markup=build_back_keyboard(f"maintain:logs:{target}"),
                )
                return
            page = int(parts[4])
        if not re.fullmatch(r"\d{8}", date_key):
            await query.edit_message_text(
                "日期参数无效，请重新选择。",
                reply_markup=build_back_keyboard(f"maintain:logs:{target}"),
            )
            return

        try:
            datetime.strptime(date_key, "%Y%m%d")
        except ValueError:
            await query.edit_message_text(
                "日期参数无效，请重新选择。",
                reply_markup=build_back_keyboard(f"maintain:logs:{target}"),
            )
            return

        unit = get_maintain_log_unit(target)
        if not unit:
            await query.edit_message_text("不支持的日志目标。", reply_markup=build_back_keyboard("action:maintain_logs"))
            return

        day_text = "{0}-{1}-{2}".format(date_key[0:4], date_key[4:6], date_key[6:8])
        code, stdout, stderr = await run_local_shell(
            "journalctl -u {0} --since '{1} 00:00:00' --until '{1} 23:59:59' --output=short-iso --no-pager".format(
                shlex.quote(unit), day_text
            ),
            timeout=30,
        )
        if code != 0:
            await query.edit_message_text(
                "读取日志失败：\n{0}".format(truncate_output(stderr or stdout)),
                reply_markup=build_back_keyboard(f"maintain:logs:{target}"),
            )
            return

        entries, level1_count, level2_count, level3_count = build_priority_log_entries(stdout)
        pages = build_log_pages(entries, max_lines_per_page=50, max_chars_per_page=3200)
        pages_truncated = False
        if len(pages) > LOG_VIEW_MAX_PAGES:
            pages = pages[:LOG_VIEW_MAX_PAGES]
            pages_truncated = True
        total_pages = len(pages)
        safe_page = 1 if page < 1 else page
        if safe_page > total_pages:
            safe_page = total_pages
        page_entries = pages[safe_page - 1] if pages else ["(当日无日志)"]
        recent_logs = "\n".join(page_entries) if page_entries else "(当日无日志)"
        truncated_notice = ""
        if pages_truncated:
            truncated_notice = (
                "\n提示：日志页数过多，当前仅展示前 {0} 页。"
                "\n如需全量查看请在服务器执行：journalctl -u {1} --since '{2} 00:00:00' --until '{2} 23:59:59' --no-pager".format(
                    LOG_VIEW_MAX_PAGES, unit, day_text
                )
            )
        await query.edit_message_text(
            "{0} {1} 重要日志（按一级→二级→三级，时间倒序，尽量完整显示）：\n"
            "一级：{2} 条，二级：{3} 条，三级：{4} 条\n"
            "当前页：{5}/{6}（每页最多 50 条，且受消息长度限制）{8}\n\n{7}".format(
                unit,
                day_text,
                level1_count,
                level2_count,
                level3_count,
                safe_page,
                total_pages,
                recent_logs,
                truncated_notice,
            ),
            reply_markup=build_maintain_log_result_keyboard(
                target, date_key, safe_page, total_pages
            ),
        )
        return

    if callback_data == "action:maintain_status":
        ctl_code, ctl_out, _ = await run_local_shell("systemctl is-active sb-controller", timeout=15)
        bot_code, bot_out, _ = await run_local_shell("systemctl is-active sb-bot", timeout=15)
        caddy_code, caddy_out, _ = await run_local_shell("systemctl is-active caddy", timeout=15)
        health, error_message, _ = await controller_request("GET", "/health")
        overview, overview_error, _ = await controller_request("GET", "/admin/overview")
        health_text = "异常"
        if not error_message and isinstance(health, dict) and health.get("ok"):
            health_text = "正常"
        if overview_error:
            overview_text = "控制面概览获取失败：{0}".format(
                localize_controller_error(overview_error)
            )
        else:
            overview_text = format_admin_overview_text(overview if isinstance(overview, dict) else {})
        await query.edit_message_text(
            "服务状态检查：\n"
            f"controller(systemd)：{(ctl_out or '').strip() if ctl_code == 0 else 'unknown'}\n"
            f"bot(systemd)：{(bot_out or '').strip() if bot_code == 0 else 'unknown'}\n"
            f"caddy(systemd)：{(caddy_out or '').strip() if caddy_code == 0 else 'unknown'}\n"
            f"controller /health：{health_text}\n\n"
            f"{overview_text}",
            reply_markup=build_back_keyboard("menu:maintain"),
        )
        return

    if callback_data.startswith("sb:mc:"):
        parts = callback_data.split(":", maxsplit=2)
        if len(parts) != 3:
            await query.edit_message_text(
                "手动清理参数无效。",
                reply_markup=build_back_keyboard("menu:maintain"),
            )
            return
        include_local = parts[2] == "1"
        await run_admin_security_maintenance_cleanup_action(
            query, include_local=include_local
        )
        return

    if callback_data.startswith("sb:ab:"):
        parts = callback_data.split(":", maxsplit=2)
        if len(parts) != 3:
            await query.edit_message_text(
                "自动封禁参数无效。",
                reply_markup=build_back_keyboard("menu:maintain"),
            )
            return
        include_local = parts[2] == "1"
        await run_admin_security_auto_block_action(
            query, include_local=include_local
        )
        return

    if callback_data.startswith("sb:bl:"):
        parts = callback_data.split(":")
        if len(parts) < 3:
            await query.edit_message_text(
                "封禁列表请求参数无效。",
                reply_markup=build_back_keyboard("menu:maintain"),
            )
            return
        include_local = parts[2] == "1"
        page = 1
        if len(parts) >= 4:
            try:
                page = int(parts[3])
            except ValueError:
                page = 1
        await run_admin_security_block_list_action(
            query, include_local=include_local, page=page
        )
        return

    if callback_data.startswith("sb:bi:"):
        parts = callback_data.split(":")
        # 新格式：sb:bi:{mode}:{ip_token}
        # 兼容旧格式：sb:bi:{duration}:{mode}:{ip_token}
        if len(parts) not in (4, 5):
            await query.edit_message_text(
                "封禁请求参数无效。",
                reply_markup=build_back_keyboard("menu:maintain"),
            )
            return

        if len(parts) == 4:
            include_local = parts[2] == "1"
            source_ip = decode_ip_token(parts[3])
            if not source_ip:
                await query.edit_message_text(
                    "封禁请求参数无效。",
                    reply_markup=build_back_keyboard("menu:maintain"),
                )
                return
            await query.edit_message_text(
                "请选择封禁时长：\n\n"
                f"目标IP：{source_ip}\n"
                f"作用范围：controller 端口 {CONTROLLER_PORT_HINT}/tcp",
                reply_markup=build_security_duration_keyboard(
                    include_local=include_local,
                    source_ip=source_ip,
                ),
            )
            return

        try:
            duration_seconds = int(parts[2])
        except ValueError:
            duration_seconds = -1
        include_local = parts[3] == "1"
        source_ip = decode_ip_token(parts[4])
        if duration_seconds < 0 or not source_ip:
            await query.edit_message_text(
                "封禁请求参数无效。",
                reply_markup=build_back_keyboard("menu:maintain"),
            )
            return
        if duration_seconds == 0:
            duration_text = "永久"
        elif duration_seconds % 86400 == 0:
            duration_text = f"{duration_seconds // 86400} 天"
        elif duration_seconds % 3600 == 0:
            duration_text = f"{duration_seconds // 3600} 小时"
        else:
            duration_text = f"{duration_seconds} 秒"
        await query.edit_message_text(
            "请确认封禁来源 IP：\n\n"
            f"IP：{source_ip}\n"
            f"时长：{duration_text}\n"
            f"作用范围：controller 端口 {CONTROLLER_PORT_HINT}/tcp\n\n"
            "提示：仅建议封禁明确的扫描来源。",
            reply_markup=build_security_block_confirm_keyboard(
                include_local=include_local,
                duration_seconds=duration_seconds,
                source_ip=source_ip,
            ),
        )
        return

    if callback_data.startswith("sb:bd:"):
        parts = callback_data.split(":", maxsplit=4)
        if len(parts) != 5:
            await query.edit_message_text(
                "封禁时长参数无效。",
                reply_markup=build_back_keyboard("menu:maintain"),
            )
            return
        try:
            duration_seconds = int(parts[2])
        except ValueError:
            duration_seconds = -1
        include_local = parts[3] == "1"
        source_ip = decode_ip_token(parts[4])
        if duration_seconds < 0 or not source_ip:
            await query.edit_message_text(
                "封禁时长参数无效。",
                reply_markup=build_back_keyboard("menu:maintain"),
            )
            return
        if duration_seconds == 0:
            duration_text = "永久"
        elif duration_seconds % 86400 == 0:
            duration_text = f"{duration_seconds // 86400} 天"
        elif duration_seconds % 3600 == 0:
            duration_text = f"{duration_seconds // 3600} 小时"
        else:
            duration_text = f"{duration_seconds} 秒"
        await query.edit_message_text(
            "请确认封禁来源 IP：\n\n"
            f"IP：{source_ip}\n"
            f"时长：{duration_text}\n"
            f"作用范围：controller 端口 {CONTROLLER_PORT_HINT}/tcp\n\n"
            "提示：仅建议封禁明确的扫描来源。",
            reply_markup=build_security_block_confirm_keyboard(
                include_local=include_local,
                duration_seconds=duration_seconds,
                source_ip=source_ip,
            ),
        )
        return

    if callback_data.startswith("sb:ba:"):
        parts = callback_data.split(":", maxsplit=4)
        if len(parts) != 5:
            await query.edit_message_text(
                "封禁请求参数无效。",
                reply_markup=build_back_keyboard("menu:maintain"),
            )
            return
        try:
            duration_seconds = int(parts[2])
        except ValueError:
            duration_seconds = -1
        include_local = parts[3] == "1"
        source_ip = decode_ip_token(parts[4])
        if duration_seconds < 0 or not source_ip:
            await query.edit_message_text(
                "封禁请求参数无效。",
                reply_markup=build_back_keyboard("menu:maintain"),
            )
            return
        await run_admin_security_block_ip_action(
            query,
            include_local=include_local,
            source_ip=source_ip,
            duration_seconds=duration_seconds,
        )
        return

    if callback_data.startswith("sb:bu:"):
        parts = callback_data.split(":", maxsplit=3)
        if len(parts) != 4:
            await query.edit_message_text(
                "解封请求参数无效。",
                reply_markup=build_back_keyboard("menu:maintain"),
            )
            return
        include_local = parts[2] == "1"
        source_ip = decode_ip_token(parts[3])
        if not source_ip:
            await query.edit_message_text(
                "解封请求参数无效。",
                reply_markup=build_back_keyboard("menu:maintain"),
            )
            return
        await run_admin_security_unblock_ip_action(
            query,
            include_local=include_local,
            source_ip=source_ip,
        )
        return

    if callback_data == "action:maintain_security_events":
        await run_admin_security_events_action(query, include_local=False)
        return

    if callback_data == "action:maintain_security_events_local":
        await run_admin_security_events_action(query, include_local=True)
        return

    if callback_data == "action:maintain_sub_policy":
        await run_admin_sub_policy_panel_action(query)
        return

    if callback_data.startswith("maintain:subpolicy:"):
        mode = callback_data.split(":", maxsplit=2)[2] if callback_data.count(":") >= 2 else ""
        await apply_admin_sub_policy_action(query, mode)
        return

    if callback_data == "action:maintain_https_status":
        active_code, active_out, active_err = await run_local_shell("systemctl is-active caddy", timeout=15)
        validate_code, validate_out, validate_err = await run_local_shell(
            "caddy validate --config /etc/caddy/Caddyfile", timeout=20
        )
        journal_code, journal_out, journal_err = await run_local_shell(
            "journalctl -u caddy -n 40 --no-pager", timeout=20
        )
        status_text = (active_out or "").strip() if active_code == 0 else (active_err or "unknown")
        validate_text = "通过" if validate_code == 0 else "失败"
        details = validate_out if validate_code == 0 else (validate_err or validate_out)
        journal_text = journal_out if journal_code == 0 else (journal_err or "")
        journal_recent = format_recent_log_output(journal_text, line_limit=50, char_limit=1800)
        await query.edit_message_text(
            "HTTPS 证书状态（Caddy）：\n"
            f"服务状态：{status_text}\n"
            f"配置校验：{validate_text}\n"
            f"校验输出：{truncate_output(details, 900)}\n\n"
            "最近日志：\n"
            f"{journal_recent}",
            reply_markup=build_back_keyboard("menu:maintain"),
        )
        return

    if callback_data == "action:maintain_https_reload":
        reload_code, reload_out, reload_err = await run_local_shell(
            "bash -lc 'caddy validate --config /etc/caddy/Caddyfile && systemctl reload caddy'",
            timeout=30,
        )
        if reload_code == 0:
            await query.edit_message_text(
                "已执行 HTTPS 证书刷新（caddy reload）。",
                reply_markup=build_back_keyboard("menu:maintain"),
            )
            return
        restart_code, restart_out, restart_err = await run_local_shell(
            "systemctl restart caddy",
            timeout=30,
        )
        if restart_code == 0:
            await query.edit_message_text(
                "reload 失败，已自动执行 caddy restart 并成功。",
                reply_markup=build_back_keyboard("menu:maintain"),
            )
            return
        await query.edit_message_text(
            "HTTPS 证书刷新失败：\n{0}".format(
                truncate_output(reload_err or reload_out or restart_err or restart_out)
            ),
            reply_markup=build_back_keyboard("menu:maintain"),
        )
        return

    if callback_data == "action:maintain_acl_status":
        await run_admin_node_access_status_action(query, "menu:maintain")
        return

    if callback_data == "action:maintain_token_collapse":
        if not os.path.exists(ADMIN_TOKEN_COLLAPSE_SCRIPT):
            await query.edit_message_text(
                "未找到收敛脚本：{0}".format(ADMIN_TOKEN_COLLAPSE_SCRIPT),
                reply_markup=build_back_keyboard("menu:maintain"),
            )
            return
        await query.edit_message_text(
            "准备收敛 AUTH_TOKEN（新,旧 -> 新）。\n"
            "影响：controller/bot 会重启；若节点仍使用旧 token，会暂时无法同步。\n"
            "请确认节点已更新到新 token 后再执行。",
            reply_markup=build_maintain_token_collapse_confirm_keyboard(),
        )
        return

    if callback_data == "maintain:token_collapse:confirm":
        collapse_cmd = "bash {0} --yes".format(shlex.quote(ADMIN_TOKEN_COLLAPSE_SCRIPT))
        log_path = launch_background_job(collapse_cmd, "maintain-token-collapse")
        await query.edit_message_text(
            "AUTH_TOKEN 收敛任务已启动（后台执行）。\n"
            f"日志文件：{log_path}\n\n"
            "执行完成后可在“安全事件(1h)”或“状态查看”中确认 token 数量是否变为 1。",
            reply_markup=build_back_keyboard("menu:maintain"),
        )
        return

    if callback_data == "action:backup_audit":
        audit_rows, error_message, _ = await controller_request("GET", "/admin/audit?limit=50")
        if error_message:
            await query.edit_message_text(
                f"获取操作日志失败：{localize_controller_error(error_message)}",
                reply_markup=build_back_keyboard("menu:backup"),
            )
            return
        text = format_admin_audit_text(audit_rows if isinstance(audit_rows, list) else [])
        await query.edit_message_text(
            text,
            reply_markup=build_backup_audit_keyboard(),
        )
        return

    if callback_data == "action:backup_stop":
        await run_emergency_disable_users_preview(query)
        return

    if callback_data == "backup:stop:confirm":
        await run_emergency_disable_users_apply(query)
        return

    if callback_data == "backup:stop:cancel":
        await query.edit_message_text(
            SUBMENUS["backup"]["title"],
            reply_markup=build_submenu("backup"),
        )
        return

    if callback_data == "action:nodes_list":
        await render_nodes_list(query)
        return

    if callback_data == "action:node_ops":
        await render_node_ops_picker(query)
        return

    if callback_data.startswith("nodeops:panel:"):
        parts = callback_data.split(":", maxsplit=2)
        if len(parts) != 3:
            await query.edit_message_text("请求无效，请重试。", reply_markup=build_submenu("nodes"))
            return
        await render_node_ops_panel(query, parts[2])
        return

    if callback_data.startswith("nodeops:run:"):
        parts = callback_data.split(":", maxsplit=3)
        if len(parts) != 4:
            await query.edit_message_text("请求无效，请重试。", reply_markup=build_submenu("nodes"))
            return
        node_code = parts[2]
        action_key = parts[3]
        await run_node_ops_action(query, node_code, action_key)
        return

    if callback_data.startswith("nodeops:tasks:"):
        parts = callback_data.split(":", maxsplit=2)
        if len(parts) != 3:
            await query.edit_message_text("请求无效，请重试。", reply_markup=build_submenu("nodes"))
            return
        await render_node_ops_tasks(query, parts[2])
        return

    if callback_data.startswith("node:detail:"):
        node_code = callback_data.split(":", maxsplit=2)[2]
        await render_node_detail(query, node_code)
        return

    if callback_data.startswith("node:sync_preview:"):
        node_code = callback_data.split(":", maxsplit=2)[2]
        await render_node_sync_preview(query, node_code)
        return

    if callback_data.startswith("node:reality_setup:"):
        node_code = callback_data.split(":", maxsplit=2)[2]
        await render_node_reality_setup(query, node_code)
        return

    if callback_data.startswith("node:toggle:"):
        node_code = callback_data.split(":", maxsplit=2)[2]
        node, error_message, status_code = await controller_request(
            "GET", f"/nodes/{node_code}"
        )
        if error_message:
            if status_code == 404:
                await render_nodes_list(query, notice=f"节点不存在：{node_code}")
            else:
                await query.edit_message_text(
                    f"切换状态失败：{error_message}",
                    reply_markup=build_submenu("nodes"),
                )
            return

        current_enabled = int(node.get("enabled", 0))
        next_enabled = 0 if current_enabled == 1 else 1
        _, patch_error, _ = await controller_request(
            "PATCH",
            f"/nodes/{node_code}",
            payload={"enabled": next_enabled},
        )
        if patch_error:
            await render_node_detail(query, node_code, notice=f"切换状态失败：{patch_error}")
            return
        await render_node_detail(query, node_code, notice="状态已更新")
        return

    if callback_data.startswith("node:monitor_toggle:"):
        node_code = callback_data.split(":", maxsplit=2)[2]
        node, error_message, status_code = await controller_request(
            "GET", f"/nodes/{node_code}"
        )
        if error_message:
            if status_code == 404:
                await render_nodes_list(query, notice=f"节点不存在：{node_code}")
            else:
                await query.edit_message_text(
                    f"切换监控失败：{error_message}",
                    reply_markup=build_submenu("nodes"),
                )
            return

        current_monitor = int(node.get("monitor_enabled", 0) or 0)
        next_monitor = 0 if current_monitor == 1 else 1
        _, patch_error, _ = await controller_request(
            "PATCH",
            f"/nodes/{node_code}",
            payload={"monitor_enabled": next_monitor},
        )
        if patch_error:
            await render_node_detail(query, node_code, notice=f"切换监控失败：{patch_error}")
            return

        state_map = context.application.bot_data.get(NODE_MONITOR_STATE_KEY, {})
        if isinstance(state_map, dict) and next_monitor == 0:
            state_map.pop(node_code, None)
        if isinstance(state_map, dict):
            context.application.bot_data[NODE_MONITOR_STATE_KEY] = state_map
        notice = (
            f"已开启节点监控（每 {NODE_MONITOR_INTERVAL_SECONDS} 秒检测）"
            if next_monitor == 1
            else "已关闭节点监控"
        )
        await render_node_detail(query, node_code, notice=notice)
        return

    if callback_data.startswith("node:delete_confirm:"):
        node_code = callback_data.split(":", maxsplit=2)[2]
        await query.edit_message_text(
            f"确认删除节点 {node_code} ？",
            reply_markup=build_node_delete_confirm_keyboard(node_code),
        )
        return

    if callback_data.startswith("node:delete:"):
        node_code = callback_data.split(":", maxsplit=2)[2]
        delete_result, error_message, status_code = await controller_request(
            "DELETE", f"/nodes/{node_code}"
        )
        if error_message:
            if status_code == 400:
                await render_node_detail(query, node_code, notice=f"删除失败：{error_message}")
                return
            if status_code == 404:
                await render_nodes_list(query, notice=f"节点不存在：{node_code}")
                return
            await render_node_detail(query, node_code, notice=f"删除失败：{error_message}")
            return

        if delete_result and delete_result.get("ok"):
            await render_nodes_list(query, notice=f"已删除：{node_code}")
            return
        await render_nodes_list(query, notice=f"删除结果异常：{node_code}")
        return

    if callback_data.startswith("usernodes:manage:"):
        parts = callback_data.split(":", maxsplit=2)
        if len(parts) == 3:
            await render_user_nodes_manage(query, parts[2])
            return

    if callback_data.startswith("node:reality_apply:"):
        node_code = callback_data.split(":", maxsplit=2)[2] if callback_data.count(":") >= 2 else ""
        if node_code:
            await render_node_detail(query, node_code, notice="待确认REALITY参数已失效，请重新配置。")
        else:
            await query.edit_message_text("请求无效，请重试。", reply_markup=build_submenu("nodes"))
        return

    if callback_data.startswith("sub:links:") or callback_data.startswith("sub:base64:"):
        parts = callback_data.split(":", maxsplit=2)
        if len(parts) != 3:
            await query.edit_message_text("请求无效，请重试。", reply_markup=build_submenu("user"))
            return
        user_code = parts[2]
        signed_data, signed_error, _ = await controller_request(
            "GET", f"/admin/sub/sign/{user_code}"
        )
        links_url = ""
        base64_url = ""
        signed_flag = False
        expire_at = 0
        if not signed_error and isinstance(signed_data, dict):
            links_url = str(signed_data.get("links_url") or "")
            base64_url = str(signed_data.get("base64_url") or "")
            signed_flag = bool(signed_data.get("signed"))
            try:
                expire_at = int(signed_data.get("expire_at", 0) or 0)
            except (TypeError, ValueError):
                expire_at = 0
        await query.edit_message_text(
            format_sub_links_info_text(
                user_code,
                links_url=links_url,
                base64_url=base64_url,
                signed=signed_flag,
                expire_at=expire_at,
            ),
            reply_markup=build_sub_links_info_keyboard(user_code),
        )
        return

    if callback_data.startswith("usernodes:assign:"):
        parts = callback_data.split(":", maxsplit=2)
        if len(parts) != 3:
            await query.edit_message_text("请求无效，请重试。", reply_markup=build_submenu("user"))
            return
        user_code = parts[2]
        nodes, nodes_error, nodes_status = await controller_request("GET", "/nodes")
        if nodes_error:
            if nodes_status == 404:
                await query.edit_message_text("用户不存在", reply_markup=build_submenu("user"))
                return
            await query.edit_message_text(
                f"获取节点列表失败：{localize_controller_error(nodes_error)}",
                reply_markup=build_user_nodes_manage_keyboard(user_code),
            )
            return
        enabled_nodes = [
            node for node in (nodes or []) if int(node.get("enabled", 0) or 0) == 1
        ]
        text = "请选择要分配的节点（仅显示启用节点）："
        if not enabled_nodes:
            text = "暂无可分配的启用节点。"
        await query.edit_message_text(
            text,
            reply_markup=build_user_nodes_assign_list_keyboard(user_code, enabled_nodes),
        )
        return

    if callback_data.startswith("usernodes:assign_pick:"):
        parts = callback_data.split(":", maxsplit=3)
        if len(parts) != 4:
            await query.edit_message_text("请求无效，请重试。", reply_markup=build_submenu("user"))
            return
        user_code = parts[2]
        node_code = parts[3]
        await query.edit_message_text(
            "请确认节点分配：\n\n"
            "协议说明：将为该用户在该节点分配 TUIC 端口（端口池自动分配）\n"
            f"影响范围：仅该用户 {user_code}，仅该节点 {node_code}\n"
            "绑定后订阅将新增该节点",
            reply_markup=build_user_nodes_assign_confirm_keyboard(user_code, node_code),
        )
        return

    if callback_data.startswith("usernodes:assign_apply:"):
        parts = callback_data.split(":", maxsplit=3)
        if len(parts) != 4:
            await query.edit_message_text("请求无效，请重试。", reply_markup=build_submenu("user"))
            return
        user_code = parts[2]
        node_code = parts[3]
        result, error_message, status_code = await controller_request(
            "POST",
            f"/users/{user_code}/assign_node",
            payload={"node_code": node_code},
        )
        if error_message:
            localized = localize_controller_error(error_message)
            if status_code == 404 and localized == "用户不存在":
                await query.edit_message_text("用户不存在", reply_markup=build_submenu("user"))
                return
            await render_user_nodes_manage(query, user_code, notice=f"分配失败：{localized}")
            return
        tuic_port = result.get("tuic_port", "") if isinstance(result, dict) else ""
        await render_user_nodes_manage(
            query,
            user_code,
            notice=f"分配成功：节点 {node_code}，TUIC端口 {tuic_port}",
        )
        return

    if callback_data.startswith("usernodes:unassign:"):
        parts = callback_data.split(":", maxsplit=2)
        if len(parts) != 3:
            await query.edit_message_text("请求无效，请重试。", reply_markup=build_submenu("user"))
            return
        user_code = parts[2]
        user_nodes, error_message, status_code = await controller_request(
            "GET", f"/users/{user_code}/nodes"
        )
        if error_message:
            localized = localize_controller_error(error_message)
            if status_code == 404 and localized == "用户不存在":
                await query.edit_message_text("用户不存在", reply_markup=build_submenu("user"))
                return
            await query.edit_message_text(
                f"获取绑定节点失败：{localized}",
                reply_markup=build_user_nodes_manage_keyboard(user_code),
            )
            return
        text = "请选择要解绑的节点："
        if not user_nodes:
            text = "（暂无绑定节点）"
        await query.edit_message_text(
            text,
            reply_markup=build_user_nodes_unassign_list_keyboard(user_code, user_nodes or []),
        )
        return

    if callback_data.startswith("usernodes:unassign_pick:"):
        parts = callback_data.split(":", maxsplit=3)
        if len(parts) != 4:
            await query.edit_message_text("请求无效，请重试。", reply_markup=build_submenu("user"))
            return
        user_code = parts[2]
        node_code = parts[3]
        await query.edit_message_text(
            "请确认解绑：\n\n"
            f"影响范围：仅该用户 {user_code}，仅该节点 {node_code}\n"
            "解绑后订阅将移除该节点",
            reply_markup=build_user_nodes_unassign_confirm_keyboard(user_code, node_code),
        )
        return

    if callback_data.startswith("usernodes:unassign_apply:"):
        parts = callback_data.split(":", maxsplit=3)
        if len(parts) != 4:
            await query.edit_message_text("请求无效，请重试。", reply_markup=build_submenu("user"))
            return
        user_code = parts[2]
        node_code = parts[3]
        _, error_message, status_code = await controller_request(
            "POST",
            f"/users/{user_code}/unassign_node",
            payload={"node_code": node_code},
        )
        if error_message:
            localized = localize_controller_error(error_message)
            if status_code == 404 and localized == "用户不存在":
                await query.edit_message_text("用户不存在", reply_markup=build_submenu("user"))
                return
            await render_user_nodes_manage(query, user_code, notice=f"解绑失败：{localized}")
            return
        await render_user_nodes_manage(query, user_code, notice=f"解绑成功：节点 {node_code}")
        return

    if callback_data.startswith("action:"):
        submenu_key = ACTION_PARENT.get(callback_data)
        if not submenu_key:
            await query.edit_message_text("主菜单", reply_markup=build_main_menu())
            return
        action_label = ACTION_LABELS[callback_data]
        submenu_title = SUBMENUS[submenu_key]["title"]
        await query.edit_message_text(
            f"{submenu_title}\n\n当前操作：{action_label}",
            reply_markup=build_submenu(submenu_key),
        )
        return

    await query.edit_message_text("主菜单", reply_markup=build_main_menu())


def main() -> None:
    token = os.getenv("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN environment variable is not set.")

    application = (
        Application.builder()
        .token(token)
        .post_init(configure_command_menu)
        .post_shutdown(close_controller_http_client)
        .build()
    )
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("menu", menu))
    application.add_handler(CommandHandler("whoami", whoami))
    create_user_conversation = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(start_create_user_wizard, pattern=r"^action:user_create$")
        ],
        states={
            CREATE_DISPLAY_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, create_user_display_name)
            ],
            CREATE_TUIC_PORT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, create_user_tuic_port)
            ],
            CREATE_SPEED_MBPS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, create_user_speed_mbps)
            ],
            CREATE_VALID_DAYS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, create_user_valid_days)
            ],
            CREATE_CONFIRM: [
                CallbackQueryHandler(create_user_confirm, pattern=r"^wizard:create_confirm$"),
                CallbackQueryHandler(cancel_create_user_callback, pattern=r"^wizard:cancel$"),
            ],
        },
        fallbacks=[
            CommandHandler("cancel", cancel_wizard_command),
            CallbackQueryHandler(cancel_create_user_callback, pattern=r"^wizard:cancel$"),
        ],
    )
    application.add_handler(create_user_conversation)
    user_nodes_conversation = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(start_user_nodes_wizard, pattern=r"^action:user_nodes$"),
            CallbackQueryHandler(start_user_nodes_manual_input, pattern=r"^usernodes:manual_input$"),
        ],
        states={
            USER_NODES_INPUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, user_nodes_input_user_code)
            ]
        },
        fallbacks=[CommandHandler("cancel", cancel_user_nodes_wizard_command)],
    )
    application.add_handler(user_nodes_conversation)
    user_speed_conversation = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(start_user_speed_wizard, pattern=r"^action:user_speed$"),
            CallbackQueryHandler(start_user_speed_input, pattern=r"^userspeed:pick:[^:]+$"),
        ],
        states={
            USER_SPEED_INPUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, user_speed_input_value)
            ],
            USER_SPEED_CONFIRM: [
                CallbackQueryHandler(
                    apply_user_speed_callback, pattern=r"^userspeed:apply:[^:]+$"
                ),
                CallbackQueryHandler(cancel_user_speed_callback, pattern=r"^menu:user$"),
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel_user_speed_command)],
    )
    application.add_handler(user_speed_conversation)
    nodes_create_conversation = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(start_nodes_create_wizard, pattern=r"^action:nodes_create$")
        ],
        states={
            NODE_CREATE_NODE_CODE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, nodes_create_node_code)
            ],
            NODE_CREATE_REGION: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, nodes_create_region)
            ],
            NODE_CREATE_HOST: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, nodes_create_host)
            ],
            NODE_CREATE_AGENT_IP: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, nodes_create_agent_ip)
            ],
            NODE_CREATE_REALITY_SERVER_NAME: [
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND, nodes_create_reality_server_name
                )
            ],
            NODE_CREATE_TUIC_PORT_START: [
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND, nodes_create_tuic_port_start
                )
            ],
            NODE_CREATE_TUIC_PORT_END: [
                MessageHandler(
                    filters.TEXT & ~filters.COMMAND, nodes_create_tuic_port_end
                )
            ],
            NODE_CREATE_NOTE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, nodes_create_note)
            ],
            NODE_CREATE_CONFIRM: [
                CallbackQueryHandler(
                    nodes_create_confirm, pattern=r"^wizard:nodes_create_confirm$"
                ),
                CallbackQueryHandler(
                    cancel_nodes_create_callback, pattern=r"^wizard:cancel$"
                ),
            ],
        },
        fallbacks=[
            CommandHandler("cancel", cancel_nodes_wizard_command),
            CallbackQueryHandler(cancel_nodes_create_callback, pattern=r"^wizard:cancel$"),
        ],
    )
    application.add_handler(nodes_create_conversation)
    node_edit_conversation = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(start_node_edit_host, pattern=r"^node:edit_host:[^:]+$"),
            CallbackQueryHandler(start_node_edit_agent_ip, pattern=r"^node:edit_agent_ip:[^:]+$"),
            CallbackQueryHandler(start_node_edit_sni, pattern=r"^node:edit_sni:[^:]+$"),
            CallbackQueryHandler(start_node_edit_tuic_sni, pattern=r"^node:edit_tuic_sni:[^:]+$"),
            CallbackQueryHandler(start_node_edit_pool, pattern=r"^node:edit_pool:[^:]+$"),
        ],
        states={
            NODE_EDIT_HOST: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, node_edit_host_input)
            ],
            NODE_EDIT_SNI: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, node_edit_sni_input)
            ],
            NODE_EDIT_POOL: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, node_edit_pool_input)
            ],
            NODE_EDIT_TUIC_SNI: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, node_edit_tuic_sni_input)
            ],
            NODE_EDIT_AGENT_IP: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, node_edit_agent_ip_input)
            ],
            NODE_EDIT_CONFIRM: [
                CallbackQueryHandler(
                    apply_node_edit_callback,
                    pattern=r"^node:apply_edit:(host|sni|pool|agent_ip):[^:]+$",
                ),
                CallbackQueryHandler(
                    cancel_node_edit_callback,
                    pattern=r"^node:detail:[^:]+$",
                ),
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel_node_edit_command)],
    )
    application.add_handler(node_edit_conversation)
    node_reality_conversation = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(start_node_reality_paste, pattern=r"^node:reality_paste:[^:]+$")
        ],
        states={
            NODE_REALITY_PASTE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, node_reality_paste_input)
            ],
            NODE_REALITY_CONFIRM: [
                CallbackQueryHandler(
                    apply_node_reality_callback,
                    pattern=r"^node:reality_apply:[^:]+$",
                ),
                CallbackQueryHandler(
                    cancel_node_reality_callback,
                    pattern=r"^node:detail:[^:]+$",
                ),
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel_node_reality_command)],
    )
    application.add_handler(node_reality_conversation)
    maintain_config_conversation = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(start_maintain_config_wizard, pattern=r"^action:maintain_config$")
        ],
        states={
            MAINTAIN_CONFIG_INPUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, maintain_config_input)
            ]
        },
        fallbacks=[CommandHandler("cancel", cancel_maintain_config_command)],
    )
    application.add_handler(maintain_config_conversation)
    maintain_import_conversation = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(start_maintain_import_wizard, pattern=r"^action:maintain_migrate_import$")
        ],
        states={
            MAINTAIN_IMPORT_INPUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, maintain_import_input)
            ]
        },
        fallbacks=[CommandHandler("cancel", cancel_maintain_import_command)],
    )
    application.add_handler(maintain_import_conversation)
    node_ops_config_conversation = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(start_node_ops_config_wizard, pattern=r"^nodeops:config:[^:]+$")
        ],
        states={
            NODE_OPS_CONFIG_INPUT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, node_ops_config_input)
            ]
        },
        fallbacks=[CommandHandler("cancel", cancel_node_ops_config_command)],
    )
    application.add_handler(node_ops_config_conversation)
    application.add_handler(CommandHandler("cancel", cancel_idle))
    application.add_handler(CallbackQueryHandler(handle_callback))
    application.add_handler(CallbackQueryHandler(refresh_callback_menu_ttl), group=1)
    if application.job_queue:
        application.job_queue.run_repeating(
            run_node_monitor_job,
            interval=NODE_MONITOR_INTERVAL_SECONDS,
            first=NODE_MONITOR_INTERVAL_SECONDS,
            name="node_monitor_job",
        )
        if NODE_TIME_SYNC_INTERVAL_SECONDS > 0:
            application.job_queue.run_repeating(
                run_node_time_sync_job,
                interval=NODE_TIME_SYNC_INTERVAL_SECONDS,
                first=NODE_TIME_SYNC_INTERVAL_SECONDS,
                name="node_time_sync_job",
            )
            logger.info(
                "node time sync job enabled: interval=%ss", NODE_TIME_SYNC_INTERVAL_SECONDS
            )
        else:
            logger.info("node time sync job disabled: BOT_NODE_TIME_SYNC_INTERVAL=%s", NODE_TIME_SYNC_INTERVAL_SECONDS)
    application.run_polling()


if __name__ == "__main__":
    main()
