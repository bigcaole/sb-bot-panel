import os
import logging
import re
from datetime import datetime

import httpx
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
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

PANEL_BASE_URL = os.getenv("PANEL_BASE_URL", "http://127.0.0.1:8080").rstrip("/")
if not PANEL_BASE_URL:
    PANEL_BASE_URL = "http://127.0.0.1:8080"

WIZARD_KEY = "create_user_wizard"
CREATE_DISPLAY_NAME, CREATE_TUIC_PORT, CREATE_SPEED_MBPS, CREATE_VALID_DAYS, CREATE_CONFIRM = range(5)
NODES_WIZARD_KEY = "nodes_create_wizard"
(
    NODE_CREATE_NODE_CODE,
    NODE_CREATE_REGION,
    NODE_CREATE_HOST,
    NODE_CREATE_REALITY_SERVER_NAME,
    NODE_CREATE_TUIC_PORT_START,
    NODE_CREATE_TUIC_PORT_END,
    NODE_CREATE_NOTE,
    NODE_CREATE_CONFIRM,
) = range(100, 108)
NODE_EDIT_KEY = "node_edit_wizard"
(
    NODE_EDIT_HOST,
    NODE_EDIT_SNI,
    NODE_EDIT_POOL,
    NODE_EDIT_CONFIRM,
    NODE_EDIT_TUIC_SNI,
) = range(200, 205)
NODE_REALITY_KEY = "node_reality_setup_wizard"
NODE_REALITY_PASTE, NODE_REALITY_CONFIRM = 500, 501
USER_NODES_WIZARD_KEY = "user_nodes_wizard"
USER_NODES_INPUT = 300
USER_SPEED_PENDING_KEY = "user_speed_pending"
USER_SPEED_ACTIVE_KEY = "user_speed_active"
USER_SPEED_INPUT, USER_SPEED_CONFIRM = 400, 401


SUBMENUS = {
    "user": {
        "title": "用户管理",
        "buttons": [
            ("创建用户", "action:user_create"),
            ("禁用/启用", "action:user_toggle"),
            ("删除用户", "action:user_delete"),
            ("修改限速", "action:user_speed"),
            ("节点分配", "action:user_nodes"),
            ("返回", "menu:main"),
        ],
    },
    "speed": {
        "title": "限速管理",
        "buttons": [
            ("设置限速", "action:speed_set"),
            ("切换限速模式", "action:speed_switch"),
            ("返回", "menu:main"),
        ],
    },
    "query": {
        "title": "查询",
        "buttons": [
            ("用户信息", "action:query_user_info"),
            ("即将到期", "action:query_expiring"),
            ("流量排行", "action:query_traffic"),
            ("返回", "menu:main"),
        ],
    },
    "backup": {
        "title": "备份与维护",
        "buttons": [
            ("立即备份", "action:backup_now"),
            ("操作日志", "action:backup_audit"),
            ("紧急停止", "action:backup_stop"),
            ("返回", "menu:main"),
        ],
    },
    "nodes": {
        "title": "节点管理",
        "buttons": [
            ("查看节点列表", "action:nodes_list"),
            ("新增节点", "action:nodes_create"),
            ("返回", "menu:main"),
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


def build_main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("用户管理", callback_data="menu:user"),
                InlineKeyboardButton("限速管理", callback_data="menu:speed"),
            ],
            [
                InlineKeyboardButton("查询", callback_data="menu:query"),
                InlineKeyboardButton("备份与维护", callback_data="menu:backup"),
            ],
            [
                InlineKeyboardButton("节点管理", callback_data="menu:nodes"),
            ],
        ]
    )


def build_submenu(submenu_key: str) -> InlineKeyboardMarkup:
    rows = []
    buttons = SUBMENUS[submenu_key]["buttons"]
    for i in range(0, len(buttons), 2):
        pair = buttons[i : i + 2]
        rows.append(
            [InlineKeyboardButton(text, callback_data=data) for text, data in pair]
        )
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


def build_node_detail_keyboard(node_code: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("启用/禁用", callback_data=f"node:toggle:{node_code}")],
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


def get_node_edit_scope_text(field: str) -> str:
    mapping = {
        "host": "VLESS+REALITY & TUIC（两种协议）",
        "sni": "仅 VLESS+REALITY",
        "pool": "仅 TUIC",
    }
    return mapping.get(field, "未知范围")


def localize_controller_error(error_message: str) -> str:
    mapping = {
        "User not found": "用户不存在",
        "Node not found": "节点不存在",
        "Node is disabled": "节点已禁用",
        "User already assigned to this node": "该用户已绑定该节点",
        "No available TUIC port in node pool": "该节点端口池已满，暂无可用TUIC端口",
        "User-node binding not found": "该用户未绑定该节点",
    }
    return mapping.get(error_message, error_message)


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


def format_node_detail_text(node: dict) -> str:
    reality_server_name = node.get("reality_server_name") or "未设置"
    tuic_server_name = node.get("tuic_server_name") or "未设置"
    enabled_text = "启用" if int(node.get("enabled", 0)) == 1 else "禁用"
    tags = format_node_tags(node)
    return (
        f"节点：{node.get('node_code', '')}\n"
        f"地区：{node.get('region', '')}\n"
        f"支持协议：{tags}（R=VLESS+REALITY，T=TUIC）\n"
        "【VLESS+REALITY】\n"
        f"入口(用于连接)：{node.get('host', '')}\n"
        f"REALITY伪装域名（R）：{reality_server_name}\n"
        "【TUIC】\n"
        f"TUIC证书域名（T）：{tuic_server_name}\n"
        f"端口池：{node.get('tuic_port_start', '')}-{node.get('tuic_port_end', '')}\n"
        f"状态：{enabled_text}"
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


async def controller_request(
    method: str, path: str, payload: dict = None
) -> tuple:
    url = f"http://127.0.0.1:8080{path}"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.request(method, url, json=payload)
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
        reply_markup=build_node_detail_keyboard(node_code),
    )


async def send_node_detail_message(
    message, node_code: str, notice: str = ""
) -> None:
    node, error_message, status_code = await controller_request(
        "GET", f"/nodes/{node_code}"
    )
    if error_message:
        if status_code == 404:
            await message.reply_text(f"节点不存在：{node_code}", reply_markup=build_submenu("nodes"))
            return
        await message.reply_text(
            f"获取节点详情失败：{error_message}",
            reply_markup=build_submenu("nodes"),
        )
        return

    detail_text = format_node_detail_text(node)
    if notice:
        detail_text = f"{notice}\n\n{detail_text}"
    await message.reply_text(
        detail_text,
        reply_markup=build_node_detail_keyboard(node_code),
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


def format_sub_links_info_text(user_code: str) -> str:
    return (
        "明文订阅链接：\n"
        f"{PANEL_BASE_URL}/sub/links/{user_code}\n\n"
        "Base64订阅链接：\n"
        f"{PANEL_BASE_URL}/sub/base64/{user_code}\n\n"
        "如果REALITY参数未配置，将在明文订阅中提示并跳过vless链接。"
    )


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
    message, user_code: str, notice: str = ""
) -> None:
    user, user_error, user_status = await controller_request("GET", f"/users/{user_code}")
    if user_error:
        if user_status == 404:
            await message.reply_text("用户不存在", reply_markup=build_submenu("user"))
            return
        await message.reply_text(
            f"获取用户信息失败：{localize_controller_error(user_error)}",
            reply_markup=build_submenu("user"),
        )
        return

    user_nodes, nodes_error, nodes_status = await controller_request(
        "GET", f"/users/{user_code}/nodes"
    )
    if nodes_error:
        if nodes_status == 404:
            await message.reply_text("用户不存在", reply_markup=build_submenu("user"))
            return
        await message.reply_text(
            f"获取用户节点信息失败：{localize_controller_error(nodes_error)}",
            reply_markup=build_submenu("user"),
        )
        return

    nodes, _, _ = await controller_request("GET", "/nodes")
    node_tags_map = build_node_tags_map(nodes if isinstance(nodes, list) else [])

    text = format_user_nodes_manage_text(
        user_code, user or {}, user_nodes or [], node_tags_map, notice
    )
    await message.reply_text(text, reply_markup=build_user_nodes_manage_keyboard(user_code))


async def show_main_menu(update: Update) -> None:
    if update.message:
        await update.message.reply_text("主菜单", reply_markup=build_main_menu())


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    await show_main_menu(update)


async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    await show_main_menu(update)


async def start_create_user_wizard(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    query = update.callback_query
    if not query:
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

    await update.message.reply_text(
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

    await query.answer()
    callback_data = query.data or ""
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

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(
                "http://127.0.0.1:8080/users/create",
                json=payload,
            )
    except httpx.HTTPError as exc:
        await query.edit_message_text(
            f"创建失败：无法连接控制器接口（{exc}）。\n\n"
            f"{format_create_summary(payload['display_name'], payload['tuic_port'], payload['speed_mbps'], payload['valid_days'])}",
            reply_markup=build_create_confirm_keyboard(),
        )
        return CREATE_CONFIRM

    if response.status_code >= 400:
        try:
            error_body = response.json()
            error_message = str(error_body.get("detail", error_body))
        except ValueError:
            error_message = response.text or f"HTTP {response.status_code}"
        await query.edit_message_text(
            f"创建失败：{error_message}\n\n"
            f"{format_create_summary(payload['display_name'], payload['tuic_port'], payload['speed_mbps'], payload['valid_days'])}",
            reply_markup=build_create_confirm_keyboard(),
        )
        return CREATE_CONFIRM

    result = response.json()
    user_code = result.get("user_code", "")
    expire_at = int(result.get("expire_at", 0))
    expire_text = datetime.fromtimestamp(expire_at).strftime("%Y-%m-%d %H:%M:%S")
    speed_mbps = int(result.get("speed_mbps", 0))
    speed_text = "不限速（0 Mbps）" if speed_mbps == 0 else f"{speed_mbps} Mbps"
    tuic_port = result.get("tuic_port", "")

    await query.edit_message_text(
        "创建成功\n\n"
        f"用户代码：{user_code}\n"
        f"到期时间：{expire_text}\n"
        f"限速：{speed_text}\n"
        f"TUIC端口：{tuic_port}\n\n"
        f"订阅链接：https://example.com/sub/{user_code}",
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
        await update.message.reply_text("已取消", reply_markup=build_submenu("user"))
    return ConversationHandler.END


async def start_nodes_create_wizard(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    query = update.callback_query
    if not query:
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

    await update.message.reply_text(
        format_nodes_create_summary(
            wizard_data["node_code"],
            wizard_data["region"],
            wizard_data["host"],
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

    await query.answer()
    callback_data = query.data or ""
    user = query.from_user.username or query.from_user.id
    logger.info("button_click user=%s data=%s", user, callback_data)

    wizard_data = context.user_data.get(NODES_WIZARD_KEY, {})
    required_fields = [
        "node_code",
        "region",
        "host",
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
        "tuic_port_start": wizard_data["tuic_port_start"],
        "tuic_port_end": wizard_data["tuic_port_end"],
        "note": wizard_data["note"],
        "enabled": 1,
    }
    reality_server_name = wizard_data.get("reality_server_name", "")
    if reality_server_name:
        payload["reality_server_name"] = reality_server_name

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(
                "http://127.0.0.1:8080/nodes/create",
                json=payload,
            )
    except httpx.HTTPError as exc:
        await query.edit_message_text(
            f"创建节点失败：无法连接控制器接口（{exc}）。",
            reply_markup=build_submenu("nodes"),
        )
        context.user_data.pop(NODES_WIZARD_KEY, None)
        return ConversationHandler.END

    if response.status_code >= 400:
        try:
            error_body = response.json()
            error_message = str(error_body.get("detail", error_body))
        except ValueError:
            error_message = response.text or f"HTTP {response.status_code}"
        await query.edit_message_text(
            f"创建节点失败：{error_message}",
            reply_markup=build_submenu("nodes"),
        )
        context.user_data.pop(NODES_WIZARD_KEY, None)
        return ConversationHandler.END

    result = response.json()
    reality_text = result.get("reality_server_name") or "未设置"
    await query.edit_message_text(
        "创建节点成功\n\n"
        f"节点代码：{result.get('node_code', payload['node_code'])}\n"
        f"地区：{result.get('region', payload['region'])}\n"
        f"主机：{result.get('host', payload['host'])}\n"
        f"Reality域名：{reality_text}\n"
        f"TUIC端口池：{result.get('tuic_port_start', payload['tuic_port_start'])}-"
        f"{result.get('tuic_port_end', payload['tuic_port_end'])}\n"
        f"状态：{'启用' if int(result.get('enabled', 1)) == 1 else '禁用'}",
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
        await update.message.reply_text("已取消", reply_markup=build_submenu("nodes"))
    return ConversationHandler.END


async def start_user_nodes_wizard(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    query = update.callback_query
    if not query:
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
    await send_user_nodes_manage_message(update.message, raw_value)
    context.user_data.pop(USER_NODES_WIZARD_KEY, None)
    return ConversationHandler.END


async def cancel_user_nodes_wizard_command(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    context.user_data.pop(USER_NODES_WIZARD_KEY, None)
    if update.message:
        await update.message.reply_text("已取消", reply_markup=build_submenu("user"))
    return ConversationHandler.END


async def start_user_speed_wizard(
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
    await render_user_speed_picker(query)
    return ConversationHandler.END


async def start_user_speed_input(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    query = update.callback_query
    if not query:
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
        await update.message.reply_text("修改状态已丢失，请重新选择用户。", reply_markup=build_submenu("user"))
        return ConversationHandler.END

    pending["new_speed"] = new_speed
    pending_map[user_code] = pending
    context.user_data[USER_SPEED_PENDING_KEY] = pending_map

    display_name = str(pending.get("display_name") or "").strip()
    user_label = f"{display_name}（{user_code}）" if display_name else user_code
    old_speed = int(pending.get("old_speed", 0) or 0)
    await update.message.reply_text(
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

    await query.answer()
    callback_data = query.data or ""
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
        await update.message.reply_text("已取消", reply_markup=build_submenu("user"))
    return ConversationHandler.END


async def start_node_edit_host(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    await query.answer()
    callback_data = query.data or ""
    user = query.from_user.username or query.from_user.id
    logger.info("button_click user=%s data=%s", user, callback_data)
    node_code = callback_data.split(":", maxsplit=2)[2]
    context.user_data[NODE_EDIT_KEY] = {"node_code": node_code, "field": "host"}
    await query.edit_message_text("请输入新的入口（IP 或域名），发送 /cancel 取消。")
    return NODE_EDIT_HOST


async def start_node_edit_sni(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    query = update.callback_query
    if not query:
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
        await update.message.reply_text("编辑状态已丢失，请重新进入节点详情。", reply_markup=build_submenu("nodes"))
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
        await update.message.reply_text("编辑状态已丢失，请重新进入节点详情。", reply_markup=build_submenu("nodes"))
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
        await update.message.reply_text("编辑状态已丢失，请重新进入节点详情。", reply_markup=build_submenu("nodes"))
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
        await update.message.reply_text("编辑状态已丢失，请重新进入节点详情。", reply_markup=build_submenu("nodes"))
        return ConversationHandler.END

    _, error_message, status_code = await controller_request(
        "PATCH",
        f"/nodes/{node_code}",
        payload={"tuic_server_name": tuic_server_name},
    )
    if error_message:
        if status_code == 404:
            context.user_data.pop(NODE_EDIT_KEY, None)
            await update.message.reply_text(f"节点不存在：{node_code}", reply_markup=build_submenu("nodes"))
            return ConversationHandler.END
        context.user_data.pop(NODE_EDIT_KEY, None)
        await send_node_detail_message(update.message, node_code, notice=f"修改失败：{error_message}")
        return ConversationHandler.END

    context.user_data.pop(NODE_EDIT_KEY, None)
    await send_node_detail_message(update.message, node_code, notice="TUIC证书域名已更新")
    return ConversationHandler.END


async def prompt_node_edit_confirmation(
    message, context: ContextTypes.DEFAULT_TYPE
) -> int:
    pending_edit = context.user_data.get(NODE_EDIT_KEY, {})
    node_code = str(pending_edit.get("node_code", ""))
    field = str(pending_edit.get("field", ""))
    patch_payload = pending_edit.get("patch_payload")

    if not node_code or field not in ("host", "sni", "pool") or not isinstance(patch_payload, dict):
        context.user_data.pop(NODE_EDIT_KEY, None)
        await message.reply_text("待确认修改数据无效，请重新进入节点详情操作。", reply_markup=build_submenu("nodes"))
        return ConversationHandler.END

    node, node_error, node_status_code = await controller_request(
        "GET", f"/nodes/{node_code}"
    )
    if node_error:
        context.user_data.pop(NODE_EDIT_KEY, None)
        if node_status_code == 404:
            await message.reply_text(f"节点不存在：{node_code}", reply_markup=build_submenu("nodes"))
        else:
            await message.reply_text(f"获取节点详情失败：{node_error}", reply_markup=build_submenu("nodes"))
        return ConversationHandler.END

    stats, stats_error, _ = await controller_request("GET", f"/nodes/{node_code}/stats")
    if stats_error:
        context.user_data.pop(NODE_EDIT_KEY, None)
        await message.reply_text(f"获取节点统计失败：{stats_error}", reply_markup=build_submenu("nodes"))
        return ConversationHandler.END

    old_value, new_value = get_node_edit_old_new_values(field, node, pending_edit)
    scope_text = get_node_edit_scope_text(field)
    bound_users = int(stats.get("bound_users", 0)) if isinstance(stats, dict) else 0

    await message.reply_text(
        "请确认修改：\n\n"
        f"正在修改的协议范围：{scope_text}\n"
        f"影响范围：仅该节点 {node_code}\n"
        f"影响用户数：{bound_users}（已绑定该节点的用户）\n"
        f"旧值 -> 新值：{old_value} -> {new_value}\n\n"
        "确认后将影响所有已绑定该节点的用户订阅内容",
        reply_markup=build_node_edit_confirm_keyboard(field, node_code),
    )
    return NODE_EDIT_CONFIRM


async def apply_node_edit_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    query = update.callback_query
    if not query:
        return ConversationHandler.END
    await query.answer()

    callback_data = query.data or ""
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
            await send_node_detail_message(update.message, node_code, notice="已取消")
        else:
            await update.message.reply_text("已取消", reply_markup=build_submenu("nodes"))
    return ConversationHandler.END


async def start_node_reality_paste(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    query = update.callback_query
    if not query:
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
        await update.message.reply_text("配置状态已丢失，请重新进入节点详情。", reply_markup=build_submenu("nodes"))
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
            await update.message.reply_text(f"节点不存在：{node_code}", reply_markup=build_submenu("nodes"))
        else:
            await update.message.reply_text(f"获取节点详情失败：{node_error}", reply_markup=build_submenu("nodes"))
        return ConversationHandler.END

    stats, stats_error, _ = await controller_request("GET", f"/nodes/{node_code}/stats")
    if stats_error:
        context.user_data.pop(NODE_REALITY_KEY, None)
        await update.message.reply_text(f"获取节点统计失败：{stats_error}", reply_markup=build_submenu("nodes"))
        return ConversationHandler.END

    old_public_key = str(node.get("reality_public_key") or "")
    old_short_id = str(node.get("reality_short_id") or "")
    bound_users = int(stats.get("bound_users", 0)) if isinstance(stats, dict) else 0

    context.user_data[NODE_REALITY_KEY] = {
        "node_code": node_code,
        "public_key": public_key,
        "short_id": short_id,
    }

    await update.message.reply_text(
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
    await query.answer()

    callback_data = query.data or ""
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
            await send_node_detail_message(update.message, node_code, notice="已取消")
        else:
            await update.message.reply_text("已取消", reply_markup=build_submenu("nodes"))
    return ConversationHandler.END


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    del context
    query = update.callback_query
    if not query:
        return

    await query.answer()
    callback_data = query.data or ""
    user = query.from_user.username or query.from_user.id
    logger.info("button_click user=%s data=%s", user, callback_data)

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

    if callback_data == "action:nodes_list":
        await render_nodes_list(query)
        return

    if callback_data.startswith("node:detail:"):
        node_code = callback_data.split(":", maxsplit=2)[2]
        await render_node_detail(query, node_code)
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
        await query.edit_message_text(
            format_sub_links_info_text(user_code),
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

    application = Application.builder().token(token).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("menu", menu))
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
            NODE_EDIT_CONFIRM: [
                CallbackQueryHandler(
                    apply_node_edit_callback,
                    pattern=r"^node:apply_edit:(host|sni|pool):[^:]+$",
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
    application.add_handler(CallbackQueryHandler(handle_callback))
    application.run_polling()


if __name__ == "__main__":
    main()
