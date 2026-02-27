import os


def _get_int_env(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        return int(raw)
    except ValueError:
        return default


def _get_bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name, "1" if default else "0").strip().lower()
    return raw in ("1", "true", "yes", "on")


def _get_csv_env(name: str) -> list:
    values = []
    raw = os.getenv(name, "")
    for item in str(raw).split(","):
        value = str(item or "").strip()
        if value:
            values.append(value)
    return values


NODE_TASK_RUNNING_TIMEOUT_SECONDS = int(_get_int_env("NODE_TASK_RUNNING_TIMEOUT", 120))
if NODE_TASK_RUNNING_TIMEOUT_SECONDS < 30:
    NODE_TASK_RUNNING_TIMEOUT_SECONDS = 30

NODE_TASK_RETENTION_SECONDS = int(_get_int_env("NODE_TASK_RETENTION_SECONDS", 7 * 86400))
if NODE_TASK_RETENTION_SECONDS < 3600:
    NODE_TASK_RETENTION_SECONDS = 3600

NODE_TASK_MAX_PENDING_PER_NODE = int(_get_int_env("NODE_TASK_MAX_PENDING_PER_NODE", 50))
if NODE_TASK_MAX_PENDING_PER_NODE < 1:
    NODE_TASK_MAX_PENDING_PER_NODE = 1
if NODE_TASK_MAX_PENDING_PER_NODE > 1000:
    NODE_TASK_MAX_PENDING_PER_NODE = 1000

SUB_LINK_SIGN_KEY = os.getenv("SUB_LINK_SIGN_KEY", "").strip()
SUB_LINK_REQUIRE_SIGNATURE = os.getenv("SUB_LINK_REQUIRE_SIGNATURE", "0").strip() in (
    "1",
    "true",
    "TRUE",
    "yes",
    "YES",
)
SUB_LINK_DEFAULT_TTL_SECONDS = int(_get_int_env("SUB_LINK_DEFAULT_TTL_SECONDS", 7 * 86400))
if SUB_LINK_DEFAULT_TTL_SECONDS < 60:
    SUB_LINK_DEFAULT_TTL_SECONDS = 60
if SUB_LINK_DEFAULT_TTL_SECONDS > 30 * 86400:
    SUB_LINK_DEFAULT_TTL_SECONDS = 30 * 86400

BACKUP_RETENTION_COUNT = int(_get_int_env("BACKUP_RETENTION_COUNT", 30))
if BACKUP_RETENTION_COUNT < 1:
    BACKUP_RETENTION_COUNT = 1

MIGRATE_RETENTION_COUNT = int(_get_int_env("MIGRATE_RETENTION_COUNT", 20))
if MIGRATE_RETENTION_COUNT < 1:
    MIGRATE_RETENTION_COUNT = 1

# 优先使用 controller 专用阈值；未设置时兼容 bot 既有变量，避免离线判定不一致。
NODE_MONITOR_OFFLINE_THRESHOLD_SECONDS = int(
    _get_int_env(
        "NODE_MONITOR_OFFLINE_THRESHOLD_SECONDS",
        _get_int_env("BOT_NODE_OFFLINE_THRESHOLD", 120),
    )
)
if NODE_MONITOR_OFFLINE_THRESHOLD_SECONDS < 30:
    NODE_MONITOR_OFFLINE_THRESHOLD_SECONDS = 30

# 默认过滤本机/测试来源（127.0.0.1/testclient）以便更准确观察真实公网扫描趋势。
SECURITY_EVENTS_EXCLUDE_LOCAL = _get_bool_env("SECURITY_EVENTS_EXCLUDE_LOCAL", True)

# 管理端 8080 放行白名单（可为空，来源于 .env）
CONTROLLER_PORT_WHITELIST_ITEMS = _get_csv_env("CONTROLLER_PORT_WHITELIST")

CONTROLLER_PORT = int(_get_int_env("CONTROLLER_PORT", 8080))
if CONTROLLER_PORT < 1 or CONTROLLER_PORT > 65535:
    CONTROLLER_PORT = 8080

SECURITY_BLOCK_CLEANUP_INTERVAL_SECONDS = int(_get_int_env("SECURITY_BLOCK_CLEANUP_INTERVAL_SECONDS", 60))
if SECURITY_BLOCK_CLEANUP_INTERVAL_SECONDS < 5:
    SECURITY_BLOCK_CLEANUP_INTERVAL_SECONDS = 5
