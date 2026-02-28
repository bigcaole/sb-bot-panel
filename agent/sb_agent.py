#!/usr/bin/env python3
import json
import logging
import os
import re
import shutil
import signal
import subprocess
import sys
import time
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


CONFIG_PATH = "/etc/sb-agent/config.json"
STATE_PATH = "/etc/sb-agent/state.json"
SING_BOX_CONFIG_PATH = "/etc/sing-box/config.json"
AGENT_LOG_DIR = "/var/log/sb-agent"
AGENT_LOG_PATH = "/var/log/sb-agent/agent.log"
SING_BOX_CERTMAGIC_DIR = "/var/lib/sing-box/certmagic"
TUIC_TC_STATE_PATH = "/etc/sb-agent/tuic_tc_state.json"
TUIC_UFW_STATE_PATH = "/etc/sb-agent/tuic_ufw_state.json"

DEFAULT_POLL_INTERVAL = 15
DEFAULT_TUIC_LISTEN_PORT = 24443
DEFAULT_RUN_COMMAND_TIMEOUT = 30

try:
    RUN_COMMAND_TIMEOUT_SECONDS = int(
        str(os.getenv("SB_AGENT_CMD_TIMEOUT", str(DEFAULT_RUN_COMMAND_TIMEOUT))).strip()
    )
except ValueError:
    RUN_COMMAND_TIMEOUT_SECONDS = DEFAULT_RUN_COMMAND_TIMEOUT
if RUN_COMMAND_TIMEOUT_SECONDS < 5:
    RUN_COMMAND_TIMEOUT_SECONDS = 5

_STOP = False


class AgentConfig:
    def __init__(
        self,
        controller_url: str,
        node_code: str,
        poll_interval: int,
        auth_token: str,
        tuic_domain: str,
        tuic_listen_port: int,
        acme_email: str,
    ) -> None:
        self.controller_url = controller_url.rstrip("/")
        self.node_code = node_code
        self.poll_interval = poll_interval
        self.auth_token = auth_token
        self.tuic_domain = tuic_domain
        self.tuic_listen_port = tuic_listen_port
        self.acme_email = acme_email


def setup_logger() -> logging.Logger:
    os.makedirs(AGENT_LOG_DIR, exist_ok=True)
    logger = logging.getLogger("sb-agent")
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    file_handler = RotatingFileHandler(
        AGENT_LOG_PATH, maxBytes=10 * 1024 * 1024, backupCount=5
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    return logger


LOGGER = setup_logger()


def _read_json(path: str, default_value: Any) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default_value
    except json.JSONDecodeError:
        LOGGER.warning("JSON 解析失败，忽略文件: %s", path)
        return default_value


def _write_json(path: str, data: Any, mode: int = 0o600) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    temp_path = "{0}.tmp".format(path)
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")
    os.chmod(temp_path, mode)
    os.replace(temp_path, path)


def load_config() -> AgentConfig:
    raw = _read_json(CONFIG_PATH, {})
    if not isinstance(raw, dict):
        raise RuntimeError("配置文件格式错误: {0}".format(CONFIG_PATH))

    controller_url = str(raw.get("controller_url", "")).strip()
    node_code = str(raw.get("node_code", "")).strip()
    auth_token = str(raw.get("auth_token", "")).strip()
    tuic_domain = str(raw.get("tuic_domain", "")).strip()
    acme_email = str(raw.get("acme_email", "")).strip()

    if not controller_url:
        raise RuntimeError("配置缺少 controller_url")
    if not node_code:
        raise RuntimeError("配置缺少 node_code")

    poll_interval_raw = raw.get("poll_interval", DEFAULT_POLL_INTERVAL)
    try:
        poll_interval = int(poll_interval_raw)
    except (TypeError, ValueError):
        poll_interval = DEFAULT_POLL_INTERVAL
    if poll_interval < 5:
        poll_interval = 5

    tuic_listen_port_raw = raw.get("tuic_listen_port", DEFAULT_TUIC_LISTEN_PORT)
    try:
        tuic_listen_port = int(tuic_listen_port_raw)
    except (TypeError, ValueError):
        tuic_listen_port = DEFAULT_TUIC_LISTEN_PORT
    if tuic_listen_port < 1 or tuic_listen_port > 65535:
        tuic_listen_port = DEFAULT_TUIC_LISTEN_PORT

    return AgentConfig(
        controller_url=controller_url,
        node_code=node_code,
        poll_interval=poll_interval,
        auth_token=auth_token,
        tuic_domain=tuic_domain,
        tuic_listen_port=tuic_listen_port,
        acme_email=acme_email,
    )


def load_state() -> Dict[str, str]:
    state = _read_json(STATE_PATH, {})
    if not isinstance(state, dict):
        state = {}
    return {
        "reality_private_key": str(state.get("reality_private_key", "")).strip(),
        "reality_public_key": str(state.get("reality_public_key", "")).strip(),
        "reality_short_id": str(state.get("reality_short_id", "")).strip().lower(),
    }


def save_state(state: Dict[str, str]) -> None:
    _write_json(STATE_PATH, state, mode=0o600)


def request_json(
    method: str,
    url: str,
    auth_token: str = "",
    payload: Optional[Dict[str, Any]] = None,
    timeout: int = 20,
) -> Tuple[Optional[Dict[str, Any]], int, str]:
    headers = {"Accept": "application/json"}
    if auth_token:
        headers["Authorization"] = "Bearer {0}".format(auth_token)

    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = Request(url=url, data=data, headers=headers, method=method.upper())
    try:
        with urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            if not body.strip():
                return {}, int(resp.status), ""
            parsed = json.loads(body)
            if isinstance(parsed, dict):
                return parsed, int(resp.status), ""
            return {"_raw": parsed}, int(resp.status), ""
    except HTTPError as exc:
        try:
            body = exc.read().decode("utf-8", errors="replace")
            payload_obj = json.loads(body) if body else {}
            message = str(payload_obj.get("detail", body or str(exc)))
        except Exception:
            message = str(exc)
        return None, int(exc.code), message
    except URLError as exc:
        return None, 0, "网络错误: {0}".format(exc)
    except Exception as exc:
        return None, 0, "请求异常: {0}".format(exc)


def sync_from_controller(config: AgentConfig) -> Dict[str, Any]:
    url = "{0}/nodes/{1}/sync".format(config.controller_url, config.node_code)
    data, status_code, error_message = request_json(
        "GET",
        url,
        auth_token=config.auth_token,
        payload=None,
        timeout=30,
    )
    if data is None:
        raise RuntimeError("拉取同步失败({0}): {1}".format(status_code, error_message))
    return data


def fetch_next_node_task(config: AgentConfig) -> Optional[Dict[str, Any]]:
    url = "{0}/nodes/{1}/tasks/next".format(config.controller_url, config.node_code)
    data, status_code, error_message = request_json(
        "POST",
        url,
        auth_token=config.auth_token,
        payload={},
        timeout=30,
    )
    if data is None:
        if status_code in (401, 403, 404):
            LOGGER.warning("拉取节点任务失败(%s): %s", status_code, error_message)
        return None
    task_obj = data.get("task") if isinstance(data, dict) else None
    if isinstance(task_obj, dict):
        return task_obj
    return None


def report_node_task(
    config: AgentConfig, task_id: int, status: str, result: str
) -> None:
    url = "{0}/nodes/{1}/tasks/{2}/report".format(
        config.controller_url, config.node_code, int(task_id)
    )
    payload = {
        "status": status,
        "result": truncate_text(result, 10000),
    }
    _, status_code, error_message = request_json(
        "POST",
        url,
        auth_token=config.auth_token,
        payload=payload,
        timeout=30,
    )
    if status_code not in (200, 201):
        LOGGER.warning("回传节点任务结果失败(%s): %s", status_code, error_message)


def run_command(command: List[str]) -> Tuple[int, str, str]:
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
            timeout=RUN_COMMAND_TIMEOUT_SECONDS,
        )
        return result.returncode, result.stdout or "", result.stderr or ""
    except subprocess.TimeoutExpired:
        return 124, "", "command timeout ({0}s)".format(RUN_COMMAND_TIMEOUT_SECONDS)
    except Exception as exc:
        return 1, "", str(exc)


def command_exists(name: str) -> bool:
    return shutil.which(name) is not None


def parse_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def truncate_text(raw: str, limit: int = 4000) -> str:
    text = str(raw or "").strip()
    if len(text) <= limit:
        return text
    return text[:limit] + "\n...（已截断）"


def parse_reality_keypair_output(output: str) -> Tuple[str, str]:
    private_key = ""
    public_key = ""
    for line in output.splitlines():
        line = line.strip()
        private_match = re.search(r"(?i)private\s*key\s*[:：]\s*([^\s]+)", line)
        if private_match:
            private_key = private_match.group(1).strip()
        public_match = re.search(r"(?i)public\s*key\s*[:：]\s*([^\s]+)", line)
        if public_match:
            public_key = public_match.group(1).strip()
    return private_key, public_key


def generate_reality_keypair() -> Tuple[str, str]:
    code, stdout, stderr = run_command(["sing-box", "generate", "reality-keypair"])
    combined = "{0}\n{1}".format(stdout, stderr)
    if code != 0:
        raise RuntimeError("生成 REALITY 密钥失败: {0}".format(combined.strip()))

    private_key, public_key = parse_reality_keypair_output(combined)
    if not private_key or not public_key:
        raise RuntimeError("无法解析 sing-box reality-keypair 输出")
    return private_key, public_key


def generate_short_id() -> str:
    code, stdout, sb_stderr = run_command(["sing-box", "generate", "rand", "8", "--hex"])
    if code == 0:
        raw = stdout.strip().splitlines()
        if raw:
            candidate = raw[-1].strip().lower()
            if re.fullmatch(r"[0-9a-f]{1,8}", candidate):
                return candidate

    code, stdout, openssl_stderr = run_command(["openssl", "rand", "-hex", "4"])
    if code != 0:
        raise RuntimeError(
            "生成 short_id 失败: sing-box={0} openssl={1}".format(
                sb_stderr.strip(), openssl_stderr.strip()
            )
        )
    candidate = stdout.strip().lower()
    if not re.fullmatch(r"[0-9a-f]{1,8}", candidate):
        raise RuntimeError("生成的 short_id 无效: {0}".format(candidate))
    return candidate


def ensure_reality_material(
    node: Dict[str, Any], state: Dict[str, str]
) -> Dict[str, str]:
    private_key = str(state.get("reality_private_key", "")).strip()
    public_key = str(state.get("reality_public_key", "")).strip()
    short_id = str(state.get("reality_short_id", "")).strip().lower()

    if not private_key or not public_key:
        LOGGER.info("REALITY 密钥不存在，开始本地生成")
        private_key, public_key = generate_reality_keypair()

    node_short_id = str(node.get("reality_short_id") or "").strip().lower()
    if node_short_id and re.fullmatch(r"[0-9a-f]{1,8}", node_short_id):
        short_id = node_short_id

    if not short_id:
        short_id = generate_short_id()

    if not re.fullmatch(r"[0-9a-f]{1,8}", short_id):
        short_id = generate_short_id()

    next_state = {
        "reality_private_key": private_key,
        "reality_public_key": public_key,
        "reality_short_id": short_id,
    }
    save_state(next_state)
    return next_state


def maybe_report_reality(
    config: AgentConfig,
    node: Dict[str, Any],
    state: Dict[str, str],
) -> None:
    node_public_key = str(node.get("reality_public_key") or "").strip()
    node_short_id = str(node.get("reality_short_id") or "").strip().lower()
    if (
        node_public_key == state.get("reality_public_key", "")
        and node_short_id == state.get("reality_short_id", "")
    ):
        return

    payload = {
        "reality_public_key": state.get("reality_public_key", ""),
        "reality_short_id": state.get("reality_short_id", ""),
    }
    url = "{0}/nodes/{1}/report_reality".format(config.controller_url, config.node_code)
    _, status_code, error_message = request_json(
        "POST",
        url,
        auth_token=config.auth_token,
        payload=payload,
        timeout=15,
    )
    if status_code in (200, 201):
        LOGGER.info("已上报 REALITY public_key/short_id 到 controller")
    elif status_code == 404:
        LOGGER.debug("controller 未实现 /report_reality，跳过上报")
    elif status_code != 0:
        LOGGER.warning("上报 REALITY 失败(%s): %s", status_code, error_message)


def normalize_users(raw_users: Any) -> List[Dict[str, Any]]:
    if not isinstance(raw_users, list):
        return []

    now_ts = int(time.time())
    normalized = []
    for user in raw_users:
        if not isinstance(user, dict):
            continue
        status = str(user.get("status", "")).lower()
        expire_at_raw = user.get("expire_at", 0)
        try:
            expire_at = int(expire_at_raw or 0)
        except (TypeError, ValueError):
            expire_at = 0
        if status and status != "active":
            continue
        if expire_at > 0 and expire_at < now_ts:
            continue
        normalized.append(user)
    return normalized


def build_vless_users(users: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    result = []
    for user in users:
        user_code = str(user.get("user_code", "")).strip()
        vless_uuid = str(user.get("vless_uuid", "")).strip()
        if not vless_uuid:
            continue
        item: Dict[str, str] = {"uuid": vless_uuid}
        if user_code:
            item["name"] = user_code
        result.append(item)
    return result


def build_tuic_users(users: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    result = []
    for user in users:
        user_code = str(user.get("user_code", "")).strip()
        tuic_secret = str(user.get("tuic_secret", "")).strip()
        if not tuic_secret:
            continue
        item: Dict[str, str] = {
            "uuid": tuic_secret,
            "password": tuic_secret,
        }
        if user_code:
            item["name"] = user_code
        result.append(item)
    return result


def sanitize_tag_suffix(raw: str) -> str:
    candidate = re.sub(r"[^a-zA-Z0-9_-]+", "-", raw).strip("-")
    if not candidate:
        return "user"
    return candidate[:32]


def build_tuic_user_records(users: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    result = []
    for user in users:
        if not isinstance(user, dict):
            continue
        user_code = str(user.get("user_code", "")).strip()
        tuic_secret = str(user.get("tuic_secret", "")).strip()
        if not tuic_secret:
            continue

        tuic_port = parse_int(user.get("tuic_port"), 0)
        if tuic_port < 1 or tuic_port > 65535:
            tuic_port = 0

        speed_mbps = parse_int(user.get("speed_mbps"), 0)
        if speed_mbps < 0:
            speed_mbps = 0

        result.append(
            {
                "user_code": user_code,
                "tuic_secret": tuic_secret,
                "tuic_port": tuic_port,
                "speed_mbps": speed_mbps,
            }
        )
    return result


def build_tuic_speed_rules(tuic_records: List[Dict[str, Any]]) -> List[Dict[str, int]]:
    # one port -> one user in node pool mode
    seen_ports = set()
    rules: List[Dict[str, int]] = []
    for record in tuic_records:
        port = parse_int(record.get("tuic_port"), 0)
        speed = parse_int(record.get("speed_mbps"), 0)
        if port < 1 or port > 65535 or speed <= 0:
            continue
        if port in seen_ports:
            continue
        seen_ports.add(port)
        rules.append({"tuic_port": port, "speed_mbps": speed})
    rules.sort(key=lambda item: (item["tuic_port"], item["speed_mbps"]))
    return rules


def build_tuic_inbounds_and_routes(
    config: AgentConfig, tuic_records: List[Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    tls_template: Dict[str, Any] = {
        "enabled": True,
        "server_name": config.tuic_domain,
        "alpn": ["h3", "h2", "http/1.1"],
        "acme": {
            "domain": [config.tuic_domain],
            "email": config.acme_email,
            "provider": "letsencrypt",
            "data_directory": SING_BOX_CERTMAGIC_DIR,
        },
    }

    inbounds: List[Dict[str, Any]] = []
    routes: List[Dict[str, Any]] = []
    used_ports = set()
    shared_users: List[Dict[str, str]] = []

    for record in tuic_records:
        user_code = str(record.get("user_code") or "").strip()
        secret = str(record.get("tuic_secret") or "").strip()
        port = parse_int(record.get("tuic_port"), 0)
        if not secret:
            continue

        if port < 1 or port > 65535:
            shared_item = {"uuid": secret, "password": secret}
            if user_code:
                shared_item["name"] = user_code
            shared_users.append(shared_item)
            continue

        if port in used_ports:
            LOGGER.warning("检测到重复 TUIC 端口(%s)，已跳过重复用户: %s", port, user_code or "-")
            continue
        used_ports.add(port)

        user_item = {"uuid": secret, "password": secret}
        if user_code:
            user_item["name"] = user_code

        tag_suffix = sanitize_tag_suffix(user_code or str(port))
        inbound_tag = "tuic-{0}".format(tag_suffix)
        inbounds.append(
            {
                "type": "tuic",
                "tag": inbound_tag,
                "listen": "::",
                "listen_port": port,
                "users": [user_item],
                "congestion_control": "bbr",
                "zero_rtt_handshake": False,
                "auth_timeout": "3s",
                "heartbeat": "10s",
                "tls": tls_template,
            }
        )
        routes.append({"inbound": [inbound_tag], "outbound": "direct"})

    if shared_users:
        inbounds.append(
            {
                "type": "tuic",
                "tag": "tuic-in",
                "listen": "::",
                "listen_port": config.tuic_listen_port,
                "users": shared_users,
                "congestion_control": "bbr",
                "zero_rtt_handshake": False,
                "auth_timeout": "3s",
                "heartbeat": "10s",
                "tls": tls_template,
            }
        )
        routes.append({"inbound": ["tuic-in"], "outbound": "direct"})

    if not inbounds:
        inbounds.append(
            {
                "type": "tuic",
                "tag": "tuic-in",
                "listen": "::",
                "listen_port": config.tuic_listen_port,
                "users": [],
                "congestion_control": "bbr",
                "zero_rtt_handshake": False,
                "auth_timeout": "3s",
                "heartbeat": "10s",
                "tls": tls_template,
            }
        )
        routes.append({"inbound": ["tuic-in"], "outbound": "direct"})

    return inbounds, routes


def build_sing_box_config(
    config: AgentConfig,
    node: Dict[str, Any],
    users: List[Dict[str, Any]],
    state: Dict[str, str],
) -> Tuple[Dict[str, Any], List[Dict[str, int]]]:
    reality_server_name = str(node.get("reality_server_name") or "").strip()
    handshake_server = reality_server_name or "www.cloudflare.com"

    inbounds: List[Dict[str, Any]] = []
    route_rules: List[Dict[str, Any]] = []

    vless_users = build_vless_users(users)
    tls_obj: Dict[str, Any] = {
        "enabled": True,
        "reality": {
            "enabled": True,
            "handshake": {
                "server": handshake_server,
                "server_port": 443,
            },
            "private_key": state["reality_private_key"],
            "short_id": [state["reality_short_id"]],
            "max_time_difference": "1m",
        },
    }
    if reality_server_name:
        tls_obj["server_name"] = reality_server_name

    vless_inbound = {
        "type": "vless",
        "tag": "vless-reality-in",
        "listen": "::",
        "listen_port": 443,
        "users": vless_users,
        "tls": tls_obj,
    }
    inbounds.append(vless_inbound)
    route_rules.append({"inbound": ["vless-reality-in"], "outbound": "direct"})

    tuic_speed_rules: List[Dict[str, int]] = []
    if config.tuic_domain and config.acme_email:
        tuic_records = build_tuic_user_records(users)
        tuic_inbounds, tuic_routes = build_tuic_inbounds_and_routes(config, tuic_records)
        inbounds.extend(tuic_inbounds)
        route_rules.extend(tuic_routes)
        tuic_speed_rules = build_tuic_speed_rules(tuic_records)
    elif config.tuic_domain and not config.acme_email:
        LOGGER.warning("已配置 tuic_domain 但缺少 acme_email，跳过 TUIC 入站生成")

    return {
        "log": {
            "disabled": False,
            "level": "info",
            "timestamp": True,
            "output": "/var/log/sing-box/sing-box.log",
        },
        "inbounds": inbounds,
        "outbounds": [{"type": "direct", "tag": "direct"}],
        "route": {
            "rules": route_rules,
            "final": "direct",
        },
    }, tuic_speed_rules


def canonical_json(data: Dict[str, Any]) -> str:
    return json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def read_existing_config() -> Dict[str, Any]:
    existing = _read_json(SING_BOX_CONFIG_PATH, {})
    if isinstance(existing, dict):
        return existing
    return {}


def write_sing_box_config_if_changed(config_data: Dict[str, Any]) -> bool:
    existing = read_existing_config()
    if canonical_json(existing) == canonical_json(config_data):
        return False

    os.makedirs(os.path.dirname(SING_BOX_CONFIG_PATH), exist_ok=True)
    _write_json(SING_BOX_CONFIG_PATH, config_data, mode=0o644)
    return True


def check_and_reload_sing_box() -> None:
    code, stdout, stderr = run_command(["sing-box", "check", "-c", SING_BOX_CONFIG_PATH])
    if code != 0:
        LOGGER.error(
            "sing-box check 失败，跳过重载\nstdout:\n%s\nstderr:\n%s",
            stdout.strip(),
            stderr.strip(),
        )
        return

    code, stdout, stderr = run_command(
        ["systemctl", "reload-or-restart", "sing-box"]
    )
    if code != 0:
        LOGGER.error(
            "重载 sing-box 失败\nstdout:\n%s\nstderr:\n%s",
            stdout.strip(),
            stderr.strip(),
        )
        return
    LOGGER.info("sing-box 配置已生效（reload-or-restart）")


def detect_default_interface() -> str:
    code, stdout, _ = run_command(["ip", "route", "get", "1.1.1.1"])
    if code == 0:
        match = re.search(r"\bdev\s+(\S+)", stdout)
        if match:
            return match.group(1)
    code, stdout, _ = run_command(["ip", "-o", "route", "show", "default"])
    if code == 0 and stdout.strip():
        match = re.search(r"\bdev\s+(\S+)", stdout)
        if match:
            return match.group(1)
    return ""


def load_tuic_tc_state() -> Dict[str, Any]:
    state = _read_json(TUIC_TC_STATE_PATH, {})
    if isinstance(state, dict):
        return state
    return {}


def save_tuic_tc_state(state: Dict[str, Any]) -> None:
    _write_json(TUIC_TC_STATE_PATH, state, mode=0o600)


def load_tuic_ufw_state() -> Dict[str, Any]:
    state = _read_json(TUIC_UFW_STATE_PATH, {})
    if isinstance(state, dict):
        return state
    return {}


def save_tuic_ufw_state(state: Dict[str, Any]) -> None:
    _write_json(TUIC_UFW_STATE_PATH, state, mode=0o600)


def build_tuic_ufw_ports(
    config: AgentConfig, node: Dict[str, Any], tuic_records: List[Dict[str, Any]]
) -> List[int]:
    ports = set()
    fallback_port = int(config.tuic_listen_port)
    if 1 <= fallback_port <= 65535:
        ports.add(fallback_port)

    start = parse_int(node.get("tuic_port_start"), 0)
    end = parse_int(node.get("tuic_port_end"), 0)
    if 1 <= start <= 65535 and 1 <= end <= 65535 and start <= end:
        # 控制端口池通常规模不大，放行整段可避免新分配端口未及时放行导致连接失败。
        if end - start <= 2048:
            for port in range(start, end + 1):
                ports.add(port)

    for record in tuic_records:
        port = parse_int(record.get("tuic_port"), 0)
        if 1 <= port <= 65535:
            ports.add(port)
    return sorted(list(ports))


def apply_tuic_ufw_rules(ports: List[int]) -> None:
    if not command_exists("ufw"):
        LOGGER.warning("未检测到 ufw 命令，跳过 TUIC 防火墙规则同步")
        return

    desired_ports = sorted(list(set(port for port in ports if 1 <= int(port) <= 65535)))
    desired_state = {"ports": desired_ports}
    current_state = load_tuic_ufw_state()
    if canonical_json(current_state) == canonical_json(desired_state):
        return
    current_ports_raw = current_state.get("ports", []) if isinstance(current_state, dict) else []
    current_ports = set()
    if isinstance(current_ports_raw, list):
        for item in current_ports_raw:
            port = parse_int(item, 0)
            if 1 <= port <= 65535:
                current_ports.add(port)
    desired_ports_set = set(desired_ports)
    to_add = sorted(list(desired_ports_set - current_ports))
    to_remove = sorted(list(current_ports - desired_ports_set))

    code, stdout, _ = run_command(["ufw", "status"])
    if code != 0:
        LOGGER.warning("获取 ufw 状态失败，跳过 TUIC 防火墙规则同步")
        return
    if "Status: inactive" in stdout:
        LOGGER.warning("UFW 未启用，跳过 TUIC 防火墙规则同步")
        return

    add_changed_count = 0
    remove_changed_count = 0

    for port in to_remove:
        code, _, stderr = run_command(["ufw", "--force", "delete", "allow", "{0}/udp".format(port)])
        if code == 0:
            remove_changed_count += 1
            continue
        LOGGER.warning("UFW 回收失败: port=%s/udp stderr=%s", port, stderr.strip())

    for port in to_add:
        code, _, stderr = run_command(["ufw", "allow", "{0}/udp".format(port)])
        if code == 0:
            add_changed_count += 1
            continue
        LOGGER.warning("UFW 放行失败: port=%s/udp stderr=%s", port, stderr.strip())

    save_tuic_ufw_state(desired_state)
    LOGGER.info(
        "TUIC 防火墙规则已同步（目标端口数=%s，新增=%s，回收=%s）",
        len(desired_ports),
        add_changed_count,
        remove_changed_count,
    )


def apply_agent_config_patch(patch_payload: Dict[str, Any]) -> Tuple[bool, str]:
    raw = _read_json(CONFIG_PATH, {})
    if not isinstance(raw, dict):
        return False, "当前配置文件不可解析"

    allowed_keys = {
        "poll_interval",
        "tuic_domain",
        "tuic_listen_port",
        "acme_email",
        "controller_url",
        "auth_token",
        "node_code",
    }
    updates: Dict[str, Any] = {}
    for key, value in patch_payload.items():
        if key in allowed_keys:
            updates[key] = value

    if not updates:
        return False, "未提供可更新字段"

    changed_keys: List[str] = []
    for key, value in updates.items():
        if key == "poll_interval":
            parsed = parse_int(value, 0)
            if parsed < 5:
                return False, "poll_interval 必须 >= 5"
            value = parsed
        elif key == "tuic_listen_port":
            parsed = parse_int(value, 0)
            if parsed < 1 or parsed > 65535:
                return False, "tuic_listen_port 必须在 1-65535"
            value = parsed
        elif key in ("tuic_domain", "acme_email", "auth_token", "node_code"):
            value = str(value or "").strip()
            if key == "node_code" and not value:
                return False, "node_code 不能为空"
        elif key == "controller_url":
            value = str(value or "").strip().rstrip("/")
            if not value:
                return False, "controller_url 不能为空"
            if not value.startswith("http://") and not value.startswith("https://"):
                value = "http://{0}".format(value)
        if raw.get(key) != value:
            raw[key] = value
            changed_keys.append(key)

    if not changed_keys:
        return True, "配置无变化"

    _write_json(CONFIG_PATH, raw, mode=0o600)
    return True, "已更新配置项: {0}".format(", ".join(changed_keys))


def apply_time_sync_task(payload: Dict[str, Any]) -> Tuple[bool, str]:
    target_ts = parse_int(payload.get("server_unix"), 0)
    if target_ts <= 0:
        return False, "sync_time payload invalid: server_unix required"

    local_before = int(time.time())
    drift_before = int(target_ts - local_before)
    if abs(drift_before) <= 1:
        return True, "时间已同步（偏差 {0}s）".format(drift_before)

    if command_exists("timedatectl"):
        run_command(["timedatectl", "set-ntp", "false"])

    code, stdout, stderr = run_command(["date", "-u", "-s", "@{0}".format(target_ts)])
    if code != 0:
        return False, truncate_text(
            "时间同步失败\nbefore_drift={0}s\nstdout:\n{1}\nstderr:\n{2}".format(
                drift_before, stdout, stderr
            ),
            4000,
        )

    if command_exists("hwclock"):
        run_command(["hwclock", "-w"])

    local_after = int(time.time())
    drift_after = int(target_ts - local_after)
    return True, "时间同步完成：before_drift={0}s after_drift={1}s".format(
        drift_before, drift_after
    )


def execute_node_task(config: AgentConfig, task: Dict[str, Any]) -> Tuple[str, str]:
    task_id = parse_int(task.get("id"), 0)
    task_type = str(task.get("task_type", "")).strip()
    payload = task.get("payload")
    if not isinstance(payload, dict):
        payload = {}
    LOGGER.info("执行节点任务 id=%s type=%s", task_id, task_type)

    if task_type == "restart_singbox":
        code, stdout, stderr = run_command(["systemctl", "restart", "sing-box"])
        if code == 0:
            return "success", "sing-box 重启成功"
        return "failed", truncate_text("重启失败\nstdout:\n{0}\nstderr:\n{1}".format(stdout, stderr))

    if task_type == "status_singbox":
        _, active_stdout, active_stderr = run_command(["systemctl", "is-active", "sing-box"])
        _, status_stdout, status_stderr = run_command(
            ["systemctl", "status", "sing-box", "--no-pager", "-n", "40"]
        )
        result_text = (
            "is-active:\n{0}\n{1}\n\nstatus:\n{2}\n{3}".format(
                active_stdout.strip(),
                active_stderr.strip(),
                status_stdout.strip(),
                status_stderr.strip(),
            )
        )
        return "success", truncate_text(result_text)

    if task_type == "status_agent":
        _, active_stdout, active_stderr = run_command(["systemctl", "is-active", "sb-agent"])
        _, status_stdout, status_stderr = run_command(
            ["systemctl", "status", "sb-agent", "--no-pager", "-n", "40"]
        )
        result_text = (
            "is-active:\n{0}\n{1}\n\nstatus:\n{2}\n{3}".format(
                active_stdout.strip(),
                active_stderr.strip(),
                status_stdout.strip(),
                status_stderr.strip(),
            )
        )
        return "success", truncate_text(result_text)

    if task_type == "logs_singbox":
        lines = parse_int(payload.get("lines"), 120)
        if lines < 20:
            lines = 20
        if lines > 300:
            lines = 300
        code, stdout, stderr = run_command(
            ["journalctl", "-u", "sing-box", "-n", str(lines), "--no-pager"]
        )
        if code == 0:
            return "success", truncate_text(stdout, 8000)
        return "failed", truncate_text(stderr or stdout, 4000)

    if task_type == "logs_agent":
        lines = parse_int(payload.get("lines"), 120)
        if lines < 20:
            lines = 20
        if lines > 300:
            lines = 300
        code, stdout, stderr = run_command(
            ["journalctl", "-u", "sb-agent", "-n", str(lines), "--no-pager"]
        )
        if code == 0:
            return "success", truncate_text(stdout, 8000)
        return "failed", truncate_text(stderr or stdout, 4000)

    if task_type == "update_sync":
        log_path = "/tmp/sb-node-update-{0}.log".format(int(time.time()))
        code, stdout, stderr = run_command(
            [
                "bash",
                "-lc",
                "if [ -d /root/sb-bot-panel ]; then "
                "cd /root/sb-bot-panel && nohup bash scripts/install.sh --sync-only > {0} 2>&1 & "
                "else exit 2; fi".format(log_path),
            ]
        )
        if code == 0:
            return "success", "已启动后台更新任务，日志: {0}".format(log_path)
        return "failed", truncate_text(stderr or stdout, 4000)

    if task_type == "config_set":
        ok, msg = apply_agent_config_patch(payload)
        if ok:
            return "success", msg
        return "failed", msg

    if task_type == "sync_time":
        ok, msg = apply_time_sync_task(payload)
        if ok:
            return "success", msg
        return "failed", msg

    return "failed", "unsupported task_type: {0}".format(task_type)


def process_node_tasks(config: AgentConfig, max_tasks: int = 3) -> None:
    for _ in range(max_tasks):
        task = fetch_next_node_task(config)
        if not isinstance(task, dict):
            return
        task_id = parse_int(task.get("id"), 0)
        if task_id <= 0:
            continue
        status, result = execute_node_task(config, task)
        report_node_task(config, task_id, status, result)


def tc_run(command: List[str], ignore_error: bool = False) -> bool:
    code, _, stderr = run_command(command)
    if code != 0 and not ignore_error:
        LOGGER.warning("tc 命令失败: %s ; stderr=%s", " ".join(command), stderr.strip())
        return False
    return True


def apply_tuic_speed_limits(rules: List[Dict[str, int]]) -> None:
    if not command_exists("tc") or not command_exists("ip"):
        LOGGER.warning("未检测到 tc/ip 命令，跳过 TUIC 限速下发")
        return

    iface = detect_default_interface()
    if not iface:
        LOGGER.warning("无法检测默认出口网卡，跳过 TUIC 限速下发")
        return

    desired_state = {
        "iface": iface,
        "rules": sorted(
            [
                {"tuic_port": int(item["tuic_port"]), "speed_mbps": int(item["speed_mbps"])}
                for item in rules
                if int(item.get("tuic_port", 0)) > 0 and int(item.get("speed_mbps", 0)) > 0
            ],
            key=lambda item: (item["tuic_port"], item["speed_mbps"]),
        ),
    }
    current_state = load_tuic_tc_state()
    if canonical_json(current_state) == canonical_json(desired_state):
        return

    # 使用 clsact + police 限速，避免改写已有 root qdisc。
    tc_run(["tc", "qdisc", "del", "dev", iface, "clsact"], ignore_error=True)
    if not desired_state["rules"]:
        save_tuic_tc_state(desired_state)
        LOGGER.info("TUIC 限速规则为空，已清理 tc clsact")
        return

    if not tc_run(["tc", "qdisc", "add", "dev", iface, "clsact"], ignore_error=False):
        return

    pref = 100
    all_ok = True
    for item in desired_state["rules"]:
        port = int(item["tuic_port"])
        speed = int(item["speed_mbps"])
        rate = "{0}mbit".format(speed)
        # egress: server -> client, source port is tuic listen port.
        ok_v4 = tc_run(
            [
                "tc",
                "filter",
                "add",
                "dev",
                iface,
                "egress",
                "protocol",
                "ip",
                "pref",
                str(pref),
                "flower",
                "ip_proto",
                "udp",
                "src_port",
                str(port),
                "action",
                "police",
                "rate",
                rate,
                "burst",
                "256k",
                "conform-exceed",
                "drop",
            ],
            ignore_error=False,
        )
        pref += 1
        ok_v6 = tc_run(
            [
                "tc",
                "filter",
                "add",
                "dev",
                iface,
                "egress",
                "protocol",
                "ipv6",
                "pref",
                str(pref),
                "flower",
                "ip_proto",
                "udp",
                "src_port",
                str(port),
                "action",
                "police",
                "rate",
                rate,
                "burst",
                "256k",
                "conform-exceed",
                "drop",
            ],
            ignore_error=False,
        )
        pref += 1
        if ok_v4 or ok_v6:
            LOGGER.info("已下发 TUIC 限速: port=%s speed=%s Mbps", port, speed)
        else:
            all_ok = False

    if all_ok:
        save_tuic_tc_state(desired_state)
    else:
        LOGGER.warning("存在 TUIC 限速规则下发失败，将在下一轮继续重试")


def handle_once(config: AgentConfig) -> None:
    sync_data = sync_from_controller(config)
    node = sync_data.get("node", {})
    users = normalize_users(sync_data.get("users", []))
    if not isinstance(node, dict):
        raise RuntimeError("sync 响应缺少 node")

    state = load_state()
    state = ensure_reality_material(node, state)
    maybe_report_reality(config, node, state)

    rendered, tuic_speed_rules = build_sing_box_config(config, node, users, state)
    tuic_records = build_tuic_user_records(users)
    changed = write_sing_box_config_if_changed(rendered)
    if changed:
        LOGGER.info("检测到配置变更，开始检查并重载 sing-box")
        check_and_reload_sing_box()
    else:
        LOGGER.info("配置无变化")
    if config.tuic_domain and config.acme_email:
        apply_tuic_ufw_rules(build_tuic_ufw_ports(config, node, tuic_records))
    apply_tuic_speed_limits(tuic_speed_rules)
    process_node_tasks(config)


def _signal_handler(signum: int, frame: Any) -> None:
    del frame
    global _STOP
    LOGGER.info("收到退出信号 %s，准备停止", signum)
    _STOP = True


def main() -> int:
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    os.makedirs("/var/log/sing-box", exist_ok=True)
    os.makedirs(SING_BOX_CERTMAGIC_DIR, exist_ok=True)

    try:
        first_config = load_config()
    except Exception as exc:
        LOGGER.error("加载配置失败: %s", exc)
        return 1

    LOGGER.info(
        "sb-agent 启动: node_code=%s poll_interval=%s tuic_domain=%s tuic_listen_port=%s",
        first_config.node_code,
        first_config.poll_interval,
        first_config.tuic_domain or "(未启用)",
        first_config.tuic_listen_port,
    )

    while not _STOP:
        sleep_seconds = DEFAULT_POLL_INTERVAL
        try:
            config = load_config()
            handle_once(config)
            sleep_seconds = config.poll_interval
        except Exception as exc:
            LOGGER.exception("同步循环异常: %s", exc)
        for _ in range(sleep_seconds):
            if _STOP:
                break
            time.sleep(1)

    LOGGER.info("sb-agent 已退出")
    return 0


if __name__ == "__main__":
    sys.exit(main())
