#!/usr/bin/env python3
import json
import logging
import os
import re
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

DEFAULT_POLL_INTERVAL = 15

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

    tuic_listen_port_raw = raw.get("tuic_listen_port", 8443)
    try:
        tuic_listen_port = int(tuic_listen_port_raw)
    except (TypeError, ValueError):
        tuic_listen_port = 8443
    if tuic_listen_port < 1 or tuic_listen_port > 65535:
        tuic_listen_port = 8443

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


def run_command(command: List[str]) -> Tuple[int, str, str]:
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode, result.stdout or "", result.stderr or ""


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


def build_sing_box_config(
    config: AgentConfig,
    node: Dict[str, Any],
    users: List[Dict[str, Any]],
    state: Dict[str, str],
) -> Dict[str, Any]:
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

    if config.tuic_domain and config.acme_email:
        tuic_users = build_tuic_users(users)
        tuic_inbound = {
            "type": "tuic",
            "tag": "tuic-in",
            "listen": "::",
            "listen_port": config.tuic_listen_port,
            "users": tuic_users,
            "congestion_control": "bbr",
            "zero_rtt_handshake": False,
            "auth_timeout": "3s",
            "heartbeat": "10s",
            "tls": {
                "enabled": True,
                "server_name": config.tuic_domain,
                "alpn": ["h3", "h2", "http/1.1"],
                "acme": {
                    "domain": [config.tuic_domain],
                    "email": config.acme_email,
                    "provider": "letsencrypt",
                    "data_directory": SING_BOX_CERTMAGIC_DIR,
                },
            },
        }
        inbounds.append(tuic_inbound)
        route_rules.append({"inbound": ["tuic-in"], "outbound": "direct"})
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
    }


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


def handle_once(config: AgentConfig) -> None:
    sync_data = sync_from_controller(config)
    node = sync_data.get("node", {})
    users = normalize_users(sync_data.get("users", []))
    if not isinstance(node, dict):
        raise RuntimeError("sync 响应缺少 node")

    state = load_state()
    state = ensure_reality_material(node, state)
    maybe_report_reality(config, node, state)

    rendered = build_sing_box_config(config, node, users, state)
    changed = write_sing_box_config_if_changed(rendered)
    if changed:
        LOGGER.info("检测到配置变更，开始检查并重载 sing-box")
        check_and_reload_sing_box()
    else:
        LOGGER.info("配置无变化")


def _signal_handler(signum: int, frame: Any) -> None:
    del frame
    global _STOP
    LOGGER.info("收到退出信号 %s，准备停止", signum)
    _STOP = True


def main() -> int:
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    try:
        config = load_config()
    except Exception as exc:
        LOGGER.error("加载配置失败: %s", exc)
        return 1

    os.makedirs("/var/log/sing-box", exist_ok=True)
    os.makedirs(SING_BOX_CERTMAGIC_DIR, exist_ok=True)

    LOGGER.info(
        "sb-agent 启动: node_code=%s poll_interval=%s tuic_domain=%s tuic_listen_port=%s",
        config.node_code,
        config.poll_interval,
        config.tuic_domain or "(未启用)",
        config.tuic_listen_port,
    )

    while not _STOP:
        try:
            handle_once(config)
        except Exception as exc:
            LOGGER.exception("同步循环异常: %s", exc)
        for _ in range(config.poll_interval):
            if _STOP:
                break
            time.sleep(1)

    LOGGER.info("sb-agent 已退出")
    return 0


if __name__ == "__main__":
    sys.exit(main())
