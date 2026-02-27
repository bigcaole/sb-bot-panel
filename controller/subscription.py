import base64
import hashlib
import hmac
import time
from typing import Dict, List, Optional, Union
from urllib.parse import quote

from fastapi import HTTPException

from controller.db import get_connection


def ensure_user_exists(user_code: str) -> None:
    with get_connection() as conn:
        row = conn.execute(
            "SELECT user_code FROM users WHERE user_code = ?",
            (user_code,),
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="User not found")


def ensure_user_subscription_available(user_code: str) -> None:
    now_ts = int(time.time())
    with get_connection() as conn:
        row = conn.execute(
            "SELECT user_code, status, expire_at FROM users WHERE user_code = ?",
            (user_code,),
        ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="User not found")

    status_value = str(row["status"] or "").strip().lower()
    if status_value != "active":
        raise HTTPException(status_code=403, detail="user is disabled")

    expire_at = int(row["expire_at"] or 0)
    if expire_at > 0 and expire_at <= now_ts:
        raise HTTPException(status_code=403, detail="user expired")


def build_sub_signature(user_code: str, expire_at: int, sign_key: str) -> str:
    if not sign_key:
        return ""
    message = "{0}:{1}".format(user_code, int(expire_at))
    return hmac.new(
        sign_key.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def verify_sub_access(
    user_code: str,
    sign_key: str,
    require_signature: bool,
    exp: str = "",
    sig: str = "",
) -> None:
    ensure_user_subscription_available(user_code)
    if not sign_key:
        return

    exp_raw = str(exp or "").strip()
    sig_raw = str(sig or "").strip()
    if not exp_raw or not sig_raw:
        if require_signature:
            raise HTTPException(status_code=403, detail="subscription signature required")
        return

    try:
        expire_at = int(exp_raw)
    except ValueError as exc:
        raise HTTPException(status_code=403, detail="invalid subscription signature") from exc
    now_ts = int(time.time())
    if expire_at <= now_ts:
        raise HTTPException(status_code=403, detail="subscription signature expired")

    expected = build_sub_signature(user_code, expire_at, sign_key=sign_key)
    if not expected or not hmac.compare_digest(sig_raw, expected):
        raise HTTPException(status_code=403, detail="invalid subscription signature")


def build_subscription_links_text(user_code: str) -> str:
    with get_connection() as conn:
        user_row = conn.execute(
            """
            SELECT user_code, vless_uuid, tuic_secret
            FROM users
            WHERE user_code = ?
            """,
            (user_code,),
        ).fetchone()
        if user_row is None:
            raise HTTPException(status_code=404, detail="User not found")

        node_rows = conn.execute(
            """
            SELECT
                un.node_code,
                un.tuic_port,
                n.host,
                n.reality_server_name,
                n.tuic_server_name,
                n.tuic_listen_port,
                n.reality_public_key,
                n.reality_short_id,
                n.supports_reality,
                n.supports_tuic
            FROM user_nodes un
            JOIN nodes n ON n.node_code = un.node_code
            WHERE un.user_code = ? AND n.enabled = 1
            ORDER BY un.node_code ASC
            """,
            (user_code,),
        ).fetchall()

    lines: List[str] = []
    generated_links = 0

    for row in node_rows:
        node_code = str(row["node_code"])
        host = str(row["host"])
        reality_sni_raw = row["reality_server_name"] or ""
        tuic_sni_raw = row["tuic_server_name"] or ""
        supports_reality = 1 if row["supports_reality"] is None else int(row["supports_reality"])
        supports_tuic = 1 if row["supports_tuic"] is None else int(row["supports_tuic"])

        if supports_reality == 1:
            pbk_raw = row["reality_public_key"] or ""
            sid_raw = row["reality_short_id"] or ""
            if reality_sni_raw and pbk_raw and sid_raw:
                sni = quote(str(reality_sni_raw), safe="")
                pbk = quote(str(pbk_raw), safe="")
                sid = quote(str(sid_raw), safe="")
                name = quote("{0}-R".format(node_code), safe="")
                lines.append(
                    "vless://{0}@{1}:443?encryption=none&type=tcp&security=reality&sni={2}"
                    "&fp=chrome&pbk={3}&sid={4}#{5}".format(
                        user_row["vless_uuid"],
                        host,
                        sni,
                        pbk,
                        sid,
                        name,
                    )
                )
                generated_links += 1
            else:
                lines.append("# {0}: REALITY params missing, skipped vless link".format(node_code))

        # B 模式：优先使用 user_nodes.tuic_port（端口池分配），可支持按用户端口能力（例如限速策略）。
        # A 模式：当 user_nodes.tuic_port 缺失时，回退 nodes.tuic_listen_port（默认 8443）。
        if supports_tuic == 1:
            try:
                assigned_port = int(row["tuic_port"] or 0)
            except (TypeError, ValueError):
                assigned_port = 0
            if assigned_port >= 1 and assigned_port <= 65535:
                tuic_port = assigned_port
            else:
                try:
                    tuic_port = int(row["tuic_listen_port"] or 8443)
                except (TypeError, ValueError):
                    tuic_port = 8443
            name = quote("{0}-T".format(node_code), safe="")
            tuic_link = "tuic://{0}:{0}@{1}:{2}?alpn=h3".format(
                user_row["tuic_secret"], host, tuic_port
            )
            tuic_sni_final = str(tuic_sni_raw or host)
            tuic_link = "{0}&sni={1}".format(
                tuic_link, quote(str(tuic_sni_final), safe="")
            )
            lines.append("{0}#{1}".format(tuic_link, name))
            generated_links += 1

    if generated_links == 0:
        return "# no available links"

    return "\n".join(lines)


def build_subscription_base64_text(user_code: str) -> str:
    text = build_subscription_links_text(user_code)
    return base64.b64encode(text.encode("utf-8")).decode("ascii")


def build_signed_subscription_urls(
    user_code: str,
    base_url: str,
    ttl_seconds: int,
    default_ttl_seconds: int,
    sign_key: str,
) -> Dict[str, Union[bool, int, str]]:
    ensure_user_exists(user_code)

    ttl = int(ttl_seconds or 0)
    if ttl <= 0:
        ttl = int(default_ttl_seconds)
    if ttl > 30 * 86400:
        ttl = 30 * 86400
    expire_at = int(time.time()) + ttl

    links_path = "/sub/links/{0}".format(user_code)
    base64_path = "/sub/base64/{0}".format(user_code)
    query_string = ""
    if sign_key:
        sig = build_sub_signature(user_code, expire_at, sign_key=sign_key)
        query_string = "?exp={0}&sig={1}".format(expire_at, sig)

    links_url = "{0}{1}{2}".format(str(base_url).rstrip("/"), links_path, query_string)
    base64_url = "{0}{1}{2}".format(str(base_url).rstrip("/"), base64_path, query_string)
    return {
        "user_code": user_code,
        "signed": bool(sign_key),
        "expire_at": expire_at,
        "ttl_seconds": ttl,
        "links_url": links_url,
        "base64_url": base64_url,
    }
