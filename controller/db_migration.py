import gzip
import hashlib
import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from controller.db import DB_PATH, get_connection


DB_EXPORT_PREFIX = "db-export-"
DB_EXPORT_FORMAT = "sb-panel-db-export-v1"
MAX_DB_EXPORT_FILE_BYTES = 200 * 1024 * 1024
DEFAULT_COMPARE_IGNORE_TABLES = {"audit_logs", "node_tasks"}


def _quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _normalize_row(row_obj: Dict[str, Any]) -> Dict[str, Any]:
    normalized: Dict[str, Any] = {}
    for key in sorted(row_obj.keys()):
        value = row_obj[key]
        if isinstance(value, bytes):
            normalized[str(key)] = {"__type__": "bytes_hex", "value": value.hex()}
        else:
            normalized[str(key)] = value
    return normalized


def _row_checksum(rows: List[Dict[str, Any]]) -> str:
    digest = hashlib.sha256()
    canonical_lines = []
    for row in rows:
        canonical_lines.append(
            json.dumps(_normalize_row(row), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        )
    canonical_lines.sort()
    for line in canonical_lines:
        digest.update(line.encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def _list_user_tables(conn: sqlite3.Connection) -> List[str]:
    rows = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
        ORDER BY name ASC
        """
    ).fetchall()
    return [str(row["name"]) for row in rows]


def _table_columns(conn: sqlite3.Connection, table_name: str) -> List[str]:
    escaped = str(table_name).replace("'", "''")
    rows = conn.execute("PRAGMA table_info('{0}')".format(escaped)).fetchall()
    return [str(row["name"]) for row in rows]


def _table_rows(conn: sqlite3.Connection, table_name: str) -> List[Dict[str, Any]]:
    rows = conn.execute("SELECT * FROM {0}".format(_quote_ident(table_name))).fetchall()
    result: List[Dict[str, Any]] = []
    for row in rows:
        row_dict: Dict[str, Any] = {}
        for key in row.keys():
            row_dict[str(key)] = row[key]
        result.append(row_dict)
    return result


def build_live_table_fingerprints(table_names: Optional[List[str]] = None) -> Dict[str, Dict[str, Any]]:
    with get_connection() as conn:
        tables = table_names if isinstance(table_names, list) else _list_user_tables(conn)
        fingerprints: Dict[str, Dict[str, Any]] = {}
        for table_name in tables:
            rows = _table_rows(conn, table_name)
            fingerprints[table_name] = {
                "columns": _table_columns(conn, table_name),
                "row_count": len(rows),
                "checksum": _row_checksum(rows),
            }
    return fingerprints


def build_export_payload(created_at: int = 0) -> Dict[str, Any]:
    now_ts = int(created_at or time.time())
    with get_connection() as conn:
        schema_version_row = conn.execute("PRAGMA user_version").fetchone()
        schema_version = int(schema_version_row[0] if schema_version_row else 0)
        table_names = _list_user_tables(conn)
        tables_data: Dict[str, Dict[str, Any]] = {}
        table_summaries: Dict[str, Dict[str, Any]] = {}
        for table_name in table_names:
            rows = _table_rows(conn, table_name)
            checksum = _row_checksum(rows)
            columns = _table_columns(conn, table_name)
            tables_data[table_name] = {
                "columns": columns,
                "rows": rows,
                "row_count": len(rows),
                "checksum": checksum,
            }
            table_summaries[table_name] = {
                "row_count": len(rows),
                "checksum": checksum,
            }

    return {
        "format": DB_EXPORT_FORMAT,
        "created_at": now_ts,
        "db_path": str(DB_PATH),
        "schema_version": schema_version,
        "tables": tables_data,
        "table_summaries": table_summaries,
    }


def _cleanup_export_files(export_dir: Path, keep_count: int) -> int:
    keep = int(keep_count or 1)
    if keep < 1:
        keep = 1
    if not export_dir.exists():
        return 0
    files = [
        item
        for item in export_dir.iterdir()
        if item.is_file() and item.name.startswith(DB_EXPORT_PREFIX) and item.name.endswith(".json.gz")
    ]
    files.sort(key=lambda path_obj: path_obj.stat().st_mtime if path_obj.exists() else 0, reverse=True)
    removed = 0
    for old_file in files[keep:]:
        try:
            old_file.unlink()
            removed += 1
        except OSError:
            continue
    return removed


def export_db_snapshot(export_dir: Path, keep_count: int = 10) -> Dict[str, Any]:
    created_at = int(time.time())
    export_dir.mkdir(parents=True, exist_ok=True)
    payload = build_export_payload(created_at=created_at)

    file_name = (
        time.strftime("{0}%Y%m%d-%H%M%S".format(DB_EXPORT_PREFIX), time.localtime(created_at))
        + ".json.gz"
    )
    file_path = export_dir / file_name
    with gzip.open(file_path, "wt", encoding="utf-8") as file_obj:
        json.dump(payload, file_obj, ensure_ascii=False, indent=2)
        file_obj.write("\n")

    size_bytes = int(file_path.stat().st_size)
    cleaned_files = _cleanup_export_files(export_dir, keep_count)

    with file_path.open("rb") as file_obj:
        snapshot_sha256 = hashlib.sha256(file_obj.read()).hexdigest()

    table_summaries = payload.get("table_summaries", {})
    return {
        "path": str(file_path),
        "size_bytes": size_bytes,
        "created_at": created_at,
        "schema_version": int(payload.get("schema_version", 0) or 0),
        "table_summaries": table_summaries if isinstance(table_summaries, dict) else {},
        "snapshot_sha256": snapshot_sha256,
        "cleaned_files": cleaned_files,
        "keep_count": int(keep_count if keep_count > 0 else 1),
    }


def load_export_payload(export_path: Path) -> Dict[str, Any]:
    if not export_path.exists() or not export_path.is_file():
        raise ValueError("export file not found")
    if int(export_path.stat().st_size) > MAX_DB_EXPORT_FILE_BYTES:
        raise ValueError("export file too large")

    path_name = export_path.name.lower()
    if path_name.endswith(".json.gz") or path_name.endswith(".gz"):
        with gzip.open(export_path, "rt", encoding="utf-8") as file_obj:
            payload = json.load(file_obj)
    else:
        with export_path.open("r", encoding="utf-8") as file_obj:
            payload = json.load(file_obj)
    if not isinstance(payload, dict):
        raise ValueError("invalid export payload")
    return payload


def validate_export_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "snapshot_valid": False,
        "format_ok": False,
        "table_results": {},
        "errors": [],
    }
    if not isinstance(payload, dict):
        result["errors"].append("payload must be object")
        return result
    if str(payload.get("format", "")) != DB_EXPORT_FORMAT:
        result["errors"].append("unsupported export format")
        return result
    result["format_ok"] = True

    tables = payload.get("tables")
    if not isinstance(tables, dict):
        result["errors"].append("tables must be object")
        return result

    all_ok = True
    for table_name, table_data in tables.items():
        table_ok = True
        table_errors: List[str] = []
        if not isinstance(table_data, dict):
            result["table_results"][table_name] = {
                "ok": False,
                "errors": ["table data invalid"],
            }
            all_ok = False
            continue
        rows = table_data.get("rows")
        if not isinstance(rows, list):
            table_errors.append("rows must be list")
            rows = []
            table_ok = False
        expected_count = int(table_data.get("row_count", -1) or 0)
        actual_count = len(rows)
        if expected_count != actual_count:
            table_errors.append("row_count mismatch")
            table_ok = False
        expected_checksum = str(table_data.get("checksum", "") or "")
        actual_checksum = _row_checksum(rows)
        if expected_checksum != actual_checksum:
            table_errors.append("checksum mismatch")
            table_ok = False
        result["table_results"][table_name] = {
            "ok": table_ok,
            "row_count": actual_count,
            "expected_row_count": expected_count,
            "checksum": actual_checksum,
            "expected_checksum": expected_checksum,
            "errors": table_errors,
        }
        if not table_ok:
            all_ok = False

    result["snapshot_valid"] = all_ok
    return result


def compare_snapshot_with_live(
    payload: Dict[str, Any], ignore_tables: Optional[List[str]] = None
) -> Dict[str, Any]:
    validation = validate_export_payload(payload)
    if not bool(validation.get("snapshot_valid")):
        return {
            "snapshot_valid": False,
            "live_match": False,
            "mismatches": ["snapshot invalid"],
            "live_tables": {},
            "validation": validation,
        }

    tables = payload.get("tables")
    ignore_set = set(DEFAULT_COMPARE_IGNORE_TABLES)
    if isinstance(ignore_tables, list):
        for item in ignore_tables:
            ignore_set.add(str(item))
    table_names = sorted(
        [name for name in list(tables.keys()) if str(name) not in ignore_set]
    ) if isinstance(tables, dict) else []
    ignored_tables = sorted([name for name in list(tables.keys()) if str(name) in ignore_set]) if isinstance(tables, dict) else []
    live_fingerprints = build_live_table_fingerprints(table_names=table_names)

    mismatches: List[Dict[str, Any]] = []
    for table_name in table_names:
        table_data = tables.get(table_name, {})
        snapshot_count = int(table_data.get("row_count", 0) or 0)
        snapshot_checksum = str(table_data.get("checksum", "") or "")

        live_item = live_fingerprints.get(table_name)
        if not isinstance(live_item, dict):
            mismatches.append({"table": table_name, "reason": "live table missing"})
            continue
        if int(live_item.get("row_count", 0) or 0) != snapshot_count:
            mismatches.append(
                {
                    "table": table_name,
                    "reason": "row_count mismatch",
                    "snapshot": snapshot_count,
                    "live": int(live_item.get("row_count", 0) or 0),
                }
            )
        if str(live_item.get("checksum", "") or "") != snapshot_checksum:
            mismatches.append(
                {
                    "table": table_name,
                    "reason": "checksum mismatch",
                    "snapshot": snapshot_checksum,
                    "live": str(live_item.get("checksum", "") or ""),
                }
            )

    return {
        "snapshot_valid": True,
        "live_match": len(mismatches) == 0,
        "mismatches": mismatches,
        "live_tables": live_fingerprints,
        "compared_tables": table_names,
        "ignored_tables": ignored_tables,
        "validation": validation,
    }


def get_db_integrity_status(include_checksums: bool = False) -> Dict[str, Any]:
    with get_connection() as conn:
        integrity_row = conn.execute("PRAGMA integrity_check").fetchone()
        integrity_value = str(integrity_row[0]) if integrity_row and len(integrity_row) > 0 else "unknown"
        foreign_key_rows = conn.execute("PRAGMA foreign_key_check").fetchall()
        table_names = _list_user_tables(conn)
        table_items: Dict[str, Dict[str, Any]] = {}
        for table_name in table_names:
            row_count_row = conn.execute(
                "SELECT COUNT(*) AS c FROM {0}".format(_quote_ident(table_name))
            ).fetchone()
            row_count = int(row_count_row["c"] if row_count_row else 0)
            table_info: Dict[str, Any] = {"row_count": row_count}
            if include_checksums:
                rows = _table_rows(conn, table_name)
                table_info["checksum"] = _row_checksum(rows)
            table_items[table_name] = table_info

    return {
        "ok": integrity_value.lower() == "ok" and len(foreign_key_rows) == 0,
        "integrity_check": integrity_value,
        "foreign_key_violations": len(foreign_key_rows),
        "table_count": len(table_items),
        "tables": table_items,
    }
