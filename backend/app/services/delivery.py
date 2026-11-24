"""抽取窗口交付凭据、完成回执与目标交付台账的共享持久化工具。

该模块把"窗口产物是否可读、是否已确认零行、是否已经形成交付结果"的
判据集中到一处，供增量窗口、传输批次和到达对账三个领域服务复用：

- 执行凭据（credential）：由真实抽取执行写入，记录本次产物的校验值与条数；
- 完成回执（receipt）：对可交付产物或"已确认零行"窗口生成的不可重复回执；
- 目标交付台账（delivery）：按目标端记录每个窗口的交付回执或空窗口跳过记录。

所有写操作均采用"临时文件 + os.replace"的原子替换，窗口级操作另外加
线程锁与文件锁，保证同一窗口的完成回执与下游建批请求同时到达时只产生
一份可核对的交付结果。
"""
from __future__ import annotations

import contextlib
import hashlib
import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Iterator

from app.services import base as _base
from app.services.base import _ensure_dir

try:  # pragma: no cover - 非 POSIX 环境退化为进程内锁
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None  # type: ignore[assignment]


# 目录由 base.DATA_ROOT 惰性派生：测试可替换 DATA_ROOT 实现彻底隔离。
def _root() -> Path:
    return Path(_base.DATA_ROOT)


def credentials_dir() -> Path:
    return _root() / "credentials"


def receipts_dir() -> Path:
    return _root() / "receipts"


def deliveries_dir() -> Path:
    return _root() / "deliveries"


def locks_dir() -> Path:
    return _root() / ".locks"

STATE_HAS_RECORDS = "HAS_RECORDS"
STATE_ZERO_ROWS = "ZERO_ROWS"
STATE_CONFIRMED_EMPTY = "CONFIRMED_EMPTY"
STATE_EMPTY_UNCONFIRMED = "EMPTY_UNCONFIRMED"
STATE_MISSING = "MISSING"
STATE_UNREADABLE = "UNREADABLE"
STATE_CORRUPT = "CORRUPT"
STATE_WINDOW_NOT_FOUND = "WINDOW_NOT_FOUND"

READABLE_STATES = {STATE_HAS_RECORDS, STATE_EMPTY_UNCONFIRMED, STATE_CONFIRMED_EMPTY}
DELIVERABLE_STATES = {STATE_HAS_RECORDS, STATE_CONFIRMED_EMPTY}


def safe_name(value: object) -> str:
    """把窗口/目标编号收敛成可安全落盘的文件名片段。"""
    text = str(value or "unknown")
    cleaned = "".join(ch if (ch.isalnum() or ch in "-_.@") else "_" for ch in text)
    return cleaned or "unknown"


def canonical_records(records: list[dict[str, Any]]) -> str:
    """规范化记录文本，作为执行凭据与回执的稳定比较基准。"""
    return json.dumps(records, ensure_ascii=False, sort_keys=True, default=str,
                      separators=(",", ":"))


def record_checksum(records: list[dict[str, Any]]) -> str:
    """计算记录集合的完整 SHA256，空列表同样得到稳定摘要。"""
    return hashlib.sha256(canonical_records(records).encode("utf-8")).hexdigest()


def short_checksum(checksum: str | None) -> str:
    return (checksum or "")[:16]


# -- 原子写入 --

def _tmp_path(path: Path) -> Path:
    return path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")


def atomic_write_text(path: Path, text: str) -> None:
    """原子写文本：先写同目录临时文件，再 os.replace 覆盖目标。"""
    _ensure_dir(path.parent)
    tmp = _tmp_path(path)
    with open(tmp, "w", encoding="utf-8") as handle:
        handle.write(text)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp, path)


def atomic_write_json(path: Path, payload: Any) -> None:
    atomic_write_text(path, json.dumps(payload, ensure_ascii=False, indent=2, default=str))


def atomic_write_jsonl(path: Path, records: list[dict[str, Any]]) -> int:
    """原子覆盖写 jsonl，避免重试时在旧产物后追加出重复行。"""
    lines = [json.dumps(record, ensure_ascii=False, default=str) for record in records]
    atomic_write_text(path, "".join(line + "\n" for line in lines))
    return len(lines)


def append_jsonl(path: Path, records: list[dict[str, Any]]) -> int:
    """追加写事件/历史日志并落盘。"""
    _ensure_dir(path.parent)
    with open(path, "a", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    return len(records)


def read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    try:
        with open(path, "r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(payload, dict):
                    rows.append(payload)
    except OSError:
        return []
    return rows


# -- 严格产物读取（区分"零行"与"读不到"） --

def read_artifact_strict(path: Path) -> dict[str, Any]:
    """严格读取抽取产物，返回状态而不是把不可读文件当作零行。

    返回值中的 state 只可能是 HAS_RECORDS / ZERO_ROWS / MISSING /
    UNREADABLE / CORRUPT；只有 ZERO_ROWS 才可能在有执行凭据时被确认零行。
    """
    result: dict[str, Any] = {
        "state": STATE_MISSING,
        "exists": False,
        "readable": False,
        "records": [],
        "line_count": 0,
        "invalid_line_count": 0,
        "checksum": "",
        "reason": "产物文件不存在",
        "reason_code": "ARTIFACT_MISSING",
    }
    if not path.exists():
        return result
    result["exists"] = True
    if not path.is_file():
        result.update(state=STATE_UNREADABLE, reason="产物路径不是普通文件，当前不可读",
                      reason_code="ARTIFACT_UNREADABLE")
        return result
    try:
        raw = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        result.update(state=STATE_UNREADABLE, reason=f"产物文件不可读：{exc.__class__.__name__}",
                      reason_code="ARTIFACT_UNREADABLE")
        return result

    lines = [line for line in raw.splitlines() if line.strip()]
    result["line_count"] = len(lines)
    records: list[dict[str, Any]] = []
    invalid = 0
    for line in lines:
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            invalid += 1
            continue
        if isinstance(payload, dict):
            records.append(payload)
        else:
            invalid += 1
    result["invalid_line_count"] = invalid
    if invalid:
        result.update(state=STATE_CORRUPT, records=[], readable=False,
                      reason=f"产物存在 {invalid} 行无法解析，不能作为可交付数据",
                      reason_code="ARTIFACT_CORRUPT")
        return result
    result.update(records=records, readable=True,
                  checksum=record_checksum(records),
                  state=STATE_HAS_RECORDS if records else STATE_ZERO_ROWS,
                  reason="" if records else "产物可读且为空文件",
                  reason_code=None)
    return result


# -- 执行凭据 --

def credential_path(window_code: str) -> Path:
    return credentials_dir() / f"{safe_name(window_code)}.json"


def credential_history_path(window_code: str) -> Path:
    return credentials_dir() / f"{safe_name(window_code)}_history.jsonl"


def load_credential(window_code: str) -> dict[str, Any] | None:
    return read_json(credential_path(window_code))


def save_credential(credential: dict[str, Any]) -> None:
    atomic_write_json(credential_path(credential["window_code"]), credential)
    append_jsonl(credential_history_path(credential["window_code"]), [credential])


# -- 完成回执 --

def receipt_path(window_code: str) -> Path:
    return receipts_dir() / f"{safe_name(window_code)}.json"


def receipt_history_path(window_code: str) -> Path:
    return receipts_dir() / f"{safe_name(window_code)}_history.jsonl"


def load_receipt(window_code: str) -> dict[str, Any] | None:
    return read_json(receipt_path(window_code))


def load_receipt_history(window_code: str) -> list[dict[str, Any]]:
    return read_jsonl(receipt_history_path(window_code))


def save_receipt(receipt: dict[str, Any]) -> None:
    """保存最新完成回执；旧回执标为已被取代，历史仍可追溯。"""
    window_code = receipt["window_code"]
    previous = load_receipt(window_code)
    if previous and previous.get("receipt_code") != receipt.get("receipt_code"):
        previous = dict(previous)
        previous["status"] = "SUPERSEDED"
        previous["superseded_by"] = receipt.get("receipt_code")
        previous["superseded_at"] = time.time()
        atomic_write_json(receipt_path(window_code), previous)
        append_jsonl(receipt_history_path(window_code), [previous])
    atomic_write_json(receipt_path(window_code), receipt)
    append_jsonl(receipt_history_path(window_code), [receipt])


# -- 目标交付台账 --

def delivery_dir(target_code: str) -> Path:
    return deliveries_dir() / safe_name(target_code)


def delivery_path(target_code: str, window_code: str) -> Path:
    return delivery_dir(target_code) / f"{safe_name(window_code)}.json"


def delivery_history_path(target_code: str, window_code: str) -> Path:
    return delivery_dir(target_code) / f"{safe_name(window_code)}_history.jsonl"


def load_delivery(target_code: str, window_code: str) -> dict[str, Any] | None:
    return read_json(delivery_path(target_code, window_code))


def list_deliveries(target_code: str) -> list[dict[str, Any]]:
    """列出目标端每个窗口的最新交付记录（历史 jsonl 不参与）。"""
    directory = delivery_dir(target_code)
    if not directory.exists():
        return []
    rows: list[dict[str, Any]] = []
    for item in sorted(directory.glob("*.json")):
        payload = read_json(item)
        if payload:
            rows.append(payload)
    return rows


def save_delivery(entry: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    """幂等写入目标交付记录，返回 (最新记录, 是否新写入)。"""
    target_code = entry["target_code"]
    window_code = entry["window_code"]
    existing = load_delivery(target_code, window_code)
    if (existing
            and existing.get("receipt_code") == entry.get("receipt_code")
            and int(existing.get("revision", -1)) == int(entry.get("revision", -2))):
        return existing, False
    payload = dict(entry)
    payload["recorded_at"] = time.time()
    atomic_write_json(delivery_path(target_code, window_code), payload)
    append_jsonl(delivery_history_path(target_code, window_code), [payload])
    return payload, True


# -- 窗口级锁（线程重入 + 文件锁，覆盖同进程并发与多进程） --

_LOCKS: dict[str, threading.RLock] = {}
_DEPTH: dict[str, int] = {}
_REGISTRY_GUARD = threading.Lock()


@contextlib.contextmanager
def window_lock(window_code: str) -> Iterator[None]:
    key = safe_name(window_code)
    with _REGISTRY_GUARD:
        lock = _LOCKS.setdefault(key, threading.RLock())
    with lock:
        with _REGISTRY_GUARD:
            depth = _DEPTH.get(key, 0)
            _DEPTH[key] = depth + 1
        handle = None
        try:
            if depth == 0:
                _ensure_dir(locks_dir())
                handle = open(locks_dir() / f"window_{key}.lock", "a+", encoding="utf-8")
                if fcntl is not None:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            yield
        finally:
            if handle is not None:
                if fcntl is not None:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                handle.close()
            with _REGISTRY_GUARD:
                remaining = _DEPTH.get(key, 1) - 1
                if remaining <= 0:
                    _DEPTH.pop(key, None)
                else:
                    _DEPTH[key] = remaining
