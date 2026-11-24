"""增量窗口领域服务：维护按增量字段切分的抽取窗口边界、抽取执行与推进水位。

本模块负责根据源端登记信息创建抽取窗口，执行数据抽取并写入抽取产物文件，
完成后自动驱动传输批次创建，形成源端登记→窗口抽取→批次传输的贯通链路。

在贯通链路之上，本模块新增窗口数据校验与交付闭环：
- 只有存在执行凭据（credential）且产物可读时，窗口数据才可被确认；
- 产物可读且为空、且执行凭据记录零条时，才允许确认零行；
- "文件不存在/不可读/无法解析"与"确认零行"严格区分，前者不允许生成模拟数据；
- 可确认的窗口生成完成回执（receipt），重复处理同一内容只返回同一份回执。
"""
from __future__ import annotations

import time
import uuid
from typing import Any

from app.services import delivery
from app.services.base import BaseModuleService
from app.store import store


CREDENTIAL_VALID = "VALID"
CREDENTIAL_MISSING = "MISSING"
CREDENTIAL_MISMATCH = "MISMATCH"

STATE_HAS_RECORDS = delivery.STATE_HAS_RECORDS
STATE_ZERO_ROWS = delivery.STATE_ZERO_ROWS
STATE_CONFIRMED_EMPTY = delivery.STATE_CONFIRMED_EMPTY
STATE_EMPTY_UNCONFIRMED = delivery.STATE_EMPTY_UNCONFIRMED
STATE_MISSING = delivery.STATE_MISSING
STATE_UNREADABLE = delivery.STATE_UNREADABLE
STATE_CORRUPT = delivery.STATE_CORRUPT
STATE_CREDENTIAL_MISMATCH = "CREDENTIAL_MISMATCH"
STATE_WINDOW_NOT_FOUND = delivery.STATE_WINDOW_NOT_FOUND


class ExtractWindowService(BaseModuleService):
    MODULE = "extract_window"
    ENTITY = "抽取窗口"
    OPERATION_TERM = "切分"
    LIST_FIELDS = ["窗口编号", "源端编号", "增量字段", "窗口起点", "窗口终点", "抽取条数", "窗口类型", "推进水位", "完成回执"]
    REQUIRED_FIELDS = ["窗口编号", "源端编号", "增量字段"]
    STATUS_ORDER = ["草稿", "待抽取", "抽取中", "已闭合"]
    ACTION_RULES = {"启动抽取": "抽取中", "闭合窗口": "已闭合", "标记作废": "已闭合"}
    NEGATIVE_ACTIONS = ["标记作废"]

    # -- 窗口创建 --

    def create_window(
        self,
        source_code: str,
        count: int = 10,
        *,
        start: str | None = None,
        end: str | None = None,
        window_type: str = "增量抽取",
    ) -> dict[str, Any]:
        """为指定源端创建增量抽取窗口并写入窗口配置快照，记录窗口边界与初始水位。

        start/end 用于构造同一源端相邻窗口；未传时按当天日期生成。窗口编号会
        去重，避免同一秒内连续创建两个相邻窗口时编号冲突。
        """
        sources = [r for r in store.rows("source_register") if r.get("源端编号") == source_code]
        if not sources:
            raise ValueError(f"源端编号 {source_code} 未登记，请先完成源端登记")
        window_code = self._unique_window_code(source_code)
        entry, missing = self.create_entry({
            "窗口编号": window_code,
            "源端编号": source_code,
            "增量字段": "updated_at",
            "窗口起点": start or time.strftime("%Y-%m-%dT00:00:00", time.localtime(time.time())),
            "窗口终点": end or time.strftime("%Y-%m-%dT23:59:59", time.localtime(time.time())),
            "抽取条数": str(count),
            "窗口类型": window_type,
            "推进水位": "0",
        })
        if entry is None:
            raise ValueError(f"创建窗口失败: {missing}")
        # 写入窗口配置快照
        delivery.atomic_write_jsonl(
            self._data_dir("windows") / f"{entry['窗口编号']}_config.jsonl",
            [{"window_id": entry["id"], "window_code": entry["窗口编号"], "source": source_code,
              "start": entry["窗口起点"], "end": entry["窗口终点"], "count": count,
              "created_at": time.time()}],
        )
        return entry

    def _unique_window_code(self, source_code: str) -> str:
        base = f"EXTR-{source_code}-{int(time.time())}"
        existing = {str(r.get("窗口编号")) for r in store.rows(self.MODULE)}
        candidate = base
        suffix = 2
        while candidate in existing:
            candidate = f"{base}-{suffix}"
            suffix += 1
        return candidate

    def find_window_by_code(self, window_code: str) -> dict[str, Any] | None:
        for row in store.rows(self.MODULE):
            if row.get("窗口编号") == window_code:
                return row
        return None

    def list_source_windows(self, source_code: str) -> list[dict[str, Any]]:
        """按窗口起点/id 排序返回同一源端的窗口，供连续范围核销使用。"""
        rows = [r for r in store.rows(self.MODULE) if r.get("源端编号") == source_code]
        return sorted(rows, key=lambda r: (str(r.get("窗口起点", "")), int(r.get("id", 0))))

    # -- 执行抽取（写入真实产物与执行凭据） --

    def execute_extract(
        self,
        window_id: int,
        record_count: int = 10,
        *,
        records: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """执行增量窗口数据抽取，生成抽取产物文件、执行凭据与完成回执。

        records 显式传入时按其原样写入（允许传入空列表，表示本次抽取确认为零行）；
        未传入时保留原有的示例数据生成行为。写入产物、执行凭据与完成回执三步
        在同一窗口锁内完成，避免下游建批请求读到半成品。
        """
        window = self.get_entry(window_id)
        if window is None:
            return {"error": f"抽取窗口 {window_id} 不存在"}
        window_code = window["窗口编号"]
        source_code = window["源端编号"]

        with delivery.window_lock(window_code):
            entry, msg = self.run_action(window_id, "启动抽取")
            if entry is None:
                return {"error": msg, "window_id": window_id}

            if records is None:
                effective = self._simulate_records(source_code, count=record_count, field_prefix="ext")
            else:
                # 真实记录逐条复制，保持原始字段；空列表同样是合法抽取结果。
                effective = [dict(record) for record in records]

            output_file = self._artifact_path(window_code)
            written = delivery.atomic_write_jsonl(output_file, effective)
            checksum = delivery.record_checksum(effective)

            credential = {
                "credential_id": f"CRED-{window_code}-{uuid.uuid4().hex[:12]}",
                "window_id": window_id,
                "window_code": window_code,
                "source_code": source_code,
                "records_extracted": written,
                "checksum": checksum,
                "artifact_path": str(output_file),
                "executed_at": time.time(),
                "mode": "explicit" if records is not None else "simulated",
            }
            delivery.save_credential(credential)

            self._write_checkpoint(f"window_{window_code}", {
                "window_code": window_code,
                "source_code": source_code,
                "records_extracted": written,
                "checksum": checksum,
                "credential_id": credential["credential_id"],
                "last_extract_at": time.time(),
                "watermark": str(written),
            })

            assessment = self._assess(window)
            receipt_result = self._ensure_receipt(window, assessment)

            result = {
                "window_code": window_code,
                "source_code": source_code,
                "records_extracted": written,
                "artifact_path": str(output_file),
                "checksum": checksum,
                "credential_id": credential["credential_id"],
                "watermark": str(written),
                "state": assessment["state"],
                "receipt": receipt_result.get("receipt"),
                "receipt_created": receipt_result.get("created", False),
            }

            # 抽取完成后自动触发传输批次创建；同一回执只会对应一个批次。
            try:
                tb_svc = self._safe_import("app.services.transfer_batch", "TransferBatchService")()
                batch_result = tb_svc.ensure_batch_for_window(window_code)
                result["auto_batch"] = batch_result
            except Exception as exc:  # pragma: no cover - 防御性兜底
                result["auto_batch"] = {"error": str(exc)}

            return result

    # -- 数据校验 --

    def _artifact_path(self, window_code: str):
        return self._data_dir("extracted") / f"{delivery.safe_name(window_code)}.jsonl"

    def validate_window_data(self, window_id: int | None = None, *,
                             window_code: str | None = None) -> dict[str, Any]:
        """校验窗口数据文件，返回可交付性判断与拒绝原因。"""
        window = self.get_entry(window_id) if window_id is not None else None
        if window is None and window_code:
            window = self.find_window_by_code(window_code)
        if window is None:
            return {
                "window_id": window_id,
                "window_code": window_code,
                "state": STATE_WINDOW_NOT_FOUND,
                "deliverable": False,
                "confirmed_zero_rows": False,
                "record_count": 0,
                "checksum": "",
                "reason_code": "WINDOW_NOT_FOUND",
                "reason": f"抽取窗口 {window_code or window_id} 不存在",
                "artifact": {},
                "credential": None,
                "receipt": None,
            }
        return self._assess(window)

    # 兼容别名：便于不同调用方按既有命名习惯接入。
    validate_artifact = validate_window_data
    check_window_data = validate_window_data

    def _assess(self, window: dict[str, Any]) -> dict[str, Any]:
        window_code = window["窗口编号"]
        artifact_path = self._artifact_path(window_code)
        artifact = delivery.read_artifact_strict(artifact_path)
        credential = delivery.load_credential(window_code)
        receipt = delivery.load_receipt(window_code)

        state = artifact["state"]
        reason_code = artifact.get("reason_code")
        reason = artifact.get("reason", "")
        record_count = len(artifact["records"])
        checksum = artifact.get("checksum", "")

        credential_state = CREDENTIAL_MISSING
        if credential is None:
            pass
        else:
            cred_count = int(credential.get("records_extracted", -1))
            cred_checksum = str(credential.get("checksum", ""))
            if (artifact["state"] in {STATE_HAS_RECORDS, STATE_ZERO_ROWS}
                    and cred_count == record_count
                    and cred_checksum == checksum):
                credential_state = CREDENTIAL_VALID
            else:
                credential_state = CREDENTIAL_MISMATCH

        # 产物与执行凭据交叉校验，任何"读不到"都不能被当作零行。
        if artifact["state"] in {STATE_MISSING, STATE_UNREADABLE, STATE_CORRUPT}:
            state = artifact["state"]
        elif artifact["state"] == STATE_HAS_RECORDS:
            if credential_state == CREDENTIAL_MISSING:
                state = "UNVERIFIED"
                reason_code = "EXECUTION_CREDENTIAL_MISSING"
                reason = "产物存在记录但缺少该窗口的执行凭据，不能确认是本窗口的抽取结果"
            elif credential_state == CREDENTIAL_MISMATCH:
                state = STATE_CREDENTIAL_MISMATCH
                reason_code = "CREDENTIAL_MISMATCH"
                reason = "执行凭据的条数或校验值与产物不一致，窗口数据不可确认"
            else:
                state = STATE_HAS_RECORDS
                reason_code = None
                reason = ""
        elif artifact["state"] == STATE_ZERO_ROWS:
            if credential_state == CREDENTIAL_VALID and credential and int(credential.get("records_extracted", -1)) == 0:
                state = STATE_CONFIRMED_EMPTY
                reason_code = None
                reason = "产物可读且为空，执行凭据确认本次抽取零行"
            elif credential_state == CREDENTIAL_MISMATCH:
                state = STATE_CREDENTIAL_MISMATCH
                reason_code = "CREDENTIAL_MISMATCH"
                reason = "执行凭据记录的条数与空产物不一致，不能确认零行"
            else:
                state = STATE_EMPTY_UNCONFIRMED
                reason_code = "EMPTY_WINDOW_UNCONFIRMED"
                reason = "产物为空但缺少该窗口的执行凭据，不能确认零行"

        deliverable = state in {STATE_HAS_RECORDS, STATE_CONFIRMED_EMPTY}
        return {
            "window_id": window.get("id"),
            "window_code": window_code,
            "source_code": window.get("源端编号"),
            "window_status": window.get("status"),
            "window_start": window.get("窗口起点"),
            "window_end": window.get("窗口终点"),
            "artifact_path": str(artifact_path),
            "artifact_state": artifact["state"],
            "artifact_exists": artifact["exists"],
            "artifact_readable": artifact["readable"],
            "line_count": artifact["line_count"],
            "invalid_line_count": artifact["invalid_line_count"],
            "record_count": record_count,
            "checksum": checksum,
            "credential": credential,
            "credential_state": credential_state,
            "receipt": receipt,
            "state": state,
            "reason_code": reason_code,
            "reason": reason,
            "deliverable": deliverable,
            "confirmed_zero_rows": state == STATE_CONFIRMED_EMPTY,
            "empty": record_count == 0,
        }

    # -- 完成回执 --

    def _ensure_receipt(self, window: dict[str, Any], assessment: dict[str, Any]) -> dict[str, Any]:
        """为可确认的窗口生成/复用完成回执；不可确认时明确拒绝。"""
        window_code = window["窗口编号"]
        if not assessment["deliverable"]:
            return {
                "ok": False,
                "created": False,
                "receipt": None,
                "state": assessment["state"],
                "reason_code": assessment["reason_code"],
                "reason": assessment["reason"]
                or "窗口数据当前不可确认，不允许生成完成回执",
            }

        existing = delivery.load_receipt(window_code)
        fingerprint = {
            "kind": "EMPTY_WINDOW" if assessment["empty"] else "DATA_WINDOW",
            "record_count": assessment["record_count"],
            "checksum": assessment["checksum"],
        }
        if existing and existing.get("status") == "ACTIVE" and all(
                existing.get(key) == value for key, value in fingerprint.items()):
            # 同一窗口、同一内容重复处理：返回同一份可核对回执。
            return {"ok": True, "created": False, "receipt": existing,
                    "state": assessment["state"], "reason_code": None,
                    "reason": "窗口内容未变化，复用既有完成回执"}

        revision = int(existing.get("revision", 0)) + 1 if existing else 1
        code_suffix = (assessment["checksum"] or "0")[:8]
        receipt = {
            "receipt_code": f"RCPT-{delivery.safe_name(window_code)}-R{revision}-{code_suffix}",
            "window_code": window_code,
            "window_id": window.get("id"),
            "source_code": window.get("源端编号"),
            "window_start": window.get("窗口起点"),
            "window_end": window.get("窗口终点"),
            "revision": revision,
            "kind": fingerprint["kind"],
            "record_count": assessment["record_count"],
            "checksum": assessment["checksum"],
            "credential_id": (assessment["credential"] or {}).get("credential_id"),
            "artifact_path": assessment["artifact_path"],
            "status": "ACTIVE",
            "created_at": time.time(),
        }
        delivery.save_receipt(receipt)
        window["完成回执"] = receipt["receipt_code"]
        delivery.append_jsonl(
            self._data_dir("events") / "window_receipt_log.jsonl",
            [{"action": "issue_receipt", "window_code": window_code,
              "receipt_code": receipt["receipt_code"], "revision": revision,
              "kind": receipt["kind"], "record_count": receipt["record_count"],
              "checksum": receipt["checksum"], "at": time.time()}],
        )
        return {"ok": True, "created": True, "receipt": receipt,
                "state": assessment["state"], "reason_code": None,
                "reason": "完成回执已生成"}

    def complete_window(self, window_id: int | None = None, *,
                        window_code: str | None = None) -> dict[str, Any]:
        """校验并生成窗口完成回执（幂等）；不可确认时返回未就绪原因。"""
        window = self.get_entry(window_id) if window_id is not None else None
        if window is None and window_code:
            window = self.find_window_by_code(window_code)
        if window is None:
            return {"ok": False, "created": False, "receipt": None,
                    "state": STATE_WINDOW_NOT_FOUND, "reason_code": "WINDOW_NOT_FOUND",
                    "reason": f"抽取窗口 {window_code or window_id} 不存在"}

        with delivery.window_lock(window["窗口编号"]):
            assessment = self._assess(window)
            result = self._ensure_receipt(window, assessment)
            result["assessment"] = assessment
            result["state"] = assessment["state"]
            if result["ok"]:
                result["receipt_code"] = result["receipt"]["receipt_code"]
                result["revision"] = result["receipt"]["revision"]
            return result

    issue_completion_receipt = complete_window
    generate_completion_receipt = complete_window

    def confirm_zero_rows(self, window_id: int | None = None, *,
                          window_code: str | None = None) -> dict[str, Any]:
        """确认零行：必须有执行凭据支撑，且产物可读为空；否则拒绝且不生成回执。"""
        result = self.complete_window(window_id, window_code=window_code)
        if not result.get("ok"):
            return result
        if result["receipt"]["kind"] != "EMPTY_WINDOW":
            return {
                "ok": False,
                "created": False,
                "receipt": None,
                "state": result["state"],
                "reason_code": "NOT_ZERO_ROWS",
                "reason": f"该窗口确认有 {result['receipt']['record_count']} 条记录，不能按零行窗口处理",
                "assessment": result.get("assessment"),
            }
        result["confirmed_zero_rows"] = True
        return result

    confirm_empty_window = confirm_zero_rows

    # -- 闭合 --

    def close_window(self, window_id: int) -> dict[str, Any]:
        """闭合抽取窗口，校验产物并生成完成回执与源侧结案凭据。

        闭合不会凭空制造数据：产物不可读或为空但未确认零行时，结果中
        delivery_ready 为 False，并给出原因；可确认窗口则同时形成完成回执。
        """
        window = self.get_entry(window_id)
        if window is None:
            return {"error": f"抽取窗口 {window_id} 不存在"}
        window_code = window["窗口编号"]

        with delivery.window_lock(window_code):
            assessment = self._assess(window)
            receipt_result = self._ensure_receipt(window, assessment)
            entry, msg = self.run_action(window_id, "闭合窗口")

            closure_code = f"CLOSE-{delivery.safe_name(window_code)}-{int(time.time())}"
            receipt = receipt_result.get("receipt") or {}
            closure = {
                "closure_code": closure_code,
                "window_code": window_code,
                "source_code": window.get("源端编号"),
                "window_status": entry["status"] if entry else "已闭合",
                "delivery_ready": bool(receipt_result.get("ok")),
                "receipt_code": receipt.get("receipt_code"),
                "receipt_revision": receipt.get("revision"),
                "receipt_kind": receipt.get("kind"),
                "record_count": assessment["record_count"],
                "checksum": assessment["checksum"],
                "state": assessment["state"],
                "reason": assessment["reason"],
                "closed_at": time.time(),
            }
            self._write_checkpoint(f"window_closure_{window_code}", closure)
            delivery.append_jsonl(
                self._data_dir("events") / "window_close_log.jsonl",
                [{"action": "close", **closure}],
            )

        return {
            "window_code": window_code,
            "status": entry["status"] if entry else "已闭合",
            "record_count": assessment["record_count"],
            "checksum": assessment["checksum"],
            "state": assessment["state"],
            "delivery_ready": closure["delivery_ready"],
            "receipt": receipt_result.get("receipt"),
            "closure_code": closure_code,
            "message": msg,
        }

    def get_closure(self, window_code: str) -> dict[str, Any] | None:
        return self._read_checkpoint(f"window_closure_{window_code}")
