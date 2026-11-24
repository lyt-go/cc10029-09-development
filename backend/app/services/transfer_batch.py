"""传输批次领域服务：维护传输批次的分片组织、记录条数与字节总量登记及批次水位推进。

本模块读取抽取窗口产物与完成回执后创建传输批次，分片写入传输数据文件，
传输完成后自动驱动暂存落地、校验核对和到达对账，形成窗口抽取→批次传输→落地校验的贯通链路。

关键约束：
- 建批必须依据窗口的完成回执；同一窗口同一回执只对应一个批次；
- 空窗口完成回执会被消费成"零记录批次"，不会生成模拟数据；
- 窗口产物缺失或暂不可读时拒绝传输，绝不补造模拟数据；
- 同一批次的重复传输请求返回同一份可核对结果。
"""
from __future__ import annotations

import time
from typing import Any

from app.services import delivery
from app.services.base import BaseModuleService
from app.store import store


class TransferBatchService(BaseModuleService):
    MODULE = "transfer_batch"
    ENTITY = "传输批次"
    OPERATION_TERM = "水位"
    LIST_FIELDS = ["批次编号", "窗口编号", "目标编号", "记录条数", "字节总量", "分片大小", "传输状态", "批次水位", "回执编号", "交付状态"]
    REQUIRED_FIELDS = ["批次编号", "窗口编号", "目标编号"]
    STATUS_ORDER = ["草稿", "待传输", "传输中", "已完成", "已失败"]
    ACTION_RULES = {"启动传输": "传输中", "标记完成": "已完成", "标记失败": "已失败"}
    NEGATIVE_ACTIONS = ["标记失败"]

    # -- 建批 --

    def create_batch(self, window_code: str, record_count: int, target_code: str = "TARG-0001",
                     receipt: dict[str, Any] | None = None) -> dict[str, Any]:
        """基于抽取窗口创建传输批次，传入产物文件的 record_count 并写入批次事件日志。"""
        batch_code = self._unique_batch_code(window_code)
        values = {
            "批次编号": batch_code,
            "窗口编号": window_code,
            "目标编号": target_code,
            "记录条数": str(record_count),
            "字节总量": "0",
            "分片大小": "1000",
            "传输状态": "草稿",
            "批次水位": "0",
        }
        entry, missing = self.create_entry(values)
        if entry is None:
            raise ValueError(f"创建传输批次失败: {missing}")
        entry["回执编号"] = (receipt or {}).get("receipt_code")
        entry["回执版本"] = (receipt or {}).get("revision")
        entry["批次类型"] = "空窗口" if (receipt or {}).get("kind") == "EMPTY_WINDOW" else "数据批次"
        entry["交付状态"] = "待交付"
        # 写入批次创建事件
        delivery.append_jsonl(
            self._data_dir("events") / "batch_create_log.jsonl",
            [{"action": "create_batch", "batch_code": batch_code, "window_code": window_code,
              "record_count": record_count, "target": target_code,
              "receipt_code": entry["回执编号"], "at": time.time()}],
        )
        return {"batch_code": batch_code, "batch_id": entry["id"], "window_code": window_code,
                "record_count": record_count, "target_code": target_code,
                "receipt_code": entry["回执编号"], "batch_type": entry["批次类型"]}

    def _unique_batch_code(self, window_code: str) -> str:
        base = f"TRAN-{window_code}-{int(time.time())}"
        existing = {str(r.get("批次编号")) for r in store.rows(self.MODULE)}
        candidate = base
        suffix = 2
        while candidate in existing:
            candidate = f"{base}-{suffix}"
            suffix += 1
        return candidate

    def find_batch_by_window(self, window_code: str) -> dict[str, Any] | None:
        for row in store.rows(self.MODULE):
            if row.get("窗口编号") == window_code:
                return row
        return None

    def ensure_batch_for_window(self, window_code: str,
                                target_code: str = "TARG-0001") -> dict[str, Any]:
        """消费窗口完成回执并幂等建批：同一窗口同一回执只产生一个批次。"""
        from app.services.extract_window import ExtractWindowService

        ew = ExtractWindowService()
        with delivery.window_lock(window_code):
            assessment = ew.validate_window_data(window_code=window_code)
            if not assessment["deliverable"]:
                return {
                    "ok": False,
                    "created": False,
                    "batch": None,
                    "window_code": window_code,
                    "state": assessment["state"],
                    "reason_code": assessment["reason_code"],
                    "reason": assessment["reason"] or "窗口数据不可交付，暂不建批",
                    "record_count": assessment["record_count"],
                }

            receipt_result = ew.complete_window(window_code=window_code)
            receipt = receipt_result.get("receipt")
            if not receipt_result.get("ok") or receipt is None:
                return {
                    "ok": False,
                    "created": False,
                    "batch": None,
                    "window_code": window_code,
                    "state": receipt_result.get("state", assessment["state"]),
                    "reason_code": receipt_result.get("reason_code", "RECEIPT_NOT_READY"),
                    "reason": receipt_result.get("reason", "完成回执未生成，暂不建批"),
                    "record_count": assessment["record_count"],
                }

            existing = self._find_batch_for_receipt(window_code, receipt["receipt_code"], target_code)
            if existing is not None:
                return {
                    "ok": True,
                    "created": False,
                    "batch": existing,
                    "window_code": window_code,
                    "state": assessment["state"],
                    "reason_code": None,
                    "reason": "该窗口完成回执已建批，复用既有批次",
                    "record_count": int(existing.get("记录条数", 0)),
                    "receipt_code": receipt["receipt_code"],
                    "batch_type": existing.get("批次类型"),
                }

            created = self.create_batch(window_code, assessment["record_count"],
                                        target_code=target_code, receipt=receipt)
            entry = store.find(self.MODULE, created["batch_id"])
            # 同一窗口的旧回执批次（例如补到真实记录前建的空批次）标记为被取代。
            self._supersede_older_batches(window_code, receipt, keep_batch_code=created["batch_code"])
            return {
                "ok": True,
                "created": True,
                "batch": entry,
                "window_code": window_code,
                "state": assessment["state"],
                "reason_code": None,
                "reason": "已按完成回执建批",
                "record_count": created["record_count"],
                "receipt_code": receipt["receipt_code"],
                "batch_type": created["batch_type"],
                "empty_window": receipt.get("kind") == "EMPTY_WINDOW",
            }

    def _find_batch_for_receipt(self, window_code: str, receipt_code: str,
                                target_code: str) -> dict[str, Any] | None:
        for row in store.rows(self.MODULE):
            if (row.get("窗口编号") == window_code
                    and row.get("回执编号") == receipt_code
                    and row.get("目标编号") == target_code):
                return row
        return None

    def _supersede_older_batches(self, window_code: str, receipt: dict[str, Any],
                                 *, keep_batch_code: str) -> None:
        for row in store.rows(self.MODULE):
            if (row.get("窗口编号") == window_code
                    and row.get("批次编号") != keep_batch_code
                    and row.get("交付状态") != "已交付"
                    and row.get("回执编号") != receipt["receipt_code"]):
                row["交付状态"] = "已取代"
                row["取代回执编号"] = receipt["receipt_code"]

    # -- 传输执行 --

    def _transfer_checkpoint(self, batch_code: str) -> dict[str, Any] | None:
        return self._read_checkpoint(f"transfer_{delivery.safe_name(batch_code)}")

    def start_transfer(self, batch_id: int, chunk_size: int = 3) -> dict[str, Any]:
        """启动传输批次执行：读取抽取窗口产物，按分片写入传输产物并推进水位。

        执行前会校验窗口产物与完成回执：文件缺失/不可读时直接拒绝，不补造数据；
        空窗口回执则生成零记录批次产物。同一批次的重复请求复用首次结果。
        """
        batch_entry = self.get_entry(batch_id)
        if batch_entry is None:
            return {"error": f"传输批次 {batch_id} 不存在"}

        batch_code = batch_entry["批次编号"]
        window_code = batch_entry.get("窗口编号", "unknown")

        existing = self._transfer_checkpoint(batch_code)
        if existing and existing.get("status") == "COMPLETED":
            return {**existing.get("result", {}), "reused": True,
                    "reason": "该批次已完成传输，复用既有交付结果"}

        # 已登记窗口：传输前按完成回执校验产物；未登记窗口保留原有的演示批次行为。
        window_row = None
        if window_code != "unknown":
            from app.services.extract_window import ExtractWindowService
            ew = ExtractWindowService()
            window_row = ew.find_window_by_code(window_code)
            if window_row is not None:
                assessment = ew.validate_window_data(window_code=window_code)
                if not assessment["deliverable"]:
                    self._record_blocked(batch_code, window_code, assessment)
                    return {
                        "error": "窗口数据不可交付，已拒绝传输",
                        "batch_code": batch_code,
                        "window_code": window_code,
                        "state": assessment["state"],
                        "reason_code": assessment["reason_code"],
                        "reason": assessment["reason"],
                    }
                linked_receipt = batch_entry.get("回执编号")
                active_receipt = (assessment.get("receipt") or {}).get("receipt_code")
                if linked_receipt and active_receipt and linked_receipt != active_receipt:
                    self._record_blocked(batch_code, window_code, {
                        "state": "STALE_RECEIPT",
                        "reason_code": "WINDOW_RECEIPT_CHANGED",
                        "reason": "窗口完成回执已更新，原批次不能再交付旧内容，请重新建批",
                    })
                    return {
                        "error": "窗口完成回执已更新，原批次不可继续交付",
                        "batch_code": batch_code,
                        "window_code": window_code,
                        "state": "STALE_RECEIPT",
                        "reason_code": "WINDOW_RECEIPT_CHANGED",
                        "reason": "窗口完成回执已更新，原批次不能再交付旧内容，请重新建批",
                    }

        entry, msg = self.run_action(batch_id, "启动传输")
        if entry is None:
            return {"error": msg, "batch_id": batch_id}

        # 读取上游产物：登记窗口严格按窗口校验结果取数；仅"未登记窗口"的
        # 历史直建批次保留演示数据路径，并显式标注数据来源以便审计。
        data_source = "artifact"
        if window_row is not None:
            strict = delivery.read_artifact_strict(
                self._data_dir("extracted") / f"{delivery.safe_name(window_code)}.jsonl")
            records = [dict(record) for record in strict["records"]]
        else:
            source_file = self._data_dir("extracted") / f"{delivery.safe_name(window_code)}.jsonl" \
                if window_code != "unknown" else None
            if source_file is not None and source_file.exists():
                records = self._read_jsonl(source_file)
            else:
                data_source = "simulated_demo"
                records = self._simulate_records(
                    batch_code, count=int(batch_entry.get("记录条数", 10)), field_prefix="trans")
                delivery.append_jsonl(
                    self._data_dir("events") / "transfer_demo_log.jsonl",
                    [{"action": "simulate_demo", "batch_code": batch_code,
                      "window_code": window_code,
                      "reason": "批次未关联已登记抽取窗口，沿用历史演示数据路径",
                      "at": time.time()}],
                )

        # 分片写入传输产物文件（原子覆盖，重试不产生重复行）
        output_file = self._data_dir("batches") / f"{delivery.safe_name(batch_code)}.jsonl"
        total_bytes = 0
        total_written = 0
        chunks: list[dict[str, Any]] = []
        staged_records: list[dict[str, Any]] = []
        for i in range(0, len(records), chunk_size):
            chunk = records[i:i + chunk_size]
            for rec in chunk:
                item = dict(rec)
                item["batch_code"] = batch_code
                item["chunk_index"] = i // chunk_size
                item["transferred_at"] = time.time()
                staged_records.append(item)
            chunk_bytes = sum(len(str(r).encode("utf-8")) for r in chunk)
            chunks.append({"chunk_index": i // chunk_size, "record_count": len(chunk),
                           "bytes": chunk_bytes})
            total_bytes += chunk_bytes
            total_written += len(chunk)
        delivery.atomic_write_jsonl(output_file, staged_records)

        # 更新批次水位和字节总量
        entry["字节总量"] = str(total_bytes)
        entry["记录条数"] = str(total_written)
        entry["批次水位"] = str(total_written)

        result: dict[str, Any] = {
            "batch_code": batch_code,
            "window_code": window_code,
            "records_transferred": total_written,
            "total_bytes": total_bytes,
            "chunks": len(chunks),
            "artifact_path": str(output_file),
            "empty_window": total_written == 0 and (batch_entry.get("批次类型") == "空窗口"),
            "receipt_code": batch_entry.get("回执编号"),
            "data_source": data_source,
        }

        # 传输完成后自动触发暂存落地
        try:
            sa_svc = self._safe_import("app.services.staging_area", "StagingAreaService")()
            staging_result = sa_svc.land_batch(batch_code, total_written)
            result["auto_staging"] = staging_result
        except Exception as exc:  # pragma: no cover - 防御性兜底
            result["auto_staging"] = {"error": str(exc)}
            staging_result = {}

        # 传输完成后自动创建校验核对任务
        try:
            cv_svc = self._safe_import("app.services.checksum_verify", "ChecksumVerifyService")()
            result["auto_verify"] = cv_svc.create_verification(batch_code, total_written)
        except Exception as exc:  # pragma: no cover
            result["auto_verify"] = {"error": str(exc)}

        # 传输完成后自动创建到达对账单，并在目标端登记交付结果
        try:
            ar_svc = self._safe_import("app.services.arrival_reconcile", "ArrivalReconcileService")()
            result["auto_reconcile"] = ar_svc.create_reconciliation(
                batch_code, total_written,
                receipt={"receipt_code": batch_entry.get("回执编号"),
                         "revision": batch_entry.get("回执版本")},
                batch_type=batch_entry.get("批次类型"),
            )
            if "error" in staging_result:
                # 落地失败时不能把批次记成已交付。
                delivery_entry = ar_svc.record_batch_delivery(
                    batch_code,
                    target_code=batch_entry.get("目标编号", "TARG-0001"),
                    actual_count=None,
                    window_code=window_code,
                    receipt=(batch_entry.get("回执编号"), batch_entry.get("回执版本")),
                    batch_type=batch_entry.get("批次类型"),
                )
                result["auto_delivery"] = {**delivery_entry,
                                           "error": "暂存落地失败，目标回执未形成"}
            else:
                delivery_entry = ar_svc.record_batch_delivery(
                    batch_code,
                    target_code=batch_entry.get("目标编号", "TARG-0001"),
                    actual_count=int(staging_result.get("records_landed", total_written)),
                    window_code=window_code,
                    receipt=(batch_entry.get("回执编号"), batch_entry.get("回执版本")),
                    batch_type=batch_entry.get("批次类型"),
                    landed=True,
                )
                result["auto_delivery"] = delivery_entry
            entry["交付状态"] = delivery_entry.get("status", "待交付")
        except Exception as exc:  # pragma: no cover
            result["auto_delivery"] = {"error": str(exc)}

        self._write_checkpoint(f"transfer_{delivery.safe_name(batch_code)}", {
            "batch_code": batch_code,
            "window_code": window_code,
            "records_transferred": total_written,
            "total_bytes": total_bytes,
            "chunk_count": len(chunks),
            "receipt_code": batch_entry.get("回执编号"),
            "status": "COMPLETED",
            "result": result,
            "last_transfer_at": time.time(),
        })

        return result

    def _record_blocked(self, batch_code: str, window_code: str,
                        assessment: dict[str, Any]) -> None:
        delivery.append_jsonl(
            self._data_dir("events") / "transfer_blocked_log.jsonl",
            [{"action": "block_transfer", "batch_code": batch_code, "window_code": window_code,
              "state": assessment.get("state"), "reason_code": assessment.get("reason_code"),
              "reason": assessment.get("reason"), "at": time.time()}],
        )

    # -- 状态动作 --

    def mark_completed(self, batch_id: int) -> dict[str, Any]:
        """标记传输批次已完成，写入完成事件日志并记录批次汇总统计。"""
        entry, msg = self.run_action(batch_id, "标记完成")
        if entry is None:
            return {"error": msg, "batch_id": batch_id}
        batch_code = entry["批次编号"]
        artifact_file = self._data_dir("batches") / f"{delivery.safe_name(batch_code)}.jsonl"
        records = self._read_jsonl(artifact_file)
        checksum = self._compute_checksum(str(records))
        delivery.append_jsonl(
            self._data_dir("events") / "batch_complete_log.jsonl",
            [{"action": "complete", "batch_code": batch_code, "record_count": len(records),
              "checksum": checksum, "completed_at": time.time()}],
        )
        return {"batch_code": batch_code, "status": "已完成", "record_count": len(records),
                "checksum": checksum}

    def mark_failed(self, batch_id: int, reason: str = "") -> dict[str, Any]:
        """标记传输批次失败，写入失败事件日志并记录中断原因。"""
        entry, msg = self.run_action(batch_id, "标记失败")
        if entry is None:
            return {"error": msg, "batch_id": batch_id}
        batch_code = entry["批次编号"]
        delivery.append_jsonl(
            self._data_dir("events") / "batch_fail_log.jsonl",
            [{"action": "fail", "batch_code": batch_code, "reason": reason, "failed_at": time.time()}],
        )
        return {"batch_code": batch_code, "status": "已失败", "reason": reason}
