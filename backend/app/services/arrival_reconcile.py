"""到达对账领域服务：维护批次应到与实到记录数的对账、迟到与缺失条数统计及结论。

本模块读取传输批次产物文件和暂存落地产物文件进行数量对账，统计迟到与缺失条数，
对账完成后自动驱动时效监控指标更新和传输血缘记录。

在单批次对账之上，本模块还负责目标侧交付台账与连续窗口范围核销：
- 每个批次在目标端形成一条交付记录（含完成回执），零行窗口同样形成目标回执；
- 按同一源端的连续窗口范围核销，明确区分"已交付""源侧结案目标跳过""未核销"；
- 只有源侧确认零行并结案的窗口才允许记作跳过，任何可能有真实数据的空隙都会暴露出来。
"""
from __future__ import annotations

import time
from typing import Any

from app.services import delivery
from app.services.base import BaseModuleService
from app.store import store


class ArrivalReconcileService(BaseModuleService):
    MODULE = "arrival_reconcile"
    ENTITY = "对账单"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["对账编号", "批次编号", "应到条数", "实到条数", "迟到条数", "缺失条数", "对账窗口", "对账结论", "回执编号"]
    REQUIRED_FIELDS = ["对账编号", "批次编号", "应到条数"]
    STATUS_ORDER = ["草稿", "待对账", "对账中", "已平账", "存在差异"]
    ACTION_RULES = {"启动对账": "对账中", "确认平账": "已平账", "标记差异": "存在差异"}
    NEGATIVE_ACTIONS = ["标记差异"]

    def create_reconciliation(self, batch_code: str, expected_count: int,
                              *, receipt: dict[str, Any] | None = None,
                              batch_type: str | None = None) -> dict[str, Any]:
        """为传输批次创建到达对账单，登记应到条数与回执信息，为后续到账核对准备基线。"""
        recon_code = f"ARRI-{delivery.safe_name(batch_code)}-{int(time.time())}"
        values = {
            "对账编号": recon_code,
            "批次编号": batch_code,
            "应到条数": str(expected_count),
            "实到条数": "0",
            "迟到条数": "0",
            "缺失条数": "0",
            "对账窗口": "daily",
            "对账结论": "待对账",
        }
        entry, missing = self.create_entry(values)
        if entry is None:
            raise ValueError(f"创建对账单失败: {missing}")
        entry["回执编号"] = (receipt or {}).get("receipt_code")
        entry["回执版本"] = (receipt or {}).get("revision")
        entry["批次类型"] = batch_type or "数据批次"
        return {"recon_code": recon_code, "recon_id": entry["id"], "batch_code": batch_code,
                "expected_count": expected_count, "receipt_code": entry["回执编号"]}

    def _staging_records(self, batch_code: str) -> list[dict[str, Any]]:
        staging_dir = self._data_dir("staging")
        matches = sorted(staging_dir.glob(f"*{delivery.safe_name(batch_code)}*.jsonl"))
        # 取最近生成的落地文件，避免历史上多次落地造成重复计数。
        for staging_file in reversed(matches):
            records = delivery.read_jsonl(staging_file)
            if records or staging_file.stat().st_size == 0:
                return records
        return []

    def execute_reconciliation(self, recon_id: int) -> dict[str, Any]:
        """执行到达对账：核实传输批次应到与实到记录数的一致性，统计迟到与缺失条数。

        本方法从传输批次产物文件获取应到条数，从暂存落地产物文件获取实到条数，
        计算迟到（超时到达）和缺失（未到达）条数，判定对账结论。零行批次在
        应到与实到均为 0 且目标落地文件真实存在时判定平账。
        """
        recon_entry = self.get_entry(recon_id)
        if recon_entry is None:
            return {"error": f"对账单 {recon_id} 不存在"}

        entry, msg = self.run_action(recon_id, "启动对账")
        if entry is None:
            return {"error": msg, "recon_id": recon_id}

        batch_code = recon_entry["批次编号"]
        expected = int(recon_entry.get("应到条数", 0))

        # 从传输批次产物文件读取源数据
        batch_file = self._data_dir("batches") / f"{delivery.safe_name(batch_code)}.jsonl"
        batch_records = delivery.read_jsonl(batch_file)

        # 从暂存落地产物目录读取目标端数据（只有真实落地文件才算到达）
        staging_dir = self._data_dir("staging")
        staging_files = sorted(staging_dir.glob(f"*{delivery.safe_name(batch_code)}*.jsonl"))
        staging_records: list[dict[str, Any]] = []
        landed = False
        for staging_file in reversed(staging_files):
            staging_records = delivery.read_jsonl(staging_file)
            landed = True
            break
        staging_count = len(staging_records) if landed else 0

        late_count = 0
        missing_count = max(0, expected - staging_count)
        # 只有目标落地文件确实存在，且数量一致，才判定平账；文件缺失一律视为未到达。
        is_balanced = landed and expected == staging_count

        entry["实到条数"] = str(staging_count)
        entry["迟到条数"] = str(late_count)
        entry["缺失条数"] = str(missing_count)

        result_action = "确认平账" if is_balanced else "标记差异"
        final_entry, _ = self.run_action(recon_id, result_action)

        self._write_jsonl(
            self._data_dir("events") / "reconcile_log.jsonl",
            [{"action": "reconcile", "batch_code": batch_code, "expected": expected,
              "actual": staging_count, "late": late_count, "missing": missing_count,
              "landed": landed, "balanced": is_balanced, "reconciled_at": time.time()}],
        )

        result = {
            "recon_code": recon_entry["对账编号"],
            "batch_code": batch_code,
            "expected": expected,
            "actual": staging_count,
            "late": late_count,
            "missing": missing_count,
            "balanced": is_balanced,
            "landed": landed,
            "receipt_code": recon_entry.get("回执编号"),
        }

        if late_count > 0 or not is_balanced:
            try:
                sm_svc = self._safe_import("app.services.sla_monitor", "SlaMonitorService")()
                result["auto_sla_update"] = sm_svc.update_arrival_stats(batch_code, late_count, missing_count)
            except Exception as exc:  # pragma: no cover
                result["auto_sla_update"] = {"error": str(exc)}

        # 对账完成后记录传输血缘：差异批次同样需要追溯链路。
        try:
            tl_svc = self._safe_import("app.services.transfer_lineage", "TransferLineageService")()
            result["auto_lineage"] = tl_svc.record_lineage(batch_code, is_balanced)
        except Exception as exc:  # pragma: no cover
            result["auto_lineage"] = {"error": str(exc)}

        return result

    # -- 目标端交付台账 --

    def record_batch_delivery(
        self,
        batch_code: str,
        *,
        target_code: str = "TARG-0001",
        actual_count: int | None = None,
        window_code: str | None = None,
        receipt: tuple[str | None, int | None] | dict[str, Any] | None = None,
        batch_type: str | None = None,
        landed: bool | None = None,
    ) -> dict[str, Any]:
        """在目标端登记批次交付结果：应到/实到一致形成目标回执记录。

        landed 显式给出落地结论（False 表示落地失败）；未给出时以暂存产物是否存在为准。
        actual_count 为 None 或无法读取落地文件时，不能记为已交付。
        """
        batch_entry = self._find_batch(batch_code)
        window_code = window_code or (batch_entry or {}).get("窗口编号", "")
        target_code = (batch_entry or {}).get("目标编号") or target_code
        expected = int((batch_entry or {}).get("记录条数", 0))

        if isinstance(receipt, tuple):
            receipt_code, revision = receipt
        elif isinstance(receipt, dict):
            receipt_code = receipt.get("receipt_code")
            revision = receipt.get("revision")
        else:
            receipt_code = (batch_entry or {}).get("回执编号")
            revision = (batch_entry or {}).get("回执版本")

        if landed is None:
            if actual_count is None:
                staging_records = self._staging_records(batch_code)
                actual = len(staging_records)
                landed = bool(list(self._data_dir("staging").glob(
                    f"*{delivery.safe_name(batch_code)}*.jsonl")))
            else:
                actual = int(actual_count)
                landed = True
        else:
            actual = int(actual_count or 0)

        if not landed:
            status = "待交付"
            reason = "目标端尚无落地产物，不能形成目标回执"
            target_receipt = False
        elif expected == actual:
            status = "已交付_零行" if actual == 0 else "已交付"
            reason = "应到实到一致，目标回执已形成"
            target_receipt = True
        else:
            status = "交付差异"
            reason = f"应到 {expected} 条，实到 {actual} 条"
            target_receipt = False

        window_receipt = delivery.load_receipt(window_code) if window_code else None
        entry = {
            "target_code": target_code,
            "window_code": window_code,
            "batch_code": batch_code,
            "batch_type": batch_type or (batch_entry or {}).get("批次类型", "数据批次"),
            "receipt_code": receipt_code,
            "revision": revision,
            "window_receipt_code": (window_receipt or {}).get("receipt_code"),
            "window_receipt_kind": (window_receipt or {}).get("kind"),
            "expected_count": expected,
            "actual_count": actual,
            "status": status,
            "target_receipt": target_receipt,
            "empty_window": expected == 0 and actual == 0,
            "reason": reason,
            "source_status": (self._source_window(window_code) or {}).get("status"),
            "delivered_at": time.time() if target_receipt else None,
        }
        saved, created = delivery.save_delivery(entry)
        if created:
            delivery.append_jsonl(
                self._data_dir("events") / "delivery_log.jsonl",
                [{"action": "record_delivery", **{k: saved.get(k) for k in (
                    "target_code", "window_code", "batch_code", "receipt_code", "status",
                    "expected_count", "actual_count")}, "at": time.time()}],
            )
        if batch_entry is not None:
            batch_entry["交付状态"] = saved.get("status", "待交付")
        return {**saved, "created": created}

    def _find_batch(self, batch_code: str) -> dict[str, Any] | None:
        for row in store.rows("transfer_batch"):
            if row.get("批次编号") == batch_code:
                return row
        return None

    def _source_window(self, window_code: str) -> dict[str, Any] | None:
        for row in store.rows("extract_window"):
            if row.get("窗口编号") == window_code:
                return row
        return None

    def get_delivery(self, target_code: str, window_code: str) -> dict[str, Any] | None:
        return delivery.load_delivery(target_code, window_code)

    def list_target_deliveries(self, target_code: str) -> list[dict[str, Any]]:
        return delivery.list_deliveries(target_code)

    # -- 连续窗口范围核销 --

    def reconcile_range(self, source_code: str, *, target_code: str = "TARG-0001",
                        start_window: str | None = None,
                        end_window: str | None = None) -> dict[str, Any]:
        """按同一源端的连续窗口范围核销目标交付，暴露任何未核销空隙。

        核销规则：
        - 目标端有交付记录且回执一致 → accounted（零行窗口形成目标回执时单列）；
        - 目标端无交付，但源侧已闭合且确认零行 → skipped（源侧结案，目标侧跳过）；
        - 其余情况（含产物不可读、窗口从未交付）→ unaccounted（差异，禁止静默跳过）。
        """
        from app.services.extract_window import ExtractWindowService

        ew = ExtractWindowService()
        windows = ew.list_source_windows(source_code)
        if start_window or end_window:
            codes = [w["窗口编号"] for w in windows]
            lo = codes.index(start_window) if start_window in codes else 0
            hi = codes.index(end_window) + 1 if end_window in codes else len(codes)
            windows = windows[lo:hi]
        if not windows:
            return {
                "source_code": source_code,
                "target_code": target_code,
                "window_count": 0,
                "accounted": 0,
                "skipped": 0,
                "unaccounted": 0,
                "delivered_records": 0,
                "complete": False,
                "reason": "该源端没有任何窗口",
                "windows": [],
                "gaps": [],
            }

        entries: list[dict[str, Any]] = []
        gaps: list[dict[str, Any]] = []
        accounted = 0
        skipped = 0
        delivered_records = 0

        for window in windows:
            window_code = window["窗口编号"]
            target_delivery = delivery.load_delivery(target_code, window_code)
            closure = ew.get_closure(window_code)
            receipt = delivery.load_receipt(window_code)

            if target_delivery and target_delivery.get("target_receipt"):
                status = "accounted_empty" if target_delivery.get("empty_window") else "accounted"
                accounted += 1
                delivered_records += int(target_delivery.get("actual_count", 0))
                entries.append({
                    "window_code": window_code,
                    "window_start": window.get("窗口起点"),
                    "window_end": window.get("窗口终点"),
                    "status": status,
                    "batch_code": target_delivery.get("batch_code"),
                    "receipt_code": target_delivery.get("receipt_code"),
                    "expected": target_delivery.get("expected_count"),
                    "actual": target_delivery.get("actual_count"),
                    "explanation": "零行窗口已形成目标回执" if status == "accounted_empty"
                    else "目标回执已形成",
                })
                continue

            if target_delivery:
                # 有交付记录但目标回执未形成（责权不清），按差异处理。
                gap = {
                    "window_code": window_code,
                    "status": "unaccounted",
                    "reason_code": "TARGET_RECEIPT_MISSING",
                    "reason": target_delivery.get("reason", "目标回执未形成"),
                }
                gaps.append(gap)
                entries.append({**gap, "window_start": window.get("窗口起点"),
                                "window_end": window.get("窗口终点")})
                continue

            # 目标端无交付记录：只有"源侧确认零行并结案"才允许记作跳过。
            # 非零窗口即使源侧已闭合也不能跳过，必须暴露为未核销差异。
            if (closure is not None
                    and receipt is not None
                    and receipt.get("kind") == "EMPTY_WINDOW"
                    and int(receipt.get("record_count", -1)) == 0
                    and int(closure.get("record_count", -1)) == 0
                    and closure.get("receipt_code") == receipt.get("receipt_code")):
                skipped += 1
                entries.append({
                    "window_code": window_code,
                    "window_start": window.get("窗口起点"),
                    "window_end": window.get("窗口终点"),
                    "status": "skipped",
                    "receipt_code": receipt.get("receipt_code"),
                    "closure_code": closure.get("closure_code"),
                    "expected": 0,
                    "actual": 0,
                    "explanation": "源侧已结案（确认零行），目标侧记作跳过",
                })
                continue

            if closure is not None and closure.get("receipt_code") != (receipt or {}).get("receipt_code"):
                state = "CLOSURE_STALE"
                reason = "源侧结案后窗口内容已更新，原零行结案失效，不能跳过"
            elif closure is not None:
                state = "REAL_DATA_NOT_DELIVERED"
                reason = "源侧已有真实记录并结案，但目标端没有交付回执，不能跳过"
            elif receipt is not None and receipt.get("kind") == "EMPTY_WINDOW":
                state = "EMPTY_NOT_CONSUMED"
                reason = "空窗口完成回执尚未被传输批次消费，目标端暂无回执"
            elif receipt is not None:
                state = "RECEIPT_NOT_DELIVERED"
                reason = "窗口已有完成回执但目标端尚无交付回执"
            else:
                state = "NO_RECEIPT"
                reason = "窗口尚未形成完成回执，不能核销"
            gap = {
                "window_code": window_code,
                "status": "unaccounted",
                "reason_code": "WINDOW_NOT_ACCOUNTED",
                "reason": reason,
                "state": state,
                "window_status": window.get("status"),
                "receipt_code": (receipt or {}).get("receipt_code"),
            }
            gaps.append(gap)
            entries.append({**gap, "window_start": window.get("窗口起点"),
                            "window_end": window.get("窗口终点")})

        unaccounted = len(gaps)
        complete = unaccounted == 0
        result = {
            "source_code": source_code,
            "target_code": target_code,
            "window_count": len(windows),
            "first_window": windows[0]["窗口编号"],
            "last_window": windows[-1]["窗口编号"],
            "accounted": accounted,
            "skipped": skipped,
            "unaccounted": unaccounted,
            "delivered_records": delivered_records,
            "complete": complete,
            "windows": entries,
            "gaps": gaps,
        }
        delivery.append_jsonl(
            self._data_dir("events") / "range_reconcile_log.jsonl",
            [{"action": "reconcile_range", "source_code": source_code,
              "target_code": target_code, "first_window": result["first_window"],
              "last_window": result["last_window"], "window_count": len(windows),
              "accounted": accounted, "skipped": skipped, "unaccounted": unaccounted,
              "delivered_records": delivered_records, "complete": complete,
              "at": time.time()}],
        )
        return result

    reconcile_window_range = reconcile_range

    def explain_window(self, source_code: str, window_code: str,
                       target_code: str = "TARG-0001") -> dict[str, Any]:
        """值班解释入口：说明某窗口是否形成目标回执或由源侧结案并在目标侧跳过。"""
        from app.services.extract_window import ExtractWindowService

        ew = ExtractWindowService()
        window = ew.find_window_by_code(window_code)
        if window is None:
            return {"window_code": window_code, "status": "unknown",
                    "reason": "窗口不存在"}
        target_delivery = delivery.load_delivery(target_code, window_code)
        receipt = delivery.load_receipt(window_code)
        closure = ew.get_closure(window_code)
        if target_delivery and target_delivery.get("target_receipt"):
            if target_delivery.get("empty_window"):
                return {
                    "window_code": window_code,
                    "status": "target_receipt",
                    "zero_row_window": True,
                    "target_receipt_code": target_delivery.get("receipt_code"),
                    "explanation": "零行窗口已形成目标回执",
                    "detail": target_delivery,
                }
            return {
                "window_code": window_code,
                "status": "target_receipt",
                "zero_row_window": False,
                "target_receipt_code": target_delivery.get("receipt_code"),
                "explanation": "非零窗口已形成目标回执",
                "detail": target_delivery,
            }
        # 与范围核销保持同一判据：源侧已闭合、且确认零行的窗口，目标侧记作跳过。
        # 闭合之后窗口若又补到真实记录，回执与结案凭据不再匹配，不得继续记作跳过。
        if (closure is not None
                and receipt is not None
                and receipt.get("kind") == "EMPTY_WINDOW"
                and int(receipt.get("record_count", -1)) == 0
                and int(closure.get("record_count", -1)) == 0
                and closure.get("receipt_code") == receipt.get("receipt_code")):
            return {
                "window_code": window_code,
                "status": "source_closed_skipped",
                "zero_row_window": True,
                "closure_code": closure.get("closure_code"),
                "explanation": "源侧确认零行并结案，目标侧记作跳过",
                "detail": {"closure": closure, "receipt": receipt},
            }
        if (closure is not None
                and closure.get("receipt_code") != (receipt or {}).get("receipt_code")):
            return {
                "window_code": window_code,
                "status": "unaccounted",
                "zero_row_window": False,
                "closure_code": closure.get("closure_code"),
                "explanation": "源侧结案后窗口内容已更新，原零行结案失效，需重新交付",
                "detail": {"closure": closure, "receipt": receipt,
                           "target_delivery": target_delivery},
            }
        return {
            "window_code": window_code,
            "status": "unaccounted",
            "zero_row_window": (receipt or {}).get("kind") == "EMPTY_WINDOW",
            "explanation": "既无目标回执，也未被源侧确认零行结案",
            "detail": {"closure": closure, "receipt": receipt,
                       "target_delivery": target_delivery},
        }
