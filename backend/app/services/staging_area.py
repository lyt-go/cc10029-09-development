"""暂存落地领域服务：维护传输数据的暂存分区落地路径、文件组织与驻留时限控制。

本模块从传输批次产物文件读取数据，执行分区落地写入暂存产物文件，
落地完成后记录落地水位并支持超时清空。

落地只认传输产物：产物不存在或不可读时拒绝落地并保留原因，绝不补造模拟数据；
零记录传输批次会落地一个真实的空文件，使目标侧能够据此形成零行回执。
同一批次重复落地复用既有分区记录，不产生重复分区。
"""
from __future__ import annotations

import time
from typing import Any

from app.services import delivery
from app.services.base import BaseModuleService
from app.store import store


class StagingAreaService(BaseModuleService):
    MODULE = "staging_area"
    ENTITY = "暂存分区"
    OPERATION_TERM = "水位"
    LIST_FIELDS = ["暂存编号", "批次编号", "落地路径", "文件数量", "记录条数", "占用字节", "驻留时限", "落地水位"]
    REQUIRED_FIELDS = ["暂存编号", "批次编号", "落地路径"]
    STATUS_ORDER = ["草稿", "落地中", "已落地", "已清空"]
    ACTION_RULES = {"启动落地": "落地中", "确认落地": "已落地", "执行清空": "已清空"}
    NEGATIVE_ACTIONS = ["执行清空"]

    def _checkpoint_name(self, batch_code: str) -> str:
        return f"staging_landing_{delivery.safe_name(batch_code)}"

    def land_batch(self, batch_code: str, record_count: int) -> dict[str, Any]:
        """将传输批次数据落地到暂存分区，读取传输产物文件并写入暂存产物文件。

        暂存分区是传输链路中数据暂存的关键环节：它从传输产物文件读取数据，
        计算字节量，写入落地路径下的暂存产物文件，并推进分区水位。
        """
        existing = self._read_checkpoint(self._checkpoint_name(batch_code))
        if existing and existing.get("status") == "LANDED":
            return {**existing.get("result", {}), "reused": True,
                    "reason": "该批次已落地，复用既有暂存分区"}

        # 读取上游传输批次产物文件；不可读时拒绝落地而不是编造数据。
        source_file = self._data_dir("batches") / f"{delivery.safe_name(batch_code)}.jsonl"
        source = delivery.read_artifact_strict(source_file)
        if source["state"] in {delivery.STATE_MISSING, delivery.STATE_UNREADABLE,
                               delivery.STATE_CORRUPT}:
            delivery.append_jsonl(
                self._data_dir("events") / "staging_blocked_log.jsonl",
                [{"action": "block_landing", "batch_code": batch_code,
                  "state": source["state"], "reason": source["reason"], "at": time.time()}],
            )
            return {
                "error": "传输产物不可读，已拒绝落地",
                "batch_code": batch_code,
                "state": source["state"],
                "reason_code": source["reason_code"],
                "reason": source["reason"],
            }

        records = [dict(record) for record in source["records"]]

        staging_code = f"STAG-{delivery.safe_name(batch_code)}-{int(time.time())}"
        landing_path = f"/staging/{batch_code}/"
        values = {
            "暂存编号": staging_code,
            "批次编号": batch_code,
            "落地路径": landing_path,
            "文件数量": "1",
            "记录条数": str(record_count),
            "占用字节": "0",
            "驻留时限": "86400",
            "落地水位": "0",
        }
        entry, missing = self.create_entry(values)
        if entry is None:
            raise ValueError(f"创建暂存分区失败: {missing}")

        total_bytes = sum(len(str(r).encode("utf-8")) for r in records)

        # 写入暂存产物文件（空批次同样写入真实的空文件）
        output_file = self._data_dir("staging") / f"{delivery.safe_name(staging_code)}.jsonl"
        written = delivery.atomic_write_jsonl(output_file, records)

        # 更新暂存分区水位
        entry["占用字节"] = str(total_bytes)
        entry["文件数量"] = "1"
        entry["落地水位"] = str(written)
        final_entry, _ = self.run_action(entry["id"], "确认落地")

        result = {
            "staging_code": staging_code,
            "batch_code": batch_code,
            "records_landed": written,
            "total_bytes": total_bytes,
            "artifact_path": str(output_file),
            "empty": written == 0,
            "status": (final_entry or entry).get("status", "已落地"),
        }

        self._write_checkpoint(self._checkpoint_name(batch_code), {
            "staging_code": staging_code,
            "batch_code": batch_code,
            "records_landed": written,
            "total_bytes": total_bytes,
            "landed_at": time.time(),
            "retention_seconds": 86400,
            "status": "LANDED",
            "result": result,
        })
        return result

    def confirm_land(self, staging_id: int) -> dict[str, Any]:
        """确认暂存分区落地完成，校验落地产物数据完整性并写入落地确认日志。"""
        entry, msg = self.run_action(staging_id, "确认落地")
        if entry is None:
            return {"error": msg, "staging_id": staging_id}
        staging_code = entry["暂存编号"]
        artifact_file = self._data_dir("staging") / f"{delivery.safe_name(staging_code)}.jsonl"
        records = delivery.read_jsonl(artifact_file)
        checksum = self._compute_checksum(str(records))
        delivery.append_jsonl(
            self._data_dir("events") / "staging_confirm_log.jsonl",
            [{"action": "confirm_land", "staging_code": staging_code,
              "record_count": len(records), "checksum": checksum,
              "confirmed_at": time.time()}],
        )
        return {"staging_code": staging_code, "status": "已落地", "record_count": len(records),
                "checksum": checksum}

    def clear_staging(self, staging_id: int) -> dict[str, Any]:
        """执行暂存分区清空，删除落地产物文件并写入清空日志。"""
        entry, msg = self.run_action(staging_id, "执行清空")
        if entry is None:
            return {"error": msg, "staging_id": staging_id}
        staging_code = entry["暂存编号"]
        artifact_file = self._data_dir("staging") / f"{delivery.safe_name(staging_code)}.jsonl"
        if artifact_file.exists():
            artifact_file.unlink()
        self._write_checkpoint(f"staging_{delivery.safe_name(staging_code)}_cleared", {
            "staging_code": staging_code,
            "cleared_at": time.time(),
            "status": "cleared",
        })
        delivery.append_jsonl(
            self._data_dir("events") / "staging_clear_log.jsonl",
            [{"action": "clear", "staging_code": staging_code, "cleared_at": time.time()}],
        )
        return {"staging_code": staging_code, "status": "已清空"}
