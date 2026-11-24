"""空窗口交付闭环测试：窗口数据校验、完成回执、并发建批与连续范围核销。

覆盖：
- 可确认零行的窗口生成空窗口完成回执；文件暂不可读不得被当作零行、不得补造数据；
- 同一源端相邻窗口：第一个确认零行，第二个产物暂不可读；
- 完成回执与下游重新建批请求同时到达时只产生一份可核对交付；
- 重复处理同一窗口只得到同一份回执/交付；
- 传输批次消费空窗口回执，目标侧形成零行回执或由源侧结案跳过；
- 第二窗补到真实记录后，连续范围核销不跳过真实数据。
"""
from __future__ import annotations

import json
import threading
from pathlib import Path

import pytest

from app.services import base as base_module
from app.services import delivery
from app.services.arrival_reconcile import ArrivalReconcileService
from app.services.extract_window import ExtractWindowService
from app.services.source_register import SourceRegisterService
from app.services.staging_area import StagingAreaService
from app.services.transfer_batch import TransferBatchService
from app.store import store


@pytest.fixture()
def data_root(tmp_path, monkeypatch):
    """把数据目录整体切到临时目录，彻底隔离每个测试的产物与回执。"""
    root = tmp_path / "data"
    root.mkdir()
    monkeypatch.setattr(base_module, "DATA_ROOT", root)
    store._tables = {name: [] for name in store._tables}
    delivery._LOCKS.clear()
    delivery._DEPTH.clear()
    yield root
    for path in sorted(root.rglob("*"), reverse=True):
        if path.is_file():
            path.unlink()
        else:
            path.rmdir()


def _register_source(code: str = "SOUR-EMPTY") -> SourceRegisterService:
    register = SourceRegisterService()
    # 只登记不确认，避免 confirm_register 自动创建的配套窗口干扰用例。
    register.register_source({
        "源端编号": code, "系统名称": "空窗口测试", "数据域": "交易域",
        "记录格式": "JSON", "编码方式": "UTF-8", "日增量级": "10万",
        "抽取方式": "增量抽取", "登记游标": "0",
    })
    return register


def _create_window(ew: ExtractWindowService, source: str, start: str, end: str) -> dict:
    return ew.create_window(source, count=0, start=start, end=end)


class TestZeroRowConfirmation:
    def test_confirms_zero_rows_with_credential(self, data_root):
        _register_source()
        ew = ExtractWindowService()
        window = _create_window(ew, "SOUR-EMPTY", "2026-11-20T00:00:00", "2026-11-20T23:59:59")

        result = ew.execute_extract(window["id"], records=[])
        assert result["records_extracted"] == 0
        assert result["state"] == "CONFIRMED_EMPTY"

        receipt = result["receipt"]
        assert receipt["kind"] == "EMPTY_WINDOW"
        assert receipt["record_count"] == 0
        assert receipt["credential_id"] == result["credential_id"]

        assessment = ew.validate_window_data(window["id"])
        assert assessment["state"] == "CONFIRMED_EMPTY"
        assert assessment["confirmed_zero_rows"] is True
        assert assessment["deliverable"] is True
        assert assessment["credential_state"] == "VALID"

    def test_empty_file_without_credential_is_not_zero_rows(self, data_root):
        _register_source()
        ew = ExtractWindowService()
        window = _create_window(ew, "SOUR-EMPTY", "2026-11-20T00:00:00", "2026-11-20T23:59:59")
        # 手工放一个可读空文件，但没有执行凭据
        (data_root / "extracted").mkdir(exist_ok=True)
        (data_root / "extracted" / f"{window['窗口编号']}.jsonl").write_text("", encoding="utf-8")

        assessment = ew.validate_window_data(window["id"])
        assert assessment["state"] == "EMPTY_UNCONFIRMED"
        assert assessment["confirmed_zero_rows"] is False
        assert assessment["deliverable"] is False

        receipt = ew.complete_window(window["id"])
        assert receipt["ok"] is False
        assert receipt["receipt"] is None
        assert receipt["reason_code"] == "EMPTY_WINDOW_UNCONFIRMED"
        assert not delivery.load_receipt(window["窗口编号"])

    def test_missing_artifact_is_not_zero_rows(self, data_root):
        _register_source()
        ew = ExtractWindowService()
        window = _create_window(ew, "SOUR-EMPTY", "2026-11-20T00:00:00", "2026-11-20T23:59:59")

        assessment = ew.validate_window_data(window["id"])
        assert assessment["state"] == "MISSING"
        assert assessment["deliverable"] is False
        assert ew.confirm_zero_rows(window["id"])["ok"] is False

    def test_unreadable_artifact_does_not_fabricate(self, data_root):
        _register_source()
        ew = ExtractWindowService()
        window = _create_window(ew, "SOUR-EMPTY", "2026-11-20T00:00:00", "2026-11-20T23:59:59")
        # 产物路径被占位为目录：存在但当前不可读
        artifact = data_root / "extracted"
        artifact.mkdir(exist_ok=True)
        (artifact / f"{window['窗口编号']}.jsonl").mkdir()

        assessment = ew.validate_window_data(window["id"])
        assert assessment["state"] == "UNREADABLE"
        assert assessment["deliverable"] is False
        assert assessment["reason_code"] == "ARTIFACT_UNREADABLE"

        # 建批被拒绝，且不会生成任何批次或模拟数据
        batch = TransferBatchService().ensure_batch_for_window(window["窗口编号"])
        assert batch["ok"] is False
        assert not store.rows("transfer_batch")
        assert not (data_root / "batches").exists() or list((data_root / "batches").glob("*.jsonl")) == []

    def test_corrupt_artifact_rejected(self, data_root):
        _register_source()
        ew = ExtractWindowService()
        window = _create_window(ew, "SOUR-EMPTY", "2026-11-20T00:00:00", "2026-11-20T23:59:59")
        ew.execute_extract(window["id"], records=[])
        artifact = data_root / "extracted" / f"{window['窗口编号']}.jsonl"
        artifact.write_text("{not json}\n", encoding="utf-8")

        assessment = ew.validate_window_data(window["id"])
        assert assessment["state"] == "CORRUPT"
        assert assessment["deliverable"] is False


class TestReceiptIdempotency:
    def test_repeat_processing_returns_same_receipt(self, data_root):
        _register_source()
        ew = ExtractWindowService()
        window = _create_window(ew, "SOUR-EMPTY", "2026-11-20T00:00:00", "2026-11-20T23:59:59")
        ew.execute_extract(window["id"], records=[])

        first = ew.complete_window(window["id"])
        second = ew.complete_window(window["id"])
        assert first["receipt_code"] == second["receipt_code"]
        assert second["created"] is False
        history = delivery.load_receipt_history(window["窗口编号"])
        assert len([r for r in history if r["status"] == "ACTIVE"]) == 1

    def test_receipt_code_is_stable_across_retry(self, data_root):
        _register_source()
        ew = ExtractWindowService()
        window = _create_window(ew, "SOUR-EMPTY", "2026-11-20T00:00:00", "2026-11-20T23:59:59")
        run_a = ew.execute_extract(window["id"], records=[])
        run_b = ew.execute_extract(window["id"], records=[])
        assert run_a["receipt"]["receipt_code"] == run_b["receipt"]["receipt_code"]
        assert run_b["receipt_created"] is False


class TestEmptyWindowDelivery:
    def test_empty_receipt_consumed_by_batch_and_target(self, data_root):
        _register_source()
        ew = ExtractWindowService()
        window = _create_window(ew, "SOUR-EMPTY", "2026-11-20T00:00:00", "2026-11-20T23:59:59")
        extracted = ew.execute_extract(window["id"], records=[])
        batch_code = extracted["auto_batch"]["batch"]["批次编号"]

        tb = TransferBatchService()
        batch_entry = tb.find_batch_by_window(window["窗口编号"])
        assert batch_entry["批次类型"] == "空窗口"
        transfer = tb.start_transfer(batch_entry["id"])
        assert transfer["records_transferred"] == 0
        assert transfer["empty_window"] is True
        assert transfer["auto_delivery"]["target_receipt"] is True
        assert transfer["auto_delivery"]["status"] == "已交付_零行"

        # 目标回执与空窗口完成回执一一对应
        target = delivery.load_delivery("TARG-0001", window["窗口编号"])
        assert target["empty_window"] is True
        assert target["receipt_code"] == extracted["receipt"]["receipt_code"]

        # 重复传输：返回同一份交付结果，不产生第二个批次
        again = tb.start_transfer(batch_entry["id"])
        assert again["reused"] is True
        assert len([r for r in store.rows("transfer_batch") if r["窗口编号"] == window["窗口编号"]]) == 1

    def test_reconcile_empty_batch_balances(self, data_root):
        _register_source()
        ew = ExtractWindowService()
        window = _create_window(ew, "SOUR-EMPTY", "2026-11-20T00:00:00", "2026-11-20T23:59:59")
        ew.execute_extract(window["id"], records=[])
        tb = TransferBatchService()
        entry = tb.find_batch_by_window(window["窗口编号"])
        tb.start_transfer(entry["id"])

        ar = ArrivalReconcileService()
        recon_id = next(r["id"] for r in store.rows("arrival_reconcile")
                        if r["批次编号"] == entry["批次编号"])
        result = ar.execute_reconciliation(recon_id)
        assert result["expected"] == 0
        assert result["actual"] == 0
        assert result["landed"] is True
        assert result["balanced"] is True


class TestAdjacentWindows:
    def _setup_adjacent(self, ew, root_marker):
        _register_source()
        first = _create_window(ew, "SOUR-EMPTY", "2026-11-20T00:00:00", "2026-11-20T23:59:59")
        second = _create_window(ew, "SOUR-EMPTY", "2026-11-21T00:00:00", "2026-11-21T23:59:59")
        assert first["窗口编号"] != second["窗口编号"]
        ew.execute_extract(first["id"], records=[])
        ew.execute_extract(second["id"], records=[])
        # 第二窗产物暂不可读
        artifact = base_module.DATA_ROOT / "extracted" / f"{second['窗口编号']}.jsonl"
        artifact.unlink()
        artifact.mkdir()
        return first, second

    def test_first_confirmed_zero_second_unreadable(self, data_root):
        ew = ExtractWindowService()
        first, second = self._setup_adjacent(ew, data_root)
        first_assessment = ew.validate_window_data(first["id"])
        second_assessment = ew.validate_window_data(second["id"])
        assert first_assessment["state"] == "CONFIRMED_EMPTY"
        assert second_assessment["state"] == "UNREADABLE"

        # 第二窗不能确认零行；已建出的空批次也不能被交付，更不能再建新批。
        assert ew.confirm_zero_rows(second["id"])["ok"] is False
        tb = TransferBatchService()
        before = len([r for r in store.rows("transfer_batch") if r["窗口编号"] == second["窗口编号"]])
        rebuilt = tb.ensure_batch_for_window(second["窗口编号"])
        assert rebuilt["ok"] is False
        after = len([r for r in store.rows("transfer_batch") if r["窗口编号"] == second["窗口编号"]])
        assert after == before
        existing = tb.find_batch_by_window(second["窗口编号"])
        assert "error" in tb.start_transfer(existing["id"])
        assert not (base_module.DATA_ROOT / "batches" / f"{existing['批次编号']}.jsonl").exists()

    def test_concurrent_receipt_and_rebatch_yields_single_delivery(self, data_root):
        ew = ExtractWindowService()
        first, second = self._setup_adjacent(ew, data_root)
        # 第二窗补到真实记录后，产物可读；此时完成回执与下游重新建批并发。
        artifact = base_module.DATA_ROOT / "extracted" / f"{second['窗口编号']}.jsonl"
        artifact.rmdir()
        ew.execute_extract(second["id"], records=[
            {"id": i, "updated_at": "2026-11-21T01:00:00", "amount": i * 10} for i in range(1, 4)
        ])

        tb = TransferBatchService()
        barrier = threading.Barrier(2)
        results: list[dict] = []
        errors: list[BaseException] = []

        def issue_receipt():
            try:
                barrier.wait()
                results.append(ew.complete_window(second["id"]))
            except BaseException as exc:  # pragma: no cover
                errors.append(exc)

        def request_rebatch():
            try:
                barrier.wait()
                results.append(tb.ensure_batch_for_window(second["窗口编号"]))
            except BaseException as exc:  # pragma: no cover
                errors.append(exc)

        threads = [threading.Thread(target=issue_receipt), threading.Thread(target=request_rebatch)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        assert not errors

        # 只有一个可交付批次、一个有效回执；历史空批次被标记取代
        second_batches = [r for r in store.rows("transfer_batch") if r["窗口编号"] == second["窗口编号"]]
        active = [r for r in second_batches if r["交付状态"] != "已取代"]
        assert len(active) == 1
        receipt = delivery.load_receipt(second["窗口编号"])
        assert receipt["kind"] == "DATA_WINDOW"
        assert active[0]["回执编号"] == receipt["receipt_code"]

        # 重复建批/重复执行抽取都只得到同一份交付
        again = tb.ensure_batch_for_window(second["窗口编号"])
        assert again["created"] is False
        assert again["batch"]["批次编号"] == active[0]["批次编号"]


class TestRangeReconciliation:
    def _prepare(self):
        """构造三窗：W1 零行已交付；W2 零行源侧结案未交付；W3 有真实记录已交付。"""
        _register_source()
        ew = ExtractWindowService()
        w1 = _create_window(ew, "SOUR-EMPTY", "2026-11-20T00:00:00", "2026-11-20T23:59:59")
        w2 = _create_window(ew, "SOUR-EMPTY", "2026-11-21T00:00:00", "2026-11-21T23:59:59")
        w3 = _create_window(ew, "SOUR-EMPTY", "2026-11-22T00:00:00", "2026-11-22T23:59:59")
        tb = TransferBatchService()

        ew.execute_extract(w1["id"], records=[])
        tb.start_transfer(tb.find_batch_by_window(w1["窗口编号"])["id"])

        ew.execute_extract(w2["id"], records=[])
        ew.close_window(w2["id"])  # 源侧结案，不建批

        ew.execute_extract(w3["id"], records=[
            {"id": i, "updated_at": "2026-11-22T01:00:00", "amount": i * 100} for i in range(1, 6)
        ])
        tb.start_transfer(tb.find_batch_by_window(w3["窗口编号"])["id"])
        return ew, [w1, w2, w3]

    def test_range_accounts_empty_skip_and_real(self, data_root):
        ew, (w1, w2, w3) = self._prepare()
        ar = ArrivalReconcileService()
        result = ar.reconcile_range("SOUR-EMPTY")
        assert result["window_count"] == 3
        assert result["accounted"] == 2
        assert result["skipped"] == 1
        assert result["unaccounted"] == 0
        assert result["delivered_records"] == 5
        assert result["complete"] is True

        by_code = {row["window_code"]: row for row in result["windows"]}
        assert by_code[w1["窗口编号"]]["status"] == "accounted_empty"
        assert by_code[w2["窗口编号"]]["status"] == "skipped"
        assert by_code[w2["窗口编号"]]["closure_code"]
        assert by_code[w3["窗口编号"]]["status"] == "accounted"

    def test_explain_zero_row_window(self, data_root):
        ew, (w1, w2, w3) = self._prepare()
        ar = ArrivalReconcileService()
        explained_delivered = ar.explain_window("SOUR-EMPTY", w1["窗口编号"])
        assert explained_delivered["status"] == "target_receipt"
        assert explained_delivered["zero_row_window"] is True
        explained_skipped = ar.explain_window("SOUR-EMPTY", w2["窗口编号"])
        assert explained_skipped["status"] == "source_closed_skipped"
        assert explained_skipped["zero_row_window"] is True

    def test_real_window_second_round_not_skipped(self, data_root):
        """第一轮第二窗暂不可读；第二轮补到真实记录后不得被范围核销跳过。"""
        _register_source()
        ew = ExtractWindowService()
        first = _create_window(ew, "SOUR-EMPTY", "2026-11-20T00:00:00", "2026-11-20T23:59:59")
        second = _create_window(ew, "SOUR-EMPTY", "2026-11-21T00:00:00", "2026-11-21T23:59:59")
        tb = TransferBatchService()
        ar = ArrivalReconcileService()

        # 第一轮：第一窗确认零行并交付；第二窗抽取后产物暂不可读
        ew.execute_extract(first["id"], records=[])
        tb.start_transfer(tb.find_batch_by_window(first["窗口编号"])["id"])
        ew.execute_extract(second["id"], records=[])
        artifact = base_module.DATA_ROOT / "extracted" / f"{second['窗口编号']}.jsonl"
        artifact.unlink()
        artifact.mkdir()

        round_one = ar.reconcile_range("SOUR-EMPTY")
        assert round_one["accounted"] == 1
        assert round_one["skipped"] == 0
        assert round_one["unaccounted"] == 1
        assert round_one["complete"] is False
        gap = round_one["gaps"][0]
        assert gap["window_code"] == second["窗口编号"]

        # 第二轮：第二窗补到真实记录，重新建批并交付
        artifact.rmdir()
        ew.execute_extract(second["id"], records=[
            {"id": i, "updated_at": "2026-11-21T02:00:00", "amount": i} for i in range(1, 3)
        ])
        second_batch = tb.ensure_batch_for_window(second["窗口编号"])
        assert second_batch["record_count"] == 2
        tb.start_transfer(second_batch["batch"]["id"])

        round_two = ar.reconcile_range("SOUR-EMPTY")
        assert round_two["skipped"] == 0
        assert round_two["unaccounted"] == 0
        assert round_two["delivered_records"] == 2
        by_code = {row["window_code"]: row for row in round_two["windows"]}
        assert by_code[second["窗口编号"]]["status"] == "accounted"
        assert by_code[second["窗口编号"]]["actual"] == 2

        # 前后窗口回执核对：第一窗仍是零行目标回执，第二窗是真实数据回执
        assert by_code[first["窗口编号"]]["status"] == "accounted_empty"
        assert by_code[first["窗口编号"]]["actual"] == 0
        assert by_code[second["窗口编号"]]["receipt_code"] != by_code[first["窗口编号"]]["receipt_code"]

    def test_stale_batch_cannot_deliver_old_empty_content(self, data_root):
        _register_source()
        ew = ExtractWindowService()
        window = _create_window(ew, "SOUR-EMPTY", "2026-11-20T00:00:00", "2026-11-20T23:59:59")
        ew.execute_extract(window["id"], records=[])
        tb = TransferBatchService()
        empty_batch = tb.find_batch_by_window(window["窗口编号"])

        # 窗口补到真实记录，回执升级；旧空批次不能被当作真实数据交付
        ew.execute_extract(window["id"], records=[{"id": 1, "updated_at": "x", "amount": 5}])
        result = tb.start_transfer(empty_batch["id"])
        assert "error" in result
        assert result["reason_code"] == "WINDOW_RECEIPT_CHANGED"
        assert not (base_module.DATA_ROOT / "batches" / f"{empty_batch['批次编号']}.jsonl").exists()

        new_batch = tb.ensure_batch_for_window(window["窗口编号"])
        assert new_batch["record_count"] == 1
        assert new_batch["batch"]["批次编号"] != empty_batch["批次编号"]
        transferred = tb.start_transfer(new_batch["batch"]["id"])
        assert transferred["records_transferred"] == 1
        assert empty_batch["交付状态"] == "已取代"


class TestArtifactStrictness:
    def test_staging_refuses_unreadable_transfer_artifact(self, data_root):
        _register_source()
        ew = ExtractWindowService()
        window = _create_window(ew, "SOUR-EMPTY", "2026-11-20T00:00:00", "2026-11-20T23:59:59")
        ew.execute_extract(window["id"], records=[])
        tb = TransferBatchService()
        batch = tb.find_batch_by_window(window["窗口编号"])
        tb.start_transfer(batch["id"])

        batch_file = base_module.DATA_ROOT / "batches" / f"{batch['批次编号']}.jsonl"
        batch_file.unlink()
        batch_file.mkdir()

        # 从未落地过的批次 + 不可读传输产物：拒绝落地，不能补造数据
        fresh = TransferBatchService().create_batch(window["窗口编号"], 0)
        result = StagingAreaService().land_batch(fresh["batch_code"], 0)
        assert "error" in result
        assert result["state"] == "MISSING"
        assert not list((base_module.DATA_ROOT / "staging").glob(
            f"*{delivery.safe_name(fresh['batch_code'])}*.jsonl"))


class TestHttpSurface:
    def test_actions_are_exposed(self, data_root):
        from app.routers import domain_actions

        _register_source()
        ew = ExtractWindowService()
        window = _create_window(ew, "SOUR-EMPTY", "2026-11-20T00:00:00", "2026-11-20T23:59:59")
        ew.execute_extract(window["id"], records=[])

        validated = domain_actions.validate_window(domain_actions.WindowRef(window_id=window["id"]))
        assert validated["state"] == "CONFIRMED_EMPTY"

        completed = domain_actions.complete_window(domain_actions.WindowRef(window_id=window["id"]))
        assert completed["receipt"]["kind"] == "EMPTY_WINDOW"

        ensured = domain_actions.ensure_batch(domain_actions.BatchEnsurePayload(
            window_code=window["窗口编号"]))
        assert ensured["ok"] is True

        explained = domain_actions.explain_window(domain_actions.ExplainPayload(
            source_code="SOUR-EMPTY", window_code=window["窗口编号"]))
        assert explained["status"] in {"target_receipt", "unaccounted"}

        ranged = domain_actions.reconcile_range(domain_actions.RangePayload(
            source_code="SOUR-EMPTY"))
        assert ranged["window_count"] == 1


class TestCredentialIntegrity:
    def test_credential_mismatch_blocks_delivery(self, data_root):
        """产物被外部篡改后执行凭据对不上，不得确认、不得建批。"""
        _register_source()
        ew = ExtractWindowService()
        window = _create_window(ew, "SOUR-EMPTY", "2026-11-20T00:00:00", "2026-11-20T23:59:59")
        ew.execute_extract(window["id"], records=[
            {"id": 1, "updated_at": "2026-11-20T01:00:00", "amount": 1}
        ])
        artifact = data_root / "extracted" / f"{window['窗口编号']}.jsonl"
        artifact.write_text(json.dumps({"id": 999, "amount": 42}) + "\n", encoding="utf-8")

        assessment = ew.validate_window_data(window["id"])
        assert assessment["state"] == "CREDENTIAL_MISMATCH"
        assert assessment["deliverable"] is False
        assert TransferBatchService().ensure_batch_for_window(window["窗口编号"])["ok"] is False

    def test_credential_count_mismatch_on_empty(self, data_root):
        """执行凭据记录有记录但产物为空，不能确认零行。"""
        _register_source()
        ew = ExtractWindowService()
        window = _create_window(ew, "SOUR-EMPTY", "2026-11-20T00:00:00", "2026-11-20T23:59:59")
        ew.execute_extract(window["id"], records=[{"id": 1}])
        artifact = data_root / "extracted" / f"{window['窗口编号']}.jsonl"
        artifact.write_text("", encoding="utf-8")

        assessment = ew.validate_window_data(window["id"])
        assert assessment["state"] == "CREDENTIAL_MISMATCH"
        assert ew.confirm_zero_rows(window["id"])["ok"] is False

    def test_superseded_receipt_kept_in_history(self, data_root):
        _register_source()
        ew = ExtractWindowService()
        window = _create_window(ew, "SOUR-EMPTY", "2026-11-20T00:00:00", "2026-11-20T23:59:59")
        empty_run = ew.execute_extract(window["id"], records=[])
        data_run = ew.execute_extract(window["id"], records=[{"id": 1, "amount": 2}])

        active = delivery.load_receipt(window["窗口编号"])
        assert active["kind"] == "DATA_WINDOW"
        assert active["revision"] == 2
        history = delivery.load_receipt_history(window["窗口编号"])
        superseded = [r for r in history if r["status"] == "SUPERSEDED"]
        assert superseded and superseded[0]["receipt_code"] == empty_run["receipt"]["receipt_code"]
        assert superseded[0]["superseded_by"] == data_run["receipt"]["receipt_code"]

    def test_unregistered_window_demo_path_is_labelled(self, data_root):
        """未关联登记窗口的直建批次仍兼容演示路径，但结果显式标注来源。"""
        tb = TransferBatchService()
        batch = tb.create_batch("EXTR-UNREGISTERED", 4, "TARG-0001")
        result = tb.start_transfer(batch["batch_id"], chunk_size=2)
        assert result["records_transferred"] == 4
        assert result["data_source"] == "simulated_demo"
        assert (data_root / "events" / "transfer_demo_log.jsonl").exists()


class TestRangeStaleness:
    def test_skipped_window_later_gets_data_is_not_silently_skipped(self, data_root):
        _register_source()
        ew = ExtractWindowService()
        window = _create_window(ew, "SOUR-EMPTY", "2026-11-20T00:00:00", "2026-11-20T23:59:59")
        ew.execute_extract(window["id"], records=[])
        ew.close_window(window["id"])

        ar = ArrivalReconcileService()
        assert ar.reconcile_range("SOUR-EMPTY")["skipped"] == 1

        # 结案后补到真实记录但尚未重新交付：旧结案失效，不能被继续记作跳过
        ew.execute_extract(window["id"], records=[{"id": 1, "amount": 7}])
        after = ar.reconcile_range("SOUR-EMPTY")
        assert after["skipped"] == 0
        assert after["unaccounted"] == 1
        assert after["gaps"][0]["state"] == "CLOSURE_STALE"
        assert ar.explain_window("SOUR-EMPTY", window["窗口编号"])["status"] == "unaccounted"

    def test_range_scope_limits_window_span(self, data_root):
        _register_source()
        ew = ExtractWindowService()
        w1 = _create_window(ew, "SOUR-EMPTY", "2026-11-20T00:00:00", "2026-11-20T23:59:59")
        w2 = _create_window(ew, "SOUR-EMPTY", "2026-11-21T00:00:00", "2026-11-21T23:59:59")
        w3 = _create_window(ew, "SOUR-EMPTY", "2026-11-22T00:00:00", "2026-11-22T23:59:59")
        ew.execute_extract(w2["id"], records=[])
        TransferBatchService().start_transfer(
            TransferBatchService().find_batch_by_window(w2["窗口编号"])["id"])

        ar = ArrivalReconcileService()
        scoped = ar.reconcile_range("SOUR-EMPTY", start_window=w2["窗口编号"],
                                    end_window=w2["窗口编号"])
        assert scoped["window_count"] == 1
        assert scoped["first_window"] == w2["窗口编号"]
        assert scoped["last_window"] == w2["窗口编号"]
        assert scoped["complete"] is True

    def test_range_log_is_written(self, data_root):
        _register_source()
        ew = ExtractWindowService()
        window = _create_window(ew, "SOUR-EMPTY", "2026-11-20T00:00:00", "2026-11-20T23:59:59")
        ew.execute_extract(window["id"], records=[])
        ArrivalReconcileService().reconcile_range("SOUR-EMPTY")
        logs = (data_root / "events" / "range_reconcile_log.jsonl").read_text(encoding="utf-8")
        assert "reconcile_range" in logs
