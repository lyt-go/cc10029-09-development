"""领域动作接口：窗口数据校验、完成回执、按回执建批、范围核销与值班解释。

这些接口把"零行窗口"与"文件暂不可读"的区分暴露给调度和值班人员；
所有业务判断仍在 app/services 中完成，路由层只做参数收敛与错误映射。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.services.arrival_reconcile import ArrivalReconcileService
from app.services.extract_window import ExtractWindowService
from app.services.transfer_batch import TransferBatchService

router = APIRouter(prefix="/api", tags=["领域动作"])


class WindowRef(BaseModel):
    window_id: int | None = None
    window_code: str | None = None


class BatchEnsurePayload(WindowRef):
    target_code: str = "TARG-0001"


class TransferPayload(BaseModel):
    chunk_size: int = Field(default=3, ge=1, le=10_000)


class RangePayload(BaseModel):
    source_code: str
    target_code: str = "TARG-0001"
    start_window: str | None = None
    end_window: str | None = None


class ExplainPayload(BaseModel):
    source_code: str
    window_code: str
    target_code: str = "TARG-0001"


def _resolve_window(payload: WindowRef) -> dict[str, Any]:
    service = ExtractWindowService()
    if payload.window_id is not None:
        window = service.get_entry(payload.window_id)
    elif payload.window_code:
        window = service.find_window_by_code(payload.window_code)
    else:
        window = None
    if window is None:
        raise HTTPException(status_code=404, detail="抽取窗口不存在")
    return window


@router.post("/extract_window/validate")
def validate_window(payload: WindowRef) -> dict[str, Any]:
    """校验窗口数据文件：区分有记录、确认零行、空但未确认、文件缺失/不可读。"""
    window = _resolve_window(payload)
    return ExtractWindowService().validate_window_data(window["id"])


@router.post("/extract_window/complete")
def complete_window(payload: WindowRef) -> dict[str, Any]:
    """为可确认（含确认零行）的窗口生成完成回执；不可确认时返回原因。"""
    window = _resolve_window(payload)
    return ExtractWindowService().complete_window(window["id"])


@router.post("/extract_window/confirm-zero")
def confirm_zero_rows(payload: WindowRef) -> dict[str, Any]:
    """确认零行窗口：必须有执行凭据，文件暂不可读会被拒绝。"""
    window = _resolve_window(payload)
    result = ExtractWindowService().confirm_zero_rows(window["id"])
    if not result.get("ok") and result.get("reason_code") in {
            "WINDOW_NOT_FOUND", "EXECUTION_CREDENTIAL_MISSING"}:
        raise HTTPException(status_code=409, detail=result.get("reason"))
    return result


@router.post("/transfer_batch/ensure")
def ensure_batch(payload: BatchEnsurePayload) -> dict[str, Any]:
    """消费窗口完成回执并幂等建批；空窗口完成回执会建出零记录批次。"""
    if not payload.window_code:
        raise HTTPException(status_code=422, detail="window_code 必填")
    return TransferBatchService().ensure_batch_for_window(
        payload.window_code, target_code=payload.target_code)


@router.post("/transfer_batch/{entry_id}/transfer")
def run_transfer(entry_id: int, payload: TransferPayload | None = None) -> dict[str, Any]:
    """执行批次传输（幂等重试：同一批次返回同一份交付结果）。"""
    chunk_size = (payload or TransferPayload()).chunk_size
    return TransferBatchService().start_transfer(entry_id, chunk_size=chunk_size)


@router.post("/arrival_reconcile/range")
def reconcile_range(payload: RangePayload) -> dict[str, Any]:
    """按连续窗口范围核销目标交付，返回已核销/源侧跳过/未核销明细。"""
    return ArrivalReconcileService().reconcile_range(
        payload.source_code, target_code=payload.target_code,
        start_window=payload.start_window, end_window=payload.end_window)


@router.post("/arrival_reconcile/explain")
def explain_window(payload: ExplainPayload) -> dict[str, Any]:
    """值班解释：窗口是否形成目标回执，或已由源侧结案并在目标侧记作跳过。"""
    return ArrivalReconcileService().explain_window(
        payload.source_code, payload.window_code, target_code=payload.target_code)
