"""
使用统计路由模块（按真实后端模型） - 处理 /usage/* 相关的HTTP请求。

认证方式与 src/panel/*.py 保持一致：使用 verify_panel_token。
"""

from typing import Optional

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from log import log

from src.usage_stats_v2 import (
    get_usage_models,
    get_usage_summary,
    get_usage_timeseries,
    reset_usage_stats,
)
from src.utils import verify_panel_token


# 创建路由器（无 prefix，路径直接以 /usage 开头，与前端 fetch 一致）
router = APIRouter(tags=["usage"])


class UsageResetBody(BaseModel):
    mode: Optional[str] = None
    model: Optional[str] = None

    model_config = {"extra": "allow"}


def _normalize_range(range_val: Optional[str]) -> str:
    if range_val in ("1d", "7d", "30d"):
        return range_val
    return "7d"


def _normalize_mode(mode_val: Optional[str]) -> str:
    if mode_val in ("all", "geminicli", "antigravity"):
        return mode_val
    return "all"


@router.get("/usage/summary")
async def usage_summary(
    range: Optional[str] = None,
    mode: Optional[str] = None,
    token: str = Depends(verify_panel_token),
):
    """获取调用汇总：总调用、成功、失败、失败率、模型数量。"""
    try:
        rk = _normalize_range(range)
        md = _normalize_mode(mode)
        data = await get_usage_summary(range_key=rk, mode=md)
        return JSONResponse(content={"success": True, "data": data})
    except Exception as e:
        log.error(f"[USAGE] /usage/summary 失败: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": str(e)},
        )


@router.get("/usage/timeseries")
async def usage_timeseries(
    range: Optional[str] = None,
    mode: Optional[str] = None,
    token: str = Depends(verify_panel_token),
):
    """获取时间序列：dates、models、series。"""
    try:
        rk = _normalize_range(range)
        md = _normalize_mode(mode)
        data = await get_usage_timeseries(range_key=rk, mode=md)
        return JSONResponse(content={"success": True, "data": data})
    except Exception as e:
        log.error(f"[USAGE] /usage/timeseries 失败: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": str(e)},
        )


@router.get("/usage/models")
async def usage_models(
    range: Optional[str] = None,
    mode: Optional[str] = None,
    token: str = Depends(verify_panel_token),
):
    """获取模型明细列表。"""
    try:
        rk = _normalize_range(range)
        md = _normalize_mode(mode)
        data = await get_usage_models(range_key=rk, mode=md)
        return JSONResponse(content={"success": True, "data": data})
    except Exception as e:
        log.error(f"[USAGE] /usage/models 失败: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": str(e)},
        )


@router.get("/usage/stats")
async def usage_stats_compat(
    range: Optional[str] = None,
    mode: Optional[str] = None,
    token: str = Depends(verify_panel_token),
):
    """兼容旧接口 /usage/stats，返回按模型聚合的数据。"""
    try:
        rk = _normalize_range(range)
        md = _normalize_mode(mode)
        models = await get_usage_models(range_key=rk, mode=md)
        data = {}
        for m in models:
            data[m["model"]] = {
                "success_calls": m["success_calls"],
                "failure_calls": m["failure_calls"],
                "total_calls": m["total_calls"],
                "failure_rate": m["failure_rate"],
                "last_called_at": m["last_called_at"],
                "status_codes": m["status_codes"],
            }
        return JSONResponse(content={"success": True, "data": data})
    except Exception as e:
        log.error(f"[USAGE] /usage/stats 失败: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": str(e)},
        )


@router.get("/usage/aggregated")
async def usage_aggregated_compat(
    token: str = Depends(verify_panel_token),
):
    """兼容旧接口 /usage/aggregated，字段名保持旧变量名。"""
    try:
        summary = await get_usage_summary(range_key="1d", mode="all")
        model_count = summary.get("model_count", 0)
        total_calls_24h = summary.get("total_calls", 0)
        avg = round(total_calls_24h / model_count, 1) if model_count else 0.0
        data = {
            "total_calls_24h": total_calls_24h,
            "total_files": model_count,
            "avg_calls_per_file": avg,
        }
        return JSONResponse(content={"success": True, "data": data})
    except Exception as e:
        log.error(f"[USAGE] /usage/aggregated 失败: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": str(e)},
        )


@router.post("/usage/reset")
async def usage_reset(
    body: UsageResetBody,
    token: str = Depends(verify_panel_token),
):
    """重置统计。支持 {} / {mode} / {model} / {mode+model}。"""
    try:
        result = await reset_usage_stats(mode=body.mode, model=body.model)
        return JSONResponse(content=result)
    except Exception as e:
        log.error(f"[USAGE] /usage/reset 失败: {e}")
        return JSONResponse(
            status_code=500,
            content={"success": False, "message": str(e)},
        )
