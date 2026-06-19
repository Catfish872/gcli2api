"""
按真实后端模型统计调用情况（usage_stats_v2）。

与旧的 src/usage_stats.py（按凭证文件名统计 24h）完全独立，互不影响。
本模块按"实际发送到后端的模型"记录，统计维度为：
    date_key + mode + model + status_code

写入位置：真实后端 API client 层（src/api/geminicli.py、src/api/antigravity.py）。
统计始终是旁路：任何初始化/写入/查询失败只写日志，保持原聊天请求继续返回结果。

存储策略（跟随项目当前激活的 StorageAdapter）：
    - mongodb  -> collection usage_daily（复用 backend._db，不新建连接）
    - sqlite   -> usage_daily 表（CREDENTIALS_DIR 习惯路径，本地/fallback）
    - postgresql -> 暂不支持，记录日志并返回空统计
"""

from __future__ import annotations

import asyncio
import os
import sqlite3
import time
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from log import log

from .storage_adapter import get_storage_adapter

# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------

COLLECTION_NAME = "usage_daily"
SQLITE_TABLE_NAME = "usage_daily"
RANGE_DAYS = {"1d": 1, "7d": 7, "30d": 30}
DEFAULT_RANGE = "7d"

# 功能后缀（从长到短，保证 -maxthinking 先于 -max 被整体处理）
_USAGE_SUFFIXES = sorted(
    [
        "-maxthinking",
        "-nothinking",
        "-minimal",
        "-medium",
        "-search",
        "-think",
        "-high",
        "-max",
        "-low",
    ],
    key=lambda s: len(s),
    reverse=True,
)

# 功能前缀（仅从开头剥离一次）
_USAGE_PREFIXES = ("假流式/", "流式抗截断/")


# ---------------------------------------------------------------------------
# 模型名归一化
# ---------------------------------------------------------------------------

def normalize_usage_model_name(model_name: str) -> str:
    """
    将用户传入的带功能前缀/后缀的模型名归一化为真实后端模型名。

    规则（通用，不枚举模型名，适配未来新增模型）：
        1. 先去掉已知功能前缀：假流式/、流式抗截断/
        2. 循环剥离功能后缀，直到不再变化
    """
    if not model_name or not isinstance(model_name, str):
        return ""

    name = model_name.strip()

    # 1. 去功能前缀（从开头剥）
    for prefix in _USAGE_PREFIXES:
        if name.startswith(prefix):
            name = name[len(prefix):]
            break  # 只剥一次

    # 2. 循环剥离功能后缀
    changed = True
    while changed:
        changed = False
        for suffix in _USAGE_SUFFIXES:
            if name.endswith(suffix) and len(name) > len(suffix):
                name = name[: -len(suffix)]
                changed = True
                break

    return name


# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------

def _today_date_key() -> str:
    """服务器本地日期，格式 YYYY-MM-DD。"""
    return date.today().isoformat()


def _resolve_range_days(range_key: str) -> int:
    days = RANGE_DAYS.get(range_key)
    if days is None:
        days = RANGE_DAYS[DEFAULT_RANGE]
    return days


def _date_range_keys(range_key: str) -> List[str]:
    """返回从最早到今天（含今天）的 date_key 列表，长度等于天数。"""
    days = _resolve_range_days(range_key)
    today = date.today()
    keys: List[str] = []
    for i in range(days - 1, -1, -1):
        d = today - timedelta(days=i)
        keys.append(d.isoformat())
    return keys


async def _get_backend():
    """
    安全获取当前存储后端类型与底层对象。

    返回 (backend_type, backend_obj)。
    backend_obj：
        - mongodb: AsyncIOMotorDatabase（adapter._backend._db）
        - sqlite: SQLiteManager（adapter._backend）
        - 其它: None
    """
    try:
        adapter = await get_storage_adapter()
    except Exception as e:
        log.warning(f"[USAGE_V2] 获取存储适配器失败: {e}")
        return "none", None

    try:
        backend_type = adapter.get_backend_type()
    except Exception as e:
        log.warning(f"[USAGE_V2] 获取后端类型失败: {e}")
        return "none", None

    backend_obj = getattr(adapter, "_backend", None)

    if backend_type == "mongodb":
        db = getattr(backend_obj, "_db", None) if backend_obj else None
        if db is None:
            log.warning("[USAGE_V2] MongoDB backend 缺少 _db 对象")
            return "none", None
        return "mongodb", db

    if backend_type == "sqlite":
        if backend_obj is None:
            return "none", None
        return "sqlite", backend_obj

    # postgresql / unknown
    return backend_type, None


# ---------------------------------------------------------------------------
# MongoDB 实现
# ---------------------------------------------------------------------------

async def _ensure_mongo_indexes(db) -> None:
    """为 usage_daily 创建索引（幂等）。"""
    try:
        from pymongo import ASCENDING, IndexModel

        col = db[COLLECTION_NAME]
        indexes = [
            IndexModel(
                [
                    ("date_key", ASCENDING),
                    ("mode", ASCENDING),
                    ("model", ASCENDING),
                    ("status_code", ASCENDING),
                ],
                unique=True,
                name="idx_usage_daily_unique",
            ),
            IndexModel([("date_key", ASCENDING)], name="idx_usage_daily_date"),
            IndexModel([("mode", ASCENDING)], name="idx_usage_daily_mode"),
            IndexModel([("model", ASCENDING)], name="idx_usage_daily_model"),
        ]
        await col.create_indexes(indexes)
    except Exception as e:
        log.debug(f"[USAGE_V2] 创建 MongoDB 索引（可能已存在）: {e}")


async def _mongo_record(db, filter_doc: Dict[str, Any], success: bool) -> None:
    now = time.time()
    col = db[COLLECTION_NAME]
    if success:
        update = {
            "$inc": {"success_count": 1},
            "$set": {"last_called_at": now, "updated_at": now},
            "$setOnInsert": {"failure_count": 0},
        }
    else:
        update = {
            "$inc": {"failure_count": 1},
            "$set": {"last_called_at": now, "updated_at": now},
            "$setOnInsert": {"success_count": 0},
        }
    await col.update_one(filter_doc, update, upsert=True)


async def _mongo_query_rows(
    db, date_keys: List[str], mode: Optional[str]
) -> List[Dict[str, Any]]:
    col = db[COLLECTION_NAME]
    query: Dict[str, Any] = {"date_key": {"$in": date_keys}}
    if mode and mode != "all":
        query["mode"] = mode
    cursor = col.find(query)
    rows: List[Dict[str, Any]] = []
    async for doc in cursor:
        rows.append(
            {
                "date_key": doc.get("date_key"),
                "mode": doc.get("mode"),
                "model": doc.get("model"),
                "status_code": doc.get("status_code"),
                "success_count": doc.get("success_count", 0),
                "failure_count": doc.get("failure_count", 0),
                "last_called_at": doc.get("last_called_at"),
                "updated_at": doc.get("updated_at"),
            }
        )
    return rows


async def _mongo_reset(db, mode: Optional[str], model: Optional[str]) -> int:
    col = db[COLLECTION_NAME]
    query: Dict[str, Any] = {}
    if mode:
        query["mode"] = mode
    if model:
        query["model"] = normalize_usage_model_name(model)
    result = await col.delete_many(query)
    return getattr(result, "deleted_count", 0) or 0


# ---------------------------------------------------------------------------
# SQLite 实现
# ---------------------------------------------------------------------------

def _sqlite_db_path() -> str:
    """跟随项目习惯：CREDENTIALS_DIR/credentials.db。"""
    creds_dir = os.getenv("CREDENTIALS_DIR", "./creds")
    return os.path.join(creds_dir, "credentials.db")


def _sqlite_get_conn() -> sqlite3.Connection:
    path = _sqlite_db_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def _sqlite_ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SQLITE_TABLE_NAME} (
            date_key TEXT NOT NULL,
            mode TEXT NOT NULL,
            model TEXT NOT NULL,
            status_code INTEGER NOT NULL,
            success_count INTEGER DEFAULT 0,
            failure_count INTEGER DEFAULT 0,
            last_called_at REAL,
            updated_at REAL,
            PRIMARY KEY (date_key, mode, model, status_code)
        )
        """
    )
    conn.commit()


def _sqlite_record_row(
    conn: sqlite3.Connection,
    date_key: str,
    mode_val: str,
    model: str,
    status_code: int,
    success: bool,
) -> None:
    now = time.time()
    if success:
        conn.execute(
            f"""
            INSERT INTO {SQLITE_TABLE_NAME} (date_key, mode, model, status_code, success_count, failure_count, last_called_at, updated_at)
            VALUES (?, ?, ?, ?, 1, 0, ?, ?)
            ON CONFLICT(date_key, mode, model, status_code) DO UPDATE SET
                success_count = success_count + 1,
                last_called_at = excluded.last_called_at,
                updated_at = excluded.updated_at
            """,
            (date_key, mode_val, model, status_code, now, now),
        )
    else:
        conn.execute(
            f"""
            INSERT INTO {SQLITE_TABLE_NAME} (date_key, mode, model, status_code, success_count, failure_count, last_called_at, updated_at)
            VALUES (?, ?, ?, ?, 0, 1, ?, ?)
            ON CONFLICT(date_key, mode, model, status_code) DO UPDATE SET
                failure_count = failure_count + 1,
                last_called_at = excluded.last_called_at,
                updated_at = excluded.updated_at
            """,
            (date_key, mode_val, model, status_code, now, now),
        )
    conn.commit()


def _sqlite_query_rows(
    conn: sqlite3.Connection, date_keys: List[str], mode: Optional[str]
) -> List[Dict[str, Any]]:
    placeholders = ",".join(["?"] * len(date_keys))
    sql = f"SELECT * FROM {SQLITE_TABLE_NAME} WHERE date_key IN ({placeholders})"
    params: List[Any] = list(date_keys)
    if mode and mode != "all":
        sql += " AND mode = ?"
        params.append(mode)
    cur = conn.execute(sql, params)
    rows: List[Dict[str, Any]] = []
    for r in cur.fetchall():
        rows.append(
            {
                "date_key": r["date_key"],
                "mode": r["mode"],
                "model": r["model"],
                "status_code": r["status_code"],
                "success_count": r["success_count"],
                "failure_count": r["failure_count"],
                "last_called_at": r["last_called_at"],
                "updated_at": r["updated_at"],
            }
        )
    return rows


def _sqlite_reset(
    conn: sqlite3.Connection, mode: Optional[str], model: Optional[str]
) -> int:
    sql = f"DELETE FROM {SQLITE_TABLE_NAME}"
    conditions: List[str] = []
    params: List[Any] = []
    if mode:
        conditions.append("mode = ?")
        params.append(mode)
    if model:
        conditions.append("model = ?")
        params.append(normalize_usage_model_name(model))
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)
    cur = conn.execute(sql, params)
    conn.commit()
    return cur.rowcount or 0


# ---------------------------------------------------------------------------
# 写入入口
# ---------------------------------------------------------------------------

async def record_usage_call(
    mode: str,
    model_name: str,
    success: bool,
    status_code: Optional[int] = None,
) -> None:
    """
    异步记录一次真实后端请求尝试。

    统计口径：每次真实后端请求尝试算一次调用。
    一次请求先 429 再重试成功 = 1 次失败 + 1 次成功。
    异常无状态码时按 500 记录。
    """
    try:
        normalized = normalize_usage_model_name(model_name)
        if not normalized:
            normalized = model_name or "unknown"

        sc = int(status_code) if status_code is not None else 500
        date_key = _today_date_key()

        backend_type, backend_obj = await _get_backend()

        if backend_type == "mongodb":
            await _ensure_mongo_indexes(backend_obj)
            filter_doc = {
                "date_key": date_key,
                "mode": mode,
                "model": normalized,
                "status_code": sc,
            }
            await _mongo_record(backend_obj, filter_doc, success)
            return

        if backend_type == "sqlite":
            try:
                attrs = getattr(backend_obj, "_db_path", None)
                creds_dir = getattr(backend_obj, "_credentials_dir", None)
            except Exception:
                attrs = None
                creds_dir = None

            # 优先复用 SQLiteManager 已有连接路径；否则用本地默认
            conn = None
            try:
                conn = _sqlite_get_conn()
                _sqlite_ensure_table(conn)
                _sqlite_record_row(
                    conn, date_key, mode, normalized, sc, success
                )
            finally:
                if conn:
                    conn.close()
            return

        # postgresql / unknown
        log.info(
            f"[USAGE_V2] 当前后端 {backend_type} 暂不支持统计写入，已跳过"
        )
    except Exception as e:
        log.warning(f"[USAGE_V2] record_usage_call 异常（已忽略）: {e}")


def safe_record_usage_call(
    mode: str,
    model_name: str,
    success: bool,
    status_code: Optional[int] = None,
) -> None:
    """
    旁路同步入口：内部用 asyncio.create_task 调度异步写入，
    并捕获同步阶段异常。绝不阻塞原请求。
    """
    try:
        asyncio.create_task(
            record_usage_call(mode, model_name, success, status_code)
        )
    except Exception as e:
        log.warning(f"[USAGE_V2] safe_record_usage_call 同步阶段异常: {e}")


# ---------------------------------------------------------------------------
# 查询：统一行收集
# ---------------------------------------------------------------------------

async def _collect_rows(
    date_keys: List[str], mode: Optional[str]
) -> List[Dict[str, Any]]:
    """从当前后端收集原始行（MongoDB/SQLite），返回统一结构。"""
    backend_type, backend_obj = await _get_backend()

    if backend_type == "mongodb":
        return await _mongo_query_rows(backend_obj, date_keys, mode)

    if backend_type == "sqlite":
        conn = None
        try:
            conn = _sqlite_get_conn()
            _sqlite_ensure_table(conn)
            return _sqlite_query_rows(conn, date_keys, mode)
        finally:
            if conn:
                conn.close()

    # postgresql / unknown -> 空结果
    log.info(
        f"[USAGE_V2] 当前后端 {backend_type} 暂不支持统计查询，返回空结果"
    )
    return []


# ---------------------------------------------------------------------------
# 查询接口
# ---------------------------------------------------------------------------

def _failure_rate(success: int, failure: int) -> float:
    total = success + failure
    if total <= 0:
        return 0.0
    return round(failure / total * 100, 2)


async def get_usage_summary(
    range_key: str = DEFAULT_RANGE, mode: str = "all"
) -> Dict[str, Any]:
    """汇总：总调用、成功、失败、失败率、模型数量。"""
    try:
        date_keys = _date_range_keys(range_key)
        rows = await _collect_rows(date_keys, mode)

        total = 0
        success_total = 0
        failure_total = 0
        models: set = set()

        for r in rows:
            s = int(r.get("success_count", 0) or 0)
            f = int(r.get("failure_count", 0) or 0)
            success_total += s
            failure_total += f
            total += s + f
            models.add(r.get("model"))

        return {
            "total_calls": total,
            "success_calls": success_total,
            "failure_calls": failure_total,
            "failure_rate": _failure_rate(success_total, failure_total),
            "model_count": len(models),
            "range": range_key,
            "mode": mode,
        }
    except Exception as e:
        log.warning(f"[USAGE_V2] get_usage_summary 异常: {e}")
        return {
            "total_calls": 0,
            "success_calls": 0,
            "failure_calls": 0,
            "failure_rate": 0.0,
            "model_count": 0,
            "range": range_key,
            "mode": mode,
        }


async def get_usage_timeseries(
    range_key: str = "7d", mode: str = "all"
) -> Dict[str, Any]:
    """时间序列：dates、models、series（补齐空日期）。"""
    try:
        date_keys = _date_range_keys(range_key)
        rows = await _collect_rows(date_keys, mode)

        # 按 (date_key, model) 聚合 status_code 分布
        agg: Dict[tuple, Dict[str, Any]] = {}
        models_set: set = set()

        for r in rows:
            dk = r.get("date_key")
            mdl = r.get("model")
            sc = int(r.get("status_code", 0) or 0)
            s = int(r.get("success_count", 0) or 0)
            f = int(r.get("failure_count", 0) or 0)
            models_set.add(mdl)

            key = (dk, mdl)
            if key not in agg:
                agg[key] = {
                    "date_key": dk,
                    "model": mdl,
                    "success_calls": 0,
                    "failure_calls": 0,
                    "status_codes": {},
                }
            agg[key]["success_calls"] += s
            agg[key]["failure_calls"] += f
            agg[key]["status_codes"][str(sc)] = (
                agg[key]["status_codes"].get(str(sc), 0) + s + f
            )

        # 补齐空日期：为每个日期每个模型生成点（仅出现过的模型）
        models = sorted(models_set)
        series: List[Dict[str, Any]] = []
        for dk in date_keys:
            for mdl in models:
                point = agg.get(
                    (dk, mdl),
                    {
                        "date_key": dk,
                        "model": mdl,
                        "success_calls": 0,
                        "failure_calls": 0,
                        "status_codes": {},
                    },
                )
                s = point["success_calls"]
                f = point["failure_calls"]
                series.append(
                    {
                        "date_key": dk,
                        "model": mdl,
                        "success_calls": s,
                        "failure_calls": f,
                        "total_calls": s + f,
                        "failure_rate": _failure_rate(s, f),
                        "status_codes": point["status_codes"],
                    }
                )

        return {"dates": date_keys, "models": models, "series": series}
    except Exception as e:
        log.warning(f"[USAGE_V2] get_usage_timeseries 异常: {e}")
        return {"dates": [], "models": [], "series": []}


async def get_usage_models(
    range_key: str = "7d", mode: str = "all"
) -> List[Dict[str, Any]]:
    """模型明细：成功/失败/总/失败率/最近调用时间/status_codes 分布。"""
    try:
        date_keys = _date_range_keys(range_key)
        rows = await _collect_rows(date_keys, mode)

        agg: Dict[str, Dict[str, Any]] = {}
        for r in rows:
            mdl = r.get("model")
            sc = int(r.get("status_code", 0) or 0)
            s = int(r.get("success_count", 0) or 0)
            f = int(r.get("failure_count", 0) or 0)
            last = r.get("last_called_at")

            if mdl not in agg:
                agg[mdl] = {
                    "model": mdl,
                    "success_calls": 0,
                    "failure_calls": 0,
                    "last_called_at": None,
                    "status_codes": {},
                }
            agg[mdl]["success_calls"] += s
            agg[mdl]["failure_calls"] += f
            agg[mdl]["status_codes"][str(sc)] = (
                agg[mdl]["status_codes"].get(str(sc), 0) + s + f
            )
            if last is not None:
                cur = agg[mdl]["last_called_at"]
                if cur is None or last > cur:
                    agg[mdl]["last_called_at"] = last

        result: List[Dict[str, Any]] = []
        for mdl, data in agg.items():
            s = data["success_calls"]
            f = data["failure_calls"]
            result.append(
                {
                    "model": mdl,
                    "success_calls": s,
                    "failure_calls": f,
                    "total_calls": s + f,
                    "failure_rate": _failure_rate(s, f),
                    "last_called_at": data["last_called_at"],
                    "status_codes": data["status_codes"],
                }
            )

        # 按总调用量降序
        result.sort(key=lambda x: x["total_calls"], reverse=True)
        return result
    except Exception as e:
        log.warning(f"[USAGE_V2] get_usage_models 异常: {e}")
        return []


async def reset_usage_stats(
    mode: Optional[str] = None, model: Optional[str] = None
) -> Dict[str, Any]:
    """重置统计。model 会先经过归一化。"""
    try:
        backend_type, backend_obj = await _get_backend()
        deleted = 0

        if backend_type == "mongodb":
            deleted = await _mongo_reset(backend_obj, mode, model)
        elif backend_type == "sqlite":
            conn = None
            try:
                conn = _sqlite_get_conn()
                _sqlite_ensure_table(conn)
                deleted = _sqlite_reset(conn, mode, model)
            finally:
                if conn:
                    conn.close()
        else:
            log.info(
                f"[USAGE_V2] 当前后端 {backend_type} 暂不支持统计重置，已跳过"
            )

        scope = "全部"
        if mode and model:
            scope = f"mode={mode}, model={normalize_usage_model_name(model)}"
        elif mode:
            scope = f"mode={mode}"
        elif model:
            scope = f"model={normalize_usage_model_name(model)}"

        return {"success": True, "deleted": deleted, "message": f"已重置统计（{scope}），共删除 {deleted} 条记录"}
    except Exception as e:
        log.warning(f"[USAGE_V2] reset_usage_stats 异常: {e}")
        return {"success": False, "deleted": 0, "message": f"重置失败: {e}"}
