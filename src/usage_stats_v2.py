"""
按真实后端模型统计调用情况（usage_stats_v2）。

统计维度：
    - usage_daily:  date_key + mode + model + status_code
    - usage_hourly: hour_key + mode + model + status_code

写入位置：真实后端 API client 层（src/api/geminicli.py、src/api/antigravity.py）。
统计始终是旁路：任何初始化/写入/查询失败只写日志，保持原聊天请求继续返回结果。

存储策略（跟随项目当前激活的 StorageAdapter）：
    - mongodb  -> collection usage_daily / usage_hourly（复用 backend._db，不新建连接）
    - sqlite   -> usage_daily / usage_hourly 表（CREDENTIALS_DIR 习惯路径，本地/fallback）
    - postgresql -> 暂不支持，记录日志并返回空统计
"""

from __future__ import annotations

import asyncio
import os
import sqlite3
import time
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from log import log
from .storage_adapter import get_storage_adapter

DAILY_NAME = "usage_daily"
HOURLY_NAME = "usage_hourly"
RANGE_DAYS = {"1d": 1, "7d": 7, "30d": 30}
DEFAULT_RANGE = "7d"

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
    key=len,
    reverse=True,
)
_USAGE_PREFIXES = ("假流式/", "流式抗截断/")

_indexes_ready: Dict[str, bool] = {"mongodb": False}


def normalize_usage_model_name(model_name: str) -> str:
    """将带功能前缀/后缀的模型名归一化为真实后端模型名。"""
    if not model_name or not isinstance(model_name, str):
        return ""

    name = model_name.strip()
    for prefix in _USAGE_PREFIXES:
        if name.startswith(prefix):
            name = name[len(prefix):]
            break

    changed = True
    while changed:
        changed = False
        for suffix in _USAGE_SUFFIXES:
            if name.endswith(suffix) and len(name) > len(suffix):
                name = name[: -len(suffix)]
                changed = True
                break
    return name


def _now_ts() -> float:
    return time.time()


def _today_date_key() -> str:
    return date.today().isoformat()


def _current_hour_key() -> str:
    return datetime.now().strftime("%Y-%m-%d %H")


def _resolve_range_days(range_key: str) -> int:
    return RANGE_DAYS.get(range_key, RANGE_DAYS[DEFAULT_RANGE])


def _date_range_keys(range_key: str) -> List[str]:
    days = _resolve_range_days(range_key)
    today = date.today()
    return [(today - timedelta(days=i)).isoformat() for i in range(days - 1, -1, -1)]


def _hour_bucket_keys_for_today() -> List[str]:
    today = _today_date_key()
    return [f"{today} {hour:02d}" for hour in range(24)]


def _hour_bucket_labels_for_today() -> List[str]:
    return [f"{hour:02d}:00" for hour in range(24)]


def _bucket_label_from_hour_key(hour_key: str) -> str:
    try:
        return f"{int(str(hour_key)[-2:]):02d}:00"
    except Exception:
        return str(hour_key)


def _bucket_label_from_date_key(date_key: str) -> str:
    try:
        return str(date_key)[5:]
    except Exception:
        return str(date_key)


async def _get_backend() -> Tuple[str, Any]:
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

    return backend_type, None


async def _ensure_mongo_indexes(db) -> None:
    if _indexes_ready.get("mongodb"):
        return
    try:
        from pymongo import ASCENDING, IndexModel

        daily_indexes = [
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
        hourly_indexes = [
            IndexModel(
                [
                    ("hour_key", ASCENDING),
                    ("mode", ASCENDING),
                    ("model", ASCENDING),
                    ("status_code", ASCENDING),
                ],
                unique=True,
                name="idx_usage_hourly_unique",
            ),
            IndexModel([("hour_key", ASCENDING)], name="idx_usage_hourly_hour"),
            IndexModel([("date_key", ASCENDING)], name="idx_usage_hourly_date"),
            IndexModel([("mode", ASCENDING)], name="idx_usage_hourly_mode"),
            IndexModel([("model", ASCENDING)], name="idx_usage_hourly_model"),
        ]
        await db[DAILY_NAME].create_indexes(daily_indexes)
        await db[HOURLY_NAME].create_indexes(hourly_indexes)
        _indexes_ready["mongodb"] = True
    except Exception as e:
        log.debug(f"[USAGE_V2] 创建 MongoDB usage 索引（可能已存在）: {e}")


async def _mongo_inc(db, collection_name: str, filter_doc: Dict[str, Any], success: bool) -> None:
    now = _now_ts()
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
    await db[collection_name].update_one(filter_doc, update, upsert=True)


async def _mongo_record(db, date_key: str, hour_key: str, mode: str, model: str, status_code: int, success: bool) -> None:
    await _ensure_mongo_indexes(db)
    await _mongo_inc(
        db,
        DAILY_NAME,
        {"date_key": date_key, "mode": mode, "model": model, "status_code": status_code},
        success,
    )
    await _mongo_inc(
        db,
        HOURLY_NAME,
        {
            "hour_key": hour_key,
            "date_key": date_key,
            "mode": mode,
            "model": model,
            "status_code": status_code,
        },
        success,
    )


def _doc_to_row(doc: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "date_key": doc.get("date_key"),
        "hour_key": doc.get("hour_key"),
        "mode": doc.get("mode"),
        "model": doc.get("model"),
        "status_code": doc.get("status_code"),
        "success_count": doc.get("success_count", 0),
        "failure_count": doc.get("failure_count", 0),
        "last_called_at": doc.get("last_called_at"),
        "updated_at": doc.get("updated_at"),
    }


async def _mongo_query_daily_rows(db, date_keys: List[str], mode: Optional[str]) -> List[Dict[str, Any]]:
    query: Dict[str, Any] = {"date_key": {"$in": date_keys}}
    if mode and mode != "all":
        query["mode"] = mode
    cursor = db[DAILY_NAME].find(query)
    rows: List[Dict[str, Any]] = []
    async for doc in cursor:
        rows.append(_doc_to_row(doc))
    return rows


async def _mongo_query_hourly_rows(db, hour_keys: List[str], mode: Optional[str]) -> List[Dict[str, Any]]:
    query: Dict[str, Any] = {"hour_key": {"$in": hour_keys}}
    if mode and mode != "all":
        query["mode"] = mode
    cursor = db[HOURLY_NAME].find(query)
    rows: List[Dict[str, Any]] = []
    async for doc in cursor:
        rows.append(_doc_to_row(doc))
    return rows


async def _mongo_reset(db, mode: Optional[str], model: Optional[str]) -> int:
    query: Dict[str, Any] = {}
    if mode:
        query["mode"] = mode
    if model:
        query["model"] = normalize_usage_model_name(model)
    result_daily = await db[DAILY_NAME].delete_many(query)
    result_hourly = await db[HOURLY_NAME].delete_many(query)
    return int(getattr(result_daily, "deleted_count", 0) or 0) + int(getattr(result_hourly, "deleted_count", 0) or 0)


def _sqlite_db_path() -> str:
    creds_dir = os.getenv("CREDENTIALS_DIR", "./creds")
    return os.path.join(creds_dir, "credentials.db")


def _sqlite_get_conn() -> sqlite3.Connection:
    path = _sqlite_db_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def _sqlite_ensure_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {DAILY_NAME} (
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
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {HOURLY_NAME} (
            hour_key TEXT NOT NULL,
            date_key TEXT NOT NULL,
            mode TEXT NOT NULL,
            model TEXT NOT NULL,
            status_code INTEGER NOT NULL,
            success_count INTEGER DEFAULT 0,
            failure_count INTEGER DEFAULT 0,
            last_called_at REAL,
            updated_at REAL,
            PRIMARY KEY (hour_key, mode, model, status_code)
        )
        """
    )
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_usage_hourly_date ON {HOURLY_NAME}(date_key)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_usage_hourly_model ON {HOURLY_NAME}(model)")
    conn.commit()


def _sqlite_inc(conn: sqlite3.Connection, table: str, keys: Dict[str, Any], success: bool) -> None:
    now = _now_ts()
    if table == DAILY_NAME:
        columns = "date_key, mode, model, status_code"
        values = (keys["date_key"], keys["mode"], keys["model"], keys["status_code"])
        conflict = "date_key, mode, model, status_code"
        placeholders = "?, ?, ?, ?"
    else:
        columns = "hour_key, date_key, mode, model, status_code"
        values = (keys["hour_key"], keys["date_key"], keys["mode"], keys["model"], keys["status_code"])
        conflict = "hour_key, mode, model, status_code"
        placeholders = "?, ?, ?, ?, ?"

    if success:
        conn.execute(
            f"""
            INSERT INTO {table} ({columns}, success_count, failure_count, last_called_at, updated_at)
            VALUES ({placeholders}, 1, 0, ?, ?)
            ON CONFLICT({conflict}) DO UPDATE SET
                success_count = success_count + 1,
                last_called_at = excluded.last_called_at,
                updated_at = excluded.updated_at
            """,
            (*values, now, now),
        )
    else:
        conn.execute(
            f"""
            INSERT INTO {table} ({columns}, success_count, failure_count, last_called_at, updated_at)
            VALUES ({placeholders}, 0, 1, ?, ?)
            ON CONFLICT({conflict}) DO UPDATE SET
                failure_count = failure_count + 1,
                last_called_at = excluded.last_called_at,
                updated_at = excluded.updated_at
            """,
            (*values, now, now),
        )


def _sqlite_record(conn: sqlite3.Connection, date_key: str, hour_key: str, mode: str, model: str, status_code: int, success: bool) -> None:
    _sqlite_inc(conn, DAILY_NAME, {"date_key": date_key, "mode": mode, "model": model, "status_code": status_code}, success)
    _sqlite_inc(
        conn,
        HOURLY_NAME,
        {"hour_key": hour_key, "date_key": date_key, "mode": mode, "model": model, "status_code": status_code},
        success,
    )
    conn.commit()


def _sqlite_query_rows(conn: sqlite3.Connection, table: str, key_name: str, keys: List[str], mode: Optional[str]) -> List[Dict[str, Any]]:
    if not keys:
        return []
    placeholders = ",".join(["?"] * len(keys))
    sql = f"SELECT * FROM {table} WHERE {key_name} IN ({placeholders})"
    params: List[Any] = list(keys)
    if mode and mode != "all":
        sql += " AND mode = ?"
        params.append(mode)
    cur = conn.execute(sql, params)
    rows: List[Dict[str, Any]] = []
    for r in cur.fetchall():
        rows.append({k: r[k] for k in r.keys()})
    return rows


def _sqlite_reset(conn: sqlite3.Connection, mode: Optional[str], model: Optional[str]) -> int:
    total = 0
    for table in (DAILY_NAME, HOURLY_NAME):
        sql = f"DELETE FROM {table}"
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
        total += cur.rowcount or 0
    conn.commit()
    return total


async def record_usage_call(mode: str, model_name: str, success: bool, status_code: Optional[int] = None) -> None:
    """异步记录一次真实后端请求尝试。"""
    try:
        normalized = normalize_usage_model_name(model_name) or model_name or "unknown"
        sc = int(status_code) if status_code is not None else (200 if success else 500)
        date_key = _today_date_key()
        hour_key = _current_hour_key()
        backend_type, backend_obj = await _get_backend()

        if backend_type == "mongodb":
            await _mongo_record(backend_obj, date_key, hour_key, mode, normalized, sc, success)
            return

        if backend_type == "sqlite":
            conn = None
            try:
                conn = _sqlite_get_conn()
                _sqlite_ensure_tables(conn)
                _sqlite_record(conn, date_key, hour_key, mode, normalized, sc, success)
            finally:
                if conn:
                    conn.close()
            return

        log.info(f"[USAGE_V2] 当前后端 {backend_type} 暂不支持统计写入，已跳过")
    except Exception as e:
        log.warning(f"[USAGE_V2] record_usage_call 异常（已忽略）: {e}")


def safe_record_usage_call(mode: str, model_name: str, success: bool, status_code: Optional[int] = None) -> None:
    """旁路同步入口：调度异步写入，绝不阻塞原请求。"""
    try:
        asyncio.create_task(record_usage_call(mode, model_name, success, status_code))
    except Exception as e:
        log.warning(f"[USAGE_V2] safe_record_usage_call 同步阶段异常: {e}")


async def _collect_daily_rows(date_keys: List[str], mode: Optional[str]) -> List[Dict[str, Any]]:
    backend_type, backend_obj = await _get_backend()
    if backend_type == "mongodb":
        return await _mongo_query_daily_rows(backend_obj, date_keys, mode)
    if backend_type == "sqlite":
        conn = None
        try:
            conn = _sqlite_get_conn()
            _sqlite_ensure_tables(conn)
            return _sqlite_query_rows(conn, DAILY_NAME, "date_key", date_keys, mode)
        finally:
            if conn:
                conn.close()
    log.info(f"[USAGE_V2] 当前后端 {backend_type} 暂不支持统计查询，返回空结果")
    return []


async def _collect_hourly_rows(hour_keys: List[str], mode: Optional[str]) -> List[Dict[str, Any]]:
    backend_type, backend_obj = await _get_backend()
    if backend_type == "mongodb":
        rows = await _mongo_query_hourly_rows(backend_obj, hour_keys, mode)
        return rows
    if backend_type == "sqlite":
        conn = None
        try:
            conn = _sqlite_get_conn()
            _sqlite_ensure_tables(conn)
            return _sqlite_query_rows(conn, HOURLY_NAME, "hour_key", hour_keys, mode)
        finally:
            if conn:
                conn.close()
    return []


def _failure_rate(success: int, failure: int) -> float:
    total = success + failure
    if total <= 0:
        return 0.0
    return round(failure / total * 100, 2)


def _row_identity_key(row: Dict[str, Any]) -> Tuple[str, int]:
    return str(row.get("model") or "unknown"), int(row.get("status_code", 0) or 0)


def _merge_daily_delta_into_hourly_rows(hourly_rows: List[Dict[str, Any]], daily_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    部署前已存在的日级统计没有小时分布。为避免今日图表和汇总不一致，
    将 daily - hourly 的差额合并到当前小时。后续新请求会正常写入真实小时桶。
    """
    daily_by_key: Dict[Tuple[str, int], Dict[str, int]] = {}
    hourly_by_key: Dict[Tuple[str, int], Dict[str, int]] = {}

    for r in daily_rows:
        key = _row_identity_key(r)
        daily_by_key.setdefault(key, {"success": 0, "failure": 0})
        daily_by_key[key]["success"] += int(r.get("success_count", 0) or 0)
        daily_by_key[key]["failure"] += int(r.get("failure_count", 0) or 0)

    for r in hourly_rows:
        key = _row_identity_key(r)
        hourly_by_key.setdefault(key, {"success": 0, "failure": 0})
        hourly_by_key[key]["success"] += int(r.get("success_count", 0) or 0)
        hourly_by_key[key]["failure"] += int(r.get("failure_count", 0) or 0)

    merged = list(hourly_rows)
    now_hour = _current_hour_key()
    today = _today_date_key()
    for (model, status_code), dvals in daily_by_key.items():
        hvals = hourly_by_key.get((model, status_code), {"success": 0, "failure": 0})
        ds = max(0, dvals["success"] - hvals["success"])
        df = max(0, dvals["failure"] - hvals["failure"])
        if ds or df:
            merged.append(
                {
                    "hour_key": now_hour,
                    "date_key": today,
                    "mode": "all",
                    "model": model,
                    "status_code": status_code,
                    "success_count": ds,
                    "failure_count": df,
                    "last_called_at": None,
                    "updated_at": None,
                }
            )
    return merged


async def get_usage_summary(range_key: str = DEFAULT_RANGE, mode: str = "all") -> Dict[str, Any]:
    try:
        date_keys = _date_range_keys(range_key)
        rows = await _collect_daily_rows(date_keys, mode)
        success_total = 0
        failure_total = 0
        models: set = set()
        for r in rows:
            s = int(r.get("success_count", 0) or 0)
            f = int(r.get("failure_count", 0) or 0)
            success_total += s
            failure_total += f
            if r.get("model"):
                models.add(r.get("model"))
        total = success_total + failure_total
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


def _build_series_from_rows(rows: List[Dict[str, Any]], buckets: List[str], bucket_key: str, label_func) -> Dict[str, Any]:
    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}
    models_set: set = set()

    for r in rows:
        raw_bucket = r.get(bucket_key)
        if not raw_bucket:
            continue
        bucket = label_func(raw_bucket)
        mdl = r.get("model") or "unknown"
        sc = int(r.get("status_code", 0) or 0)
        s = int(r.get("success_count", 0) or 0)
        f = int(r.get("failure_count", 0) or 0)
        models_set.add(mdl)
        key = (bucket, mdl)
        if key not in agg:
            agg[key] = {
                "bucket": bucket,
                "date_key": r.get("date_key"),
                "model": mdl,
                "success_calls": 0,
                "failure_calls": 0,
                "status_codes": {},
            }
        agg[key]["success_calls"] += s
        agg[key]["failure_calls"] += f
        agg[key]["status_codes"][str(sc)] = agg[key]["status_codes"].get(str(sc), 0) + s + f

    models = sorted(models_set)
    series: List[Dict[str, Any]] = []
    for bucket in buckets:
        for mdl in models:
            point = agg.get(
                (bucket, mdl),
                {
                    "bucket": bucket,
                    "date_key": None,
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
                    "bucket": bucket,
                    "date_key": point.get("date_key"),
                    "model": mdl,
                    "success_calls": s,
                    "failure_calls": f,
                    "total_calls": s + f,
                    "failure_rate": _failure_rate(s, f),
                    "status_codes": point["status_codes"],
                }
            )
    return {"models": models, "series": series}


async def get_usage_timeseries(range_key: str = "7d", mode: str = "all") -> Dict[str, Any]:
    try:
        if range_key == "1d":
            hour_keys = _hour_bucket_keys_for_today()
            daily_rows = await _collect_daily_rows([_today_date_key()], mode)
            hourly_rows = await _collect_hourly_rows(hour_keys, mode)
            rows = _merge_daily_delta_into_hourly_rows(hourly_rows, daily_rows)
            buckets = _hour_bucket_labels_for_today()
            result = _build_series_from_rows(rows, buckets, "hour_key", _bucket_label_from_hour_key)
            return {
                "bucket_type": "hour",
                "buckets": buckets,
                "dates": buckets,
                "models": result["models"],
                "series": result["series"],
            }

        date_keys = _date_range_keys(range_key)
        rows = await _collect_daily_rows(date_keys, mode)
        buckets = [_bucket_label_from_date_key(d) for d in date_keys]
        for r in rows:
            r["day_bucket"] = r.get("date_key")
        result = _build_series_from_rows(rows, buckets, "day_bucket", _bucket_label_from_date_key)
        return {
            "bucket_type": "day",
            "buckets": buckets,
            "dates": date_keys,
            "models": result["models"],
            "series": result["series"],
        }
    except Exception as e:
        log.warning(f"[USAGE_V2] get_usage_timeseries 异常: {e}")
        return {"bucket_type": "day", "buckets": [], "dates": [], "models": [], "series": []}


async def get_usage_models(range_key: str = "7d", mode: str = "all") -> List[Dict[str, Any]]:
    try:
        date_keys = _date_range_keys(range_key)
        rows = await _collect_daily_rows(date_keys, mode)
        agg: Dict[str, Dict[str, Any]] = {}
        for r in rows:
            mdl = r.get("model") or "unknown"
            sc = int(r.get("status_code", 0) or 0)
            s = int(r.get("success_count", 0) or 0)
            f = int(r.get("failure_count", 0) or 0)
            last = r.get("last_called_at")
            if mdl not in agg:
                agg[mdl] = {"model": mdl, "success_calls": 0, "failure_calls": 0, "last_called_at": None, "status_codes": {}}
            agg[mdl]["success_calls"] += s
            agg[mdl]["failure_calls"] += f
            agg[mdl]["status_codes"][str(sc)] = agg[mdl]["status_codes"].get(str(sc), 0) + s + f
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
        result.sort(key=lambda x: x["total_calls"], reverse=True)
        return result
    except Exception as e:
        log.warning(f"[USAGE_V2] get_usage_models 异常: {e}")
        return []


async def reset_usage_stats(mode: Optional[str] = None, model: Optional[str] = None) -> Dict[str, Any]:
    try:
        backend_type, backend_obj = await _get_backend()
        deleted = 0
        if backend_type == "mongodb":
            deleted = await _mongo_reset(backend_obj, mode, model)
        elif backend_type == "sqlite":
            conn = None
            try:
                conn = _sqlite_get_conn()
                _sqlite_ensure_tables(conn)
                deleted = _sqlite_reset(conn, mode, model)
            finally:
                if conn:
                    conn.close()
        else:
            log.info(f"[USAGE_V2] 当前后端 {backend_type} 暂不支持统计重置，已跳过")

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
