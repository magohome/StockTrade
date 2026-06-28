from __future__ import annotations

import datetime as dt
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd
import tushare as ts
from tqdm import tqdm

from pipeline.fetch_kline import (
    REQUEST_INTERVAL_SECS,
    _atomic_write_csv,
    _get_kline_tushare,
    _load_config,
    _resolve_cfg_path,
    _validate_fetched_frame,
    _wait_for_request_slot,
    load_codes_from_stocklist,
    setup_logging,
)
import pipeline.fetch_kline as fetch_kline


logger = logging.getLogger("fetch_incremental")


@dataclass
class IncrementalTask:
    code: str
    start: str
    end: str
    mode: str


@dataclass
class IncrementalResult:
    code: str
    ok: bool
    status: str
    start: str = ""
    end: str = ""
    old_latest: str = ""
    new_latest: str = ""
    rows_added: int = 0
    error: str = ""


def _today_yyyymmdd() -> str:
    return dt.date.today().strftime("%Y%m%d")


def _next_day_yyyymmdd(value: pd.Timestamp) -> str:
    return (value.date() + dt.timedelta(days=1)).strftime("%Y%m%d")


def _read_existing(csv_path: Path) -> Optional[pd.DataFrame]:
    if not csv_path.exists():
        return None
    df = pd.read_csv(csv_path)
    if df.empty or "date" not in df.columns:
        return None
    df.columns = [c.lower() for c in df.columns]
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date").reset_index(drop=True)


def _latest_date(csv_path: Path) -> Optional[pd.Timestamp]:
    df = _read_existing(csv_path)
    if df is None or df.empty:
        return None
    return pd.to_datetime(df["date"]).max()


def _market_open_dates(start: str, end: str) -> list[str]:
    _wait_for_request_slot()
    cal = fetch_kline.pro.trade_cal(
        exchange="",
        start_date=start,
        end_date=end,
        is_open="1",
        fields="cal_date,is_open",
    )
    if cal is None or cal.empty:
        return []
    return sorted(cal["cal_date"].astype(str).tolist())


def _append_incremental(
    code: str,
    csv_path: Path,
    new_df: pd.DataFrame,
    *,
    min_rows: int,
    allow_empty: bool,
) -> IncrementalResult:
    old_df = _read_existing(csv_path)
    if old_df is None:
        checked = _validate_fetched_frame(
            code,
            new_df,
            min_rows=min_rows,
            allow_empty=allow_empty,
        )
        _atomic_write_csv(checked, csv_path)
        latest = checked["date"].max().strftime("%Y-%m-%d") if not checked.empty else ""
        return IncrementalResult(
            code=code,
            ok=True,
            status="created",
            new_latest=latest,
            rows_added=len(checked),
        )

    old_latest = old_df["date"].max()
    if new_df is None or new_df.empty:
        return IncrementalResult(
            code=code,
            ok=True,
            status="no_new_rows",
            old_latest=old_latest.strftime("%Y-%m-%d"),
            new_latest=old_latest.strftime("%Y-%m-%d"),
        )

    required_cols = ["date", "open", "close", "high", "low", "volume"]
    merged = pd.concat([old_df[required_cols], new_df[required_cols]], ignore_index=True)
    merged = merged.drop_duplicates(subset="date", keep="last").sort_values("date").reset_index(drop=True)
    checked = _validate_fetched_frame(
        code,
        merged,
        min_rows=min_rows,
        allow_empty=allow_empty,
    )
    _atomic_write_csv(checked, csv_path)
    new_latest = checked["date"].max()
    return IncrementalResult(
        code=code,
        ok=True,
        status="updated" if new_latest > old_latest else "unchanged",
        old_latest=old_latest.strftime("%Y-%m-%d"),
        new_latest=new_latest.strftime("%Y-%m-%d"),
        rows_added=max(0, len(checked) - len(old_df)),
    )


def _fetch_task(
    task: IncrementalTask,
    out_dir: Path,
    *,
    min_rows: int,
    allow_empty: bool,
) -> IncrementalResult:
    csv_path = out_dir / f"{task.code}.csv"
    try:
        new_df = _get_kline_tushare(task.code, task.start, task.end)
        result = _append_incremental(
            task.code,
            csv_path,
            new_df,
            min_rows=min_rows,
            allow_empty=allow_empty,
        )
        result.start = task.start
        result.end = task.end
        return result
    except Exception as exc:
        return IncrementalResult(
            code=task.code,
            ok=False,
            status=f"{task.mode}_failed",
            start=task.start,
            end=task.end,
            error=str(exc),
        )


def main() -> None:
    cfg = _load_config()
    setup_logging()

    os.environ["NO_PROXY"] = "api.waditu.com,.waditu.com,waditu.com"
    os.environ["no_proxy"] = os.environ["NO_PROXY"]
    token = os.environ.get("TUSHARE_TOKEN")
    if not token:
        raise ValueError("请先设置环境变量 TUSHARE_TOKEN")
    ts.set_token(token)
    fetch_kline.pro = ts.pro_api()

    fetch_kline.REQUEST_INTERVAL_SECS = float(cfg.get("request_interval_seconds", REQUEST_INTERVAL_SECS))
    workers = int(cfg.get("workers", 2))
    min_rows = int(cfg.get("min_rows", 1))
    allow_empty = bool(cfg.get("allow_empty", False))
    start_full = str(cfg.get("start", "20190101"))
    end = _today_yyyymmdd() if str(cfg.get("end", "today")).lower() == "today" else str(cfg.get("end"))
    if start_full.lower() == "today":
        start_full = end

    out_dir = _resolve_cfg_path(cfg.get("out", "./data/raw"))
    out_dir.mkdir(parents=True, exist_ok=True)
    stocklist_path = _resolve_cfg_path(cfg.get("stocklist", "./pipeline/stocklist.csv"))
    exclude_boards = set(cfg.get("exclude_boards") or [])
    codes = load_codes_from_stocklist(stocklist_path, exclude_boards)

    local_latest = {
        code: _latest_date(out_dir / f"{code}.csv")
        for code in codes
    }
    existing_latest = [d for d in local_latest.values() if d is not None]
    global_latest = max(existing_latest) if existing_latest else None
    has_new_market_dates = True
    if global_latest is not None:
        missing_market_dates = _market_open_dates(_next_day_yyyymmdd(global_latest), end)
        has_new_market_dates = bool(missing_market_dates)
        logger.info(
            "本地整体最新交易日：%s；到 %s 缺少交易日：%s",
            global_latest.date(),
            end,
            ", ".join(missing_market_dates) or "无",
        )

    tasks: list[IncrementalTask] = []
    for code, latest in local_latest.items():
        if latest is None:
            tasks.append(IncrementalTask(code=code, start=start_full, end=end, mode="missing_stock"))
            continue
        if latest.strftime("%Y%m%d") < end:
            if global_latest is not None and latest >= global_latest and not has_new_market_dates:
                continue
            tasks.append(IncrementalTask(code=code, start=_next_day_yyyymmdd(latest), end=end, mode="incremental"))

    missing_count = sum(1 for t in tasks if t.mode == "missing_stock")
    incremental_count = len(tasks) - missing_count
    logger.info("缺少股票文件：%d；需要增量检查/补抓：%d", missing_count, incremental_count)
    if not tasks:
        logger.info("无需增量抓取。")
        return

    results: list[IncrementalResult] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                _fetch_task,
                task,
                out_dir,
                min_rows=min_rows,
                allow_empty=allow_empty,
            ): task
            for task in tasks
        }
        for future in tqdm(as_completed(futures), total=len(futures), desc="增量抓取"):
            results.append(future.result())

    ok_count = sum(1 for r in results if r.ok)
    updated_count = sum(1 for r in results if r.status in {"updated", "created"})
    no_new_count = sum(1 for r in results if r.status in {"no_new_rows", "unchanged"})
    failed = [r for r in results if not r.ok]
    logger.info(
        "增量完成：成功 %d/%d，更新/创建 %d，无新增 %d，失败 %d",
        ok_count,
        len(results),
        updated_count,
        no_new_count,
        len(failed),
    )

    report_path = _resolve_cfg_path("data/logs/incremental_fetch_report.csv")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([r.__dict__ for r in results]).to_csv(report_path, index=False)
    logger.info("增量报告：%s", report_path)

    if failed:
        for item in failed[:20]:
            logger.error("失败样例：%s | %s", item.code, item.error)
        sys.exit(1)


if __name__ == "__main__":
    main()
