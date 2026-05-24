from __future__ import annotations

import datetime as dt
import logging
import random
import sys
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
import os
import threading

import pandas as pd
import tushare as ts
import yaml
from tqdm import tqdm

warnings.filterwarnings("ignore")

# --------------------------- pandas 兼容补丁 --------------------------- #
# tushare 内部使用了 fillna(method='ffill'/'bfill')，在 pandas 2.2+ 中已移除该参数。
# 此补丁将旧式调用自动转发到 ffill()/bfill()，无需降级 pandas。
import pandas as _pd

_orig_fillna = _pd.DataFrame.fillna

def _patched_fillna(self, value=None, *, method=None, axis=None, inplace=False, limit=None, **kwargs):
    if method is not None:
        if method == "ffill":
            result = self.ffill(axis=axis, inplace=inplace, limit=limit)
        elif method == "bfill":
            result = self.bfill(axis=axis, inplace=inplace, limit=limit)
        else:
            raise ValueError(f"Unsupported fillna method: {method}")
        return result
    return _orig_fillna(self, value, axis=axis, inplace=inplace, limit=limit, **kwargs)

_pd.DataFrame.fillna = _patched_fillna  # type: ignore[method-assign]

_orig_series_fillna = _pd.Series.fillna

def _patched_series_fillna(self, value=None, *, method=None, axis=None, inplace=False, limit=None, **kwargs):
    if method is not None:
        if method == "ffill":
            result = self.ffill(axis=axis, inplace=inplace, limit=limit)
        elif method == "bfill":
            result = self.bfill(axis=axis, inplace=inplace, limit=limit)
        else:
            raise ValueError(f"Unsupported fillna method: {method}")
        return result
    return _orig_series_fillna(self, value, axis=axis, inplace=inplace, limit=limit, **kwargs)

_pd.Series.fillna = _patched_series_fillna  # type: ignore[method-assign]

# --------------------------- 全局日志配置 --------------------------- #
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_LOG_DIR = _PROJECT_ROOT / "data" / "logs"

def _resolve_cfg_path(path_like: str | Path, base_dir: Path = _PROJECT_ROOT) -> Path:
    """将配置中的路径统一解析为绝对路径：相对路径基于项目根目录。"""
    p = Path(path_like)
    return p if p.is_absolute() else (base_dir / p)

def _default_log_path() -> Path:
    today = dt.date.today().strftime("%Y-%m-%d")
    return _DEFAULT_LOG_DIR / f"fetch_{today}.log"

def setup_logging(log_path: Optional[Path] = None) -> None:
    """初始化日志：同时输出到 stdout 和指定文件。"""
    if log_path is None:
        log_path = _default_log_path()
    log_path = Path(log_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_path, mode="a", encoding="utf-8"),
        ],
    )

logger = logging.getLogger("fetch_from_stocklist")

# --------------------------- 限流/封禁处理配置 --------------------------- #
COOLDOWN_SECS = 600
REQUEST_INTERVAL_SECS = 0.4
_REQUEST_LOCK = threading.Lock()
_LAST_REQUEST_TS = 0.0
BAN_PATTERNS = (
    "访问频繁", "请稍后", "超过频率", "频繁访问",
    "too many requests", "429",
    "forbidden", "403",
    "max retries exceeded"
)

def _looks_like_ip_ban(exc: Exception) -> bool:
    msg = (str(exc) or "").lower()
    return any(pat in msg for pat in BAN_PATTERNS)

class RateLimitError(RuntimeError):
    """表示命中限流/封禁，需要长时间冷却后重试。"""
    pass


@dataclass
class FetchResult:
    code: str
    ok: bool
    rows: int = 0
    status: str = "ok"
    error: str = ""

def _cool_sleep(base_seconds: int) -> None:
    jitter = random.uniform(0.9, 1.2)
    sleep_s = max(1, int(base_seconds * jitter))
    logger.warning("疑似被限流/封禁，进入冷却期 %d 秒...", sleep_s)
    time.sleep(sleep_s)

def _wait_for_request_slot() -> None:
    global _LAST_REQUEST_TS
    if REQUEST_INTERVAL_SECS <= 0:
        return
    with _REQUEST_LOCK:
        now = time.monotonic()
        wait_s = REQUEST_INTERVAL_SECS - (now - _LAST_REQUEST_TS)
        if wait_s > 0:
            time.sleep(wait_s)
        _LAST_REQUEST_TS = time.monotonic()

# --------------------------- 历史K线（Tushare 日线，固定qfq） --------------------------- #
pro: Optional[ts.pro_api] = None  # 模块级会话

def set_api(session) -> None:
    """由外部(比如GUI)注入已创建好的 ts.pro_api() 会话"""
    global pro
    pro = session
    

def _to_ts_code(code: str) -> str:
    """把6位code映射到标准 ts_code 后缀。"""
    code = str(code).zfill(6)
    if code.startswith(("60", "68", "9")):
        return f"{code}.SH"
    elif code.startswith(("4", "8")):
        return f"{code}.BJ"
    else:
        return f"{code}.SZ"

def _get_kline_tushare(code: str, start: str, end: str) -> pd.DataFrame:
    ts_code = _to_ts_code(code)
    try:
        _wait_for_request_slot()
        df = ts.pro_bar(
            ts_code=ts_code,
            adj="qfq",
            start_date=start,
            end_date=end,
            freq="D",
            api=pro
        )
    except Exception as e:
        if _looks_like_ip_ban(e):
            raise RateLimitError(str(e)) from e
        raise

    if df is None or df.empty:
        return pd.DataFrame()

    df = df.rename(columns={"trade_date": "date", "vol": "volume"})[
        ["date", "open", "close", "high", "low", "volume"]
    ].copy()
    df["date"] = pd.to_datetime(df["date"])
    for c in ["open", "close", "high", "low", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.sort_values("date").reset_index(drop=True)

def validate(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.drop_duplicates(subset="date").sort_values("date").reset_index(drop=True)
    if df["date"].isna().any():
        raise ValueError("存在缺失日期！")
    if (df["date"] > pd.Timestamp.today()).any():
        raise ValueError("数据包含未来日期，可能抓取错误！")
    return df


def _validate_fetched_frame(
    code: str,
    df: pd.DataFrame,
    *,
    min_rows: int,
    allow_empty: bool,
) -> pd.DataFrame:
    df = validate(df)
    if df is None or df.empty:
        if allow_empty:
            return pd.DataFrame(columns=["date", "open", "close", "high", "low", "volume"])
        raise ValueError(f"{code} 返回空数据")
    if len(df) < min_rows:
        raise ValueError(f"{code} 行数不足：{len(df)} < {min_rows}")
    return df


def _atomic_write_csv(df: pd.DataFrame, csv_path: Path) -> None:
    tmp_path = csv_path.with_suffix(csv_path.suffix + ".tmp")
    df.to_csv(tmp_path, index=False)
    os.replace(tmp_path, csv_path)


def _existing_csv_is_valid(csv_path: Path, *, min_rows: int, allow_empty: bool) -> bool:
    if not csv_path.exists():
        return False
    try:
        df = pd.read_csv(csv_path)
        if df.empty:
            return allow_empty
        df.columns = [c.lower() for c in df.columns]
        required = {"date", "open", "close", "high", "low", "volume"}
        if not required.issubset(df.columns):
            return False
        df["date"] = pd.to_datetime(df["date"])
        _validate_fetched_frame(csv_path.stem, df, min_rows=min_rows, allow_empty=allow_empty)
        return True
    except Exception as e:
        logger.warning("%s 已有文件校验失败，将重新抓取：%s", csv_path.name, e)
        return False

# --------------------------- 读取 stocklist.csv & 过滤板块 --------------------------- #

def _filter_by_boards_stocklist(df: pd.DataFrame, exclude_boards: set[str]) -> pd.DataFrame:
    ts = df["ts_code"].astype(str).str.upper()
    num = ts.str.extract(r"(\d{6})", expand=False).str.zfill(6)
    mask = pd.Series(True, index=df.index)

    if "gem" in exclude_boards:
        mask &= ~((ts.str.endswith(".SZ")) & num.str.startswith(("300", "301")))
    if "star" in exclude_boards:
        mask &= ~((ts.str.endswith(".SH")) & num.str.startswith(("688",)))
    if "bj" in exclude_boards:
        mask &= ~((ts.str.endswith(".BJ")) | num.str.startswith(("4", "8")))

    return df[mask].copy()


def load_codes_from_stocklist(stocklist_csv: Path, exclude_boards: set[str]) -> List[str]:
    df = pd.read_csv(stocklist_csv)    
    df = _filter_by_boards_stocklist(df, exclude_boards)
    codes = df["symbol"].astype(str).str.zfill(6).tolist()
    codes = list(dict.fromkeys(codes))  # 去重保持顺序
    logger.info("从 %s 读取到 %d 只股票（排除板块：%s）",
                stocklist_csv, len(codes), ",".join(sorted(exclude_boards)) or "无")
    return codes

# --------------------------- 单只抓取（全量覆盖保存） --------------------------- #
def fetch_one(
    code: str,
    start: str,
    end: str,
    out_dir: Path,
    *,
    min_rows: int = 1,
    allow_empty: bool = False,
    skip_existing: bool = True,
) -> FetchResult:
    csv_path = out_dir / f"{code}.csv"

    if skip_existing and _existing_csv_is_valid(csv_path, min_rows=min_rows, allow_empty=allow_empty):
        return FetchResult(code=code, ok=True, rows=-1, status="skipped_existing")

    last_error = ""

    for attempt in range(1, 4):
        try:
            new_df = _get_kline_tushare(code, start, end)
            new_df = _validate_fetched_frame(
                code,
                new_df,
                min_rows=min_rows,
                allow_empty=allow_empty,
            )
            _atomic_write_csv(new_df, csv_path)
            return FetchResult(code=code, ok=True, rows=len(new_df), status="downloaded")
        except Exception as e:
            last_error = str(e)
            if _looks_like_ip_ban(e):
                logger.error(f"{code} 第 {attempt} 次抓取疑似被封禁，沉睡 {COOLDOWN_SECS} 秒")
                _cool_sleep(COOLDOWN_SECS)
            else:
                silent_seconds = 30 * attempt
                logger.info(f"{code} 第 {attempt} 次抓取失败，{silent_seconds} 秒后重试：{e}")
                time.sleep(silent_seconds)
    else:
        logger.error("%s 三次抓取均失败，已跳过！", code)       
        return FetchResult(code=code, ok=False, status="failed", error=last_error)


def _success_rate(results: list[FetchResult]) -> float:
    return sum(1 for r in results if r.ok) / len(results) if results else 0.0


def _write_failure_report(results: list[FetchResult], report_path: Path) -> None:
    failed = [r for r in results if not r.ok]
    if not failed:
        return
    report_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([r.__dict__ for r in failed]).to_csv(report_path, index=False)
    logger.error("失败清单已写入：%s", report_path.resolve())


def _run_fetch_batch(
    codes: list[str],
    *,
    start: str,
    end: str,
    out_dir: Path,
    workers: int,
    min_rows: int,
    allow_empty: bool,
    skip_existing: bool,
    desc: str,
) -> list[FetchResult]:
    results: list[FetchResult] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(
                fetch_one,
                code,
                start,
                end,
                out_dir,
                min_rows=min_rows,
                allow_empty=allow_empty,
                skip_existing=skip_existing,
            )
            for code in codes
        ]
        for fut in tqdm(as_completed(futures), total=len(futures), desc=desc):
            try:
                results.append(fut.result())
            except Exception as e:
                logger.exception("抓取线程出现未捕获异常：%s", e)
                results.append(FetchResult(code="unknown", ok=False, status="crashed", error=str(e)))
    return results



# --------------------------- 配置加载 --------------------------- #
_CONFIG_PATH = Path(__file__).parent.parent / "config" / "fetch_kline.yaml"

def _load_config(config_path: Path = _CONFIG_PATH) -> dict:
    if not config_path.exists():
        raise FileNotFoundError(f"找不到配置文件：{config_path}")
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    logger.info("已加载配置文件：%s", config_path.resolve())
    return cfg


# --------------------------- 主入口 --------------------------- #
def main(log_path: Optional[Path] = None):
    # ---------- 读取 YAML 配置 ---------- #
    cfg = _load_config()

    # ---------- 日志路径（优先参数，其次 YAML，最后默认值） ---------- #
    if log_path is None:
        cfg_log = cfg.get("log")
        log_path = _resolve_cfg_path(cfg_log) if cfg_log else _default_log_path()
    setup_logging(log_path)
    logger.info("日志文件：%s", Path(log_path).resolve())

    # ---------- Tushare Token ---------- #
    os.environ["NO_PROXY"] = "api.waditu.com,.waditu.com,waditu.com"
    os.environ["no_proxy"] = os.environ["NO_PROXY"]
    ts_token = os.environ.get("TUSHARE_TOKEN")
    if not ts_token:
        raise ValueError("请先设置环境变量 TUSHARE_TOKEN，例如：export TUSHARE_TOKEN=你的token")
    ts.set_token(ts_token)
    global pro
    pro = ts.pro_api()

    # ---------- 日期解析 ---------- #
    raw_start = str(cfg.get("start", "20190101"))
    raw_end   = str(cfg.get("end",   "today"))
    start = dt.date.today().strftime("%Y%m%d") if raw_start.lower() == "today" else raw_start
    end   = dt.date.today().strftime("%Y%m%d") if raw_end.lower()   == "today" else raw_end

    out_dir = _resolve_cfg_path(cfg.get("out", "./data"))
    out_dir.mkdir(parents=True, exist_ok=True)

    # ---------- 从 stocklist.csv 读取股票池 ---------- #
    stocklist_path = _resolve_cfg_path(cfg.get("stocklist", "./pipeline/stocklist.csv"))
    exclude_boards = set(cfg.get("exclude_boards") or [])
    codes = load_codes_from_stocklist(stocklist_path, exclude_boards)

    if not codes:
        logger.error("stocklist 为空或被过滤后无代码，请检查。")
        sys.exit(1)

    logger.info(
        "开始抓取 %d 支股票 | 数据源:Tushare(日线,qfq) | 日期:%s → %s | 排除:%s",
        len(codes), start, end, ",".join(sorted(exclude_boards)) or "无",
    )

    # ---------- 抓取有效性与断点配置 ---------- #
    workers = int(cfg.get("workers", 8))
    global REQUEST_INTERVAL_SECS
    REQUEST_INTERVAL_SECS = float(cfg.get("request_interval_seconds", REQUEST_INTERVAL_SECS))
    min_rows = int(cfg.get("min_rows", 1))
    allow_empty = bool(cfg.get("allow_empty", False))
    skip_existing = bool(cfg.get("skip_existing", True))
    min_success_rate = float(cfg.get("min_success_rate", 0.95))
    preflight_enabled = bool(cfg.get("preflight_enabled", True))
    preflight_count = max(0, int(cfg.get("preflight_count", 5)))
    preflight_min_success_rate = float(cfg.get("preflight_min_success_rate", 0.8))
    failure_report = _resolve_cfg_path(cfg.get("failure_report", "data/logs/fetch_failures.csv"))
    logger.info("Tushare 请求间隔：%.2f 秒（全线程共享）", REQUEST_INTERVAL_SECS)

    if preflight_enabled and preflight_count > 0:
        sample_codes = codes[: min(preflight_count, len(codes))]
        logger.info(
            "开始预检：先抓取 %d 支样本，成功率需 ≥ %.0f%%",
            len(sample_codes),
            preflight_min_success_rate * 100,
        )
        preflight_results = _run_fetch_batch(
            sample_codes,
            start=start,
            end=end,
            out_dir=out_dir,
            workers=min(workers, len(sample_codes)),
            min_rows=min_rows,
            allow_empty=allow_empty,
            skip_existing=skip_existing,
            desc="预检进度",
        )
        preflight_rate = _success_rate(preflight_results)
        logger.info(
            "预检完成：成功 %d/%d，成功率 %.1f%%",
            sum(1 for r in preflight_results if r.ok),
            len(preflight_results),
            preflight_rate * 100,
        )
        if preflight_rate < preflight_min_success_rate:
            _write_failure_report(preflight_results, failure_report)
            logger.error(
                "预检未通过，已中止全量抓取。请先检查 token、网络、Tushare 权限或日期区间。"
            )
            sys.exit(1)

    # ---------- 多线程抓取（支持跳过已有有效文件 + 失败汇总） ---------- #
    results = _run_fetch_batch(
        codes,
        start=start,
        end=end,
        out_dir=out_dir,
        workers=workers,
        min_rows=min_rows,
        allow_empty=allow_empty,
        skip_existing=skip_existing,
        desc="下载进度",
    )
    ok_count = sum(1 for r in results if r.ok)
    failed_count = len(results) - ok_count
    success_rate = _success_rate(results)
    skipped_count = sum(1 for r in results if r.status == "skipped_existing")

    logger.info(
        "抓取完成：成功 %d/%d，失败 %d，跳过已有 %d，成功率 %.1f%%",
        ok_count,
        len(results),
        failed_count,
        skipped_count,
        success_rate * 100,
    )
    if failed_count:
        _write_failure_report(results, failure_report)
        for r in [item for item in results if not item.ok][:20]:
            logger.error("失败样例：%s | %s", r.code, r.error)

    if success_rate < min_success_rate:
        logger.error(
            "成功率 %.1f%% 低于配置门槛 %.1f%%，流程中止，避免用不完整数据继续初选。",
            success_rate * 100,
            min_success_rate * 100,
        )
        sys.exit(1)

    logger.info("全部任务完成，数据已保存至 %s", out_dir.resolve())

if __name__ == "__main__":
    main()
