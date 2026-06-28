"""
pipeline/calc_sector_turnover.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
计算分板块每日成交额，并落盘供看板或后续分析使用。

用法：
    python -m pipeline.calc_sector_turnover
    python -m pipeline.calc_sector_turnover --max-days 250
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parent.parent
logger = logging.getLogger("calc_sector_turnover")


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def _resolve_path(path_like: str | Path) -> Path:
    path = Path(path_like)
    return path if path.is_absolute() else ROOT / path


def _load_stocklist(stocklist_path: Path) -> pd.DataFrame:
    if not stocklist_path.exists():
        raise FileNotFoundError(f"找不到股票清单：{stocklist_path}")

    stocklist = pd.read_csv(stocklist_path, dtype={"symbol": str})
    if "symbol" not in stocklist.columns:
        raise ValueError(f"{stocklist_path} 缺少 symbol 列")

    stocklist["symbol"] = (
        stocklist["symbol"].astype(str).str.extract(r"(\d{6})", expand=False).str.zfill(6)
    )
    for col in ("name", "industry"):
        if col not in stocklist.columns:
            stocklist[col] = ""

    stocklist["name"] = stocklist["name"].fillna("")
    stocklist["industry"] = stocklist["industry"].fillna("未分类").replace("", "未分类")
    return stocklist[["symbol", "name", "industry"]].drop_duplicates("symbol")


def _load_stock_turnover(raw_dir: Path, stocklist: pd.DataFrame, max_days: int) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    csv_files = sorted(raw_dir.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"未找到日线数据：{raw_dir}/*.csv")

    for csv_path in csv_files:
        code = csv_path.stem.zfill(6)
        try:
            df = pd.read_csv(
                csv_path,
                usecols=lambda c: c.lower() in {"date", "open", "close", "volume"},
            )
        except Exception as exc:
            logger.warning("跳过 %s：读取失败：%s", csv_path.name, exc)
            continue

        df.columns = [c.lower() for c in df.columns]
        required = {"date", "open", "close", "volume"}
        if not required.issubset(df.columns):
            logger.warning("跳过 %s：缺少必要列 %s", csv_path.name, sorted(required - set(df.columns)))
            continue

        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"]).sort_values("date")
        if max_days > 0:
            df = df.tail(max_days)

        for col in ("open", "close", "volume"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["open", "close", "volume"])
        if df.empty:
            continue

        df["code"] = code
        df["turnover"] = (df["open"] + df["close"]) / 2 * df["volume"] * 100
        frames.append(df[["date", "code", "turnover"]])

    if not frames:
        raise ValueError("没有可用于计算成交额的日线数据")

    stock_turnover = pd.concat(frames, ignore_index=True)
    stock_turnover = stock_turnover.merge(
        stocklist,
        left_on="code",
        right_on="symbol",
        how="left",
    )
    stock_turnover["name"] = stock_turnover["name"].fillna("")
    stock_turnover["industry"] = stock_turnover["industry"].fillna("未分类")
    return stock_turnover.drop(columns=["symbol"])


def calculate_sector_turnover(raw_dir: Path, stocklist_path: Path, max_days: int = 0) -> tuple[pd.DataFrame, pd.DataFrame]:
    stocklist = _load_stocklist(stocklist_path)
    stock_turnover = _load_stock_turnover(raw_dir, stocklist, max_days)

    sector_turnover = (
        stock_turnover.groupby(["date", "industry"], as_index=False)
        .agg(turnover=("turnover", "sum"), stocks=("code", "nunique"))
        .sort_values(["industry", "date"])
        .reset_index(drop=True)
    )
    sector_turnover["turnover_chg"] = sector_turnover.groupby("industry")["turnover"].diff()
    sector_turnover["turnover_chg_pct"] = sector_turnover.groupby("industry")["turnover"].pct_change()

    stock_turnover = stock_turnover.sort_values(["date", "industry", "turnover"], ascending=[True, True, False])
    return stock_turnover, sector_turnover


def _write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    df.to_csv(tmp_path, index=False)
    tmp_path.replace(path)


def main() -> None:
    parser = argparse.ArgumentParser(description="计算分板块每日成交额")
    parser.add_argument("--raw-dir", default="data/raw", help="日线 CSV 目录")
    parser.add_argument("--stocklist", default="pipeline/stocklist.csv", help="股票清单 CSV")
    parser.add_argument("--out-dir", default="data/sector_turnover", help="输出目录")
    parser.add_argument("--max-days", type=int, default=0, help="每只股票最多读取最近 N 天，0 表示全部")
    args = parser.parse_args()

    _setup_logging()

    raw_dir = _resolve_path(args.raw_dir)
    stocklist_path = _resolve_path(args.stocklist)
    out_dir = _resolve_path(args.out_dir)

    stock_turnover, sector_turnover = calculate_sector_turnover(
        raw_dir=raw_dir,
        stocklist_path=stocklist_path,
        max_days=args.max_days,
    )

    latest_date = sector_turnover["date"].max()
    latest_stock = stock_turnover[stock_turnover["date"] == latest_date].copy()
    latest_sector = sector_turnover[sector_turnover["date"] == latest_date].copy()

    _write_csv(sector_turnover, out_dir / "sector_turnover.csv")
    _write_csv(stock_turnover, out_dir / "stock_turnover.csv")
    _write_csv(latest_sector.sort_values("turnover", ascending=False), out_dir / "sector_turnover_latest.csv")
    _write_csv(latest_stock.sort_values("turnover", ascending=False), out_dir / "stock_turnover_latest.csv")

    logger.info(
        "板块成交额计算完成：最新交易日 %s，板块 %d 个，个股 %d 只，输出目录 %s",
        latest_date.strftime("%Y-%m-%d"),
        latest_sector["industry"].nunique(),
        latest_stock["code"].nunique(),
        out_dir,
    )


if __name__ == "__main__":
    main()
