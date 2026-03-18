#!/usr/bin/env python3
"""
雪球 ETF 日更工具：休市后自动拉取当日数据写入 db/*.csv。

- 控制台常驻，每天 16:00 后检查是否为交易日，若是则用 quotec 接口补当日数据。
- 支持 --from YYYYMMDD 用 kline 接口从指定日期补历史，只新增、不覆盖已有日期。
- 雪球接口限流：每 20 秒请求一次。

使用:
  python xueqiu_etf_daily.py --daemon
  python xueqiu_etf_daily.py --once
  python xueqiu_etf_daily.py --from 20260316
"""

import argparse
import json
import time
from datetime import datetime, date, timedelta
from pathlib import Path

try:
    import requests
except ImportError:
    requests = None

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_DB_DIR = BASE_DIR / "db"
XUEQIU_DELAY = 20  # 秒

# 简单交易日：周一=0 至 周五=4，排除以下节假日（仅示例，可自行补充）
CN_HOLIDAYS = {
    date(2024, 1, 1), date(2024, 2, 9), date(2024, 2, 10), date(2024, 2, 11), date(2024, 2, 12),
    date(2024, 4, 4), date(2024, 5, 1), date(2024, 6, 10), date(2024, 10, 1), date(2024, 10, 2),
    date(2025, 1, 1), date(2025, 1, 28), date(2025, 1, 29), date(2025, 1, 30), date(2025, 1, 31),
    date(2025, 4, 4), date(2025, 5, 1), date(2025, 10, 1), date(2025, 10, 2),
    date(2026, 1, 1), date(2026, 2, 17), date(2026, 2, 18), date(2026, 2, 19), date(2026, 2, 20),
    date(2026, 4, 6), date(2026, 5, 1), date(2026, 10, 1), date(2026, 10, 2),
}


def is_trading_day(d: date) -> bool:
    """简单判断是否为 A 股交易日：周一～周五且不在节假日。"""
    if d.weekday() >= 5:
        return False
    return d not in CN_HOLIDAYS


def get_symbols(db_dir: Path) -> list[str]:
    """从 db 目录下已有 CSV 文件名得到标的列表（如 SH510500）。"""
    out = []
    for fp in sorted(db_dir.glob("*.csv")):
        out.append(fp.stem.strip().upper())
    return out


def fetch_quotec(symbol: str, session: requests.Session | None = None) -> dict | None:
    """请求雪球实时行情 quotec.json，返回单条 data 或 None。"""
    url = "https://stock.xueqiu.com/v5/stock/realtime/quotec.json"
    params = {"symbol": symbol, "_": int(time.time() * 1000)}
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    if session is None:
        session = requests.Session()
    try:
        r = session.get(url, params=params, headers=headers, timeout=15)
        r.raise_for_status()
        data = r.json()
        if data.get("error_code") != 0 or not data.get("data"):
            return None
        return data["data"][0]
    except Exception:
        return None


def quotec_to_row(d: dict, trade_date: date) -> dict | None:
    """将 quotec 单条转为 db CSV 一行：Open, High, Low, Close, Volume。"""
    try:
        o = float(d.get("open") or d.get("last_close") or 0)
        h = float(d.get("high") or 0)
        l_ = float(d.get("low") or 0)
        c = float(d.get("current") or d.get("last_close") or 0)
        v = int(d.get("volume") or 0)
        if c <= 0:
            return None
        return {
            "Date": trade_date.strftime("%Y-%m-%d"),
            "Open": o,
            "High": h or max(o, c),
            "Low": l_ or min(o, c),
            "Close": c,
            "Volume": v,
        }
    except (TypeError, ValueError):
        return None


def append_row_to_csv(csv_path: Path, row: dict, date_column: str = "Date") -> bool:
    """若 date_column 对应的日期尚未出现在 CSV 中，则追加一行。返回是否写入了新行。"""
    import pandas as pd
    date_val = row.get(date_column)
    if not date_val:
        return False
    if csv_path.exists():
        existing = pd.read_csv(csv_path, index_col=0, parse_dates=True)
        # 统一索引为 datetime，避免 str/Timestamp 混用导致 sort 出错
        existing.index = pd.to_datetime(existing.index)
        if not existing.empty and date_val in [d.strftime("%Y-%m-%d") for d in existing.index]:
            return False
        new_df = pd.DataFrame([row]).set_index(date_column)
        new_df.index = pd.to_datetime(new_df.index)
        existing = pd.concat([existing, new_df])
    else:
        existing = pd.DataFrame([row]).set_index(date_column)
        existing.index = pd.to_datetime(existing.index)
    existing = existing[~existing.index.duplicated(keep="last")]
    existing.sort_index(inplace=True)
    existing.to_csv(csv_path)
    return True


def append_rows_to_csv(csv_path: Path, rows: list[dict], date_column: str = "Date") -> int:
    """批量追加多行，只写入 CSV 中尚不存在的日期。返回新写入行数。"""
    import pandas as pd
    if not rows:
        return 0
    new_df = pd.DataFrame(rows).set_index(date_column)
    new_df.index = pd.to_datetime(new_df.index)
    if csv_path.exists():
        existing = pd.read_csv(csv_path, index_col=0, parse_dates=True)
        existing.index = pd.to_datetime(existing.index)
        new_df = new_df.loc[~new_df.index.isin(existing.index)]
        if new_df.empty:
            return 0
        combined = pd.concat([existing, new_df])
    else:
        combined = new_df
    combined = combined[~combined.index.duplicated(keep="last")]
    combined.sort_index(inplace=True)
    combined.to_csv(csv_path)
    return len(new_df)


def run_today_update(db_dir: Path, delay: int = XUEQIU_DELAY) -> tuple[int, int]:
    """用 quotec 拉取「今天」的数据，逐只追加到 db；每只之间间隔 delay 秒。返回 (成功数, 跳过数)。"""
    if requests is None:
        raise RuntimeError("请安装 requests: pip install requests")
    symbols = get_symbols(db_dir)
    if not symbols:
        print("db 目录下无 CSV，无法更新")
        return 0, 0
    today = date.today()
    ok = skip = 0
    session = requests.Session()
    for i, symbol in enumerate(symbols, 1):
        d = fetch_quotec(symbol, session)
        row = quotec_to_row(d, today) if d else None
        csv_path = db_dir / f"{symbol}.csv"
        if row is None:
            print(f"[{i}/{len(symbols)}] {symbol} 无有效行情，跳过")
            skip += 1
        else:
            if append_row_to_csv(csv_path, row):
                print(f"[{i}/{len(symbols)}] {symbol} 已追加 {today}")
                ok += 1
            else:
                skip += 1
        if i < len(symbols):
            time.sleep(delay)
    return ok, skip


def fetch_kline(symbol: str, begin_ms: int, count: int, session: requests.Session | None = None) -> list[dict] | None:
    """请求雪球 kline.json，返回 list of {Date, Open, High, Low, Close, Volume}。"""
    url = "https://stock.xueqiu.com/v5/stock/chart/kline.json"
    params = {
        "symbol": symbol,
        "begin": begin_ms,
        "period": "day",
        "type": "before",
        "count": count,
        "indicator": "kline,pe,pb,ps,pcf,market_capital,agt,ggt,balance",
    }
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    if session is None:
        session = requests.Session()
    try:
        r = session.get(url, params=params, headers=headers, timeout=20)
        r.raise_for_status()
        data = r.json()
        if data.get("error_code") != 0:
            return None
        d = data.get("data") or {}
        col = d.get("column") or []
        item = d.get("item") or []
        if not col or not item:
            return None
        def _idx(name: str, default: int) -> int:
            return col.index(name) if name in col else default
        idx_ts = _idx("timestamp", 0)
        idx_open = _idx("open", 1)
        idx_high = _idx("high", 3)
        idx_low = _idx("low", 4)
        idx_close = _idx("close", 2)
        idx_vol = _idx("volume", 5)
        rows = []
        for arr in item:
            if idx_ts >= len(arr):
                continue
            ts = arr[idx_ts]
            if isinstance(ts, (int, float)):
                dt = datetime.fromtimestamp(ts / 1000.0).date()
            else:
                continue
            try:
                rows.append({
                    "Date": dt.strftime("%Y-%m-%d"),
                    "Open": float(arr[idx_open]),
                    "High": float(arr[idx_high]),
                    "Low": float(arr[idx_low]),
                    "Close": float(arr[idx_close]),
                    "Volume": int(float(arr[idx_vol])) if idx_vol < len(arr) else 0,
                })
            except (IndexError, TypeError, ValueError):
                continue
        return rows
    except Exception:
        return None


def run_backfill(db_dir: Path, from_date: date, delay: int = XUEQIU_DELAY) -> tuple[int, int]:
    """从 from_date 到昨天用 kline 拉取历史，只追加 CSV 中不存在的日期。返回 (成功数, 跳过数)。"""
    if requests is None:
        raise RuntimeError("请安装 requests: pip install requests")
    symbols = get_symbols(db_dir)
    if not symbols:
        print("db 目录下无 CSV")
        return 0, 0
    end = date.today() - timedelta(days=1)
    if from_date > end:
        print("from 日期已晚于昨天，无需补历史")
        return 0, 0
    # begin 用「今天 0 点」时间戳（毫秒），count 为从 from_date 到 end 的交易日数，多取一点
    n_days = (end - from_date).days + 1
    begin_ms = int(datetime(end.year, end.month, end.day, 23, 59, 59).timestamp() * 1000)
    count = -min(n_days + 30, 500)
    ok = skip = 0
    session = requests.Session()
    for i, symbol in enumerate(symbols, 1):
        rows = fetch_kline(symbol, begin_ms, count, session)
        if not rows:
            print(f"[{i}/{len(symbols)}] {symbol} kline 无数据，跳过")
            skip += 1
        else:
            # 只保留 >= from_date 的日期
            rows = [r for r in rows if r["Date"] >= from_date.strftime("%Y-%m-%d")]
            csv_path = db_dir / f"{symbol}.csv"
            added = append_rows_to_csv(csv_path, rows)
            if added:
                print(f"[{i}/{len(symbols)}] {symbol} 追加 {added} 条")
                ok += 1
            else:
                skip += 1
        if i < len(symbols):
            time.sleep(delay)
    return ok, skip


def wait_until_16() -> None:
    """阻塞到当前时间 >= 16:00（当天已过 16 点则直接返回）。"""
    now = datetime.now()
    target = now.replace(hour=16, minute=0, second=0, microsecond=0)
    if now >= target:
        return
    delta = (target - now).total_seconds()
    print(f"等待至 16:00，约 {int(delta)} 秒后执行")
    time.sleep(delta)


def main() -> None:
    parser = argparse.ArgumentParser(description="雪球 ETF 日更：16 点后补当日数据，或 --from 补历史")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_DIR, help="db 目录")
    parser.add_argument("--delay", type=int, default=XUEQIU_DELAY, help="请求间隔秒数，默认 20")
    parser.add_argument("--daemon", action="store_true", help="常驻：每天 16 点后检查交易日并更新当日")
    parser.add_argument("--once", action="store_true", help="只执行一次：若今天为交易日则更新当日")
    parser.add_argument("--from", dest="from_date", metavar="YYYYMMDD", help="从该日期起用 kline 补历史（只新增不覆盖）")
    args = parser.parse_args()
    db_dir = args.db
    db_dir.mkdir(parents=True, exist_ok=True)

    if args.from_date:
        try:
            from_d = datetime.strptime(args.from_date, "%Y%m%d").date()
        except ValueError:
            print("--from 格式须为 YYYYMMDD")
            return
        print(f"补历史 from {from_d}，间隔 {args.delay}s")
        ok, skip = run_backfill(db_dir, from_d, args.delay)
        print(f"完成: 成功 {ok}，跳过 {skip}")
        return

    if args.daemon:
        while True:
            wait_until_16()
            today = date.today()
            if is_trading_day(today):
                print(f"[{today}] 交易日，开始更新当日数据")
                ok, skip = run_today_update(db_dir, args.delay)
                print(f"当日更新: 成功 {ok}，跳过 {skip}")
            else:
                print(f"[{today}] 非交易日，跳过")
            # 等到明天再判断，避免 16 点后重复执行
            tomorrow_16 = (today + timedelta(days=1)).strftime("%Y-%m-%d") + " 16:00:00"
            next_ts = datetime.strptime(tomorrow_16, "%Y-%m-%d %H:%M:%S")
            wait_sec = (next_ts - datetime.now()).total_seconds()
            if wait_sec > 0:
                print(f"下一轮: {tomorrow_16}，休眠 {int(wait_sec)}s")
                time.sleep(wait_sec)
        return

    if args.once:
        today = date.today()
        if not is_trading_day(today):
            print(f"{today} 非交易日，不更新")
            return
        ok, skip = run_today_update(db_dir, args.delay)
        print(f"完成: 成功 {ok}，跳过 {skip}")
        return

    # 默认行为：等同 --once
    today = date.today()
    if not is_trading_day(today):
        print(f"{today} 非交易日，不更新。可用 --daemon 常驻或 --from YYYYMMDD 补历史")
        return
    ok, skip = run_today_update(db_dir, args.delay)
    print(f"完成: 成功 {ok}，跳过 {skip}")


if __name__ == "__main__":
    main()
