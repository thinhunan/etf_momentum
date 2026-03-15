"""增量计算跟单收益率——仅对尚未计算的持仓记录拉取行情并计算。"""

import os
import logging
from datetime import datetime, timedelta

for _k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"):
    os.environ.pop(_k, None)

import pandas as pd
import baostock as bs
from dateutil.relativedelta import relativedelta

from .config import DATA_FILES

logger = logging.getLogger(__name__)

# ── 披露截止日 ───────────────────────────────────────────────

def disclosure_buy_date(report_date: datetime) -> datetime:
    """报告期 → 披露截止次日（跟单最早可买入日）。

    Q1  (≤3月)  → 当年 5/1    半年报 (≤6月) → 当年 9/1
    Q3  (≤9月)  → 当年 11/1   年报   (12月) → 次年 5/1
    """
    m, y = report_date.month, report_date.year
    if m == 12:
        return datetime(y + 1, 5, 1)
    if m <= 3:
        return datetime(y, 5, 1)
    if m <= 6:
        return datetime(y, 9, 1)
    if m <= 9:
        return datetime(y, 11, 1)
    return datetime(y + 1, 1, 1)


# ── baostock 行情 ────────────────────────────────────────────

_cache: dict[str, pd.DataFrame] = {}
_logged_in = False


def _login():
    global _logged_in
    if not _logged_in:
        bs.login()
        _logged_in = True


def cleanup_bs():
    global _logged_in
    if _logged_in:
        bs.logout()
        _logged_in = False


def _bs_code(code: str) -> str:
    c = str(code).zfill(6)
    return f"sh.{c}" if c[0] in ("6", "9") else f"sz.{c}"


def get_hist(code: str, start: str, end: str) -> pd.DataFrame:
    """获取日线数据（前复权），带内存缓存。start/end: YYYYMMDD。"""
    key = f"{code}_{start}_{end}"
    if key in _cache:
        return _cache[key]

    _login()
    sd = f"{start[:4]}-{start[4:6]}-{start[6:]}"
    ed = f"{end[:4]}-{end[4:6]}-{end[6:]}"

    try:
        rs = bs.query_history_k_data_plus(
            _bs_code(code), "date,open,close",
            start_date=sd, end_date=ed,
            frequency="d", adjustflag="2",
        )
        rows = []
        while rs.error_code == "0" and rs.next():
            rows.append(rs.get_row_data())
        if rows:
            df = pd.DataFrame(rows, columns=["日期", "开盘", "收盘"])
            df["日期"] = pd.to_datetime(df["日期"])
            df["开盘"] = pd.to_numeric(df["开盘"], errors="coerce")
            df["收盘"] = pd.to_numeric(df["收盘"], errors="coerce")
            df = df.dropna().query("收盘>0 and 开盘>0").sort_values("日期").reset_index(drop=True)
        else:
            df = pd.DataFrame()
    except Exception as e:
        logger.error("获取 %s 行情失败: %s", code, e)
        df = pd.DataFrame()

    _cache[key] = df
    return df


def _first_on_or_after(df: pd.DataFrame, target):
    sub = df.loc[df["日期"] >= pd.Timestamp(target)]
    return sub.iloc[0] if not sub.empty else None


def _last_on_or_before(df: pd.DataFrame, target):
    sub = df.loc[df["日期"] <= pd.Timestamp(target)]
    return sub.iloc[-1] if not sub.empty else None


def get_latest_price(stock_code: str) -> float | None:
    """获取该股票最近一个交易日的收盘价（前复权）。无数据返回 None。"""
    from datetime import datetime
    code = str(stock_code).zfill(6)
    end = datetime.now().strftime("%Y%m%d")
    start = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
    df = get_hist(code, start, end)
    if df.empty:
        return None
    return float(df.iloc[-1]["收盘"])


# ── 计算单条收益 ─────────────────────────────────────────────

def calc_one(row: pd.Series) -> dict | None:
    """为一条持仓记录计算跟单收益，返回 dict 或 None。"""
    try:
        rpt = pd.Timestamp(row["报告期"]).to_pydatetime()
    except Exception:
        return None

    code = str(row["股票代码"]).zfill(6)
    buy_target = disclosure_buy_date(rpt)
    now = datetime.now()

    if buy_target > now:
        return None

    data_start = (rpt - timedelta(days=10)).strftime("%Y%m%d")
    data_end = min((buy_target + relativedelta(months=4)).strftime("%Y%m%d"),
                   now.strftime("%Y%m%d"))

    hist = get_hist(code, data_start, data_end)
    if hist.empty:
        return None

    rpt_row = _last_on_or_before(hist, rpt)
    report_close = float(rpt_row["收盘"]) if rpt_row is not None else None

    buy_row = _first_on_or_after(hist, buy_target)
    if buy_row is None:
        return None

    buy_price = float(buy_row["开盘"])
    buy_date = buy_row["日期"]
    if buy_price <= 0:
        return None

    rec = {
        "牛散": row["牛散"],
        "股票代码": code,
        "股票名称": row["股票名称"],
        "持股变动": row["持股变动"],
        "报告期": str(rpt.date()) if hasattr(rpt, "date") else str(rpt)[:10],
        "报告期收盘价": report_close,
        "跟踪买入日": str(buy_date.date()),
        "跟踪成本价": buy_price,
    }

    for m in (1, 2, 3):
        sell_target = buy_date + relativedelta(months=m)
        sell_row = _first_on_or_after(hist, sell_target)
        if sell_row is not None:
            sp = float(sell_row["收盘"])
            rec[f"持仓{m}月后日期"] = str(sell_row["日期"].date())
            rec[f"持仓{m}月后价"] = sp
            rec[f"持仓{m}月收益率"] = round((sp - buy_price) / buy_price, 4)
        else:
            rec[f"持仓{m}月后日期"] = None
            rec[f"持仓{m}月后价"] = None
            rec[f"持仓{m}月收益率"] = None

    return rec


# ── 增量更新 ─────────────────────────────────────────────────

def load_local_returns() -> pd.DataFrame:
    f = DATA_FILES["returns"]
    if f.exists():
        df = pd.read_csv(f, dtype={"股票代码": str})
        df["股票代码"] = df["股票代码"].apply(lambda x: str(x).zfill(6))
        return df
    return pd.DataFrame()


def update_returns() -> dict:
    """
    增量计算跟单收益。

    1. 读取 holdings.csv（全部持仓记录）
    2. 读取 returns_detail.csv（已计算的记录）
    3. 找出差集（尚未计算的记录）
    4. 仅对差集拉取行情并计算
    5. 合并保存
    """
    from .holdings import load_local_holdings

    holdings = load_local_holdings()
    if holdings.empty:
        logger.warning("holdings.csv 为空，无数据可计算")
        return {"total": 0, "new_calc": 0}

    existing = load_local_returns()

    if not existing.empty:
        existing_keys = set(
            zip(existing["牛散"], existing["股票代码"], existing["报告期"].astype(str).str[:10])
        )
    else:
        existing_keys = set()

    holdings["_key"] = list(zip(
        holdings["牛散"],
        holdings["股票代码"],
        holdings["报告期"].astype(str).str[:10],
    ))
    todo = holdings[~holdings["_key"].isin(existing_keys)].drop(columns=["_key"])

    logger.info("持仓总计 %d 条, 已计算 %d 条, 需新算 %d 条",
                len(holdings), len(existing_keys), len(todo))

    if todo.empty:
        logger.info("所有记录已计算完毕，无需更新")
        return {"total": len(existing), "new_calc": 0}

    new_records = []
    total = len(todo)
    for i, (_, row) in enumerate(todo.iterrows()):
        if (i + 1) % 200 == 0 or i == 0:
            logger.info("计算进度 [%d/%d]", i + 1, total)
        rec = calc_one(row)
        if rec:
            new_records.append(rec)

    cleanup_bs()

    if new_records:
        new_df = pd.DataFrame(new_records)
        merged = pd.concat([existing, new_df], ignore_index=True)
        merged = merged.drop_duplicates(subset=["牛散", "股票代码", "报告期"])
        merged.sort_values(["牛散", "报告期"], inplace=True)
    else:
        merged = existing

    merged.to_csv(DATA_FILES["returns"], index=False, encoding="utf-8-sig")
    n_new = len(new_records)
    logger.info("收益计算完成: 总计 %d 条, 本次新增 %d 条", len(merged), n_new)
    return {"total": len(merged), "new_calc": n_new}
