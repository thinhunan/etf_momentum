#!/usr/bin/env python3
"""
从 niusan_20260315.csv 复用持仓记录，按严格的财报披露截止日重新计算买入时间和收益率。

A股财报披露截止日规则：
  Q1 (3/31)   -> 4月30日前  -> 跟单买入日：5月1日后第一个交易日开盘价
  半年报 (6/30) -> 8月31日前 -> 跟单买入日：9月1日后第一个交易日开盘价
  Q3 (9/30)   -> 10月31日前 -> 跟单买入日：11月1日后第一个交易日开盘价
  年报 (12/31) -> 次年4月30日前 -> 跟单买入日：次年5月1日后第一个交易日开盘价

生成:
  1. niusan_detail_{date}.csv   — 逐条明细
  2. niusan_summary_{date}.csv  — 每位牛散汇总（全量 + 近两年）
"""

import os
import logging
from datetime import datetime, timedelta

for _k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"):
    os.environ.pop(_k, None)

import pandas as pd
import baostock as bs
from dateutil.relativedelta import relativedelta

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

SOURCE_CSV = "niusan_20260315.csv"

# ---------------------------------------------------------------------------
# 1. 披露截止后买入日
# ---------------------------------------------------------------------------

def disclosure_buy_date(report_date: datetime) -> datetime:
    """报告期 -> 披露截止次日（跟单最早可买入日）。"""
    m = report_date.month
    y = report_date.year
    if m == 12:
        return datetime(y + 1, 5, 1)
    elif m <= 3:
        return datetime(y, 5, 1)
    elif m <= 6:
        return datetime(y, 9, 1)
    elif m <= 9:
        return datetime(y, 11, 1)
    else:
        # 10/11月的非标准报告期，保守 +2 个月
        return datetime(y + 1, 1, 1)


# ---------------------------------------------------------------------------
# 2. baostock 行情
# ---------------------------------------------------------------------------

_cache: dict[str, pd.DataFrame] = {}
_logged_in = False


def _login():
    global _logged_in
    if not _logged_in:
        bs.login()
        _logged_in = True


def _bs_code(code: str) -> str:
    c = str(code).zfill(6)
    return f"sh.{c}" if c[0] in ("6", "9") else f"sz.{c}"


def get_hist(code: str, start: str, end: str) -> pd.DataFrame:
    key = f"{code}_{start}_{end}"
    if key in _cache:
        return _cache[key]
    _login()
    sd = f"{start[:4]}-{start[4:6]}-{start[6:]}"
    ed = f"{end[:4]}-{end[4:6]}-{end[6:]}"
    try:
        rs = bs.query_history_k_data_plus(
            _bs_code(str(code).zfill(6)),
            "date,open,close", start_date=sd, end_date=ed,
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
            df = df.dropna().query("收盘 > 0 and 开盘 > 0").sort_values("日期").reset_index(drop=True)
        else:
            df = pd.DataFrame()
    except Exception as e:
        logger.error("获取 %s 行情失败: %s", code, e)
        df = pd.DataFrame()
    _cache[key] = df
    return df


def first_on_or_after(df: pd.DataFrame, target):
    sub = df.loc[df["日期"] >= pd.Timestamp(target)]
    return sub.iloc[0] if not sub.empty else None


def last_on_or_before(df: pd.DataFrame, target):
    sub = df.loc[df["日期"] <= pd.Timestamp(target)]
    return sub.iloc[-1] if not sub.empty else None


# ---------------------------------------------------------------------------
# 3. 批量重算
# ---------------------------------------------------------------------------

def recalculate(src: pd.DataFrame) -> pd.DataFrame:
    records = []
    total = len(src)

    for i, (_, row) in enumerate(src.iterrows()):
        if (i + 1) % 200 == 0 or i == 0:
            logger.info("进度 [%d/%d] ...", i + 1, total)

        rpt = pd.Timestamp(row["报告期"])
        code = str(row["股票代码"]).zfill(6)
        buy_target = disclosure_buy_date(rpt.to_pydatetime())
        now = datetime.now()

        if buy_target > now:
            continue

        data_start = (rpt - timedelta(days=10)).strftime("%Y%m%d")
        data_end = (buy_target + relativedelta(months=4)).strftime("%Y%m%d")
        if data_end > now.strftime("%Y%m%d"):
            data_end = now.strftime("%Y%m%d")

        hist = get_hist(code, data_start, data_end)
        if hist.empty:
            continue

        rpt_row = last_on_or_before(hist, rpt)
        report_close = float(rpt_row["收盘"]) if rpt_row is not None else None

        buy_row = first_on_or_after(hist, buy_target)
        if buy_row is None:
            continue
        buy_price = float(buy_row["开盘"])
        buy_date = buy_row["日期"]
        if buy_price <= 0:
            continue

        rec = {
            "牛散": row["牛散"],
            "股票代码": code,
            "股票名称": row["股票名称"],
            "持股变动": row["持股变动"],
            "报告期": str(rpt.date()),
            "报告期收盘价": report_close,
            "跟踪买入日": str(buy_date.date()),
            "跟踪成本价": buy_price,
        }

        for m in (1, 2, 3):
            sell_target = buy_date + relativedelta(months=m)
            sell_row = first_on_or_after(hist, sell_target)
            if sell_row is not None:
                sp = float(sell_row["收盘"])
                rec[f"持仓{m}月后日期"] = str(sell_row["日期"].date())
                rec[f"持仓{m}月后价"] = sp
                rec[f"持仓{m}月收益率"] = round((sp - buy_price) / buy_price, 4)
            else:
                rec[f"持仓{m}月后日期"] = None
                rec[f"持仓{m}月后价"] = None
                rec[f"持仓{m}月收益率"] = None

        records.append(rec)

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# 4. 汇总统计
# ---------------------------------------------------------------------------

def build_summary(detail: pd.DataFrame, label: str = "") -> pd.DataFrame:
    rows = []
    for name, g in detail.groupby("牛散"):
        rec = {"牛散": name, "记录数": len(g)}
        rec["最早记录日期"] = g["报告期"].min()
        rec["最新记录日期"] = g["报告期"].max()
        rec["新进数"] = (g["持股变动"] == "新进").sum()
        rec["增持数"] = (g["持股变动"] == "增持").sum()

        for m in (1, 2, 3):
            col = f"持仓{m}月收益率"
            v = g[col].dropna()
            if len(v) == 0:
                continue
            pre = f"跟单{m}月"
            rec[f"{pre}_样本数"] = len(v)
            rec[f"{pre}_平均收益"] = round(v.mean(), 4)
            rec[f"{pre}_中位收益"] = round(v.median(), 4)
            rec[f"{pre}_胜率"] = round((v > 0).mean(), 4)
            rec[f"{pre}_最大收益"] = round(v.max(), 4)
            rec[f"{pre}_最大回撤"] = round(v.min(), 4)
        rows.append(rec)

    df = pd.DataFrame(rows)
    if not df.empty:
        df.sort_values("跟单2月_平均收益", ascending=False, inplace=True, na_position="last")
        df.reset_index(drop=True, inplace=True)
    return df


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main():
    logger.info("=" * 60)
    logger.info("重算：严格披露截止日跟单回测")
    logger.info("=" * 60)

    src = pd.read_csv(SOURCE_CSV)
    src["报告期"] = pd.to_datetime(src["报告期"])
    logger.info("读取 %s: %d 条记录", SOURCE_CSV, len(src))

    logger.info("开始重算收益率 ...")
    detail = recalculate(src)
    logger.info("有效结果: %d 条", len(detail))

    if detail.empty:
        logger.warning("无有效结果"); return

    today = datetime.now().strftime("%Y%m%d")

    detail_file = f"niusan_detail_{today}.csv"
    detail.to_csv(detail_file, index=False, encoding="utf-8-sig")
    logger.info("明细已保存: %s", detail_file)

    # 全量汇总
    summary_all = build_summary(detail, "全量")
    # 近两年
    cutoff = (datetime.now() - relativedelta(years=2)).strftime("%Y-%m-%d")
    recent = detail[detail["报告期"] >= cutoff]
    summary_recent = build_summary(recent, "近两年")

    # 写入同一个 Excel 两个 sheet + CSV
    xlsx_file = f"niusan_summary_{today}.xlsx"
    with pd.ExcelWriter(xlsx_file, engine="openpyxl") as w:
        summary_all.to_excel(w, index=False, sheet_name="全量汇总")
        summary_recent.to_excel(w, index=False, sheet_name="近两年汇总")

        for ws in w.sheets.values():
            for col_idx in range(1, ws.max_column + 1):
                header = ws.cell(row=1, column=col_idx).value or ""
                if "收益" in header or "胜率" in header or "回撤" in header:
                    for r in range(2, ws.max_row + 1):
                        cell = ws.cell(row=r, column=col_idx)
                        if cell.value is not None:
                            cell.number_format = "0.00%"

    summary_all.to_csv(f"niusan_summary_all_{today}.csv", index=False, encoding="utf-8-sig")
    summary_recent.to_csv(f"niusan_summary_recent2y_{today}.csv", index=False, encoding="utf-8-sig")
    logger.info("汇总已保存: %s", xlsx_file)

    # 收益率统计
    logger.info("=" * 50)
    logger.info("全量收益率统计")
    for m in (1, 2, 3):
        v = detail[f"持仓{m}月收益率"].dropna()
        if v.empty: continue
        logger.info("持仓%d月: 样本=%d 平均=%.2f%% 中位=%.2f%% 胜率=%.1f%%",
                     m, len(v), v.mean()*100, v.median()*100, (v>0).mean()*100)

    logger.info("-" * 50)
    logger.info("近两年收益率统计")
    for m in (1, 2, 3):
        v = recent[f"持仓{m}月收益率"].dropna()
        if v.empty: continue
        logger.info("持仓%d月: 样本=%d 平均=%.2f%% 中位=%.2f%% 胜率=%.1f%%",
                     m, len(v), v.mean()*100, v.median()*100, (v>0).mean()*100)

    if _logged_in:
        bs.logout()


if __name__ == "__main__":
    main()
