#!/usr/bin/env python3
"""
牛散跟踪回测工具

1. 从 tetegu.com 获取牛散名单
2. 通过东方财富数据中心 API 获取每位牛散的全量历史持仓（可回溯至 2004 年）
3. 筛选「新进」和「增加(增持)」记录
4. 按报告期 +3 个月后第一个交易日开盘价模拟跟踪买入
5. 计算持仓 1/2/3 个月后的收益率，输出到 niusan_{date}.xlsx
"""

import os
import re
import time
import logging
from datetime import datetime, timedelta

for _k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"):
    os.environ.pop(_k, None)

import requests
from bs4 import BeautifulSoup
import pandas as pd
import baostock as bs
from dateutil.relativedelta import relativedelta

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

HEADERS_WEB = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}
HEADERS_EM = {
    "User-Agent": HEADERS_WEB["User-Agent"],
    "Referer": "https://data.eastmoney.com/",
}
BASE_URL = "http://www.tetegu.com"
EM_API_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"


# ---------------------------------------------------------------------------
# 1. 从 tetegu.com 获取牛散名单
# ---------------------------------------------------------------------------

def get_niusan_list(top_n: int = 50) -> list[str]:
    """从牛散名单页获取牛散姓名列表（去重）。"""
    url = f"{BASE_URL}/niusan/"
    resp = requests.get(url, headers=HEADERS_WEB, timeout=30)
    resp.encoding = "utf-8"
    soup = BeautifulSoup(resp.text, "html.parser")

    seen, result = set(), []
    for a in soup.find_all("a", href=re.compile(r"/gudong/\d+\.html")):
        name = a.get_text(strip=True)
        if not name or name in seen:
            continue
        # 排除机构类名称
        if any(kw in name for kw in ("公司", "银行", "基金", "有限", "plc", "morgan", "ubs")):
            continue
        seen.add(name)
        result.append(name)
        if len(result) >= top_n:
            break

    logger.info("从 tetegu 获取到 %d 位牛散", len(result))
    return result


# ---------------------------------------------------------------------------
# 2. 通过东方财富 API 获取牛散全量历史持仓
# ---------------------------------------------------------------------------

def _fetch_em_holdings_page(holder_name: str, page: int, page_size: int = 500) -> dict:
    """查询东方财富数据中心单页数据。"""
    params = {
        "sortColumns": "END_DATE",
        "sortTypes": "-1",
        "pageSize": str(page_size),
        "pageNumber": str(page),
        "reportName": "RPT_F10_EH_FREEHOLDERS",
        "columns": (
            "SECURITY_CODE,SECURITY_NAME_ABBR,END_DATE,"
            "HOLD_NUM,FREE_HOLDNUM_RATIO,HOLDNUM_CHANGE_NAME,"
            "HOLDER_RANK,HOLDER_TYPE"
        ),
        "filter": f'(HOLDER_NAME="{holder_name}")',
    }
    resp = requests.get(EM_API_URL, params=params, headers=HEADERS_EM, timeout=20)
    return resp.json()


def get_niusan_holdings_em(name: str) -> list[dict]:
    """获取单个牛散的全量历史持仓，仅保留「新进」和「增加」。"""
    holdings = []
    page = 1
    total_fetched = 0

    while True:
        try:
            data = _fetch_em_holdings_page(name, page)
        except Exception as e:
            logger.warning("  %s 第%d页请求失败: %s", name, page, e)
            break

        result = data.get("result")
        if not result or not result.get("data"):
            break

        rows = result["data"]
        total_count = result.get("count", 0)

        for r in rows:
            change = r.get("HOLDNUM_CHANGE_NAME", "")
            if change not in ("新进", "增加"):
                continue

            end_date = r.get("END_DATE", "")
            if end_date:
                end_date = end_date[:10]  # "2025-09-30 00:00:00" -> "2025-09-30"

            stock_code = r.get("SECURITY_CODE", "")
            stock_name = r.get("SECURITY_NAME_ABBR", "")
            if not stock_code or not end_date:
                continue

            change_type = "新进" if change == "新进" else "增持"
            holdings.append({
                "niusan": name,
                "stock_name": stock_name,
                "stock_code": stock_code,
                "change_type": change_type,
                "report_date": end_date,
            })

        total_fetched += len(rows)
        if total_fetched >= total_count:
            break
        page += 1
        time.sleep(0.2)

    logger.info("  %s: 东方财富共 %d 条新进/增持记录", name, len(holdings))
    return holdings


# ---------------------------------------------------------------------------
# 3. baostock 行情数据
# ---------------------------------------------------------------------------

_hist_cache: dict[str, pd.DataFrame] = {}
_bs_logged_in = False


def _ensure_bs_login():
    global _bs_logged_in
    if not _bs_logged_in:
        lg = bs.login()
        if lg.error_code != "0":
            logger.error("baostock 登录失败: %s", lg.error_msg)
        _bs_logged_in = True


def _to_bs_code(stock_code: str) -> str:
    if stock_code.startswith("6") or stock_code.startswith("9"):
        return f"sh.{stock_code}"
    return f"sz.{stock_code}"


def get_stock_hist(stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    """获取股票日线行情（前复权），带缓存。"""
    key = f"{stock_code}_{start_date}_{end_date}"
    if key in _hist_cache:
        return _hist_cache[key]

    _ensure_bs_login()
    bs_code = _to_bs_code(stock_code)
    sd = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}"
    ed = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]}"

    try:
        rs = bs.query_history_k_data_plus(
            bs_code,
            "date,open,high,low,close,volume",
            start_date=sd, end_date=ed,
            frequency="d", adjustflag="2",
        )
        rows = []
        while rs.error_code == "0" and rs.next():
            rows.append(rs.get_row_data())

        if rows:
            df = pd.DataFrame(rows, columns=["日期", "开盘", "最高", "最低", "收盘", "成交量"])
            df["日期"] = pd.to_datetime(df["日期"])
            for col in ["开盘", "最高", "最低", "收盘"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df["成交量"] = pd.to_numeric(df["成交量"], errors="coerce")
            df = df.dropna(subset=["收盘"])
            df = df[df["收盘"] > 0]
            df.sort_values("日期", inplace=True)
            df.reset_index(drop=True, inplace=True)
        else:
            df = pd.DataFrame()
    except Exception as e:
        logger.error("  获取 %s 行情失败: %s", stock_code, e)
        df = pd.DataFrame()

    _hist_cache[key] = df
    return df


def _cleanup_bs():
    global _bs_logged_in
    if _bs_logged_in:
        bs.logout()
        _bs_logged_in = False


# ---------------------------------------------------------------------------
# 4. 计算收益率
# ---------------------------------------------------------------------------

def _disclosure_deadline(report_date: datetime) -> datetime:
    """根据报告期计算A股财报实际披露截止日（可交易日）。

    Q1 (3/31)  -> 4/30 同年     -> 买入日 5/1
    半年报 (6/30) -> 8/31 同年  -> 买入日 9/1
    Q3 (9/30)  -> 10/31 同年    -> 买入日 11/1
    年报 (12/31) -> 4/30 次年   -> 买入日 5/1 次年
    """
    month = report_date.month
    year = report_date.year
    if month == 12:
        return datetime(year + 1, 5, 1)
    elif month == 3:
        return datetime(year, 5, 1)
    elif month == 6:
        return datetime(year, 9, 1)
    elif month == 9:
        return datetime(year, 11, 1)
    else:
        # 非标准报告期（如 11/3 三季报修正），保守 +3 个月
        return report_date + relativedelta(months=3)


def _first_trade_day_on_or_after(df: pd.DataFrame, target) -> pd.Series | None:
    ts = pd.Timestamp(target)
    sub = df.loc[df["日期"] >= ts]
    return sub.iloc[0] if not sub.empty else None


def _last_trade_day_on_or_before(df: pd.DataFrame, target) -> pd.Series | None:
    ts = pd.Timestamp(target)
    sub = df.loc[df["日期"] <= ts]
    return sub.iloc[-1] if not sub.empty else None


def calculate_returns(holdings: list[dict]) -> list[dict]:
    results = []
    total = len(holdings)

    for i, h in enumerate(holdings):
        if (i + 1) % 100 == 0 or i == 0:
            logger.info("计算进度 [%d/%d] ...", i + 1, total)

        try:
            report_date = datetime.strptime(h["report_date"], "%Y-%m-%d")
        except ValueError:
            continue

        buy_target = _disclosure_deadline(report_date)
        data_start = (report_date - timedelta(days=10)).strftime("%Y%m%d")
        data_end = (buy_target + relativedelta(months=4)).strftime("%Y%m%d")

        today_str = datetime.now().strftime("%Y%m%d")
        if data_end > today_str:
            data_end = today_str

        # 如果买入目标日已超过今天，跳过（无法回测未来数据）
        if buy_target > datetime.now():
            continue

        df = get_stock_hist(h["stock_code"], data_start, data_end)
        if df.empty:
            continue

        rpt_row = _last_trade_day_on_or_before(df, report_date)
        report_close = float(rpt_row["收盘"]) if rpt_row is not None else None

        buy_row = _first_trade_day_on_or_after(df, buy_target)
        if buy_row is None:
            continue

        buy_price = float(buy_row["开盘"])
        buy_date = buy_row["日期"]
        if buy_price <= 0:
            continue

        row = {
            "牛散": h["niusan"],
            "股票代码": h["stock_code"],
            "股票名称": h["stock_name"],
            "持股变动": h["change_type"],
            "报告期": h["report_date"],
            "报告期收盘价": report_close,
            "跟踪买入日": buy_date.strftime("%Y-%m-%d"),
            "跟踪成本价": buy_price,
        }

        for m in (1, 2, 3):
            sell_target = buy_date + relativedelta(months=m)
            sell_row = _first_trade_day_on_or_after(df, sell_target)
            if sell_row is not None:
                sell_price = float(sell_row["收盘"])
                sell_date = sell_row["日期"]
                ret = (sell_price - buy_price) / buy_price
                row[f"持仓{m}月后日期"] = sell_date.strftime("%Y-%m-%d")
                row[f"持仓{m}月后价"] = sell_price
                row[f"持仓{m}月收益率"] = round(ret, 4)
            else:
                row[f"持仓{m}月后日期"] = None
                row[f"持仓{m}月后价"] = None
                row[f"持仓{m}月收益率"] = None

        results.append(row)

    return results


# ---------------------------------------------------------------------------
# 5. 输出
# ---------------------------------------------------------------------------

COLUMNS_ORDER = [
    "牛散", "股票代码", "股票名称", "持股变动",
    "报告期", "报告期收盘价",
    "跟踪买入日", "跟踪成本价",
    "持仓1月后日期", "持仓1月后价", "持仓1月收益率",
    "持仓2月后日期", "持仓2月后价", "持仓2月收益率",
    "持仓3月后日期", "持仓3月后价", "持仓3月收益率",
]


def save_to_excel(results: list[dict], filename: str):
    df = pd.DataFrame(results)
    cols = [c for c in COLUMNS_ORDER if c in df.columns]
    df = df[cols]

    pct_cols = [c for c in df.columns if "收益率" in c]
    writer = pd.ExcelWriter(filename, engine="openpyxl")
    df.to_excel(writer, index=False, sheet_name="牛散跟踪回测")

    ws = writer.sheets["牛散跟踪回测"]
    for col_name in pct_cols:
        col_idx = cols.index(col_name) + 1
        for row_num in range(2, len(df) + 2):
            cell = ws.cell(row=row_num, column=col_idx)
            if cell.value is not None:
                cell.number_format = "0.00%"

    writer.close()

    csv_name = filename.replace(".xlsx", ".csv")
    df.to_csv(csv_name, index=False, encoding="utf-8-sig")

    logger.info("已保存到 %s / %s （%d 条记录）", filename, csv_name, len(df))
    return df


def print_summary(df: pd.DataFrame):
    logger.info("=" * 50)
    logger.info("收益率统计")
    logger.info("=" * 50)
    for m in (1, 2, 3):
        col = f"持仓{m}月收益率"
        if col not in df.columns:
            continue
        v = df[col].dropna()
        if v.empty:
            continue
        win_rate = (v > 0).sum() / len(v)
        logger.info(
            "持仓%d月: 样本=%d  平均=%.2f%%  中位=%.2f%%  "
            "最大=%.2f%%  最小=%.2f%%  胜率=%.2f%%",
            m, len(v),
            v.mean() * 100, v.median() * 100,
            v.max() * 100, v.min() * 100,
            win_rate * 100,
        )


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main():
    logger.info("=" * 60)
    logger.info("牛散跟踪回测工具（东方财富全量历史数据）")
    logger.info("=" * 60)

    # ---- Step 1: 获取牛散名单 ----
    logger.info("[Step 1] 从 tetegu 获取牛散名单...")
    niusan_names = get_niusan_list(top_n=50)
    if not niusan_names:
        logger.error("未获取到牛散名单，退出")
        return

    # ---- Step 2: 从东方财富获取全量历史持仓 ----
    logger.info("[Step 2] 从东方财富获取全量历史持仓...")
    all_holdings = []
    for name in niusan_names:
        time.sleep(0.3)
        try:
            records = get_niusan_holdings_em(name)
            all_holdings.extend(records)
        except Exception as e:
            logger.error("  获取 %s 持仓失败: %s", name, e)

    # 去重
    seen = set()
    unique = []
    for h in all_holdings:
        key = (h["niusan"], h["stock_code"], h["report_date"])
        if key not in seen:
            seen.add(key)
            unique.append(h)
    all_holdings = unique

    logger.info("共 %d 条去重后的新进/增持记录", len(all_holdings))
    if not all_holdings:
        logger.warning("无可用持仓数据，退出")
        return

    # 打印数据时间范围
    dates = sorted(set(h["report_date"] for h in all_holdings))
    logger.info("数据时间范围: %s ~ %s", dates[0], dates[-1])

    # ---- Step 3: 计算跟踪收益率 ----
    logger.info("[Step 3] 计算跟踪收益率（共 %d 条）...", len(all_holdings))
    results = calculate_returns(all_holdings)

    if not results:
        logger.warning("未计算出任何结果")
        _cleanup_bs()
        return

    # ---- Step 4: 保存 Excel ----
    today = datetime.now().strftime("%Y%m%d")
    filename = f"niusan_{today}.xlsx"
    df = save_to_excel(results, filename)
    print_summary(df)
    _cleanup_bs()


if __name__ == "__main__":
    main()
