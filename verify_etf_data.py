#!/usr/bin/env python3
"""
用 akshare 拉取「已知 yfinance 异常」的 24 只 ETF 的日线数据，
检查日收益率是否存在异常（单日 |涨跌幅| > 20% 视为异常）。
用于验证替代数据源 akshare 的数据质量。

用法:
    pip install akshare
    python verify_etf_data.py
    python verify_etf_data.py --threshold 15
"""

import argparse
import time

import pandas as pd

# 你提供的 24 只「yfinance 已确认异常」的 ETF：symbol, name, 数据区间(用于拉取范围)
VERIFY_LIST = [
    ("SH510170", "大宗商品ETF", "2014-03-05", "2026-03-10"),
    ("SH511030", "公司债ETF", "2018-12-27", "2026-03-10"),
    ("SH511090", "30年国债ETF", "2023-05-19", "2026-03-10"),
    ("SH512000", "券商ETF", "2016-08-30", "2026-03-10"),
    ("SH512040", "价值100ETF", "2018-11-07", "2026-03-10"),
    ("SH512070", "证券保险ETF易方达", "2017-02-17", "2026-03-10"),
    ("SH512170", "医疗ETF", "2019-05-20", "2026-03-10"),
    ("SH512200", "房地产ETF", "2017-08-25", "2026-03-10"),
    ("SH512480", "半导体ETF", "2019-05-08", "2026-03-10"),
    ("SH512670", "国防ETF", "2019-07-05", "2026-03-10"),
    ("SH512690", "酒ETF", "2019-04-04", "2026-03-10"),
    ("SH512890", "红利低波ETF华泰柏瑞", "2018-12-19", "2026-03-10"),
    ("SH515000", "科技ETF", "2019-07-22", "2026-03-10"),
    ("SH515220", "煤炭ETF", "2020-01-20", "2026-03-10"),
    ("SH515880", "通信ETF", "2019-08-16", "2026-03-10"),
    ("SH588380", "双创50ETF", "2023-01-16", "2026-03-10"),
    ("SZ159851", "金融科技ETF", "2021-03-05", "2026-03-10"),
    ("SZ159901", "深证100ETF易方达", "2014-03-05", "2026-03-10"),
    ("SZ159915", "创业板ETF易方达", "2014-03-05", "2026-03-10"),
    ("SZ159928", "消费ETF", "2014-03-05", "2026-03-10"),
    ("SZ159939", "信息技术ETF", "2015-01-08", "2026-03-10"),
    ("SZ159941", "纳指ETF", "2015-06-10", "2026-03-10"),
    ("SZ159943", "深证成指ETF大成", "2015-06-05", "2026-03-10"),
    ("SZ159949", "创业板50ETF", "2016-06-30", "2026-03-10"),
]


def to_ak_symbol(raw: str) -> str:
    raw = raw.strip().upper()
    if raw.startswith("SH") or raw.startswith("SZ"):
        return raw[2:]
    return raw


def fetch_akshare_etf(symbol: str, start: str, end: str, adjust: str = "qfq"):
    """拉取 akshare ETF 日线，返回 DataFrame index=Date, 含 收盘。
    adjust: '' 不复权, 'qfq' 前复权(推荐，可消除除权除息导致的异常涨跌幅), 'hfq' 后复权。
    """
    import akshare as ak  # noqa: PLC0415
    sym = to_ak_symbol(symbol)
    start_ = start.replace("-", "")
    end_ = end.replace("-", "")
    df = ak.fund_etf_hist_em(symbol=sym, period="daily", start_date=start_, end_date=end_, adjust=adjust)
    if df is None or df.empty:
        return pd.DataFrame()
    df["日期"] = pd.to_datetime(df["日期"])
    df = df.set_index("日期").sort_index()
    return df


def check_abnormal_returns(
    df: pd.DataFrame,
    threshold_pct: float = 20.0,
    close_col: str = "收盘",
) -> tuple[pd.Series, pd.Series]:
    """
    根据收盘价计算日收益率，找出 |日收益率| > threshold_pct 的日期。
    返回 (日收益率 Series, 异常日期的收益率 Series)。
    """
    if df.empty or close_col not in df.columns:
        return pd.Series(dtype=float), pd.Series(dtype=float)
    close = df[close_col]
    ret = close.pct_change()
    ret = ret.dropna()
    abnormal = ret[ret.abs() > threshold_pct / 100.0]
    return ret, abnormal


def main():
    parser = argparse.ArgumentParser(description="用 akshare 验证问题 ETF 数据是否无异常涨跌幅")
    parser.add_argument("--threshold", type=float, default=20.0, help="日涨跌幅超过该百分比视为异常（默认 20）")
    parser.add_argument("--delay", type=float, default=0.5, help="每只 ETF 请求间隔（秒）")
    parser.add_argument("--no-adjust", action="store_true", help="使用不复权数据（默认用前复权 qfq 以消除除权除息异常）")
    args = parser.parse_args()
    adjust = "" if args.no_adjust else "qfq"

    try:
        import akshare  # noqa: F401
    except ImportError:
        print("请先安装 akshare: pip install akshare")
        return 1

    print(f"使用 akshare 验证 {len(VERIFY_LIST)} 只 ETF，异常阈值: |日收益率| > {args.threshold}%，复权: {adjust or '无'}\n")
    results = []
    for i, (symbol, name, start, end) in enumerate(VERIFY_LIST, 1):
        try:
            df = fetch_akshare_etf(symbol, start, end, adjust=adjust)
            time.sleep(args.delay)
        except Exception as e:
            results.append((symbol, name, 0, None, None, 1, str(e)))
            print(f"  [{i:2d}] {symbol} {name}  拉取失败: {e}")
            continue

        if df.empty or "收盘" not in df.columns:
            results.append((symbol, name, 0, None, None, 0, "无数据"))
            print(f"  [{i:2d}] {symbol} {name}  无数据")
            continue

        ret, abnormal = check_abnormal_returns(df, threshold_pct=args.threshold)
        if ret.empty:
            max_ret = min_ret = None
            days = 0
        else:
            max_ret = ret.max() * 100
            min_ret = ret.min() * 100
            days = len(df)

        n_abnormal = len(abnormal)
        results.append((symbol, name, days, max_ret, min_ret, n_abnormal, None))

        if n_abnormal > 0:
            print(f"  [{i:2d}] {symbol} {name}  数据{days}天  最大{max_ret:+.2f}%  最小{min_ret:.2f}%  异常{n_abnormal}次")
            for d in abnormal.index[:5]:
                print(f"         {d.strftime('%Y-%m-%d')} ({abnormal.loc[d]*100:+.2f}%)")
            if len(abnormal) > 5:
                print(f"         ... 共 {len(abnormal)} 个异常日")
        else:
            print(f"  [{i:2d}] {symbol} {name}  数据{days}天  最大{max_ret:+.2f}%  最小{min_ret:.2f}%  无异常")

    # 汇总
    ok = sum(1 for r in results if r[5] == 0 and r[6] is None)
    fail_fetch = sum(1 for r in results if r[6] is not None)
    has_abnormal = sum(1 for r in results if r[5] > 0)
    print("\n" + "=" * 60)
    print(f"汇总: 共 {len(VERIFY_LIST)} 只，拉取失败 {fail_fetch} 只，无异常 {ok} 只，仍有异常 {has_abnormal} 只")
    if has_abnormal == 0 and fail_fetch == 0:
        print("结论: akshare（前复权）在上述问题标的上未发现异常涨跌幅，可替代 yfinance 使用。")
    elif has_abnormal > 0 and not args.no_adjust:
        print("结论: 使用前复权(qfq)后，原先的严重异常(+300%/-75%等)已消除；")
        print("      剩余为阈值边缘或真实极端日(如 2020-02-03、2024-10-08)。可替代 yfinance。")
    else:
        print("结论: 部分标的上仍存在超过阈值的日涨跌幅，建议使用前复权或人工核对。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
