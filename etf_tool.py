"""ETF 动量策略日常工具

一键完成：更新数据 → 回测 → 选出最优参数 → 生成当日报告

用法:
    python etf_tool.py                              # 默认 proxy
    python etf_tool.py --proxy http://127.0.0.1:1087
    python etf_tool.py --no-update                  # 跳过数据更新
    python etf_tool.py --top 3                      # 12年/3年各取 top3
"""

import argparse
import multiprocessing as mp
import os
import time
import warnings
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime
from itertools import product
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

_NUM_WORKERS = max(1, os.cpu_count() - 1)  # 留 1 核给系统

SLIPPAGE = 0.001
COMMISSION = 0.00006

N_LIST = [5, 10, 15, 20, 22, 25, 30, 40]
R2_LIST = [0.5, 0.6, 0.7, 0.8, 0.9]
REBAL_LIST = [1, 2, 3, 5, 10, 15, 20]
TOP_K_LIST = [1, 2, 3, 5, 10]


# ------------------------------------------------------------------
# 回测核心（与 momentum_backtest.ipynb Cell 5 一致）
# ------------------------------------------------------------------

def rolling_linreg(log_close: pd.DataFrame, n: int):
    sum_x = n * (n - 1) / 2
    sum_x2 = n * (n - 1) * (2 * n - 1) / 6
    denom_x = n * sum_x2 - sum_x ** 2
    weights = np.arange(n, dtype=float)

    roll_sum_y = log_close.rolling(n).sum()
    roll_sum_y2 = (log_close ** 2).rolling(n).sum()
    roll_sum_xy = log_close.rolling(n).apply(lambda w: np.dot(weights, w), raw=True)

    slope = (n * roll_sum_xy - sum_x * roll_sum_y) / denom_x
    ss_tot = n * roll_sum_y2 - roll_sum_y ** 2
    ss_reg = (n * roll_sum_xy - sum_x * roll_sum_y) ** 2 / denom_x
    r2 = (ss_reg / ss_tot.replace(0, np.nan)).clip(0, 1)
    return slope, r2


def backtest_momentum(
    slope, r2, daily_ret, panel, panel_open,
    n, r2_threshold, rebal_period, top_k=1,
    slippage=SLIPPAGE, commission=COMMISSION,
) -> pd.Series:
    """T+1 执行模型回测，返回逐日 NAV。"""
    start_idx = n + 5
    dates = daily_ret.index[start_idx:]

    rebal_indices = list(range(0, len(dates), rebal_period))
    signal_map = {}
    for idx_pos in rebal_indices:
        date = dates[idx_pos]
        s = slope.loc[date].dropna()
        r = r2.loc[date].dropna()
        common = s.index.intersection(r.index)
        s, r = s[common], r[common]
        candidates = s[r >= r2_threshold].sort_values(ascending=False)
        selected = candidates.head(top_k).index.tolist() if len(candidates) >= 1 else []
        signal_map[idx_pos] = selected

    exec_map = {}
    for sig_idx, selected in signal_map.items():
        exec_idx = sig_idx + 1
        if exec_idx < len(dates):
            exec_map[exec_idx] = selected

    cum = 1.0
    nav_values = np.ones(len(dates))
    current_holdings = []

    for d in range(len(dates)):
        date = dates[d]

        if d in exec_map:
            new_holdings = exec_map[d]
            if sorted(new_holdings) != sorted(current_holdings):
                if current_holdings and d > 0:
                    prev_date = dates[d - 1]
                    k_sell = len(current_holdings)
                    sell_rets = []
                    for sym in current_holdings:
                        prev_cl = panel.loc[prev_date, sym]
                        op = (
                            panel_open.loc[date, sym]
                            if (date in panel_open.index and sym in panel_open.columns)
                            else panel.loc[date, sym]
                        )
                        sell_rets.append(op * (1 - slippage) / prev_cl - 1)
                    cum *= 1 + np.nansum(sell_rets) / top_k
                    cum *= 1 - k_sell / top_k * commission

                if new_holdings:
                    k_buy = len(new_holdings)
                    cum *= 1 - k_buy / top_k * commission
                    buy_rets = []
                    for sym in new_holdings:
                        op = (
                            panel_open.loc[date, sym]
                            if (date in panel_open.index and sym in panel_open.columns)
                            else panel.loc[date, sym]
                        )
                        cl = panel.loc[date, sym]
                        buy_rets.append(cl / (op * (1 + slippage)) - 1)
                    cum *= 1 + np.nansum(buy_rets) / top_k

                current_holdings = new_holdings
            else:
                if current_holdings:
                    rets = daily_ret.loc[date, current_holdings]
                    cum *= 1 + np.nansum(rets.values) / top_k
        else:
            if current_holdings:
                rets = daily_ret.loc[date, current_holdings]
                cum *= 1 + np.nansum(rets.values) / top_k

        nav_values[d] = cum

    return pd.Series(nav_values, index=dates, name="NAV")


# ------------------------------------------------------------------
# 多进程并行回测
# ------------------------------------------------------------------

# fork 模式下子进程通过 copy-on-write 共享这些全局变量，无序列化开销
_g_panel = None
_g_panel_open = None
_g_daily_ret = None
_g_linreg_cache = None


def _init_shared(panel, panel_open, daily_ret, linreg_cache):
    """进程池 initializer：将共享数据写入 worker 全局变量。"""
    global _g_panel, _g_panel_open, _g_daily_ret, _g_linreg_cache
    _g_panel = panel
    _g_panel_open = panel_open
    _g_daily_ret = daily_ret
    _g_linreg_cache = linreg_cache


def _backtest_one(params):
    """单个参数组合的回测 + 指标计算（在 worker 进程中执行）。"""
    n, r2_thresh, rebal, top_k = params
    slope, r2 = _g_linreg_cache[n]
    nav = backtest_momentum(
        slope, r2, _g_daily_ret, _g_panel, _g_panel_open,
        n=n, r2_threshold=r2_thresh,
        rebal_period=rebal, top_k=top_k,
    )

    years = sorted(nav.index.year.unique())
    annual_rets = {}
    for yr in years:
        yr_nav = nav[nav.index.year == yr]
        if len(yr_nav) < 10:
            continue
        annual_rets[yr] = yr_nav.iloc[-1] / yr_nav.iloc[0] - 1

    full_years = [y for y in annual_rets if y != years[0] and y != years[-1]]
    avg_ret = np.mean([annual_rets[y] for y in full_years]) if full_years else np.nan

    total_ret = nav.iloc[-1] / nav.iloc[0] - 1
    n_years = (nav.index[-1] - nav.index[0]).days / 365.25

    row = {"n": n, "R2_threshold": r2_thresh, "rebal_period": rebal, "top_k": top_k}
    row.update(annual_rets)
    row["avg_full_year"] = avg_ret
    row["annualized"] = (1 + total_ret) ** (1 / n_years) - 1 if n_years > 0 else np.nan
    row["total_ret"] = total_ret
    row["max_drawdown"] = ((nav / nav.cummax()) - 1).min()
    return row


def run_batch_backtest(panel, panel_open, start_date=None, workers=_NUM_WORKERS):
    """对全部参数网格跑一次回测（多进程并行），返回 result_df。"""
    if start_date:
        panel = panel.loc[start_date:]
        panel_open = panel_open.loc[start_date:]

    log_close = np.log(panel.replace(0, np.nan))
    daily_ret = panel.pct_change()

    print(f"  预计算 rolling linreg（{len(N_LIST)} 个 n 值）...")
    linreg_cache = {}
    for n in N_LIST:
        linreg_cache[n] = rolling_linreg(log_close, n)

    param_grid = list(product(N_LIST, R2_LIST, REBAL_LIST, TOP_K_LIST))
    print(f"  参数组合: {len(param_grid)}，使用 {workers} 个进程并行回测...")

    t0 = time.time()
    ctx = mp.get_context("fork")
    with ProcessPoolExecutor(
        max_workers=workers,
        mp_context=ctx,
        initializer=_init_shared,
        initargs=(panel, panel_open, daily_ret, linreg_cache),
    ) as executor:
        results = list(executor.map(_backtest_one, param_grid, chunksize=20))

    elapsed = time.time() - t0
    print(f"  回测完成: {len(results)} 组参数，耗时 {elapsed:.0f}s")
    return pd.DataFrame(results)


# ------------------------------------------------------------------
# 主流程
# ------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="ETF 动量策略日报工具")
    parser.add_argument("--proxy", default="http://127.0.0.1:1087", help="HTTP 代理地址")
    parser.add_argument("--no-update", action="store_true", help="跳过数据更新")
    parser.add_argument("--source", default="akshare", choices=("akshare", "yfinance"), help="数据更新源，默认 akshare 避免异常涨跌幅")
    parser.add_argument("--top", type=int, default=5, help="12年/3年各取 top N 参数")
    parser.add_argument("--output", default="report", help="报告输出目录")
    args = parser.parse_args()

    from etf_momentum import EtfMomentum

    engine = EtfMomentum()
    today_str = datetime.now().strftime("%Y%m%d")
    out_dir = Path(args.output) / today_str
    out_dir.mkdir(parents=True, exist_ok=True)

    # ── Step 1: 更新数据 ──
    if not args.no_update:
        print("=" * 60)
        print("Step 1: 更新数据")
        print("=" * 60)
        result = engine.update_db_incremental(proxy=args.proxy, max_rounds=10, update_source=args.source)
        print(f"  成功: {result['ok']}，失败: {result['fail']}")
        if result["failed_symbols"]:
            print(f"  失败标的: {result['failed_symbols']}")
        print(f"  数据截止: {engine._panel.index[-1].strftime('%Y-%m-%d')}")
    else:
        print("跳过数据更新")
    print()

    # 加载 panel / panel_open
    db_dir = Path("db")
    closes, opens = {}, {}
    for fp in sorted(db_dir.glob("*.csv")):
        df = pd.read_csv(fp, index_col=0, parse_dates=True)
        if not df.empty and "Close" in df.columns and len(df) >= 30:
            closes[fp.stem] = df["Close"]
            if "Open" in df.columns:
                opens[fp.stem] = df["Open"]
    panel = pd.DataFrame(closes).sort_index().ffill()
    panel_open = pd.DataFrame(opens).sort_index().ffill()

    data_end = panel.index[-1].strftime("%Y-%m-%d")
    print(f"数据范围: {panel.index[0].strftime('%Y-%m-%d')} ~ {data_end}")
    print(f"ETF 数量: {panel.shape[1]}")
    print()

    # ── Step 2: 12年回测 ──
    print("=" * 60)
    print("Step 2: 12 年全量回测")
    print("=" * 60)
    df_12y = run_batch_backtest(panel, panel_open)
    df_12y.to_csv(out_dir / "backtest_12y.csv", index=False, encoding="utf-8-sig")

    # ── Step 3: 3年回测 ──
    three_years_ago = (datetime.now() - pd.DateOffset(years=3)).strftime("%Y-%m-%d")
    print()
    print("=" * 60)
    print(f"Step 3: 近 3 年回测（{three_years_ago} 起）")
    print("=" * 60)
    df_3y = run_batch_backtest(panel, panel_open, start_date=three_years_ago)
    df_3y.to_csv(out_dir / "backtest_3y.csv", index=False, encoding="utf-8-sig")

    # ── Step 4: 选出 Top 参数 ──
    TOP_N = args.top
    top_12y = df_12y.nlargest(TOP_N, "annualized")[["n", "R2_threshold", "rebal_period", "top_k"]]
    top_3y = df_3y.nlargest(TOP_N, "annualized")[["n", "R2_threshold", "rebal_period", "top_k"]]
    params_all = pd.concat([top_12y, top_3y]).drop_duplicates().reset_index(drop=True)
    params_all = params_all.astype({"n": int, "rebal_period": int, "top_k": int})

    print()
    print("=" * 60)
    print(f"Step 4: 最优参数（12年 Top{TOP_N} + 3年 Top{TOP_N}，去重后 {len(params_all)} 组）")
    print("=" * 60)
    print()
    print(f"{'编号':>4}  {'n':>3}  {'R2阈值':>6}  {'调仓周期':>6}  {'持仓数':>5}")
    print("-" * 40)
    for idx, row in params_all.iterrows():
        src = []
        mask_12 = (
            (top_12y["n"] == row["n"])
            & (top_12y["R2_threshold"] == row["R2_threshold"])
            & (top_12y["rebal_period"] == row["rebal_period"])
            & (top_12y["top_k"] == row["top_k"])
        )
        mask_3 = (
            (top_3y["n"] == row["n"])
            & (top_3y["R2_threshold"] == row["R2_threshold"])
            & (top_3y["rebal_period"] == row["rebal_period"])
            & (top_3y["top_k"] == row["top_k"])
        )
        if mask_12.any():
            src.append("12y")
        if mask_3.any():
            src.append("3y")
        tag = "+".join(src)
        print(
            f"  {idx+1:>2}   {int(row['n']):>3}   {row['R2_threshold']:>5}   "
            f"{int(row['rebal_period']):>6}   {int(row['top_k']):>4}   [{tag}]"
        )

    # ── Step 5: 生成报告 ──
    print()
    print("=" * 60)
    print("Step 5: 生成结果报告")
    print("=" * 60)
    print()

    report_rows = []
    signal_rows = []

    for _, row in params_all.iterrows():
        n = int(row["n"])
        r2_th = row["R2_threshold"]
        rebal = int(row["rebal_period"])
        topk = int(row["top_k"])
        label = f"n={n},R2>={r2_th},T={rebal},k={topk}"

        # 回测指标
        m12 = df_12y[
            (df_12y["n"] == n) & (df_12y["R2_threshold"] == r2_th)
            & (df_12y["rebal_period"] == rebal) & (df_12y["top_k"] == topk)
        ]
        m3 = df_3y[
            (df_3y["n"] == n) & (df_3y["R2_threshold"] == r2_th)
            & (df_3y["rebal_period"] == rebal) & (df_3y["top_k"] == topk)
        ]

        r12 = m12.iloc[0] if len(m12) > 0 else None
        r3 = m3.iloc[0] if len(m3) > 0 else None

        report_rows.append({
            "参数": label,
            "12年年化": r12["annualized"] if r12 is not None else np.nan,
            "12年累计": r12["total_ret"] if r12 is not None else np.nan,
            "12年最大回撤": r12["max_drawdown"] if r12 is not None else np.nan,
            "3年年化": r3["annualized"] if r3 is not None else np.nan,
            "3年累计": r3["total_ret"] if r3 is not None else np.nan,
            "3年最大回撤": r3["max_drawdown"] if r3 is not None else np.nan,
        })

        # 当前信号
        sig = engine.next(n=n, r2_threshold=r2_th, top_k=topk)
        if sig["action"] == "buy":
            holdings_str = "  ".join(
                f"{h['symbol']}({h['name']})" for h in sig["holdings"]
            )
        else:
            holdings_str = "空仓"

        signal_rows.append({
            "参数": label,
            "操作": sig["action"],
            "持仓标的": holdings_str,
        })

    report_df = pd.DataFrame(report_rows)
    signal_df = pd.DataFrame(signal_rows)

    # 打印回测绩效
    print("【回测绩效】")
    print()
    header = f"{'参数':<30} {'12年年化':>8} {'12年累计':>8} {'12年回撤':>8} {'3年年化':>7} {'3年累计':>7} {'3年回撤':>7}"
    print(header)
    print("-" * len(header))
    for _, r in report_df.iterrows():
        def _pct(v):
            return f"{v:.1%}" if not np.isnan(v) else "   N/A"
        print(
            f"{r['参数']:<30} {_pct(r['12年年化']):>8} {_pct(r['12年累计']):>8} "
            f"{_pct(r['12年最大回撤']):>8} {_pct(r['3年年化']):>7} {_pct(r['3年累计']):>7} "
            f"{_pct(r['3年最大回撤']):>7}"
        )

    # 打印当前持仓建议
    sig_date = engine._panel.index[-1].strftime("%Y-%m-%d")
    print()
    print(f"【当前持仓建议】（信号日: {sig_date}，T+1 执行）")
    print()
    for _, r in signal_df.iterrows():
        action_ch = "买入" if r["操作"] == "buy" else "清仓"
        print(f"  {r['参数']:<30}  [{action_ch}]  {r['持仓标的']}")

    # 统计被多组参数推荐的标的
    all_syms = []
    for _, r in signal_df.iterrows():
        if r["操作"] == "buy":
            for part in r["持仓标的"].split("  "):
                sym = part.split("(")[0]
                all_syms.append(sym)

    if all_syms:
        print()
        print("【共识标的】（被多组参数同时推荐）:")
        freq = Counter(all_syms).most_common(10)
        for sym, cnt in freq:
            name = engine._get_name(sym)
            print(f"  {sym} {name}: {cnt} 次 / {len(params_all)} 组")

    # 导出
    report_df.to_csv(out_dir / "performance.csv", index=False, encoding="utf-8-sig")
    signal_df.to_csv(out_dir / "signals.csv", index=False, encoding="utf-8-sig")

    # 合并一份完整报告 txt
    report_path = out_dir / "daily_report.txt"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(f"ETF 动量策略日报 — {today_str}\n")
        f.write(f"数据截止: {data_end}\n")
        f.write(f"ETF 数量: {panel.shape[1]}\n")
        f.write(f"参数组合: 12年Top{TOP_N} + 3年Top{TOP_N}，去重后 {len(params_all)} 组\n")
        f.write("=" * 70 + "\n\n")

        f.write("一、回测绩效\n\n")
        f.write(report_df.to_string(index=False, float_format="%.4f"))
        f.write("\n\n")

        f.write(f"二、当前持仓建议（信号日: {sig_date}，T+1 执行）\n\n")
        for _, r in signal_df.iterrows():
            action_ch = "买入" if r["操作"] == "buy" else "清仓"
            f.write(f"  {r['参数']:<30}  [{action_ch}]  {r['持仓标的']}\n")

        if all_syms:
            f.write("\n三、共识标的\n\n")
            for sym, cnt in freq:
                name = engine._get_name(sym)
                f.write(f"  {sym} {name}: {cnt} 次 / {len(params_all)} 组\n")

    print()
    print("=" * 60)
    print(f"报告已保存到: {out_dir}/")
    print(f"  - daily_report.txt   (完整文本报告)")
    print(f"  - performance.csv    (回测绩效)")
    print(f"  - signals.csv        (当前信号)")
    print(f"  - backtest_12y.csv   (12年全量回测)")
    print(f"  - backtest_3y.csv    (3年回测)")
    print("=" * 60)


if __name__ == "__main__":
    main()
