#!/usr/bin/env python3
"""
排查 n=30, R²≥0.3, T=30, top_k=1 在 2014 年 398.88% 异常收益：
复现该参数在 2014 年的调仓与持仓，找出异常收益来自哪只 ETF/哪段区间，并检查原始日线是否有异常涨跌幅。

结论: 高收益来自 SH510880（红利ETF华泰柏瑞），两段持仓区间收益约 +60.5%、+113%。
      经确认 akshare 数据正确，该标的 2014 年涨幅属实，非数据错误。
"""
import numpy as np
import pandas as pd
from pathlib import Path

DB_DIR = Path("db")

# 与 notebook 一致的回归与回测逻辑（精简版）
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


def main():
    # 1. 加载数据
    closes = {}
    opens = {}
    for fp in sorted(DB_DIR.glob("*.csv")):
        df = pd.read_csv(fp, index_col=0, parse_dates=True)
        if not df.empty and "Close" in df.columns and len(df) >= 30:
            closes[fp.stem] = df["Close"]
            if "Open" in df.columns:
                opens[fp.stem] = df["Open"]
    panel = pd.DataFrame(closes).sort_index().ffill()
    panel_open = pd.DataFrame(opens).sort_index().ffill()
    daily_ret = panel.pct_change()
    log_close = np.log(panel.replace(0, np.nan))

    n, r2_thresh, rebal, top_k = 30, 0.3, 30, 1
    slope, r2 = rolling_linreg(log_close, n)
    start_idx = n + 5
    dates = daily_ret.index[start_idx:]
    rebal_indices = list(range(0, len(dates), rebal))

    # 2. 遍历 2014 年的调仓，记录每期持仓与区间收益
    SLIPPAGE = 0.001
    COMMISSION = 0.00006
    trades_2014 = []
    prev_selected = []
    cum = 1.0

    for i, idx_pos in enumerate(rebal_indices):
        date = dates[idx_pos]
        if date.year != 2014 and date.year > 2014:
            break
        if date.year < 2014:
            # 仍要更新 cum 以便 2014 年初净值正确
            s = slope.loc[date].dropna()
            r = r2.loc[date].dropna()
            common = s.index.intersection(r.index)
            s, r = s[common], r[common]
            candidates = s[r >= r2_thresh].sort_values(ascending=False)
            selected = candidates.head(top_k).index.tolist() if len(candidates) >= 1 else []
            exec_idx = idx_pos + 1
            if exec_idx < len(dates):
                exec_date = dates[exec_idx]
                next_signal_idx = rebal_indices[i + 1] if i + 1 < len(rebal_indices) else len(dates) - 1
                next_exec_idx = next_signal_idx + 1
                changed = sorted(selected) != sorted(prev_selected)
                if changed:
                    if prev_selected:
                        cum *= 1 - COMMISSION
                    if selected:
                        cum *= 1 - COMMISSION
                if selected:
                    buy_prices = {}
                    for sym in selected:
                        op = panel_open.loc[exec_date, sym] if (exec_date in panel_open.index and sym in panel_open.columns) else panel.loc[exec_date, sym]
                        buy_prices[sym] = op * (1 + SLIPPAGE)
                    if next_exec_idx < len(dates):
                        sell_date = dates[next_exec_idx]
                        sell_prices = {}
                        for sym in selected:
                            op = panel_open.loc[sell_date, sym] if (sell_date in panel_open.index and sym in panel_open.columns) else panel.loc[sell_date, sym]
                            sell_prices[sym] = op * (1 - SLIPPAGE)
                        per_ret = np.mean([sell_prices[s] / buy_prices[s] - 1 for s in selected])
                    else:
                        sell_date = dates[-1]
                        per_ret = np.mean([panel.loc[sell_date, s] / buy_prices[s] - 1 for s in selected])
                    cum *= 1 + per_ret
                prev_selected = selected
            continue

        # 2014 年：记录
        s = slope.loc[date].dropna()
        r = r2.loc[date].dropna()
        common = s.index.intersection(r.index)
        s, r = s[common], r[common]
        candidates = s[r >= r2_thresh].sort_values(ascending=False)
        selected = candidates.head(top_k).index.tolist() if len(candidates) >= 1 else []

        exec_idx = idx_pos + 1
        if exec_idx >= len(dates):
            break
        next_signal_idx = rebal_indices[i + 1] if i + 1 < len(rebal_indices) else len(dates) - 1
        next_exec_idx = next_signal_idx + 1
        signal_date = dates[idx_pos]
        exec_date = dates[exec_idx]
        changed = sorted(selected) != sorted(prev_selected)
        trade_cost = 0.0
        if changed:
            if prev_selected:
                trade_cost += COMMISSION
            if selected:
                trade_cost += COMMISSION
            cum *= 1 - trade_cost

        period_ret = 0.0
        sell_date = None
        if selected:
            buy_prices = {}
            for sym in selected:
                op = panel_open.loc[exec_date, sym] if (exec_date in panel_open.index and sym in panel_open.columns) else panel.loc[exec_date, sym]
                buy_prices[sym] = op * (1 + SLIPPAGE)
            if next_exec_idx < len(dates):
                sell_date = dates[next_exec_idx]
                sell_prices = {}
                for sym in selected:
                    op = panel_open.loc[sell_date, sym] if (sell_date in panel_open.index and sym in panel_open.columns) else panel.loc[sell_date, sym]
                    sell_prices[sym] = op * (1 - SLIPPAGE)
                per_stock_rets = [sell_prices[s] / buy_prices[s] - 1 for s in selected]
                period_ret = np.mean(per_stock_rets)
            else:
                sell_date = dates[-1]
                period_ret = np.mean([panel.loc[sell_date, s] / buy_prices[s] - 1 for s in selected])
            cum *= 1 + period_ret
        else:
            sell_date = dates[min(next_signal_idx, len(dates) - 1)]

        prev_selected = selected
        trades_2014.append({
            "signal_date": signal_date,
            "exec_date": exec_date,
            "sell_date": sell_date,
            "holding": selected,
            "period_ret": period_ret,
            "cum_nav": cum,
        })

    # 3. 打印 2014 年每期持仓与区间收益
    print("=" * 80)
    print("n=30, R²≥0.3, T=30, top_k=1  在 2014 年的调仓与区间收益")
    print("=" * 80)
    for t in trades_2014:
        h = t["holding"]
        hstr = h[0] if h else "空仓"
        print(f"信号日 {t['signal_date'].strftime('%Y-%m-%d')} → 执行 {t['exec_date'].strftime('%Y-%m-%d')} 至 卖出 {t['sell_date'].strftime('%Y-%m-%d')}  持仓: {hstr}  区间收益: {t['period_ret']:.2%}  累计净值: {t['cum_nav']:.4f}")

    # 4. 找出区间收益异常的那一期（例如 >50%）
    abnormal = [t for t in trades_2014 if abs(t["period_ret"]) > 0.5]
    if not abnormal:
        abnormal = [t for t in trades_2014 if abs(t["period_ret"]) > 0.2]
    print()
    print("区间收益异常（>20%）的期数:")
    for t in abnormal:
        h = t["holding"]
        print(f"  {t['signal_date'].strftime('%Y-%m-%d')} ~ {t['sell_date'].strftime('%Y-%m-%d')}  持仓 {h}  区间收益 {t['period_ret']:.2%}")

    # 5. 对这些持仓 ETF，检查该区间内是否有单日收益率异常（>15%）
    print()
    print("=" * 80)
    print("上述持仓在对应区间内的日收益率检查（|日收益|>15% 视为可疑）")
    print("=" * 80)
    for t in abnormal:
        for sym in t["holding"]:
            start_d = t["exec_date"]
            end_d = t["sell_date"]
            if sym not in daily_ret.columns:
                continue
            ret_series = daily_ret.loc[start_d:end_d, sym].dropna()
            extreme = ret_series[ret_series.abs() > 0.15]
            if len(extreme) > 0:
                print(f"\n  {sym}  在 {start_d.strftime('%Y-%m-%d')} ~ {end_d.strftime('%Y-%m-%d')}:")
                for d in extreme.index:
                    print(f"     {d.strftime('%Y-%m-%d')}  日收益 {ret_series.loc[d]:.2%}")
                # 打印该区间前后几天的 close，便于核对
                sub = panel.loc[start_d:end_d, sym]
                print(f"     该区间收盘价: 首 {sub.iloc[0]:.4f}  末 {sub.iloc[-1]:.4f}  区间涨跌 {(sub.iloc[-1]/sub.iloc[0]-1):.2%}")

    # 6. 全库 2014 年单日收益 >20% 的汇总（可能的数据错误）
    print()
    print("=" * 80)
    print("全库 2014 年单日收益率 |ret| > 20% 的纪录（疑似数据错误）")
    print("=" * 80)
    dr_2014 = daily_ret.loc[daily_ret.index.year == 2014]
    for sym in dr_2014.columns:
        r = dr_2014[sym].dropna()
        bad = r[r.abs() > 0.20]
        if len(bad) > 0:
            for d in bad.index:
                print(f"  {sym}  {d.strftime('%Y-%m-%d')}  日收益 {bad.loc[d]:.2%}")


if __name__ == "__main__":
    main()
