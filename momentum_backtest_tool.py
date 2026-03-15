# -*- coding: utf-8 -*-
"""
ETF 动量 + R² 策略回测工具模块。
策略逻辑：每隔 rebal_period 个交易日调仓，用过去 n 天 log(收盘价) 线性回归的斜率衡量动量、R² 衡量趋势质量，
仅保留 R² ≥ 阈值的标的，选斜率最大的 top_k 只等权持有。
"""
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

# ══════════════════════════════════════════════════════════════════════
# 交易成本参数
# ══════════════════════════════════════════════════════════════════════
SLIPPAGE = 0.001       # 滑点 0.1%
COMMISSION = 0.00006   # 单边手续费 0.006%


def load_panel(db_dir: Path, min_days: int = 30) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """从 db_dir 加载所有 ETF 的 Close/Open，返回 (panel, panel_open)。"""
    db_dir = Path(db_dir)
    closes = {}
    opens = {}
    for fp in sorted(db_dir.glob("*.csv")):
        df = pd.read_csv(fp, index_col=0, parse_dates=True)
        if not df.empty and "Close" in df.columns and len(df) >= min_days:
            closes[fp.stem] = df["Close"]
            if "Open" in df.columns:
                opens[fp.stem] = df["Open"]
    panel = pd.DataFrame(closes).sort_index().ffill()
    panel_open = pd.DataFrame(opens).sort_index().ffill()
    return panel, panel_open


def rolling_linreg(log_close: pd.DataFrame, n: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    对 log(close) 面板做滚动 n 日线性回归，返回 (slope, R²) 两个面板。
    slope > 0 表示上涨趋势，R² 接近 1 表示趋势线性稳定。
    """
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
    r2 = ss_reg / ss_tot.replace(0, np.nan)
    r2 = r2.clip(0, 1)
    return slope, r2


def precompute_linreg(
    log_close: pd.DataFrame,
    n_list: List[int],
    cache: Optional[Dict[int, Tuple[pd.DataFrame, pd.DataFrame]]] = None,
    verbose: bool = True,
) -> Dict[int, Tuple[pd.DataFrame, pd.DataFrame]]:
    """对所有 n 值预计算 slope / R²，返回/更新 linreg_cache。"""
    if cache is None:
        cache = {}
    for n in n_list:
        if n not in cache:
            cache[n] = rolling_linreg(log_close, n)
            if verbose:
                print(f"  预计算 rolling_linreg(n={n}) 完成")
    return cache


def backtest_momentum(
    slope: pd.DataFrame,
    r2: pd.DataFrame,
    daily_ret: pd.DataFrame,
    panel: pd.DataFrame,
    panel_open: pd.DataFrame,
    n: int,
    r2_threshold: float,
    rebal_period: int,
    top_k: int = 1,
    slippage: float = SLIPPAGE,
    commission: float = COMMISSION,
    take_profit: Optional[float] = None,
    max_dd_stop: Optional[float] = None,
) -> pd.Series:
    """
    基于预计算的 slope/R² 运行回测，返回每日净值序列。
    take_profit: 单笔止盈阈值（如 0.2=20%），None=不限。
    max_dd_stop: 单笔回撤止损阈值（如 0.1=10%），None=不限。
    """
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
    entry_prices = {}
    position_alive = {}
    position_peak = {}

    for d in range(len(dates)):
        date = dates[d]

        if d in exec_map:
            new_holdings = exec_map[d]
            any_dead = any(not position_alive.get(s, True) for s in current_holdings)
            positions_changed = (
                sorted(new_holdings) != sorted(current_holdings) or any_dead
            )

            if positions_changed:
                if current_holdings and d > 0:
                    prev_date = dates[d - 1]
                    alive_old = [s for s in current_holdings if position_alive.get(s, True)]
                    k_sell = len(alive_old)
                    if k_sell > 0:
                        sell_rets = []
                        for sym in alive_old:
                            prev_cl = panel.loc[prev_date, sym]
                            op = (
                                panel_open.loc[date, sym]
                                if (date in panel_open.index and sym in panel_open.columns)
                                else panel.loc[date, sym]
                            )
                            sell_price = op * (1 - slippage)
                            sell_rets.append(sell_price / prev_cl - 1)
                        portfolio_sell_ret = np.nansum(sell_rets) / top_k
                        cum *= 1 + portfolio_sell_ret
                        cum *= 1 - k_sell / top_k * commission

                entry_prices = {}
                position_peak = {}
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
                        buy_price = op * (1 + slippage)
                        cl = panel.loc[date, sym]
                        entry_prices[sym] = buy_price
                        position_peak[sym] = cl
                        buy_rets.append(cl / buy_price - 1)
                    portfolio_buy_ret = np.nansum(buy_rets) / top_k
                    cum *= 1 + portfolio_buy_ret

                current_holdings = new_holdings
                position_alive = {s: True for s in new_holdings}
            else:
                if current_holdings:
                    alive_syms = [s for s in current_holdings if position_alive.get(s, True)]
                    if alive_syms:
                        rets = daily_ret.loc[date, alive_syms]
                        portfolio_ret = np.nansum(rets.values) / top_k
                        cum *= 1 + portfolio_ret
        else:
            if current_holdings:
                alive_syms = [s for s in current_holdings if position_alive.get(s, True)]
                if alive_syms:
                    rets = daily_ret.loc[date, alive_syms]
                    portfolio_ret = np.nansum(rets.values) / top_k
                    cum *= 1 + portfolio_ret

        if take_profit is not None and current_holdings:
            for sym in current_holdings:
                if position_alive.get(sym, False) and sym in entry_prices:
                    current_price = panel.loc[date, sym]
                    if current_price / entry_prices[sym] - 1 >= take_profit:
                        position_alive[sym] = False
                        cum *= 1 - 1 / top_k * commission

        if max_dd_stop is not None and current_holdings:
            for sym in current_holdings:
                if position_alive.get(sym, False) and sym in position_peak:
                    current_price = panel.loc[date, sym]
                    if current_price > position_peak[sym]:
                        position_peak[sym] = current_price
                    if 1 - current_price / position_peak[sym] >= max_dd_stop:
                        position_alive[sym] = False
                        cum *= 1 - 1 / top_k * commission

        nav_values[d] = cum

    return pd.Series(nav_values, index=dates, name="NAV")


def backtest_momentum_record_trades(
    slope: pd.DataFrame,
    r2: pd.DataFrame,
    daily_ret: pd.DataFrame,
    panel: pd.DataFrame,
    panel_open: pd.DataFrame,
    n: int,
    r2_threshold: float,
    rebal_period: int,
    top_k: int = 1,
    slippage: float = SLIPPAGE,
    commission: float = COMMISSION,
) -> Tuple[pd.Series, List[List[Dict[str, Any]]]]:
    """与 backtest_momentum 相同逻辑但不设 TP/SL，并记录每笔交易的 path（ratio=价格/入场价）。返回 (nav, periods)。"""
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
    entry_prices = {}
    position_paths = {}
    periods = []

    for d in range(len(dates)):
        date = dates[d]

        if d in exec_map:
            new_holdings = exec_map[d]
            positions_changed = sorted(new_holdings) != sorted(current_holdings)

            if positions_changed:
                if current_holdings and d > 0:
                    prev_date = dates[d - 1]
                    period_trades = []
                    for sym in current_holdings:
                        if sym not in entry_prices:
                            continue
                        op = (
                            panel_open.loc[date, sym]
                            if (date in panel_open.index and sym in panel_open.columns)
                            else panel.loc[date, sym]
                        )
                        sell_price = op * (1 - slippage)
                        path = position_paths.get(sym, [])
                        path.append((date, sell_price / entry_prices[sym]))
                        period_trades.append({"sym": sym, "path": path})
                    if period_trades:
                        periods.append(period_trades)
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
                    portfolio_sell_ret = np.nansum(sell_rets) / top_k
                    cum *= 1 + portfolio_sell_ret
                    cum *= 1 - k_sell / top_k * commission

                entry_prices = {}
                position_paths = {}
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
                        buy_price = op * (1 + slippage)
                        cl = panel.loc[date, sym]
                        entry_prices[sym] = buy_price
                        position_paths[sym] = [(date, cl / buy_price)]
                        buy_rets.append(cl / buy_price - 1)
                    portfolio_buy_ret = np.nansum(buy_rets) / top_k
                    cum *= 1 + portfolio_buy_ret
                current_holdings = new_holdings
            else:
                if current_holdings:
                    rets = daily_ret.loc[date, current_holdings]
                    portfolio_ret = np.nansum(rets.values) / top_k
                    cum *= 1 + portfolio_ret
                    for sym in current_holdings:
                        if sym in entry_prices:
                            ratio = panel.loc[date, sym] / entry_prices[sym]
                            position_paths.setdefault(sym, []).append((date, ratio))
        else:
            if current_holdings:
                rets = daily_ret.loc[date, current_holdings]
                portfolio_ret = np.nansum(rets.values) / top_k
                cum *= 1 + portfolio_ret
                for sym in current_holdings:
                    if sym in entry_prices:
                        ratio = panel.loc[date, sym] / entry_prices[sym]
                        position_paths.setdefault(sym, []).append((date, ratio))

        nav_values[d] = cum

    return pd.Series(nav_values, index=dates, name="NAV"), periods


def apply_tp_sl_to_periods(
    periods: List[List[Dict[str, Any]]],
    take_profit: Optional[float],
    max_dd_stop: Optional[float],
    top_k: int,
) -> List[float]:
    """对每笔 path 模拟 TP/SL，得到每期有效收益。"""
    period_rets = []
    for period in periods:
        if not period:
            period_rets.append(0.0)
            continue
        eff_rets = []
        for t in period:
            path = t["path"]
            if len(path) < 2:
                eff_rets.append(path[-1][1] - 1.0 if path else 0.0)
                continue
            peak = path[0][1]
            exit_ratio = None
            for _, ratio in path:
                if ratio > peak:
                    peak = ratio
                if take_profit is not None and ratio >= 1 + take_profit:
                    exit_ratio = 1 + take_profit
                    break
                if (
                    max_dd_stop is not None
                    and peak > 0
                    and (peak - ratio) / peak >= max_dd_stop
                ):
                    exit_ratio = ratio
                    break
            if exit_ratio is None:
                exit_ratio = path[-1][1]
            eff_rets.append(exit_ratio - 1.0)
        period_rets.append(np.nanmean(eff_rets) if eff_rets else 0.0)
    return period_rets


def period_returns_to_nav(
    dates: pd.DatetimeIndex, rebal_period: int, period_rets: List[float]
) -> pd.Series:
    """将每期收益按调仓日对齐，还原为每日净值（期内等权复利）。"""
    rebal_indices = list(range(0, len(dates), rebal_period))
    exec_indices = [i + 1 for i in rebal_indices if i + 1 < len(dates)]
    if not exec_indices:
        return pd.Series(np.ones(len(dates)), index=dates, name="NAV")
    nav = np.ones(len(dates))
    cum = 1.0
    for p, start_d in enumerate(exec_indices):
        end_d = exec_indices[p + 1] if p + 1 < len(exec_indices) else len(dates)
        r = period_rets[p] if p < len(period_rets) else 0.0
        n_days = max(1, end_d - start_d)
        for j in range(start_d, end_d):
            cum *= (1 + r) ** (1.0 / n_days)
            nav[j] = cum
        cum = nav[end_d - 1]
    return pd.Series(nav, index=dates, name="NAV")


def compute_metrics(
    nav: pd.Series,
    year_col_prefix: str = "",
    include_last_3y: bool = True,
) -> Dict[str, Any]:
    """
    根据净值序列计算总收益、年化、最大回撤、Sharpe、各年年收益等。
    year_col_prefix: 年化/总收益等键前缀，如 '12y' 则得到 annualized_12y, total_ret_12y 等。
    """
    out = {}
    years = sorted(nav.index.year.unique())
    annual_rets = {}
    annual_max_dd = {}
    for yr in years:
        yr_nav = nav[nav.index.year == yr]
        if len(yr_nav) < 10:
            continue
        annual_rets[yr] = yr_nav.iloc[-1] / yr_nav.iloc[0] - 1
        annual_max_dd[yr] = ((yr_nav / yr_nav.cummax()) - 1).min()

    for yr in years:
        if yr in annual_rets:
            out[yr] = annual_rets[yr]
        if yr in annual_max_dd:
            out[f"max_dd_{yr}"] = annual_max_dd[yr]

    total_ret = nav.iloc[-1] / nav.iloc[0] - 1
    n_years = (nav.index[-1] - nav.index[0]).days / 365.25
    annualized = (1 + total_ret) ** (1 / n_years) - 1 if n_years > 0 else np.nan
    max_dd = ((nav / nav.cummax()) - 1).min()
    dr = nav.pct_change().dropna()
    sharpe = (dr.mean() / dr.std() * np.sqrt(252)) if dr.std() > 0 else np.nan

    suffix = f"_{year_col_prefix}" if year_col_prefix else ""
    out["total_ret" + suffix] = total_ret
    out["annualized" + suffix] = annualized
    out["max_drawdown" + suffix] = max_dd
    out["sharpe" + suffix] = sharpe

    if include_last_3y and len(years) >= 3:
        last_3_years = sorted(years)[-3:]
        nav_3y = nav[nav.index.year.isin(last_3_years)]
        if len(nav_3y) >= 10:
            total_ret_3y = nav_3y.iloc[-1] / nav_3y.iloc[0] - 1
            n_years_3 = (nav_3y.index[-1] - nav_3y.index[0]).days / 365.25
            annualized_3y = (
                (1 + total_ret_3y) ** (1 / n_years_3) - 1 if n_years_3 > 0 else np.nan
            )
            max_drawdown_3y = ((nav_3y / nav_3y.cummax()) - 1).min()
            dr_3y = nav_3y.pct_change().dropna()
            sharpe_3y = (
                (dr_3y.mean() / dr_3y.std() * np.sqrt(252))
                if dr_3y.std() > 0
                else np.nan
            )
            for yr in last_3_years:
                out[f"y{yr}_3y"] = annual_rets.get(yr, np.nan)
            out["annualized_3y"] = annualized_3y
            out["total_ret_3y"] = total_ret_3y
            out["sharpe_3y"] = sharpe_3y
            out["max_drawdown_3y"] = max_drawdown_3y
        else:
            for yr in last_3_years:
                out[f"y{yr}_3y"] = np.nan
            out["annualized_3y"] = out["total_ret_3y"] = out["sharpe_3y"] = out[
                "max_drawdown_3y"
            ] = np.nan
    return out


def nav_key_from_row(row: pd.Series, tp_str: str = "inf", sl_str: str = "inf") -> str:
    """从结果行生成 nav_cache 的 key。"""
    n = int(row["n"])
    r2 = round(float(row["R2_threshold"]), 4)
    rebal = int(row["rebal_period"])
    top_k = int(row["top_k"])
    tp = row.get("take_profit", None)
    sl = row.get("max_dd_stop", None)
    tp_s = "inf" if (pd.isna(tp) or tp is None) else str(tp)
    sl_s = "inf" if (pd.isna(sl) or sl is None) else str(sl)
    return f"n={n}_R2={r2}_rebal={rebal}_topk={top_k}_tp={tp_s}_sl={sl_s}"


def find_nav(
    cache: Dict[str, pd.Series],
    row: pd.Series,
    period: str = "full",
    year_list: Optional[List[int]] = None,
) -> Optional[pd.Series]:
    """从 nav_cache 中根据结果行找到对应净值序列。period='3y' 时截取最近3年并归一化。"""
    import re
    key = nav_key_from_row(row)
    nav = cache.get(key)
    if nav is None:
        n, r2 = int(row["n"]), round(float(row["R2_threshold"]), 4)
        rebal = int(row["rebal_period"])
        top_k = int(row["top_k"])
        tp = row.get("take_profit", None)
        sl = row.get("max_dd_stop", None)
        tp_s = "inf" if (pd.isna(tp) or tp is None) else str(tp)
        sl_s = "inf" if (pd.isna(sl) or sl is None) else str(sl)
        for k in cache:
            if (
                f"n={n}_" in k
                and f"_rebal={rebal}_" in k
                and f"_topk={top_k}" in k
                and f"_tp={tp_s}_" in k
                and f"_sl={sl_s}" in k
            ):
                m = re.search(r"R2=([\d.]+)", k)
                if m and abs(float(m.group(1)) - r2) < 1e-5:
                    nav = cache[k]
                    break
    if nav is None:
        return None
    if period == "3y" and len(nav) >= 10 and year_list:
        last_3 = sorted(year_list)[-3:]
        nav = nav[nav.index.year.isin(last_3)].copy()
        nav = nav / nav.iloc[0]
    return nav


def _param_diff_count(row_a: pd.Series, row_b: pd.Series, param_cols: List[str]) -> int:
    """两行在 param_cols 上不同取值的个数（NaN 与 None 视为相等）。"""
    diff = 0
    for c in param_cols:
        va, vb = row_a[c], row_b[c]
        if pd.isna(va) and pd.isna(vb):
            continue
        if pd.isna(va) or pd.isna(vb):
            diff += 1
            continue
        if va != vb:
            diff += 1
    return diff


def _rows_adjacent_one_param_diff(
    row_a: pd.Series, row_b: pd.Series, param_cols: List[str]
) -> bool:
    """两行是否仅在某一维参数上不同（其余一致）。"""
    return _param_diff_count(row_a, row_b, param_cols) == 1


def top5_diverse(
    result_df: pd.DataFrame,
    score_col: str,
    param_cols: List[str],
    top_k: int = 5,
    similarity_rtol: float = 0.03,
) -> pd.DataFrame:
    """
    从按 score_col 降序排列的结果中选取 Top5，若相邻几组参数成绩相近且只有一个参数有差别，
    则合并为只保留成绩最好的那一组，为 Top5 留出多样性。
    similarity_rtol: 认为「成绩相近」的相对误差，如 0.03 表示 3% 以内算相近。
    """
    param_cols = [c for c in param_cols if c in result_df.columns]
    df = result_df.sort_values(score_col, ascending=False).reset_index(drop=True)
    chosen = []
    used = set()
    for i in range(len(df)):
        if len(chosen) >= top_k:
            break
        if i in used:
            continue
        row_i = df.iloc[i]
        score_i = row_i[score_col]
        # 找与 i 相邻且成绩相近、且仅一个参数不同的所有行，合并为只保留 i（当前最优）
        merge_set = {i}
        for j in range(i + 1, len(df)):
            if j in used:
                continue
            row_j = df.iloc[j]
            score_j = row_j[score_col]
            if score_j <= 0 and score_i <= 0:
                rel_diff = abs(score_j - score_i)
            elif score_i == 0:
                rel_diff = 1.0
            else:
                rel_diff = abs(score_j - score_i) / abs(score_i)
            if rel_diff > similarity_rtol:
                break  # 成绩已经明显变差，不再合并
            if _rows_adjacent_one_param_diff(row_i, row_j, param_cols):
                merge_set.add(j)
                used.add(j)
        used.add(i)
        chosen.append(df.iloc[i])
    return pd.DataFrame(chosen)


def recency_weighted_return(
    row: pd.Series,
    year_columns: List[int],
    weights: Optional[List[float]] = None,
) -> float:
    """
    按由近到远对年收益加权，放大近期收益。默认权重：最近年权重大。
    例如 year_columns=[2024,2025,2026]，weights=[1,2,3] 表示 2026 权重 3，2025 权重 2，2024 权重 1。
    """
    if weights is None:
        weights = list(range(1, len(year_columns) + 1))  # 1,2,...,k 由远到近
    assert len(weights) == len(year_columns)
    total = 0.0
    wsum = 0.0
    for yr, w in zip(year_columns, weights):
        if yr in row.index:
            v = row[yr]
            if pd.notna(v):
                total += w * float(v)
                wsum += w
    return total / wsum if wsum > 0 else np.nan
