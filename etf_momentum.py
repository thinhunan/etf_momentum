"""ETF 动量 + R² 策略引擎

提供两个核心 API:
  - back_history(): 生成指定参数组合的完整调仓明细 CSV
  - next():         根据最新数据给出当前应执行的操作
"""

import json
import os
import random
import time
import warnings
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd
import yfinance as yf

warnings.filterwarnings("ignore")


class EtfMomentum:
    """ETF 动量 + R² 策略"""

    def __init__(self, db_dir: str = "db", etf_json: str = "etf_all.json"):
        self._db_dir = Path(db_dir)
        self._etf_name_map = self._load_etf_names(etf_json)
        self._panel = self._load_panel()
        self._log_close = np.log(self._panel.replace(0, np.nan))
        self._daily_ret = self._panel.pct_change()
        self._linreg_cache: dict = {}

    # ------------------------------------------------------------------
    # 数据加载
    # ------------------------------------------------------------------

    @staticmethod
    def _load_etf_names(path: str) -> dict:
        p = Path(path)
        if not p.exists():
            return {}
        with open(p, encoding="utf-8") as f:
            data = json.load(f)
        return {item["symbol"]: item["name"] for item in data["data"]["list"]}

    def _load_panel(self) -> pd.DataFrame:
        closes = {}
        for fp in sorted(self._db_dir.glob("*.csv")):
            df = pd.read_csv(fp, index_col=0, parse_dates=True)
            if not df.empty and "Close" in df.columns and len(df) >= 30:
                closes[fp.stem] = df["Close"]
        panel = pd.DataFrame(closes).sort_index().ffill()
        return panel

    @staticmethod
    def _to_yf_symbol(raw: str) -> str:
        """SH510500 -> 510500.SS, SZ159941 -> 159941.SZ"""
        raw = raw.strip().upper()
        if raw.startswith("SH"):
            return raw[2:] + ".SS"
        if raw.startswith("SZ"):
            return raw[2:] + ".SZ"
        return raw

    def _csv_path(self, raw_symbol: str) -> Path:
        return self._db_dir / f"{raw_symbol.strip().upper()}.csv"

    def reload_data(self) -> None:
        """重新加载 db 数据并刷新缓存（更新后调用）。"""
        self._panel = self._load_panel()
        self._log_close = np.log(self._panel.replace(0, np.nan))
        self._daily_ret = self._panel.pct_change()
        self._linreg_cache = {}

    def update_db_incremental(
        self,
        symbols: Optional[List[str]] = None,
        proxy: Optional[str] = None,
        max_rounds: int = 10,
        sleep_min: float = 1.0,
        sleep_max: float = 2.0,
        period_for_new: str = "12y",
        reload_after: bool = True,
    ) -> dict:
        """增量更新本地 db 目录下的数据（避免每次全量下载）。

        - 若本地已有 CSV：从最后日期 +1 天开始下载并追加
        - 若本地没有 CSV：下载 period_for_new 对应的全量数据并写入
        - 若遇到限流/网络错误：按“轮次”重试失败标的，最多 max_rounds 轮
        """
        if proxy:
            os.environ["HTTP_PROXY"] = proxy
            os.environ["HTTPS_PROXY"] = proxy

        if symbols is None:
            symbols = sorted([fp.stem for fp in self._db_dir.glob("*.csv")])

        remaining = [s.strip().upper() for s in symbols]
        ok: set[str] = set()
        last_errors: dict[str, str] = {}

        for round_idx in range(1, max_rounds + 1):
            if not remaining:
                break

            failed: list[str] = []

            for sym in remaining:
                try:
                    self._update_one_symbol(sym, period_for_new=period_for_new)
                    ok.add(sym)
                except Exception as e:
                    last_errors[sym] = str(e)
                    failed.append(sym)

                time.sleep(random.uniform(sleep_min, sleep_max))

            remaining = failed

            if remaining:
                # 每轮失败后稍作等待再继续，缓解限流
                backoff = min(60, 2 ** round_idx)
                time.sleep(backoff)

        summary = {
            "ok": len(ok),
            "fail": len(remaining),
            "failed_symbols": remaining,
            "last_errors": {k: last_errors[k] for k in remaining},
        }

        if reload_after:
            self.reload_data()

        if remaining:
            raise RuntimeError(f"增量更新仍失败（共 {len(remaining)} 只）: {remaining[:10]}")

        return summary

    def _update_one_symbol(self, raw_symbol: str, period_for_new: str = "12y") -> None:
        raw_symbol = raw_symbol.strip().upper()
        fp = self._csv_path(raw_symbol)
        yf_sym = self._to_yf_symbol(raw_symbol)

        existing = None
        start = None
        # yfinance 的 end 为左闭右开，要包含“今天”需传 end=明天
        end_exclusive = (datetime.now().date() + timedelta(days=1)).strftime("%Y-%m-%d")

        if fp.exists():
            existing = pd.read_csv(fp, index_col=0, parse_dates=True)
            if not existing.empty:
                last_date = existing.index.max()
                start = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
                if start >= end_exclusive:
                    return

        if start:
            hist = yf.Ticker(yf_sym).history(start=start, end=end_exclusive)
        else:
            hist = yf.Ticker(yf_sym).history(period=period_for_new)

        if hist is None or hist.empty:
            # 无新增也算成功（可能是停牌/无交易/刚好到最新）
            return

        keep_cols = ["Open", "High", "Low", "Close", "Volume"]
        hist = hist[[c for c in keep_cols if c in hist.columns]].copy()
        if hist.index.tz is not None:
            hist.index = hist.index.tz_localize(None)
        hist.index.name = "Date"

        if existing is not None and not existing.empty:
            combined = pd.concat([existing, hist])
            combined = combined[~combined.index.duplicated(keep="last")]
            combined.sort_index(inplace=True)
            combined.to_csv(fp)
        else:
            hist.to_csv(fp)

    def _get_name(self, symbol: str) -> str:
        return self._etf_name_map.get(symbol, "")

    # ------------------------------------------------------------------
    # 滚动线性回归（向量化）
    # ------------------------------------------------------------------

    def _rolling_linreg(self, n: int):
        if n in self._linreg_cache:
            return self._linreg_cache[n]

        log_close = self._log_close
        sum_x = n * (n - 1) / 2
        sum_x2 = n * (n - 1) * (2 * n - 1) / 6
        denom_x = n * sum_x2 - sum_x ** 2
        weights = np.arange(n, dtype=float)

        roll_sum_y = log_close.rolling(n).sum()
        roll_sum_y2 = (log_close ** 2).rolling(n).sum()
        roll_sum_xy = log_close.rolling(n).apply(
            lambda w: np.dot(weights, w), raw=True
        )

        slope = (n * roll_sum_xy - sum_x * roll_sum_y) / denom_x
        ss_tot = n * roll_sum_y2 - roll_sum_y ** 2
        ss_reg = (n * roll_sum_xy - sum_x * roll_sum_y) ** 2 / denom_x
        r2 = (ss_reg / ss_tot.replace(0, np.nan)).clip(0, 1)

        self._linreg_cache[n] = (slope, r2)
        return slope, r2

    # ------------------------------------------------------------------
    # 选股逻辑（单次）
    # ------------------------------------------------------------------

    def _select(
        self, date: pd.Timestamp, n: int, r2_threshold: float, top_k: int
    ) -> List[dict]:
        slope, r2 = self._rolling_linreg(n)
        s = slope.loc[date].dropna()
        r = r2.loc[date].dropna()
        common = s.index.intersection(r.index)
        s, r = s[common], r[common]

        candidates = s[r >= r2_threshold].sort_values(ascending=False)
        selected = candidates.head(top_k) if len(candidates) >= 1 else pd.Series(dtype=float)

        result = []
        for sym, slp in selected.items():
            result.append(
                {
                    "symbol": sym,
                    "name": self._get_name(sym),
                    "slope": slp,
                    "r2": r[sym],
                    "price": self._panel.loc[date, sym],
                }
            )
        return result

    # ------------------------------------------------------------------
    # API 1: back_history
    # ------------------------------------------------------------------

    def back_history(
        self,
        n: int = 5,
        r2_threshold: float = 0.6,
        rebal_period: int = 5,
        top_k: int = 1,
        output_dir: str = ".",
    ) -> pd.DataFrame:
        """生成完整调仓明细并导出 CSV。

        返回 DataFrame，同时写入
        history_{n}_{r2}_{rebal}_{topk}_{date}.csv
        """
        slope, r2 = self._rolling_linreg(n)
        daily_ret = self._daily_ret

        start_idx = n + 5
        dates = daily_ret.index[start_idx:]
        rebal_indices = list(range(0, len(dates), rebal_period))

        trades = []
        cum = 1.0

        for i, idx_pos in enumerate(rebal_indices):
            date = dates[idx_pos]
            s = slope.loc[date].dropna()
            r = r2.loc[date].dropna()
            common = s.index.intersection(r.index)
            s, r = s[common], r[common]

            candidates = s[r >= r2_threshold].sort_values(ascending=False)
            selected = (
                candidates.head(top_k).index.tolist()
                if len(candidates) >= 1
                else []
            )

            next_pos = (
                rebal_indices[i + 1] if i + 1 < len(rebal_indices) else len(dates)
            )
            hold_start = dates[idx_pos]
            hold_end = dates[min(next_pos, len(dates) - 1)]

            period_ret = 0.0
            for d in range(idx_pos, next_pos):
                dt = dates[d]
                if selected and dt in daily_ret.index:
                    ret = daily_ret.loc[dt, selected].mean()
                    if np.isnan(ret):
                        ret = 0.0
                else:
                    ret = 0.0
                cum *= 1 + ret
                period_ret = (1 + period_ret) * (1 + ret) - 1

            symbols = ",".join(selected) if selected else "空仓"
            names = (
                ",".join(self._get_name(s) for s in selected)
                if selected
                else "-"
            )

            if selected:
                start_prices = [self._panel.loc[hold_start, s] for s in selected]
                end_prices = [self._panel.loc[hold_end, s] for s in selected]
                sp_str = ",".join(f"{p:.4f}" for p in start_prices)
                ep_str = ",".join(f"{p:.4f}" for p in end_prices)
            else:
                sp_str = "-"
                ep_str = "-"

            trades.append(
                {
                    "标的": symbols,
                    "名称": names,
                    "开始日期": hold_start.strftime("%Y-%m-%d"),
                    "开始价格": sp_str,
                    "结束日期": hold_end.strftime("%Y-%m-%d"),
                    "结束价格": ep_str,
                    "本次收益": f"{period_ret:.2%}",
                    "总净值": round(cum, 4),
                }
            )

        trade_df = pd.DataFrame(trades)

        today_str = datetime.now().strftime("%Y%m%d")
        out_dir = Path(output_dir) / "history" / today_str
        out_dir.mkdir(parents=True, exist_ok=True)
        filename = f"history_{n}_{r2_threshold}_{rebal_period}_{top_k}.csv"
        out_path = out_dir / filename
        trade_df.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"已导出 {out_path}（共 {len(trade_df)} 次调仓）")

        return trade_df

    # ------------------------------------------------------------------
    # API 2: next
    # ------------------------------------------------------------------

    def next(
        self,
        n: int = 5,
        r2_threshold: float = 0.6,
        top_k: int = 1,
    ) -> dict:
        """根据最新数据给出当前操作建议。

        Returns:
            {
                "date": "2025-12-31",
                "action": "buy" | "clear",
                "holdings": [
                    {"symbol": "SH510050", "name": "50ETF",
                     "slope": 0.012, "r2": 0.85, "price": 3.45},
                    ...
                ],
            }
        """
        latest_date = self._panel.index[-1]
        picks = self._select(latest_date, n, r2_threshold, top_k)

        if picks:
            action = "buy"
        else:
            action = "clear"

        return {
            "date": latest_date.strftime("%Y-%m-%d"),
            "action": action,
            "holdings": picks,
        }


# ----------------------------------------------------------------------
# 命令行快捷入口
# ----------------------------------------------------------------------
if __name__ == "__main__":
    engine = EtfMomentum()

    print("=" * 60)
    print("back_history(n=5, r2=0.6, rebal=5, top_k=1)")
    print("=" * 60)
    df = engine.back_history(n=5, r2_threshold=0.6, rebal_period=5, top_k=1)
    print(df.tail(10))
    print()

    print("=" * 60)
    print("next(n=5, r2=0.6, top_k=1)")
    print("=" * 60)
    signal = engine.next(n=5, r2_threshold=0.6, top_k=1)
    print(f"日期: {signal['date']}")
    print(f"操作: {signal['action']}")
    for h in signal["holdings"]:
        print(f"  {h['symbol']} {h['name']}  斜率={h['slope']:.6f}  R²={h['r2']:.4f}  价格={h['price']:.4f}")
