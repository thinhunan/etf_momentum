"""
ETF 历史数据管理器 —— 基于 yfinance 下载 etf_pool.csv 中的 ETF 日线数据，
按标的逐只存储到 db/ 目录，支持增量更新，适配动量策略回测需求。
"""

import os
import time
import random
import logging
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_POOL_CSV = BASE_DIR / "etf_pool.csv"
DEFAULT_DB_DIR = BASE_DIR / "db"

# 动量策略通常需要 12 个月回看 + 一定缓冲，默认拉 12 年
DEFAULT_PERIOD = "12y"


def _to_yf_symbol(raw: str) -> str:
    """SH510500 -> 510500.SS, SZ159941 -> 159941.SZ"""
    raw = raw.strip().upper()
    if raw.startswith("SH"):
        return raw[2:] + ".SS"
    if raw.startswith("SZ"):
        return raw[2:] + ".SZ"
    return raw


def _to_local_symbol(raw: str) -> str:
    """SH510500 -> SH510500（用作文件名，保持原始格式）"""
    return raw.strip().upper()


class EtfDataManager:
    """管理 ETF 池历史行情的下载与本地持久化。

    Parameters
    ----------
    pool_csv : str | Path
        ETF 候选池 CSV 路径，需包含 symbol 列（如 SH510500）。
    db_dir : str | Path
        本地存储目录，每只 ETF 一个 CSV 文件。
    proxy : str | None
        HTTP(S) 代理地址，例如 ``http://127.0.0.1:1087``。
    max_retries : int
        单只 ETF 下载失败时的最大重试次数。
    """

    def __init__(
        self,
        pool_csv: str | Path = DEFAULT_POOL_CSV,
        db_dir: str | Path = DEFAULT_DB_DIR,
        proxy: str | None = None,
        max_retries: int = 3,
    ):
        self.pool_csv = Path(pool_csv)
        self.db_dir = Path(db_dir)
        self.db_dir.mkdir(parents=True, exist_ok=True)
        self.max_retries = max_retries

        if proxy:
            os.environ["HTTP_PROXY"] = proxy
            os.environ["HTTPS_PROXY"] = proxy

    # ------------------------------------------------------------------
    # 加载 ETF 池
    # ------------------------------------------------------------------

    def load_pool(self) -> pd.DataFrame:
        """读取 etf_pool.csv，返回包含 symbol / name / type 等列的 DataFrame。

        CSV 中的大数值列可能包含千分位逗号（如 22,113,608,392.60），
        这里只提取前三列 (symbol, name, type) 以避免解析歧义。
        """
        rows = []
        with open(self.pool_csv, "r", encoding="utf-8") as f:
            header = f.readline()  # skip header
            for line in f:
                parts = line.strip().split(",")
                if len(parts) >= 3:
                    rows.append({"symbol": parts[0], "name": parts[1], "type": parts[2]})
        return pd.DataFrame(rows)

    def pool_symbols(self) -> list[str]:
        """返回原始 symbol 列表，如 ['SH510500', 'SZ159941', ...]。"""
        return self.load_pool()["symbol"].tolist()

    # ------------------------------------------------------------------
    # 下载单只 ETF
    # ------------------------------------------------------------------

    def _file_path(self, raw_symbol: str) -> Path:
        return self.db_dir / f"{_to_local_symbol(raw_symbol)}.csv"

    def _fetch_history(self, yf_sym: str, **kwargs) -> pd.DataFrame:
        """带指数退避重试的 yfinance 数据获取。"""
        last_err = None
        for attempt in range(1, self.max_retries + 1):
            try:
                hist = yf.Ticker(yf_sym).history(**kwargs)
                return hist
            except Exception as e:
                last_err = e
                wait = 2 ** attempt
                logger.debug(
                    "%s 第 %d 次尝试失败 (%s)，%ds 后重试",
                    yf_sym, attempt, e, wait,
                )
                time.sleep(wait)
        raise last_err  # type: ignore[misc]

    def download_one(
        self,
        raw_symbol: str,
        period: str = DEFAULT_PERIOD,
        incremental: bool = True,
    ) -> pd.DataFrame | None:
        """下载单只 ETF 的日线数据并保存。

        Parameters
        ----------
        raw_symbol : str
            原始代码，如 SH510500。
        period : str
            yfinance period 参数，如 '3y'、'5y'、'max'。
            当 incremental=True 且本地已有数据时，仅补齐最新数据。
        incremental : bool
            是否增量更新。若 True 且本地已有文件，则仅下载本地最后日期之后的数据。

        Returns
        -------
        pd.DataFrame | None
            下载到的数据（合并后），失败返回 None。
        """
        yf_sym = _to_yf_symbol(raw_symbol)
        file_path = self._file_path(raw_symbol)

        existing = None
        if incremental and file_path.exists():
            existing = pd.read_csv(file_path, index_col=0, parse_dates=True)
            if not existing.empty:
                last_date = existing.index.max()
                start = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
                end = datetime.now().strftime("%Y-%m-%d")
                if start >= end:
                    logger.debug("%s 已是最新，跳过", raw_symbol)
                    return existing
                try:
                    new_data = self._fetch_history(yf_sym, start=start, end=end)
                except Exception as e:
                    logger.warning("增量下载 %s 失败: %s", raw_symbol, e)
                    return existing
                if new_data.empty:
                    return existing
                new_data = self._clean(new_data)
                combined = pd.concat([existing, new_data])
                combined = combined[~combined.index.duplicated(keep="last")]
                combined.sort_index(inplace=True)
                combined.to_csv(file_path)
                return combined

        try:
            hist = self._fetch_history(yf_sym, period=period)
        except Exception as e:
            logger.warning("下载 %s 失败: %s", raw_symbol, e)
            return None

        if hist.empty or len(hist) < 5:
            logger.warning("%s 数据不足（%d 行），跳过", raw_symbol, len(hist))
            return None

        hist = self._clean(hist)
        hist.to_csv(file_path)
        return hist

    @staticmethod
    def _clean(df: pd.DataFrame) -> pd.DataFrame:
        """统一清洗 yfinance 返回的 DataFrame。"""
        keep_cols = ["Open", "High", "Low", "Close", "Volume"]
        df = df[[c for c in keep_cols if c in df.columns]].copy()
        if df.index.tz is not None:
            df.index = df.index.tz_localize(None)
        df.index.name = "Date"
        return df

    # ------------------------------------------------------------------
    # 批量下载
    # ------------------------------------------------------------------

    def download_all(
        self,
        period: str = DEFAULT_PERIOD,
        incremental: bool = True,
        sleep_min: float = 1.0,
        sleep_max: float = 2.0,
    ) -> dict[str, int]:
        """批量下载 ETF 池中所有标的。

        Parameters
        ----------
        period : str
            yfinance period（仅在全量下载时生效）。
        incremental : bool
            是否增量更新。
        sleep_min, sleep_max : float
            每下载一只 ETF 后，随机等待 [sleep_min, sleep_max] 秒再下载下一只，防止限流。

        Returns
        -------
        dict
            {'ok': 成功数, 'skip': 跳过数, 'fail': 失败数, 'failed_symbols': [...]}
        """
        symbols = self.pool_symbols()
        total = len(symbols)
        ok = skip = fail = 0
        failed_symbols: list[str] = []

        logger.info("开始下载，共 %d 只 ETF ...", total)

        for i, sym in enumerate(symbols, 1):
            file_path = self._file_path(sym)
            already_fresh = False
            if incremental and file_path.exists():
                existing = pd.read_csv(file_path, index_col=0, parse_dates=True)
                if not existing.empty:
                    last_date = existing.index.max()
                    if (datetime.now() - last_date).days <= 1:
                        skip += 1
                        logger.debug("[%d/%d] %s 已是最新，跳过", i, total, sym)
                        already_fresh = True

            if not already_fresh:
                result = self.download_one(sym, period=period, incremental=incremental)
                if result is not None:
                    ok += 1
                    logger.info(
                        "[%d/%d] %s 完成，共 %d 行", i, total, sym, len(result)
                    )
                else:
                    fail += 1
                    failed_symbols.append(sym)
                    logger.warning("[%d/%d] %s 失败", i, total, sym)
                # 每下载一只后随机间隔 1~2 秒再下载下一只
                time.sleep(random.uniform(sleep_min, sleep_max))

        summary = {
            "ok": ok,
            "skip": skip,
            "fail": fail,
            "failed_symbols": failed_symbols,
        }
        logger.info("下载完成: %s", summary)
        return summary

    # ------------------------------------------------------------------
    # 读取本地数据
    # ------------------------------------------------------------------

    def load_one(self, raw_symbol: str) -> pd.DataFrame | None:
        """从本地加载单只 ETF 的日线数据。"""
        fp = self._file_path(raw_symbol)
        if not fp.exists():
            return None
        df = pd.read_csv(fp, index_col=0, parse_dates=True)
        return df

    def load_all(self) -> dict[str, pd.DataFrame]:
        """加载本地 db 目录下所有 ETF 数据，返回 {raw_symbol: DataFrame}。"""
        result: dict[str, pd.DataFrame] = {}
        for fp in sorted(self.db_dir.glob("*.csv")):
            sym = fp.stem
            df = pd.read_csv(fp, index_col=0, parse_dates=True)
            if not df.empty:
                result[sym] = df
        return result

    def load_close_panel(self) -> pd.DataFrame:
        """将本地所有 ETF 的收盘价合并为面板 DataFrame（index=日期, columns=symbol）。"""
        all_data = self.load_all()
        closes = {sym: df["Close"] for sym, df in all_data.items() if "Close" in df.columns}
        panel = pd.DataFrame(closes)
        panel = panel.ffill().dropna(how="all")
        panel.index.name = "Date"
        return panel

    # ------------------------------------------------------------------
    # 辅助
    # ------------------------------------------------------------------

    def status(self) -> pd.DataFrame:
        """返回每只 ETF 的本地数据概览：最早日期、最新日期、行数。"""
        rows = []
        for sym in self.pool_symbols():
            fp = self._file_path(sym)
            if fp.exists():
                df = pd.read_csv(fp, index_col=0, parse_dates=True)
                rows.append({
                    "symbol": sym,
                    "rows": len(df),
                    "first_date": df.index.min().strftime("%Y-%m-%d") if not df.empty else None,
                    "last_date": df.index.max().strftime("%Y-%m-%d") if not df.empty else None,
                })
            else:
                rows.append({"symbol": sym, "rows": 0, "first_date": None, "last_date": None})
        return pd.DataFrame(rows)


# ------------------------------------------------------------------
# 命令行入口
# ------------------------------------------------------------------


def main():
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-5s %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="ETF 历史数据下载管理器")
    parser.add_argument("--pool", default=str(DEFAULT_POOL_CSV), help="ETF 池 CSV 路径")
    parser.add_argument("--db", default=str(DEFAULT_DB_DIR), help="本地存储目录")
    parser.add_argument("--proxy", default=None, help="HTTP(S) 代理，如 http://127.0.0.1:1087")
    parser.add_argument("--period", default=DEFAULT_PERIOD, help="下载周期（全量时生效），默认 12y")
    parser.add_argument("--full", action="store_true", help="全量下载（忽略本地已有数据）")
    parser.add_argument("--sleep-min", type=float, default=1.0, help="下载间隔下限（秒）")
    parser.add_argument("--sleep-max", type=float, default=2.0, help="下载间隔上限（秒）")
    parser.add_argument("--status", action="store_true", help="仅查看本地数据状态")
    args = parser.parse_args()

    mgr = EtfDataManager(pool_csv=args.pool, db_dir=args.db, proxy=args.proxy)

    if args.status:
        print(mgr.status().to_string(index=False))
        return

    result = mgr.download_all(
        period=args.period,
        incremental=not args.full,
        sleep_min=args.sleep_min,
        sleep_max=args.sleep_max,
    )
    print(f"\n下载完成: 成功 {result['ok']}, 跳过 {result['skip']}, 失败 {result['fail']}")
    if result["failed_symbols"]:
        print(f"失败标的: {result['failed_symbols']}")


if __name__ == "__main__":
    main()
