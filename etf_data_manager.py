"""
ETF 历史数据管理器 —— 支持多数据源下载 etf_pool.csv 中的 ETF 日线数据，
按标的逐只存储到 db/ 目录，支持增量更新，适配动量策略回测需求。

数据源: akshare(推荐) / efinance / eastmoney(东方财富直连) / yfinance / baostock(多仅 A 股)。
"""

import os
import time
import random
import logging
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd

try:
    import yfinance as yf
except ImportError:
    yf = None

try:
    import akshare as ak
except ImportError:
    ak = None

try:
    import baostock as bs
except ImportError:
    bs = None

try:
    import efinance as ef
except ImportError:
    ef = None

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


def _to_ak_symbol(raw: str) -> str:
    """SH510500 -> 510500, SZ159941 -> 159941（akshare 使用纯数字代码）"""
    raw = raw.strip().upper()
    if raw.startswith("SH") or raw.startswith("SZ"):
        return raw[2:]
    return raw


def _to_bs_symbol(raw: str) -> str:
    """SH510500 -> sh.510500, SZ159941 -> sz.159941（baostock 小写 sh/sz + 点 + 代码）"""
    raw = raw.strip().upper()
    if raw.startswith("SH"):
        return "sh." + raw[2:]
    if raw.startswith("SZ"):
        return "sz." + raw[2:]
    return raw


def _to_em_secid(raw: str) -> str:
    """东方财富 K 线接口 secid：SH510500 -> 1.510500，SZ159941 -> 0.159941"""
    raw = raw.strip().upper()
    if raw.startswith("SH"):
        return "1." + raw[2:]
    if raw.startswith("SZ"):
        return "0." + raw[2:]
    return raw


# 全量下载时默认从 2014 年开始（池中多数 ETF 可追溯至此）
START_DATE_2014 = "20140101"


def _parse_period_to_dates(period: str) -> tuple[str, str]:
    """将 yfinance 风格 period（如 12y）或 from2014 转为 (start_date, end_date) YYYYMMDD。"""
    end = datetime.now().date()
    end_str = end.strftime("%Y%m%d")
    period = (period or "").strip().lower()
    if period == "from2014":
        return START_DATE_2014, end_str
    if not period or period == "max":
        start = end - timedelta(days=365 * 20)
    elif period.endswith("y"):
        years = int(period[:-1])
        start = end - timedelta(days=365 * years)
    elif period.endswith("mo"):
        months = int(period[:-2])
        start = end - timedelta(days=30 * months)
    elif period.endswith("d"):
        days = int(period[:-1])
        start = end - timedelta(days=days)
    else:
        start = end - timedelta(days=365 * 12)
    start_str = start.strftime("%Y%m%d")
    return start_str, end_str


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
        data_source: str = "akshare",
        baostock_user: str | None = None,
        baostock_password: str | None = None,
    ):
        """
        data_source : "yfinance" | "akshare"
            默认 "akshare"，避免沪深 ETF 在 Yahoo 上的异常涨跌幅；需安装 akshare。
        """
        self.pool_csv = Path(pool_csv)
        self.db_dir = Path(db_dir)
        self.db_dir.mkdir(parents=True, exist_ok=True)
        self.max_retries = max_retries
        self._data_source = (data_source or "akshare").strip().lower()
        if self._data_source not in ("yfinance", "akshare", "baostock", "efinance", "eastmoney"):
            self._data_source = "akshare"
        if self._data_source == "akshare" and ak is None:
            raise ImportError("使用 data_source='akshare' 请先安装: pip install akshare")
        if self._data_source == "yfinance" and yf is None:
            raise ImportError("使用 data_source='yfinance' 请先安装: pip install yfinance")
        if self._data_source == "baostock" and bs is None:
            raise ImportError("使用 data_source='baostock' 请先安装: pip install baostock")
        if self._data_source == "efinance" and ef is None:
            raise ImportError("使用 data_source='efinance' 请先安装: pip install efinance")
        # eastmoney 直连仅用标准库 urllib，无需额外安装
        self._bs_user = baostock_user or os.environ.get("BOOSTOCK_USER", "anonymous")
        self._bs_pass = baostock_password or os.environ.get("BOOSTOCK_PASS", "123456")

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

    def _fetch_history_yf(self, yf_sym: str, **kwargs) -> pd.DataFrame:
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

    def _fetch_history_akshare(
        self, raw_symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """akshare 东方财富 ETF 日线，返回与 _clean 兼容的 DataFrame（Open/High/Low/Close/Volume）。
        使用前复权 qfq 避免除权除息导致的单日虚假涨跌幅（如 +300%/-75%）。
        遇限流或网络错误时指数退避重试。
        """
        sym = _to_ak_symbol(raw_symbol)
        last_err = None
        for attempt in range(1, self.max_retries + 1):
            try:
                df = ak.fund_etf_hist_em(
                    symbol=sym,
                    period="daily",
                    start_date=start_date,
                    end_date=end_date,
                    adjust="qfq",
                )
                if df is None or df.empty:
                    return pd.DataFrame()
                # 列名: 日期 开盘 收盘 最高 最低 成交量 成交额 振幅 涨跌幅 涨跌额 换手率
                rename = {"日期": "Date", "开盘": "Open", "收盘": "Close", "最高": "High", "最低": "Low", "成交量": "Volume"}
                df = df.rename(columns=rename)
                keep = [c for c in ["Date", "Open", "High", "Low", "Close", "Volume"] if c in df.columns]
                df = df[keep].copy()
                df["Date"] = pd.to_datetime(df["Date"])
                df = df.set_index("Date").sort_index()
                df.index = df.index.tz_localize(None)
                df.index.name = "Date"
                return df
            except Exception as e:
                last_err = e
                wait = min(60, 2 ** attempt)
                logger.debug(
                    "%s 第 %d 次尝试失败 (%s)，%ds 后重试",
                    raw_symbol, attempt, e, wait,
                )
                time.sleep(wait)
        raise last_err  # type: ignore[misc]

    def _fetch_history_baostock(
        self, raw_symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """Baostock 日线（前复权）。返回与 _clean 兼容的 DataFrame。
        注意：Baostock 可能仅支持 A 股，ETF 常返回 0 行，此时请用 akshare。
        start_date/end_date 格式 YYYYMMDD，内部会转为 YYYY-MM-DD。
        """
        code = _to_bs_symbol(raw_symbol)
        start_d = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}"
        end_d = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"
        try:
            lg = bs.login(user_id=self._bs_user, password=self._bs_pass)
            if lg.error_code != "0":
                logger.warning("Baostock 登录失败: %s %s", lg.error_code, lg.error_msg)
                return pd.DataFrame()
        except Exception as e:
            logger.warning("Baostock 登录异常: %s", e)
            return pd.DataFrame()
        try:
            rs = bs.query_history_k_data_plus(
                code,
                "date,open,high,low,close,volume",
                start_date=start_d,
                end_date=end_d,
                frequency="d",
                adjustflag="2",
            )
            if rs.error_code != "0":
                logger.debug("Baostock %s: %s %s", code, rs.error_code, rs.error_msg)
                return pd.DataFrame()
            data = []
            while rs.error_code == "0" and rs.next():
                data.append(rs.get_row_data())
            if not data:
                logger.debug("Baostock %s 返回 0 行（ETF 可能不受支持）", code)
                return pd.DataFrame()
            df = pd.DataFrame(data, columns=rs.fields)
            df = df.rename(columns={"date": "Date", "open": "Open", "high": "High", "low": "Low", "close": "Close", "volume": "Volume"})
            keep = [c for c in ["Date", "Open", "High", "Low", "Close", "Volume"] if c in df.columns]
            df = df[keep].copy()
            for c in ["Open", "High", "Low", "Close", "Volume"]:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce")
            df["Date"] = pd.to_datetime(df["Date"])
            df = df.set_index("Date").sort_index()
            df.index.name = "Date"
            return df
        finally:
            try:
                bs.logout()
            except Exception:
                pass

    def _fetch_history_efinance(
        self, raw_symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """efinance 获取 ETF 日线（数据源为东方财富），返回与 _clean 兼容的 DataFrame。
        start_date/end_date 格式 YYYYMMDD。
        """
        code = _to_ak_symbol(raw_symbol)
        for attempt in range(1, self.max_retries + 1):
            try:
                df = ef.stock.get_quote_history(code)
                if df is None or df.empty:
                    return pd.DataFrame()
                df = df.rename(columns={"日期": "Date", "开盘": "Open", "收盘": "Close", "最高": "High", "最低": "Low", "成交量": "Volume"})
                keep = [c for c in ["Date", "Open", "High", "Low", "Close", "Volume"] if c in df.columns]
                df = df[keep].copy()
                df["Date"] = pd.to_datetime(df["Date"])
                df = df.set_index("Date").sort_index()
                start_d = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}"
                end_d = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"
                df = df.loc[start_d:end_d]
                for c in ["Open", "High", "Low", "Close", "Volume"]:
                    if c in df.columns:
                        df[c] = pd.to_numeric(df[c], errors="coerce")
                df.index.name = "Date"
                return df
            except Exception as e:
                if attempt >= self.max_retries:
                    logger.warning("efinance %s 失败: %s", raw_symbol, e)
                    return pd.DataFrame()
                time.sleep(min(30, 2 ** attempt))
        return pd.DataFrame()

    def _fetch_history_eastmoney(
        self, raw_symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """东方财富 K 线直连 API，前复权。start_date/end_date 格式 YYYYMMDD。"""
        import json
        import ssl
        import urllib.request
        secid = _to_em_secid(raw_symbol)
        url = (
            "https://push2his.eastmoney.com/api/qt/stock/kline/get?"
            f"secid={secid}&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58"
            f"&klt=101&fqt=1&beg={start_date}&end={end_date}&lmt=10000"
        )
        for attempt in range(1, self.max_retries + 1):
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
                ctx = ssl.create_default_context()
                if os.environ.get("EASTMONEY_SSL_VERIFY", "1") == "0":
                    ctx.check_hostname = False
                    ctx.verify_mode = ssl.CERT_NONE
                with urllib.request.urlopen(req, timeout=15, context=ctx) as r:
                    data = json.loads(r.read().decode())
                if not data.get("data") or not data["data"].get("klines"):
                    return pd.DataFrame()
                rows = []
                for s in data["data"]["klines"]:
                    parts = s.split(",")
                    if len(parts) >= 6:
                        rows.append({
                            "Date": parts[0],
                            "Open": float(parts[1]),
                            "Close": float(parts[2]),
                            "High": float(parts[3]),
                            "Low": float(parts[4]),
                            "Volume": float(parts[5]),
                        })
                df = pd.DataFrame(rows)
                df["Date"] = pd.to_datetime(df["Date"])
                df = df.set_index("Date").sort_index()
                df.index.name = "Date"
                return df
            except Exception as e:
                if attempt >= self.max_retries:
                    logger.warning("东方财富直连 %s 失败: %s", raw_symbol, e)
                    return pd.DataFrame()
                time.sleep(min(30, 2 ** attempt))
        return pd.DataFrame()

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
        file_path = self._file_path(raw_symbol)

        existing = None
        if incremental and file_path.exists():
            existing = pd.read_csv(file_path, index_col=0, parse_dates=True)
            if not existing.empty:
                last_date = existing.index.max()
                start_d = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
                end_d = datetime.now().strftime("%Y-%m-%d")
                if start_d >= end_d:
                    logger.debug("%s 已是最新，跳过", raw_symbol)
                    return existing
                try:
                    if self._data_source == "akshare":
                        new_data = self._fetch_history_akshare(
                            raw_symbol,
                            start_d.replace("-", ""),
                            end_d.replace("-", ""),
                        )
                    elif self._data_source == "baostock":
                        new_data = self._fetch_history_baostock(
                            raw_symbol,
                            start_d.replace("-", ""),
                            end_d.replace("-", ""),
                        )
                    elif self._data_source == "efinance":
                        new_data = self._fetch_history_efinance(
                            raw_symbol,
                            start_d.replace("-", ""),
                            end_d.replace("-", ""),
                        )
                    elif self._data_source == "eastmoney":
                        new_data = self._fetch_history_eastmoney(
                            raw_symbol,
                            start_d.replace("-", ""),
                            end_d.replace("-", ""),
                        )
                    else:
                        end_exclusive = (datetime.now().date() + timedelta(days=1)).strftime("%Y-%m-%d")
                        new_data = self._fetch_history_yf(
                            _to_yf_symbol(raw_symbol), start=start_d, end=end_exclusive
                        )
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
            if self._data_source == "akshare":
                start_str, end_str = _parse_period_to_dates(period)
                hist = self._fetch_history_akshare(raw_symbol, start_str, end_str)
            elif self._data_source == "baostock":
                start_str, end_str = _parse_period_to_dates(period)
                hist = self._fetch_history_baostock(raw_symbol, start_str, end_str)
            elif self._data_source == "efinance":
                start_str, end_str = _parse_period_to_dates(period)
                hist = self._fetch_history_efinance(raw_symbol, start_str, end_str)
            elif self._data_source == "eastmoney":
                start_str, end_str = _parse_period_to_dates(period)
                hist = self._fetch_history_eastmoney(raw_symbol, start_str, end_str)
            else:
                hist = self._fetch_history_yf(_to_yf_symbol(raw_symbol), period=period)
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
        retry_rounds: int = 1,
    ) -> dict[str, int]:
        """批量下载 ETF 池中所有标的。

        Parameters
        ----------
        period : str
            yfinance period 或 "from2014"（仅在全量下载时生效）。
        incremental : bool
            是否增量更新。
        sleep_min, sleep_max : float
            每下载一只 ETF 后，随机等待 [sleep_min, sleep_max] 秒再下载下一只，防止限流。
        retry_rounds : int
            失败标的自动重试轮数；每轮前等待约 60 秒以缓解限流。

        Returns
        -------
        dict
            {'ok': 成功数, 'skip': 跳过数, 'fail': 失败数, 'failed_symbols': [...]}
        """
        symbols = self.pool_symbols()
        total = len(symbols)
        ok = skip = fail = 0
        failed_symbols: list[str] = []

        logger.info("开始下载，共 %d 只 ETF，period=%s，重试轮数=%d ...", total, period, retry_rounds)

        for round_no in range(max(1, retry_rounds)):
            if round_no > 0:
                remaining = failed_symbols.copy()
                if not remaining:
                    break
                wait_sec = min(90, 45 + round_no * 20)
                logger.info("第 %d 轮重试，共 %d 只失败标的，等待 %ds 后继续 ...", round_no + 1, len(remaining), wait_sec)
                time.sleep(wait_sec)
                symbols_this_round = remaining
                failed_symbols = []
            else:
                symbols_this_round = symbols

            for i, sym in enumerate(symbols_this_round, 1):
                file_path = self._file_path(sym)
                already_fresh = False
                if incremental and file_path.exists():
                    existing = pd.read_csv(file_path, index_col=0, parse_dates=True)
                    if not existing.empty:
                        last_date = existing.index.max()
                        if (datetime.now() - last_date).days <= 1:
                            skip += 1
                            logger.debug("[%d/%d] %s 已是最新，跳过", i, len(symbols_this_round), sym)
                            already_fresh = True

                if not already_fresh:
                    result = self.download_one(sym, period=period, incremental=incremental)
                    if result is not None:
                        ok += 1
                        logger.info(
                            "[%d/%d] %s 完成，共 %d 行", i, len(symbols_this_round), sym, len(result)
                        )
                    else:
                        failed_symbols.append(sym)
                        logger.warning("[%d/%d] %s 失败", i, len(symbols_this_round), sym)
                    time.sleep(random.uniform(sleep_min, sleep_max))

        summary = {
            "ok": ok,
            "skip": skip,
            "fail": len(failed_symbols),
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
    parser.add_argument("--source", default="akshare", choices=("akshare", "yfinance", "baostock", "efinance", "eastmoney"), help="数据源: akshare/efinance/eastmoney(东方财富)/yfinance/baostock")
    parser.add_argument("--period", default=DEFAULT_PERIOD, help="下载周期（全量时生效），12y / from2014 等")
    parser.add_argument("--from-2014", action="store_true", help="全量时从 2014-01-01 开始下载（等同 --period from2014）")
    parser.add_argument("--full", action="store_true", help="全量下载（忽略本地已有数据）")
    parser.add_argument("--clear-db", action="store_true", help="下载前清空 db 目录下所有 CSV（与 --full 配合使用）")
    parser.add_argument("--sleep-min", type=float, default=1.0, help="下载间隔下限（秒）")
    parser.add_argument("--sleep-max", type=float, default=2.0, help="下载间隔上限（秒）")
    parser.add_argument("--retry-rounds", type=int, default=1, help="失败标的自动重试轮数（全量建议 5）")
    parser.add_argument("--status", action="store_true", help="仅查看本地数据状态")
    args = parser.parse_args()

    db_dir = Path(args.db)
    if args.clear_db:
        cleared = 0
        for fp in db_dir.glob("*.csv"):
            fp.unlink()
            cleared += 1
        if cleared:
            logger.info("已清空 db：删除 %d 个 CSV", cleared)

    mgr = EtfDataManager(pool_csv=args.pool, db_dir=args.db, proxy=args.proxy, data_source=args.source)

    if args.status:
        print(mgr.status().to_string(index=False))
        return

    period = "from2014" if args.from_2014 else args.period
    if args.full and not args.from_2014 and args.period == DEFAULT_PERIOD:
        period = "from2014"
        logger.info("全量下载使用 from2014（2014-01-01 起）")
    sleep_min = args.sleep_min
    sleep_max = args.sleep_max
    retry_rounds = args.retry_rounds
    if args.full:
        sleep_min = max(sleep_min, 1.2)
        sleep_max = max(sleep_max, 2.5)
        retry_rounds = max(retry_rounds, 3)

    result = mgr.download_all(
        period=period,
        incremental=not args.full,
        sleep_min=sleep_min,
        sleep_max=sleep_max,
        retry_rounds=retry_rounds,
    )
    print(f"\n下载完成: 成功 {result['ok']}, 跳过 {result['skip']}, 失败 {result['fail']}")
    if result["failed_symbols"]:
        print(f"失败标的: {result['failed_symbols']}")


if __name__ == "__main__":
    main()
