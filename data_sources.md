# ETF 数据源说明

`etf_data_manager.py` 支持多种数据源，通过 `--source` 或 `data_source=` 指定。

## 数据源对比

| 数据源 | 说明 | ETF 支持 | 推荐度 |
|--------|------|----------|--------|
| **akshare** | 东方财富（fund_etf_hist_em），前复权 | ✅ 完整 | ⭐⭐⭐ 默认推荐 |
| **efinance** | 东方财富封装库，ETF 用股票接口 | ✅ 完整 | ⭐⭐⭐ 可作备用 |
| **eastmoney** | 东方财富 K 线直连 API | ✅ 完整 | ⭐⭐ 串行请求+限流(2~4秒/只)；增量仅拉最近 7 日；证书异常可设 `EASTMONEY_SSL_VERIFY=0`；代理异常可设 `NO_PROXY=push2his.eastmoney.com` |
| **yfinance** | Yahoo Finance | ⚠️ 部分沪深 ETF 异常涨跌幅 | ⭐ 不推荐 |
| **baostock** | 证券宝 | ❌ 实测 ETF 返回 0 行，仅 A 股 | 仅 A 股时用 |

## 使用示例

```bash
# 默认 akshare
python etf_data_manager.py --full --from-2014

# 东方财富系：efinance
python etf_data_manager.py --source efinance --full --from-2014

# 东方财富直连（证书错误时）
EASTMONEY_SSL_VERIFY=0 python etf_data_manager.py --source eastmoney --full --from-2014
```

## 依赖

- akshare: `pip install akshare`
- efinance: `pip install efinance`
- eastmoney: 仅用标准库，无需安装
- baostock: `pip install baostock`
- yfinance: `pip install yfinance`
