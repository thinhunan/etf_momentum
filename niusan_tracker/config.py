"""全局配置：路径、常量、公共工具。"""

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DB_DIR = BASE_DIR / "niusan_db"
DB_DIR.mkdir(exist_ok=True)

DATA_FILES = {
    "niusan_list": DB_DIR / "niusan_list.csv",
    "holdings":    DB_DIR / "holdings.csv",
    "returns":     DB_DIR / "returns_detail.csv",
    "summary_all": DB_DIR / "summary_all.csv",
    "summary_2y":  DB_DIR / "summary_recent2y.csv",
    "report":      DB_DIR / "analysis_report.md",
    "last_analysis_meta": DB_DIR / "last_analysis_meta.json",
}

TETEGU_BASE = "http://www.tetegu.com"

HEADERS_WEB = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}

HEADERS_EM = {
    **HEADERS_WEB,
    "Referer": "https://data.eastmoney.com/",
}

TETEGU_URL = "http://www.tetegu.com/niusan/"
EM_API_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"

# 排除机构关键词
INSTITUTION_KEYWORDS = ("公司", "银行", "基金", "有限", "plc", "morgan", "ubs", "资产", "合伙")

for _k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"):
    os.environ.pop(_k, None)
