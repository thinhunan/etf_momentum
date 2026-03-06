#!/usr/bin/env python3
"""仅重新下载指定失败标的，使用代理。用法: python redownload_failed.py"""
import os
import time
import random
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")

# 下载前设置代理（请按本机代理修改）
PROXY = os.environ.get("ETF_PROXY", "http://127.0.0.1:1087")
os.environ["HTTP_PROXY"] = PROXY
os.environ["HTTPS_PROXY"] = PROXY

from etf_data_manager import EtfDataManager

# 重试上次全量下载仍失败的标的（限流类）；SH511880 为货基 Yahoo 无日线，不重试
RETRY_SYMBOLS = ["SH513130", "SZ159615", "SH562550", "SH515220"]

def main():
    mgr = EtfDataManager(proxy=PROXY)
    for i, sym in enumerate(RETRY_SYMBOLS, 1):
        print(f"[{i}/{len(RETRY_SYMBOLS)}] 下载 {sym} ...")
        result = mgr.download_one(sym, period="12y", incremental=False)
        if result is not None:
            print(f"  -> 成功，{len(result)} 行")
        else:
            print(f"  -> 失败")
        if i < len(RETRY_SYMBOLS):
            wait = random.uniform(1, 2)
            time.sleep(wait)

if __name__ == "__main__":
    main()
