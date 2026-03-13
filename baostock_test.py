#!/usr/bin/env python3
"""
Baostock 连接与数据测试（你已注册时可用自己的 user_id / password 登录）。

用法:
  python baostock_test.py
  python baostock_test.py --user YOUR_ID --password YOUR_PASS
  BOOSTOCK_USER=xxx BOOSTOCK_PASS=xxx python baostock_test.py

说明:
  Baostock 官方文档 https://www.baostock.com/mainContent?file=pythonAPI.md
  当前实测：A 股（如 sh.600000）可正常拉取日线；ETF（如 sh.510880）返回 0 行，
  可能平台仅支持股票不包含 ETF，ETF 数据请继续用 akshare。
"""

import argparse
import os
import sys

def main():
    parser = argparse.ArgumentParser(description="Baostock 登录与日线数据测试")
    parser.add_argument("--user", default=os.environ.get("BOOSTOCK_USER", "anonymous"), help="用户 ID")
    parser.add_argument("--password", default=os.environ.get("BOOSTOCK_PASS", "123456"), help="密码")
    args = parser.parse_args()

    try:
        import baostock as bs
    except ImportError:
        print("请先安装: pip install baostock")
        return 1

    lg = bs.login(user_id=args.user, password=args.password)
    if lg.error_code != "0":
        print("登录失败:", lg.error_code, lg.error_msg)
        return 1
    print("登录成功:", lg.error_msg)

    # 测试 A 股
    rs = bs.query_history_k_data_plus(
        "sh.600000",
        "date,open,high,low,close,volume",
        "2014-06-01", "2014-06-10",
        frequency="d", adjustflag="2"
    )
    rows = []
    while rs.error_code == "0" and rs.next():
        rows.append(rs.get_row_data())
    print(f"  sh.600000 (浦发银行) 2014-06 日线: {len(rows)} 行")

    # 测试 ETF
    rs = bs.query_history_k_data_plus(
        "sh.510880",
        "date,open,high,low,close,volume",
        "2014-01-01", "2014-12-31",
        frequency="d", adjustflag="2"
    )
    rows_etf = []
    while rs.error_code == "0" and rs.next():
        rows_etf.append(rs.get_row_data())
    print(f"  sh.510880 (红利ETF) 2014 日线: {len(rows_etf)} 行")
    if len(rows_etf) == 0:
        print("  → ETF 当前返回 0 行，Baostock 可能仅支持 A 股，ETF 请用 akshare。")

    bs.logout()
    print("已登出")
    return 0


if __name__ == "__main__":
    sys.exit(main())
