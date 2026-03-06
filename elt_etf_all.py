"""
将 etf_all.json 中的 ETF 数据导出为 Excel 表格。
"""
import json
import pandas as pd
from pathlib import Path

# 路径（与脚本同目录）
BASE = Path(__file__).resolve().parent
JSON_PATH = BASE / "etf_all.json"
EXCEL_PATH = BASE / "etf_all.xlsx"


def main():
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        raw = json.load(f)

    rows = raw["data"]["list"]

    # 展平为可写入表格的字典列表；列表/复杂类型转为字符串
    def flatten(item):
        out = {}
        for k, v in item.items():
            if v is None:
                out[k] = ""
            elif isinstance(v, list):
                out[k] = ",".join(str(x) for x in v) if v else ""
            elif isinstance(v, dict):
                out[k] = json.dumps(v, ensure_ascii=False)
            else:
                out[k] = v
        return out

    data = [flatten(r) for r in rows]
    df = pd.DataFrame(data)

    # 列顺序：常用字段靠前
    priority = ["symbol", "name", "current", "chg", "percent", "volume", "amount", "market_capital"]
    rest = [c for c in df.columns if c not in priority]
    df = df[priority + rest]

    df.to_excel(EXCEL_PATH, index=False, engine="openpyxl")
    print(f"已导出: {EXCEL_PATH}，共 {len(df)} 行")


if __name__ == "__main__":
    main()
