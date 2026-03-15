"""从东方财富数据中心增量拉取牛散持仓记录。"""

import time
import logging

import requests
import pandas as pd

from .config import EM_API_URL, HEADERS_EM, DATA_FILES

logger = logging.getLogger(__name__)


def _fetch_em_page(holder_name: str, page: int, page_size: int = 500) -> dict:
    params = {
        "sortColumns": "END_DATE",
        "sortTypes": "-1",
        "pageSize": str(page_size),
        "pageNumber": str(page),
        "reportName": "RPT_F10_EH_FREEHOLDERS",
        "columns": (
            "SECURITY_CODE,SECURITY_NAME_ABBR,END_DATE,"
            "HOLD_NUM,FREE_HOLDNUM_RATIO,HOLDNUM_CHANGE_NAME,"
            "HOLDER_RANK,HOLDER_TYPE"
        ),
        "filter": f'(HOLDER_NAME="{holder_name}")',
    }
    for attempt in range(3):
        try:
            resp = requests.get(EM_API_URL, params=params, headers=HEADERS_EM, timeout=20)
            return resp.json()
        except Exception as e:
            if attempt == 2:
                raise
            time.sleep(1)
    return {}


def fetch_holdings_from_em(name: str) -> pd.DataFrame:
    """获取单个牛散全量历史持仓（新进+增持），返回 DataFrame。"""
    records = []
    page, total_fetched = 1, 0

    while True:
        try:
            data = _fetch_em_page(name, page)
        except Exception as e:
            logger.warning("  %s 第%d页请求失败: %s", name, page, e)
            break

        result = data.get("result")
        if not result or not result.get("data"):
            break

        rows = result["data"]
        total_count = result.get("count", 0)

        for r in rows:
            change = r.get("HOLDNUM_CHANGE_NAME", "")
            if change not in ("新进", "增加"):
                continue

            end_date = (r.get("END_DATE") or "")[:10]
            stock_code = r.get("SECURITY_CODE", "")
            stock_name = r.get("SECURITY_NAME_ABBR", "")
            if not stock_code or not end_date:
                continue

            records.append({
                "牛散": name,
                "股票代码": str(stock_code).zfill(6),
                "股票名称": stock_name,
                "持股变动": "新进" if change == "新进" else "增持",
                "报告期": end_date,
            })

        total_fetched += len(rows)
        if total_fetched >= total_count:
            break
        page += 1
        time.sleep(0.2)

    df = pd.DataFrame(records)
    if not df.empty:
        df = df.drop_duplicates(subset=["牛散", "股票代码", "报告期"])
    logger.info("  %s: %d 条新进/增持", name, len(df))
    return df


def load_local_holdings() -> pd.DataFrame:
    """读取本地持仓记录。"""
    f = DATA_FILES["holdings"]
    if f.exists():
        df = pd.read_csv(f, dtype={"股票代码": str})
        df["股票代码"] = df["股票代码"].apply(lambda x: str(x).zfill(6))
        return df
    return pd.DataFrame(columns=["牛散", "股票代码", "股票名称", "持股变动", "报告期"])


def update_holdings(names: list[str], force_full: bool = False) -> dict:
    """
    增量更新持仓记录。

    对于每位牛散：
      - 查看本地已有最新报告期
      - 从东财拉取全量数据，仅保留 > 本地最新的记录合并
      - force_full=True 时强制全量替换

    返回 {"total": int, "new_records": int}
    """
    local = load_local_holdings()
    new_records = []
    skipped = 0

    for i, name in enumerate(names):
        if (i + 1) % 10 == 0:
            logger.info("获取持仓进度: [%d/%d]", i + 1, len(names))

        person_local = local[local["牛散"] == name]

        if not force_full and not person_local.empty:
            latest_local = person_local["报告期"].max()
        else:
            latest_local = None

        try:
            remote = fetch_holdings_from_em(name)
        except Exception as e:
            logger.error("  获取 %s 失败: %s", name, e)
            continue

        if remote.empty:
            continue

        if latest_local and not force_full:
            remote = remote[remote["报告期"] > latest_local]

        if remote.empty:
            skipped += 1
            continue

        new_records.append(remote)
        time.sleep(0.3)

    if new_records:
        new_df = pd.concat(new_records, ignore_index=True)
        merged = pd.concat([local, new_df], ignore_index=True)
        merged = merged.drop_duplicates(subset=["牛散", "股票代码", "报告期"])
        merged.sort_values(["牛散", "报告期"], inplace=True)
        merged.to_csv(DATA_FILES["holdings"], index=False, encoding="utf-8-sig")
        n_new = len(new_df)
    else:
        merged = local
        n_new = 0

    logger.info("持仓更新完成: 总计 %d 条, 新增 %d 条, %d 人无需更新",
                len(merged), n_new, skipped)
    return {"total": len(merged), "new_records": n_new}
