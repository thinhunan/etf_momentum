"""从 tetegu.com 获取牛散名单，增量对比本地数据。"""

import re
import logging

import requests
import pandas as pd
from bs4 import BeautifulSoup

from .config import TETEGU_URL, TETEGU_BASE, HEADERS_WEB, INSTITUTION_KEYWORDS, DATA_FILES

logger = logging.getLogger(__name__)


# 名单页姓名可能带排名前缀，如 _1_徐开东、_331_宁琛，需规范化为纯姓名
_NAME_PREFIX_RE = re.compile(r"^_\d+_", re.U)


def _normalize_name(raw: str) -> str:
    """去掉 _数字_ 前缀，得到纯姓名。"""
    return _NAME_PREFIX_RE.sub("", raw).strip() or raw


def fetch_niusan_from_web(top_n: int = 500) -> list[tuple[str, str]]:
    """
    抓取 tetegu.com 牛散名单页，只保留带 /gudong/数字.html 的链接，
    返回 [(规范姓名, 详情页完整url), ...]，同一人只保留一条（先出现的链接）。
    """
    resp = requests.get(TETEGU_URL, headers=HEADERS_WEB, timeout=30)
    resp.encoding = "utf-8"
    soup = BeautifulSoup(resp.text, "html.parser")

    seen: set[str] = set()
    result: list[tuple[str, str]] = []
    for a in soup.find_all("a", href=re.compile(r"/gudong/\d+\.html")):
        raw_name = a.get_text(strip=True)
        name = _normalize_name(raw_name)
        if not name or name in seen:
            continue
        if any(kw in name for kw in INSTITUTION_KEYWORDS):
            continue
        href = a.get("href", "").strip()
        if href.startswith("/"):
            url = TETEGU_BASE + href
        else:
            url = href if href.startswith("http") else ""
        if not url:
            continue
        seen.add(name)
        result.append((name, url))
        if len(result) >= top_n:
            break

    logger.info("从 tetegu 获取到 %d 位牛散（含 gudong 详情链接）", len(result))
    return result


def load_local_list() -> list[str]:
    """读取本地牛散名单（仅姓名）。"""
    f = DATA_FILES["niusan_list"]
    if f.exists():
        df = pd.read_csv(f)
        return df["牛散"].tolist()
    return []


def get_niusan_links() -> dict[str, str]:
    """读取本地牛散名单，返回 {牛散姓名: 详情页url}。无链接的用名单页代替。"""
    f = DATA_FILES["niusan_list"]
    if not f.exists():
        return {}
    df = pd.read_csv(f)
    if "链接" not in df.columns:
        return {n: f"{TETEGU_BASE}/niusan/" for n in df["牛散"].dropna().tolist()}
    return dict(zip(df["牛散"], df["链接"].fillna(f"{TETEGU_BASE}/niusan/")))


def update_niusan_list(top_n: int = 500) -> dict:
    """
    更新牛散名单，返回 {"all": [...], "new": [...], "total": int, "links": {name: url}}。

    - 从网页获取最新名单及详情页链接
    - 与本地名单合并（保留历史牛散 + 新增牛散），链接以网页为准、本地已有则保留
    - 保存到 niusan_list.csv（牛散, 链接）
    """
    web_items = fetch_niusan_from_web(top_n)
    web_names = [x[0] for x in web_items]
    web_links = dict(web_items)

    local_df = pd.DataFrame()
    if DATA_FILES["niusan_list"].exists():
        local_df = pd.read_csv(DATA_FILES["niusan_list"])
    local_names = local_df["牛散"].tolist() if not local_df.empty else []
    local_links = dict(zip(local_df["牛散"], local_df["链接"])) if "链接" in local_df.columns else {}

    local_set = set(local_names)
    new_names = [n for n in web_names if n not in local_set]
    merged_names = list(dict.fromkeys(local_names + web_names))

    # 链接：网页优先，否则保留本地
    merged_links = []
    for n in merged_names:
        url = web_links.get(n) or local_links.get(n) or f"{TETEGU_BASE}/niusan/"
        merged_links.append(url)

    df = pd.DataFrame({"牛散": merged_names, "链接": merged_links})
    df.to_csv(DATA_FILES["niusan_list"], index=False, encoding="utf-8-sig")

    logger.info("牛散名单: 本地 %d, 网页 %d, 新增 %d, 合计 %d",
                len(local_names), len(web_names), len(new_names), len(merged_names))

    return {"all": merged_names, "new": new_names, "total": len(merged_names), "links": dict(zip(merged_names, merged_links))}
