"""生成牛散排行榜和推荐报告。"""

import json
import logging
from datetime import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta

from .config import DATA_FILES, TETEGU_BASE
from .returns import load_local_returns, get_latest_price, cleanup_bs

logger = logging.getLogger(__name__)


# ── 本期应关注的报告期范围 ───────────────────────────────────

def get_relevant_report_dates() -> list[str]:
    """
    根据当前月份，返回「本期应关注」的报告期列表（YYYY-MM-DD）。
    - 1-4月：上年年报 12/31 + 本年一季报 3/31
    - 5-6月：本年一季报 3/31
    - 7-9月：半年报 6/30
    - 10-12月：三季报 9/30
    """
    now = datetime.now()
    y, m = now.year, now.month
    dates = []
    if m in (1, 2, 3, 4):
        dates.append(f"{y-1}-12-31")
        dates.append(f"{y}-03-31")
    elif m in (5, 6):
        dates.append(f"{y}-03-31")
    elif m in (7, 8, 9):
        dates.append(f"{y}-06-30")
    else:  # 10, 11, 12
        dates.append(f"{y}-09-30")
    return list(dict.fromkeys(dates))


def load_last_analysis_meta() -> dict:
    """上次分析时的各牛散最大报告期、运行日期。"""
    f = DATA_FILES["last_analysis_meta"]
    if f.exists():
        try:
            with open(f, "r", encoding="utf-8") as fp:
                return json.load(fp)
        except Exception:
            pass
    return {"last_run": None, "max_report_per_niusan": {}}


def save_last_analysis_meta(max_report_per_niusan: dict):
    """分析完成后保存各牛散最大报告期。"""
    f = DATA_FILES["last_analysis_meta"]
    data = {
        "last_run": datetime.now().strftime("%Y-%m-%d"),
        "max_report_per_niusan": max_report_per_niusan,
    }
    with open(f, "w", encoding="utf-8") as fp:
        json.dump(data, fp, ensure_ascii=False, indent=2)
    logger.info("已保存上次分析 meta: %d 位牛散", len(max_report_per_niusan))


# ── 汇总统计 ─────────────────────────────────────────────────

def _build_summary(detail: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for name, g in detail.groupby("牛散"):
        rec = {
            "牛散": name,
            "记录数": len(g),
            "最早记录日期": g["报告期"].min(),
            "最新记录日期": g["报告期"].max(),
            "新进数": int((g["持股变动"] == "新进").sum()),
            "增持数": int((g["持股变动"] == "增持").sum()),
        }
        for m in (1, 2, 3):
            col = f"持仓{m}月收益率"
            v = g[col].dropna()
            if v.empty:
                continue
            pre = f"跟单{m}月"
            rec[f"{pre}_样本数"] = int(len(v))
            rec[f"{pre}_平均收益"] = round(v.mean(), 4)
            rec[f"{pre}_中位收益"] = round(v.median(), 4)
            rec[f"{pre}_胜率"] = round((v > 0).mean(), 4)
            rec[f"{pre}_最大收益"] = round(v.max(), 4)
            rec[f"{pre}_最大回撤"] = round(v.min(), 4)
        rows.append(rec)

    df = pd.DataFrame(rows)
    if not df.empty:
        df.sort_values("跟单2月_平均收益", ascending=False, inplace=True, na_position="last")
        df.reset_index(drop=True, inplace=True)
    return df


# ── 综合评分 ─────────────────────────────────────────────────

def _score(row: pd.Series) -> float:
    s = 0.0
    s += row.get("全量_跟单3月_胜率", 0) * 0.15
    s += min(row.get("全量_跟单3月_平均收益", 0), 0.5) * 0.30
    s += row.get("近2年_跟单3月_胜率", 0) * 0.25
    s += min(row.get("近2年_跟单3月_平均收益", 0), 0.5) * 0.30
    return round(s, 4)


# ── 推荐筛选 ─────────────────────────────────────────────────

def _recommend(summary_all: pd.DataFrame, summary_2y: pd.DataFrame) -> pd.DataFrame:
    """合并全量和近两年指标，评分排序。"""
    merged = summary_all[["牛散", "记录数", "最早记录日期", "新进数", "增持数"]].copy()
    for c in ["跟单1月_平均收益", "跟单1月_胜率", "跟单2月_平均收益", "跟单2月_胜率",
              "跟单3月_平均收益", "跟单3月_胜率"]:
        merged[f"全量_{c}"] = summary_all[c].values if c in summary_all.columns else None

    r2y_cols = ["牛散", "记录数", "跟单1月_平均收益", "跟单1月_胜率",
                "跟单2月_平均收益", "跟单2月_胜率", "跟单3月_平均收益", "跟单3月_胜率"]
    r2y_cols = [c for c in r2y_cols if c in summary_2y.columns]
    r2y = summary_2y[r2y_cols].copy()
    rename_map = {"记录数": "近2年_记录数"}
    for c in r2y.columns:
        if c not in ("牛散", "记录数"):
            rename_map[c] = f"近2年_{c}"
    r2y = r2y.rename(columns=rename_map)

    merged = merged.merge(r2y, on="牛散", how="left")

    cond = (
        (merged["记录数"] >= 20) &
        (merged.get("全量_跟单2月_胜率", 0) > 0.5) &
        (merged.get("全量_跟单2月_平均收益", 0) > 0) &
        (merged.get("近2年_记录数", 0) >= 5) &
        (merged.get("近2年_跟单2月_胜率", 0) > 0.5)
    )
    top = merged[cond].copy()
    top["综合评分"] = top.apply(_score, axis=1)
    top.sort_values("综合评分", ascending=False, inplace=True)
    top.reset_index(drop=True, inplace=True)
    return top


# ── 生成推荐报告文本（牛散名带链接）──────────────────────────

def _format_pct(v):
    return f"{v:.1%}" if pd.notna(v) else "-"


def build_recommendation_text(top: pd.DataFrame, niusan_links: dict | None = None) -> str:
    """生成 Markdown 报告正文。niusan_links: {牛散姓名: 详情页url}，报告中牛散名渲染为链接。"""
    links = niusan_links or {}
    lines = []
    lines.append("# 牛散跟单推荐报告")
    lines.append(f"\n> 更新日期：{datetime.now().strftime('%Y-%m-%d')}\n")

    lines.append("## 推荐牛散排行\n")
    lines.append(f"共筛选出 **{len(top)}** 位适合跟单的牛散。\n")

    fallback_url = f"{TETEGU_BASE}/niusan/"
    for i, (_, r) in enumerate(top.iterrows(), 1):
        tier = "★★★" if i <= 5 else ("★★" if i <= 10 else "★")
        name = r["牛散"]
        url = (links or {}).get(name) or fallback_url
        name_display = f"[{name}]({url})"
        n_all = int(r["记录数"])
        n_2y = int(r.get("近2年_记录数", 0))
        earliest = r.get("最早记录日期", "?")

        lines.append(f"### #{i} {name_display} {tier}")
        lines.append(f"- 全量: {n_all} 条 (始于 {earliest})")
        lines.append(f"- 近两年: {n_2y} 条")

        for period in ("全量", "近2年"):
            vals = []
            for m in (1, 2, 3):
                avg = r.get(f"{period}_跟单{m}月_平均收益")
                wr = r.get(f"{period}_跟单{m}月_胜率")
                vals.append(f"{m}月: 均{_format_pct(avg)} 胜{_format_pct(wr)}")
            lines.append(f"- {period}: {' | '.join(vals)}")
        lines.append("")

    lines.append("## 最新持仓跟单建议\n")
    lines.append("（以下为当期披露季报/年报中推荐牛散的新进与增持记录，含「本期新增」标记；星级与综合推荐值见表格。）\n")
    lines.append("### 范围与推荐度说明\n")
    lines.append("- **范围**：① 本期应关注的报告期（如 3 月看上年度 12/31 年报，4 月看上年 12/31+当年 3/31 一季报）；② 上次分析之后新出现的记录标为「本期新增」。\n")
    lines.append("- **推荐度**：表格含星级、持股变动(新进/增持)、报告期收盘价、最新价、至今涨幅(=(最新价-报告日收盘价)/报告日收盘价)、综合推荐值；股东人数/营收/利润需 F10 接口，当前为占位。\n")
    lines.append("- **牛散链接**：点击报告中牛散姓名可跳转至 tetegu 该牛散持股详情页（需先运行「更新牛散名单」以写入链接）。\n")
    return "\n".join(lines)


# ── 公开接口 ─────────────────────────────────────────────────

def generate_report() -> dict:
    """
    生成排行报告。

    返回:
      - summary_all: DataFrame
      - summary_2y:  DataFrame
      - recommended:  DataFrame (带评分)
      - report_text:  str (markdown)
    """
    detail = load_local_returns()
    if detail.empty:
        logger.warning("无收益数据，无法生成报告")
        return {}

    summary_all = _build_summary(detail)
    summary_all.to_csv(DATA_FILES["summary_all"], index=False, encoding="utf-8-sig")

    cutoff = (datetime.now() - relativedelta(years=2)).strftime("%Y-%m-%d")
    recent = detail[detail["报告期"].astype(str) >= cutoff]
    summary_2y = _build_summary(recent)
    summary_2y.to_csv(DATA_FILES["summary_2y"], index=False, encoding="utf-8-sig")

    recommended = _recommend(summary_all, summary_2y)

    try:
        from .scraper import get_niusan_links
        niusan_links = get_niusan_links()
    except Exception:
        niusan_links = {}
    report_text = build_recommendation_text(recommended, niusan_links)

    with open(DATA_FILES["report"], "w", encoding="utf-8") as f:
        f.write(report_text)

    # 保存本次各牛散最大报告期，供下次「本期新增」判断
    max_report_per_niusan = detail.groupby("牛散")["报告期"].max().astype(str).str[:10].to_dict()
    save_last_analysis_meta(max_report_per_niusan)

    # 整体统计
    stats = {}
    for m in (1, 2, 3):
        v = detail[f"持仓{m}月收益率"].dropna()
        if v.empty:
            continue
        stats[f"全量_{m}月"] = {
            "样本": len(v),
            "平均": f"{v.mean():.2%}",
            "中位": f"{v.median():.2%}",
            "胜率": f"{(v > 0).mean():.1%}",
        }
        v2 = recent[f"持仓{m}月收益率"].dropna()
        if not v2.empty:
            stats[f"近2年_{m}月"] = {
                "样本": len(v2),
                "平均": f"{v2.mean():.2%}",
                "中位": f"{v2.median():.2%}",
                "胜率": f"{(v2 > 0).mean():.1%}",
            }

    logger.info("报告已生成: %s", DATA_FILES["report"])
    return {
        "summary_all": summary_all,
        "summary_2y": summary_2y,
        "recommended": recommended,
        "report_text": report_text,
        "stats": stats,
    }


def get_follow_suggestions(top_n: int = 10) -> pd.DataFrame:
    """
    获取「本期应关注」的持仓跟单建议表。

    范围逻辑：
    - 本期报告期：由 get_relevant_report_dates() 按当前月份得出（如 3 月看上年 12/31，4 月看上年 12/31+当年 3/31）
    - 本期新增：报告期晚于上次分析时该牛散的最大报告期
    推荐度：星级(排名)、持股变动(新进/增持)、跟踪成本价、最新价、至今涨幅、综合推荐值。
    股东人数/营收/利润需另接 F10 接口，当前表内用「—」占位。
    """
    from .holdings import load_local_holdings
    from .scraper import get_niusan_links

    detail = load_local_returns()
    if detail.empty:
        return pd.DataFrame()

    summary_all = _build_summary(detail)
    cutoff_2y = (datetime.now() - relativedelta(years=2)).strftime("%Y-%m-%d")
    recent = detail[detail["报告期"].astype(str) >= cutoff_2y]
    summary_2y = _build_summary(recent)
    recommended = _recommend(summary_all, summary_2y)
    top_names = recommended["牛散"].head(top_n).tolist()

    relevant_dates = get_relevant_report_dates()
    meta = load_last_analysis_meta()
    max_rpt = meta.get("max_report_per_niusan") or {}

    holdings = load_local_holdings()
    if holdings.empty:
        return pd.DataFrame()

    # 筛选：推荐牛散 +（报告期在本期关注列表 或 报告期晚于上次该牛散最大报告期）
    def _in_scope(row):
        rpt = str(row["报告期"])[:10]
        if rpt in relevant_dates:
            return True
        last = max_rpt.get(row["牛散"], "")
        return bool(last and rpt > last)

    h = holdings[holdings["牛散"].isin(top_names)].copy()
    mask = h.apply(_in_scope, axis=1)
    h = h.loc[mask].copy()
    if h.empty:
        logger.info("本期无在 scope 内的持仓记录")
        return pd.DataFrame()

    # 与收益明细对齐，取跟踪成本价、报告期收盘价
    detail_cols = ["牛散", "股票代码", "报告期", "跟踪成本价", "报告期收盘价"]
    detail_sub = detail[[c for c in detail_cols if c in detail.columns]].copy()
    detail_sub["报告期"] = detail_sub["报告期"].astype(str).str[:10]
    h["报告期"] = h["报告期"].astype(str).str[:10]
    merged = h.merge(
        detail_sub,
        on=["牛散", "股票代码", "报告期"],
        how="left",
    )
    # 是否本期新增：报告期 > 上次该牛散最大报告期
    merged["本期新增"] = merged.apply(
        lambda r: (max_rpt.get(r["牛散"]) or "") < str(r["报告期"])[:10],
        axis=1,
    )

    # 最新价、至今涨幅（至今涨幅 = (最新价 - 报告日收盘价) / 报告日收盘价）
    codes = merged["股票代码"].unique().tolist()
    price_map = {}
    for code in codes:
        p = get_latest_price(code)
        if p is not None:
            price_map[str(code).zfill(6)] = p
    merged["最新价"] = merged["股票代码"].apply(lambda c: price_map.get(str(c).zfill(6)))
    report_close = merged["报告期收盘价"] if "报告期收盘价" in merged.columns else None
    merged["至今涨幅"] = None
    if report_close is not None and "最新价" in merged.columns:
        mask = report_close.notna() & merged["最新价"].notna() & (report_close > 0)
        merged.loc[mask, "至今涨幅"] = (
            (merged.loc[mask, "最新价"] - report_close.loc[mask]) / report_close.loc[mask]
        )

    cleanup_bs()

    # 星级、综合推荐值
    rank_map = {n: i + 1 for i, n in enumerate(top_names)}
    merged["推荐排名"] = merged["牛散"].map(rank_map)
    merged["星级"] = merged["推荐排名"].map(
        lambda r: "★★★" if r <= 5 else ("★★" if r <= 10 else "★")
    )

    # 综合推荐值：0.4*星级归一化(1~5->0.2~1) + 0.3*近2年3月胜率 + 0.3*至今涨幅归一化(cap 50%)
    rec_map = recommended.set_index("牛散")
    merged["_wr3"] = merged["牛散"].map(
        lambda n: rec_map.loc[n, "近2年_跟单3月_胜率"] if n in rec_map.index else 0
    )
    merged["_ret_norm"] = merged["至今涨幅"].fillna(0).astype(float).clip(-0.5, 0.5)  # -50%~50% -> 归一化
    merged["_star_norm"] = merged["推荐排名"].map(lambda r: max(0, 1 - (r - 1) * 0.08))  # 1->1, 10->0.28
    merged["综合推荐值"] = (
        0.4 * merged["_star_norm"]
        + 0.3 * merged["_wr3"].fillna(0)
        + 0.3 * (merged["_ret_norm"].fillna(0) + 0.5)  # [0,1]
    ).round(3)
    merged = merged.drop(columns=["_wr3", "_ret_norm", "_star_norm"], errors="ignore")

    # 占位列（后续可接 F10）
    merged["股东人数变化"] = "—"
    merged["营收同比"] = "—"
    merged["利润同比"] = "—"

    merged.sort_values(["推荐排名", "综合推荐值", "报告期"], ascending=[True, False, False], inplace=True)
    merged.reset_index(drop=True, inplace=True)

    logger.info("最新跟单建议: %d 条 (本期报告期: %s)", len(merged), relevant_dates)
    return merged
