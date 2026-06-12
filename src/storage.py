"""
数据持久化：CSV 读写与 RSS Feed 生成。

提供以下功能：
- load_existing()  — 读取已有 CSV，不存在时返回空 DataFrame
- dedup_merge()    — 将新抓取的 Item 列表与已有数据合并去重
- generate_rss()   — 将 DataFrame 导出为 RSS 2.0 XML 文件
"""

import os
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from dateutil import parser as dtparser

from models import Item, SG_TZ
from utils import (
    canonicalize_url_for_dedup,
    clean_title,
    normalize_fetched_at,
    normalize_pub_date,
    normalize_publisher,
)

# GitHub Pages 发布地址（用于 RSS <link> 字段）
_PAGES_URL = "https://guluor-w.github.io/Automated-news-scraping/"
# 发布日期缺失时用于排序的哨兵值（使用 pandas 可表示范围内的时间戳）
_DATE_SENTINEL = pd.Timestamp.min

# 标题指纹归一化：去除标点/空白后用于二级去重比对
_TITLE_FINGERPRINT_STRIP = re.compile(
    r"[\s\u3000\W_]+", flags=re.UNICODE
)

# 来源权重：数值越高越优先保留（用于二级去重的择优）。
# 官方/部委 > 央企/地方 > 微博/聚合搜索。
_SOURCE_WEIGHTS = (
    ("工业和信息化部", 100),
    ("中国政府网", 95),
    ("国务院国有资产监督管理委员会", 90),
    ("国家数据局", 90),
    ("国家发展和改革委员会", 90),
    ("科学技术部", 85),
    ("教育部", 85),
    ("央企-", 70),
    ("地方政府-", 60),
    ("地方工信-", 60),
    ("官网监控-", 55),
    ("腾讯新闻", 30),
    ("微博-", 20),
)


def _title_fingerprint(title: str) -> str:
    """生成用于二级去重比对的标题指纹（去除标点、空白、大小写差异）。"""
    if not title:
        return ""
    t = clean_title(title).lower()
    return _TITLE_FINGERPRINT_STRIP.sub("", t)


def _source_weight(source: str) -> int:
    """根据来源字符串给出权重；未命中映射时返回默认 50。"""
    s = str(source or "")
    for prefix, w in _SOURCE_WEIGHTS:
        if prefix in s:
            return w
    return 50


def load_existing(csv_path: str) -> pd.DataFrame:
    """读取已有 CSV 文件；若文件不存在则返回含标准列名的空 DataFrame。"""
    if not os.path.exists(csv_path):
        return pd.DataFrame(columns=["标题", "发布单位", "新闻URL", "发布日期", "来源", "查询时间"])

    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    df.columns = [str(col).lstrip("\ufeff") for col in df.columns]
    return df


def dedup_merge(
    existing: pd.DataFrame,
    new_items: List[Item],
    publisher_alias: Optional[Dict[str, str]] = None,
) -> Tuple[pd.DataFrame, int]:
    """
    将 new_items 追加到 existing，按 URL 去重，并按发布日期降序排序。

    去重策略：
    1. 一级去重：以 canonicalize_url_for_dedup 的结果为键，URL 完全等价则丢弃后入；
    2. 二级去重：在 URL 不同但标题指纹 + 发布日期相同的情况下，
       按来源权重择优保留，其余记录丢弃。

    Args:
        existing:        既有 CSV 内容。
        new_items:       本次抓取得到的 Item 列表。
        publisher_alias: 发布单位别名映射，由调用方从 config.yaml 读取；
                         为 None 时仅做轻量字符串清洗，不做归一替换。

    Returns:
        (merged_df, added_count) — 合并后的 DataFrame 以及本次新增条数。
    """
    alias_map = publisher_alias or {}

    # ── 1. 既有数据 URL 集合
    if existing.empty:
        existing_urls: set = set()
    else:
        existing_urls = {
            canonicalize_url_for_dedup(u)
            for u in existing["新闻URL"].astype(str).tolist()
        }

    # ── 2. 一级 URL 去重：将新条目追加进 DataFrame
    rows = []
    added = 0
    for it in new_items:
        canonical = canonicalize_url_for_dedup(it.url)
        if canonical in existing_urls:
            continue
        rows.append({
            "标题": clean_title(it.title),
            "发布单位": normalize_publisher(it.publisher, alias_map),
            "新闻URL": it.url,  # 保留原始 URL，不写入规范化结果
            "发布日期": it.pub_date or "",
            "来源": it.source,
            "查询时间": it.fetched_at,
        })
        existing_urls.add(canonical)
        added += 1

    if rows:
        new_df = pd.concat([existing, pd.DataFrame(rows)], ignore_index=True)
    else:
        new_df = existing.copy()

    # ── 3. 字段统一清洗（对全表执行，使历史脏数据也被回填）
    if not new_df.empty:
        if "标题" in new_df.columns:
            new_df["标题"] = new_df["标题"].fillna("").astype(str).apply(clean_title)
        if "发布单位" in new_df.columns:
            new_df["发布单位"] = (
                new_df["发布单位"]
                .fillna("")
                .astype(str)
                .apply(lambda v: normalize_publisher(v, alias_map))
            )
        if "发布日期" in new_df.columns:
            new_df["发布日期"] = (
                new_df["发布日期"].fillna("").astype(str).apply(normalize_pub_date)
            )
        if "查询时间" in new_df.columns:
            now = datetime.now(tz=SG_TZ)
            new_df["查询时间"] = (
                new_df["查询时间"]
                .fillna("")
                .astype(str)
                .apply(lambda v: normalize_fetched_at(v, fallback=now))
            )

    # ── 4. 二级去重：标题指纹 + 发布日期 相同时按来源权重择优
    if not new_df.empty:
        new_df["__fp"] = new_df["标题"].astype(str).apply(_title_fingerprint)
        new_df["__weight"] = new_df["来源"].astype(str).apply(_source_weight)
        # 保留指纹为空（无法生成签名）的行；其余按 (指纹, 发布日期) 分组取权重最高
        mask_no_fp = new_df["__fp"] == ""
        keepers = new_df[mask_no_fp]
        candidates = new_df[~mask_no_fp]
        if not candidates.empty:
            candidates = candidates.sort_values(
                by=["__weight", "查询时间"], ascending=[False, False]
            ).drop_duplicates(subset=["__fp", "发布日期"], keep="first")
        new_df = pd.concat([keepers, candidates], ignore_index=True)
        new_df = new_df.drop(columns=["__fp", "__weight"])

    # ── 5. 按发布日期（空值排后）和查询时间排序
    def sort_key(row):
        d = row.get("发布日期", "")
        if not d:
            return _DATE_SENTINEL
        try:
            return dtparser.parse(d)
        except (ValueError, TypeError):
            return _DATE_SENTINEL

    if not new_df.empty:
        new_df["__sortdate"] = new_df.apply(sort_key, axis=1)
        new_df = new_df.sort_values(
            by=["__sortdate", "查询时间"], ascending=[False, False]
        ).drop(columns=["__sortdate"])

    return new_df, added


# ─────────────── RSS 生成 ───────────────

def _xml_escape(s: str) -> str:
    """对 XML 文本内容转义特殊字符。"""
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&apos;")
    )


def _pub_date_to_rfc822(pub_date: str) -> str:
    """将日期字符串（YYYY-MM-DD）转换为 RSS 所需的 RFC 822 格式。"""
    try:
        dt = dtparser.parse(pub_date).replace(tzinfo=SG_TZ)
        return dt.strftime("%a, %d %b %Y 00:00:00 +0800")
    except (ValueError, TypeError):
        return ""


def generate_rss(
    df: "pd.DataFrame",
    out_path: Path,
    title: str,
    description: str,
    link: str = _PAGES_URL,
) -> None:
    """
    将 DataFrame 导出为 RSS 2.0 XML 文件。

    Args:
        df:          包含标题、新闻URL、发布日期、来源、发布单位列的 DataFrame。
        out_path:    输出 XML 文件路径（父目录不存在时自动创建）。
        title:       RSS 频道标题。
        description: RSS 频道描述。
        link:        RSS 频道链接，默认为 GitHub Pages 地址。
    """
    build_date = datetime.now(tz=SG_TZ).strftime("%a, %d %b %Y %H:%M:%S +0800")

    # 将 NaN 替换为空字符串，防止序列化为 "nan"
    df = df.fillna("")

    items_xml: List[str] = []
    for _, row in df.iterrows():
        item_title = _xml_escape(str(row.get("标题", "")))
        item_link = _xml_escape(str(row.get("新闻URL", "")))
        item_pub_date = _pub_date_to_rfc822(str(row.get("发布日期", "")))
        item_source = _xml_escape(str(row.get("来源", "")))
        item_publisher = _xml_escape(str(row.get("发布单位", "")))

        item_parts = [
            "    <item>",
            f"      <title>{item_title}</title>",
        ]
        if item_link:
            item_parts.append(f"      <link>{item_link}</link>")
            item_parts.append(f'      <guid isPermaLink="true">{item_link}</guid>')
        if item_pub_date:
            item_parts.append(f"      <pubDate>{item_pub_date}</pubDate>")
        item_parts.append(
            f"      <description>{item_publisher}｜{item_source}</description>"
        )
        item_parts.append("    </item>")
        items_xml.append("\n".join(item_parts))

    feed = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<rss version="2.0">\n'
        '  <channel>\n'
        f'    <title>{_xml_escape(title)}</title>\n'
        f'    <link>{_xml_escape(link)}</link>\n'
        f'    <description>{_xml_escape(description)}</description>\n'
        '    <language>zh-CN</language>\n'
        f'    <lastBuildDate>{build_date}</lastBuildDate>\n'
        + "\n".join(items_xml)
        + "\n  </channel>\n</rss>\n"
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(feed)
