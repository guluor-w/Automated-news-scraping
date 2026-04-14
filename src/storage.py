"""
数据持久化：CSV 读写与 RSS Feed 生成。

提供以下功能：
- load_existing()  — 读取已有 CSV，不存在时返回空 DataFrame
- dedup_merge()    — 将新抓取的 Item 列表与已有数据合并去重
- generate_rss()   — 将 DataFrame 导出为 RSS 2.0 XML 文件
"""

import os
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

import pandas as pd

from models import Item, SG_TZ

# GitHub Pages 发布地址（用于 RSS <link> 字段）
_PAGES_URL = "https://guluor-w.github.io/Automated-news-scraping/"


def load_existing(csv_path: str) -> pd.DataFrame:
    """读取已有 CSV 文件；若文件不存在则返回含标准列名的空 DataFrame。"""
    if not os.path.exists(csv_path):
        return pd.DataFrame(columns=["标题", "发布单位", "新闻URL", "发布日期", "来源", "查询时间"])

    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    df.columns = [str(col).lstrip("\ufeff") for col in df.columns]
    return df
def dedup_merge(existing: pd.DataFrame, new_items: List[Item]) -> Tuple[pd.DataFrame, int]:
    """
    将 new_items 追加到 existing，按 URL 去重，并按发布日期降序排序。

    Returns:
        (merged_df, added_count) — 合并后的 DataFrame 以及本次新增条数。
    """
    if existing.empty:
        existing_urls = set()
    else:
        existing_urls = set(existing["新闻URL"].astype(str).tolist())

    rows = []
    added = 0
    for it in new_items:
        if it.url in existing_urls:
            continue
        rows.append({
            "标题": it.title,
            "发布单位": it.publisher,
            "新闻URL": it.url,
            "发布日期": it.pub_date or "",
            "来源": it.source,
            "查询时间": it.fetched_at,
        })
        existing_urls.add(it.url)
        added += 1

    if rows:
        new_df = pd.concat([existing, pd.DataFrame(rows)], ignore_index=True)
    else:
        new_df = existing.copy()

    # 按发布日期（空值排后）和查询时间排序
    def sort_key(row):
        d = row.get("发布日期", "")
        return d if d else "0000-00-00"

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
    """将 YYYY-MM-DD 日期字符串转换为 RSS 所需的 RFC 822 格式。"""
    try:
        dt = datetime.strptime(pub_date, "%Y-%m-%d").replace(tzinfo=SG_TZ)
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
