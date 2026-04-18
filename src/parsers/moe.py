"""
教育部官网（moe.gov.cn）解析器。

入口函数
--------
parse_moe_news(config, now) -> List[Item]
    抓取 http://www.moe.gov.cn/jyb_xwfb/ 新闻发布页，
    按关键词和时间窗口过滤后返回新闻列表。

页面结构说明
------------
新闻发布页包含多个板块：
  1. 工作动态 (gzdt_gzdt)
  2. 政策解读 (s271)
  3. 媒体聚焦 (s5147)
  4. 教育评论 (s5148)
  5. 战线联播 (s6192)
  6. 发布会/通气会 (xw_fbh)
  7. 图解教育 (s7600)
  8. 图说新闻 (s5984)

链接格式示例：
  http://www.moe.gov.cn/jyb_xwfb/gzdt_gzdt/YYYYMM/tYYYYMMDD_XXXXXXX.html
  http://www.moe.gov.cn/jyb_xwfb/s271/YYYYMM/tYYYYMMDD_XXXXXXX.html

日期提取策略：
  - 优先从 URL 路径中的 /YYYYMM/tYYYYMMDD_ 模式提取精确日期
  - 其次从链接附近文本中的 MM-DD 格式提取（补当前年份）
  - 再次回退到 URL 路径中的 /YYYYMM/ 年月（日默认 01）
  - 最后尝试从标题文本提取

页面/RSS 配置（config.yaml）
-----------------------------
sources:
  moe_news:
    name: 教育部
    url: http://www.moe.gov.cn/jyb_xwfb/
"""

import re
from datetime import datetime
from typing import Dict, List, Optional

from bs4 import BeautifulSoup
from dateutil import parser as dtparser

from models import Item
from utils import (
    canonicalize_url_for_dedup,
    extract_date,
    format_fetched_at,
    http_get,
    keyword_hit,
    normalize_url,
    within_window,
)

# ── URL 路径 → 板块映射 ────────────────────────────────────────────────────────

_SECTION_MAP = {
    "gzdt_gzdt": "工作动态",
    "s271":      "政策解读",
    "s5147":     "媒体聚焦",
    "s5148":     "教育评论",
    "s6192":     "战线联播",
    "xw_fbh":    "发布会/通气会",
    "s7600":     "图解教育",
    "s5984":     "图说新闻",
}

# ── 日期提取正则（moe.gov.cn 专用） ────────────────────────────────────────────

# /YYYYMM/tYYYYMMDD_  —— 精确到日
_RE_MOE_FULL_DATE = re.compile(r"/(\d{4})(\d{2})/t(\d{4})(\d{2})(\d{2})_")
# /YYYYMM/  —— 仅年月
_RE_MOE_YYYYMM = re.compile(r"/(20\d{2})(0[1-9]|1[0-2])/")
# 页面文本中的 MM-DD 格式
_RE_MMDD = re.compile(r"(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])")


def _extract_precise_date_from_url(url: str) -> Optional[str]:
    """从 moe.gov.cn URL 路径中提取精确日期（tYYYYMMDD_ 模式），无则返回 None。"""
    m = _RE_MOE_FULL_DATE.search(url)
    if m:
        yyyy, mm, dd = m.group(3), m.group(4), m.group(5)
        return f"{yyyy}-{mm}-{dd}"
    return None


def _extract_yyyymm_from_url(url: str) -> Optional[str]:
    """从 moe.gov.cn URL 路径中提取年月，返回 YYYY-MM-01（日默认 01）。"""
    m = _RE_MOE_YYYYMM.search(url)
    if m:
        return f"{m.group(1)}-{m.group(2)}-01"
    return None


def _extract_date_from_text(text: str, year: int) -> Optional[str]:
    """从附近文本中提取 MM-DD 并补当前年份。"""
    m = _RE_MMDD.search(text)
    if m:
        mm, dd = m.group(1), m.group(2)
        return f"{year}-{mm}-{dd}"
    return None


def _classify_section(href: str) -> Optional[str]:
    """根据 URL 路径判断所属板块，返回中文标签或 None。"""
    for key, label in _SECTION_MAP.items():
        if key in href:
            return label
    return None


# ── 主入口 ────────────────────────────────────────────────────────────────────

def parse_moe_news(config: dict, now: datetime) -> List[Item]:
    """
    抓取教育部新闻发布页并返回符合条件的新闻列表。

    Args:
        config: 来自 config.yaml 的全量配置字典。
        now:    当前时间（带时区）。

    Returns:
        过滤并去重后的 Item 列表。
    """
    src = config["sources"]["moe_news"]
    base_url = src["url"]
    html = http_get(base_url)
    soup = BeautifulSoup(html, "lxml")

    fetched_at = format_fetched_at(now)
    items: List[Item] = []

    keywords = config["keywords"]
    window_days = int(config["window_days"])
    hard_cap_days = int(config["hard_cap_days"])
    current_year = now.year

    # ── 内部辅助函数 ──────────────────────────────────────────────────────────

    def build_item(
        title: str,
        href: str,
        pub_date: Optional[str],
        source_tag: str,
    ) -> Optional[Item]:
        title = (title or "").strip()
        href = (href or "").strip()
        if not title or len(title) < 6 or not href:
            return None
        url = normalize_url(base_url, href)
        return Item(
            title=title,
            publisher=src["name"],
            url=url,
            pub_date=pub_date,
            source=source_tag,
            fetched_at=fetched_at,
        )

    # ── 遍历所有 <a> 标签 ────────────────────────────────────────────────────

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]

        # 仅处理属于已知板块的链接
        section_label = _classify_section(href)
        if section_label is None:
            continue

        title = a_tag.get_text(" ", strip=True)
        source_tag = f"教育部官网-{section_label}"

        # 日期提取策略（精确度从高到低）：
        # 1. URL 中的精确日期（tYYYYMMDD_ 模式）
        # 2. 链接附近文本中的 MM-DD（避免被 YYYYMM 回退抢先）
        # 3. URL 中的 YYYYMM → YYYY-MM-01 回退
        # 4. 标题文本中的日期
        pub_date = _extract_precise_date_from_url(href)
        if not pub_date:
            # 尝试从链接周围的文本（父元素）中提取 MM-DD
            parent = a_tag.parent
            if parent:
                sibling_text = parent.get_text(" ", strip=True)
                pub_date = _extract_date_from_text(sibling_text, current_year)
        if not pub_date:
            pub_date = _extract_yyyymm_from_url(href)
        if not pub_date:
            pub_date = extract_date(title)

        it = build_item(title, href, pub_date, source_tag)
        if it:
            items.append(it)

    # ── 过滤（关键词 + 时间窗口） ─────────────────────────────────────────────
    filtered: List[Item] = []
    for it in items:
        if not keyword_hit(it.title, keywords):
            continue
        if not within_window(it.pub_date, now, window_days, hard_cap_days):
            continue
        filtered.append(it)

    # ── 去重（URL 规范化） ────────────────────────────────────────────────────
    uniq: Dict[str, Item] = {}
    for it in filtered:
        key = canonicalize_url_for_dedup(it.url)
        if key not in uniq:
            uniq[key] = it
        else:
            old = uniq[key]
            if (not old.pub_date) and it.pub_date:
                uniq[key] = it
            elif old.pub_date and it.pub_date:
                try:
                    if dtparser.parse(it.pub_date) > dtparser.parse(old.pub_date):
                        uniq[key] = it
                except Exception:
                    pass

    return list(uniq.values())
