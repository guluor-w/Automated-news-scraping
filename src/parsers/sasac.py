"""
国务院国有资产监督管理委员会官网（sasac.gov.cn）解析器。

入口函数
--------
parse_sasac_home(config, now) -> List[Item]
    抓取 http://www.sasac.gov.cn/ 首页，按关键词和时间窗口过滤后返回新闻列表。

页面结构说明
------------
首页包含以下板块：
  1. 时政要闻 / 国资要闻 — 切换标签
  2. 监管动态 — 国企党建、国企改革、资本运营、社会责任
  3. 政务公开
  4. 专题

URL 格式：TRS WCM，使用 Channel-ID 路径
  /nCHANNEL/cCONTENT/content.html
  例如 /n2588025/n2588129/c35407052/content.html

日期显示：[MM-DD] 格式（无年份），需结合当前年份推断。

配置（config.yaml）
-------------------
sources:
  sasac_home:
    name: 国务院国有资产监督管理委员会
    url: http://www.sasac.gov.cn/
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

# ── URL 识别 ─────────────────────────────────────────────────────────────────

# TRS Channel-Content 路径 /nNNN/cNNN/content.html
_RE_CHANNEL_CONTENT = re.compile(r"/n\d+/c\d+/content\.html")
# 备用：带日期的 TRS 路径（部分二级页面）
_RE_SASAC_DATE = re.compile(r"/(20\d{2})(0[1-9]|1[0-2])/")

# ── 日期提取 ─────────────────────────────────────────────────────────────────

# 页面文本中的 [MM-DD] 或 MM-DD 格式
_RE_BRACKET_MMDD = re.compile(r"\[?(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])\]?")
# YYYY-MM-DD 或 YYYY.MM.DD 完整日期
_RE_FULL_DATE = re.compile(
    r"(20\d{2})[.\-/](0?[1-9]|1[0-2])[.\-/](0?[1-9]|[12]\d|3[01])"
)


def _extract_date_from_context(a_tag, year: int) -> Optional[str]:
    """从 <a> 标签的相邻文本中提取日期。SASAC 页面使用 [MM-DD] 格式。"""
    # 1) 紧邻的兄弟 <span>
    for sibling_fn in (a_tag.find_next_sibling, a_tag.find_previous_sibling):
        sib = sibling_fn("span")
        if sib:
            text = sib.get_text(" ", strip=True)
            # 先尝试完整日期
            m = _RE_FULL_DATE.search(text)
            if m:
                return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
            # 再尝试 [MM-DD]
            m = _RE_BRACKET_MMDD.search(text)
            if m:
                return f"{year}-{m.group(1)}-{m.group(2)}"

    # 2) 父元素全文
    parent = a_tag.parent
    if parent:
        text = parent.get_text(" ", strip=True)
        m = _RE_FULL_DATE.search(text)
        if m:
            return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
        m = _RE_BRACKET_MMDD.search(text)
        if m:
            return f"{year}-{m.group(1)}-{m.group(2)}"

    return None


# ── 主入口 ────────────────────────────────────────────────────────────────────

def parse_sasac_home(config: dict, now: datetime) -> List[Item]:
    """抓取国资委首页并返回符合条件的新闻列表。"""
    src = config["sources"]["sasac_home"]
    base_url = src["url"]
    html = http_get(base_url)
    soup = BeautifulSoup(html, "lxml")

    fetched_at = format_fetched_at(now)
    items: List[Item] = []
    keywords = config["keywords"]
    window_days = int(config["window_days"])
    hard_cap_days = int(config["hard_cap_days"])
    current_year = now.year

    for a_tag in soup.find_all("a", href=True):
        href = (a_tag.get("href") or "").strip()
        if not href or href.startswith("#") or href.startswith("javascript"):
            continue

        title = a_tag.get("title") or a_tag.get_text(" ", strip=True)
        title = (title or "").strip()
        if not title or len(title) < 6:
            continue

        url = normalize_url(base_url, href)

        # 仅保留 Channel-Content 格式的文章链接
        if not _RE_CHANNEL_CONTENT.search(url):
            continue

        # 日期提取
        pub_date = _extract_date_from_context(a_tag, current_year)
        if not pub_date:
            pub_date = extract_date(title)

        # 关键词过滤
        if not keyword_hit(title, keywords):
            continue

        # 时间窗口过滤
        if not within_window(pub_date, now, window_days, hard_cap_days):
            continue

        items.append(Item(
            title=title,
            publisher=src["name"],
            url=url,
            pub_date=pub_date,
            source="国资委官网",
            fetched_at=fetched_at,
        ))

    # ── 去重 ──────────────────────────────────────────────────────────────────
    uniq: Dict[str, Item] = {}
    for it in items:
        key = canonicalize_url_for_dedup(it.url)
        if key not in uniq:
            uniq[key] = it
        else:
            old = uniq[key]
            if (not old.pub_date) and it.pub_date:
                uniq[key] = it

    return list(uniq.values())
