"""
中国政府网（gov.cn）解析器。

入口函数
--------
parse_gov_home(config, now) -> List[Item]
    抓取 https://www.gov.cn/ 首页，可选地进入文章页补充发布日期，
    按关键词和时间窗口过滤后返回新闻列表。

parse_gov_rss(config, now) -> List[Item]
    解析 RSSHub 提供的中国政府网最新政策 RSS 源，
    按关键词和时间窗口过滤后返回新闻列表。

注意：gov.cn 相关函数均不使用"印发"和"体系建设"关键词进行过滤。

页面/RSS 配置（config.yaml）
-----------------------------
sources:
  gov_home:
    name: 中国政府网
    url: https://www.gov.cn/
  gov_latest_policy_rss:
    name: 中国政府网
    rss: https://rsshub.app/gov/zhengce/zuixin

resolve_pub_date: true         # 是否进入文章页补发布日期（仅 gov_home）
resolve_pub_date_cap: 30       # 最多补日期的文章数量（仅 gov_home）

新增来源提示
------------
若需在此模块扩展更多 gov.cn 子频道，只需在 parse_gov_home 内
调用 add_links(selector, source_tag) 即可。
"""

import re
from datetime import datetime
from typing import Dict, List, Optional

from bs4 import BeautifulSoup
from dateutil import parser as dtparser

from models import Item, MIIT_ONLY_KEYWORDS
from utils import (
    canonicalize_url_for_dedup,
    extract_date,
    format_fetched_at,
    http_get,
    keyword_hit,
    normalize_url,
    within_window,
)

# ── 日期提取辅助（gov.cn 专用） ────────────────────────────────────────────────

_RE_GOV_YYYYMM = re.compile(r"/(20\d{2})(0[1-9]|1[0-2])/(?:content_|index\.htm|)$")
_RE_GOV_ANY_YYYYMM = re.compile(r"/(20\d{2})(0[1-9]|1[0-2])/")
_RE_DATE_YMD = re.compile(
    r"(20\d{2})[.\-/年](0?[1-9]|1[0-2])[.\-/月](0?[1-9]|[12]\d|3[01])日?"
)


def extract_gov_date_from_url(url: str) -> Optional[str]:
    """从 gov.cn URL 路径中粗略提取年月（日默认为 01）。"""
    m = _RE_GOV_YYYYMM.search(url)
    if not m:
        m = _RE_GOV_ANY_YYYYMM.search(url)
    if not m:
        return None
    yyyy, mm = m.group(1), m.group(2)
    return f"{yyyy}-{int(mm):02d}-01"


def extract_gov_pub_date_from_article_html(article_html: str) -> Optional[str]:
    """
    从文章页 HTML 中提取发布日期。
    优先查找"发布时间/日期/时间/稿源"附近的日期，其次取全文第一个合法日期。
    """
    text = article_html

    ctx_patterns = [
        r"发布时间[:：\s]*" + _RE_DATE_YMD.pattern,
        r"日期[:：\s]*" + _RE_DATE_YMD.pattern,
        r"时间[:：\s]*" + _RE_DATE_YMD.pattern,
        r"稿源[:：\s\S]{0,40}?" + _RE_DATE_YMD.pattern,
    ]
    for pat in ctx_patterns:
        m = re.search(pat, text)
        if m:
            y, mo, d = m.group(1), m.group(2).zfill(2), m.group(3).zfill(2)
            return f"{y}-{int(mo):02d}-{int(d):02d}"

    m = _RE_DATE_YMD.search(text)
    if m:
        y, mo, d = m.group(1), m.group(2).zfill(2), m.group(3).zfill(2)
        return f"{y}-{int(mo):02d}-{int(d):02d}"

    return None


# ── parse_gov_home ─────────────────────────────────────────────────────────────

def parse_gov_home(config: dict, now: datetime) -> List[Item]:
    """
    抓取中国政府网首页并返回符合条件的新闻列表。

    Args:
        config: 来自 config.yaml 的全量配置字典。
        now:    当前时间（带时区）。

    Returns:
        过滤并去重后的 Item 列表。
    """
    src = config["sources"]["gov_home"]
    base_url = src["url"]

    html = http_get(base_url)
    soup = BeautifulSoup(html, "lxml")

    fetched_at = format_fetched_at(now)
    items: List[Item] = []

    keywords = config["keywords"]
    window_days = int(config["window_days"])
    hard_cap_days = int(config["hard_cap_days"])

    resolve_pub_date = bool(config.get("resolve_pub_date", True))
    resolve_cap = int(config.get("resolve_pub_date_cap", 60))

    pub_cache: Dict[str, Optional[str]] = {}

    def resolve_pub_date_for_url(url: str) -> Optional[str]:
        if url in pub_cache:
            return pub_cache[url]
        try:
            art_html = http_get(url)
            d = extract_gov_pub_date_from_article_html(art_html)
            pub_cache[url] = d
            return d
        except Exception:
            pub_cache[url] = None
            return None

    def build_item(title: str, href: str, source_tag: str) -> Optional[Item]:
        title = (title or "").strip()
        href = (href or "").strip()
        if not title or len(title) < 4 or not href:
            return None

        url = normalize_url(base_url, href)

        pub_date = extract_date(title)
        if not pub_date:
            pub_date = extract_gov_date_from_url(url)

        return Item(
            title=title,
            publisher=src["name"],
            url=url,
            pub_date=pub_date,
            source=source_tag,
            fetched_at=fetched_at,
        )

    def add_links(selector: str, source_tag: str):
        for a in soup.select(selector):
            href = a.get("href")
            if not href:
                continue
            title = a.get_text(" ", strip=True)
            it = build_item(title, href, source_tag)
            if it:
                items.append(it)

    # ── 板块抓取 ────────────────────────────────────────────────────────────
    add_links("#index_tpxw .slider_carousel .item h4 a[href]", "中国政府网-焦点图片")
    add_links("#index_ywowen ul li a[href]", "中国政府网-要闻")
    add_links("#index_zxzc ul li a[href]", "中国政府网-最新政策")
    add_links("#index_zcjd ul li a[href]", "中国政府网-政策解读")
    add_links("#index_gwygzjxs ul.ul1 li a[href]", "中国政府网-国新办")
    add_links("#index_zwlb ul.ul2 li a[href]", "中国政府网-政务联播")
    add_links("#index_jyzj ul.ul01 li a[href]", "中国政府网-建言征集/回应关切")

    # ── 补齐真实发布时间 ──────────────────────────────────────────────────────
    if resolve_pub_date:
        temp_uniq: Dict[str, Item] = {}
        for it in items:
            k = canonicalize_url_for_dedup(it.url)
            if k not in temp_uniq:
                temp_uniq[k] = it
        uniq_items = list(temp_uniq.values())

        for it in uniq_items[:resolve_cap]:
            d = resolve_pub_date_for_url(it.url)
            if d:
                it.pub_date = d

        items = uniq_items

    # ── 过滤（gov_home 不使用"印发"和"体系建设"关键词） ───────────────────────
    gov_keywords = [k for k in keywords if k not in MIIT_ONLY_KEYWORDS]

    filtered: List[Item] = []
    for it in items:
        if not keyword_hit(it.title, gov_keywords):
            continue
        if it.pub_date:
            if not within_window(it.pub_date, now, window_days, hard_cap_days):
                continue
        filtered.append(it)

    # ── 去重 ──────────────────────────────────────────────────────────────────
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


# ── parse_gov_rss ──────────────────────────────────────────────────────────────

def parse_gov_rss(config: dict, now: datetime) -> List[Item]:
    """
    解析 gov_latest_policy_rss RSS 源（由 RSSHub 提供）。

    注意：本渠道同样不使用"印发"和"体系建设"关键词过滤。

    Args:
        config: 来自 config.yaml 的全量配置字典。
        now:    当前时间（带时区）。

    Returns:
        过滤后的 Item 列表；若来源未配置或抓取失败则返回空列表。
    """
    src = config["sources"].get("gov_latest_policy_rss")
    if not src:
        return []

    rss_url = src.get("rss")
    if not rss_url:
        return []

    try:
        xml_content = http_get(rss_url)
    except Exception:
        return []

    soup = BeautifulSoup(xml_content, "xml")

    fetched_at = format_fetched_at(now)
    items: List[Item] = []

    raw_keywords = config.get("keywords", [])
    rss_keywords = [k for k in raw_keywords if k not in MIIT_ONLY_KEYWORDS]

    window_days = int(config.get("window_days", 15))
    hard_cap_days = int(config.get("hard_cap_days", 15))

    for entry in soup.find_all("item"):
        title_tag = entry.find("title")
        link_tag = entry.find("link")
        pub_date_tag = entry.find("pubDate")

        title = (title_tag.get_text() if title_tag else "").strip()
        link = (link_tag.get_text() if link_tag else "").strip()
        pub_date_str = (pub_date_tag.get_text() if pub_date_tag else "").strip()

        if not title or not link:
            continue

        pub_date = None
        if pub_date_str:
            try:
                dt = dtparser.parse(pub_date_str)
                pub_date = dt.strftime("%Y-%m-%d")
            except Exception:
                pass

        if not keyword_hit(title, rss_keywords):
            continue

        if not within_window(pub_date, now, window_days, hard_cap_days):
            continue

        items.append(Item(
            title=title,
            publisher=src.get("name", "中国政府网"),
            url=link,
            pub_date=pub_date,
            source="GOV-最新政策-RSS",
            fetched_at=fetched_at,
        ))

    return items
