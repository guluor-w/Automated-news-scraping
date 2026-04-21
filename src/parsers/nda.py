"""
国家数据局官网（nda.gov.cn）解析器。

入口函数
--------
parse_nda_home(config, now) -> List[Item]
    抓取 https://www.nda.gov.cn/sjj/index_pc.html 首页，
    按关键词和时间窗口过滤后返回新闻列表。

页面结构说明
------------
首页包含以下板块（Tab 切换）：
  ■ 时政要闻  — 外部 gov.cn 链接，跳过
  ■ 新闻发布  /sjj/swdt/xwfb/
  ■ 通知公告  /sjj/zwgk/tzgg/
  ■ 领导活动  /sjj/jgsz/jld/ 或 /sjj/swdt/jlddt/
  ■ 司局动态  /sjj/swdt/sjdt/
  ■ 地方动态  /sjj/swdt/dfdt/
  ■ 媒体声音  /sjj/swdt/mtsy/
  ■ 政策发布  /sjj/zwgk/zcfb/
  ■ 政策解读  /sjj/zwgk/zcjd/
  ■ 专家解读  /sjj/zwgk/zjjd/
  ■ 业务频道  /sjj/ywpd/

URL 格式：自建 CMS，时间戳文件名
  /sjj/{section}/{MMDD}/{YYYYMMDDHHmmssSSS}_pc.html
  例如 /sjj/swdt/xwfb/0415/20260415190239098954885_pc.html

日期显示：YYYY.MM.DD 格式，位于 <a> 相邻的 <span> 中。

配置（config.yaml）
-------------------
sources:
  nda_home:
    name: 国家数据局
    url: https://www.nda.gov.cn/sjj/index_pc.html
"""

import re
from datetime import datetime
from typing import Dict, List, Optional

from bs4 import BeautifulSoup

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

# ── URL 识别 ─────────────────────────────────────────────────────────────────

# NDA 时间戳文件名：{17位数字}_pc.html
_RE_NDA_ARTICLE = re.compile(r"/sjj/.+/\d{17,}_pc\.html")

# ── 日期提取 ─────────────────────────────────────────────────────────────────

# 从 URL 文件名中提取日期：/MMDD/YYYYMMDDHHmmss..._pc.html
_RE_NDA_URL_DATE = re.compile(
    r"/(20\d{2})(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])\d+_pc\.html"
)

# YYYY.MM.DD 格式（页面文本中的日期显示）
_RE_DOT_DATE = re.compile(
    r"(20\d{2})\.(0?[1-9]|1[0-2])\.(0?[1-9]|[12]\d|3[01])"
)


def _extract_date_nda(a_tag, url: str) -> Optional[str]:
    """从 URL 文件名或相邻 <span> 中提取日期。"""
    # 1) URL 文件名
    m = _RE_NDA_URL_DATE.search(url)
    if m:
        return f"{m.group(1)}/{int(m.group(2))}/{int(m.group(3))}"

    # 2) 相邻 <span> 中的 YYYY.MM.DD
    for sibling_fn in (a_tag.find_next_sibling, a_tag.find_previous_sibling):
        sib = sibling_fn("span")
        if sib:
            text = sib.get_text(" ", strip=True)
            dm = _RE_DOT_DATE.search(text)
            if dm:
                return f"{dm.group(1)}/{int(dm.group(2))}/{int(dm.group(3))}"

    # 3) 父元素
    parent = a_tag.parent
    if parent:
        text = parent.get_text(" ", strip=True)
        dm = _RE_DOT_DATE.search(text)
        if dm:
            return f"{dm.group(1)}/{int(dm.group(2))}/{int(dm.group(3))}"

    return None


# ── 板块分类 ─────────────────────────────────────────────────────────────────

# NDA 首页各板块 URL 前缀 → 板块名称
_SECTION_MAP = {
    "/sjj/swdt/xwfb/": "新闻发布",
    "/sjj/zwgk/tzgg/": "通知公告",
    "/sjj/jgsz/jld/": "领导活动",
    "/sjj/swdt/jlddt/": "领导活动",
    "/sjj/swdt/sjdt/": "司局动态",
    "/sjj/swdt/dfdt/": "地方动态",
    "/sjj/swdt/mtsy/": "媒体声音",
    "/sjj/zwgk/zcfb/": "政策发布",
    "/sjj/zwgk/zcjd/": "政策解读",
    "/sjj/zwgk/zjjd/": "专家解读",
    "/sjj/ywpd/": "业务频道",
}

# 外部域名（时政要闻链接通常指向 gov.cn），跳过以避免与 gov 解析器重复
# 注意：不能简单过滤 gov.cn，因为 nda.gov.cn 本身就属于此域名
_SKIP_EXTERNAL_PATTERNS = [
    "gov.cn/yaowen",
    "xinhuanet.com",
    "people.com.cn",
    "cctv.com",
    "mp.weixin.qq.com",
]


def _classify_link(url: str) -> Optional[str]:
    """根据 URL 路径判断板块名称，返回 None 表示应跳过。"""
    for prefix, section in _SECTION_MAP.items():
        if prefix in url:
            return section
    return None


# ── 主入口 ────────────────────────────────────────────────────────────────────

def parse_nda_home(config: dict, now: datetime) -> List[Item]:
    """抓取国家数据局首页并返回符合条件的新闻列表。"""
    src = config["sources"]["nda_home"]
    base_url = src["url"]
    html = http_get(base_url)
    soup = BeautifulSoup(html, "lxml")

    fetched_at = format_fetched_at(now)
    items: List[Item] = []
    keywords = [k for k in config["keywords"] if k not in MIIT_ONLY_KEYWORDS]
    window_days = int(config["window_days"])
    hard_cap_days = int(config["hard_cap_days"])

    for a_tag in soup.find_all("a", href=True):
        href = (a_tag.get("href") or "").strip()
        if not href or href.startswith("#") or href.startswith("javascript"):
            continue

        title = a_tag.get("title") or a_tag.get_text(" ", strip=True)
        title = (title or "").strip()
        if not title or len(title) < 6:
            continue

        url = normalize_url(base_url, href)

        # 跳过外部链接（时政要闻 → gov.cn/yaowen 等）
        if any(p in url for p in _SKIP_EXTERNAL_PATTERNS):
            continue

        # 仅保留 NDA 文章链接
        if not _RE_NDA_ARTICLE.search(url):
            continue

        # 板块分类
        section = _classify_link(url)
        if not section:
            section = "国家数据局"

        # 日期提取
        pub_date = _extract_date_nda(a_tag, url)
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
            source=f"国家数据局-{section}",
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
