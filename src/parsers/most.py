"""
科学技术部官网（most.gov.cn）解析器。

入口函数
--------
parse_most_home(config, now) -> List[Item]
    抓取 https://www.most.gov.cn/ 首页，按关键词和时间窗口过滤后返回新闻列表。

页面结构说明
------------
首页包含多个板块：
  1. 科技部工作 (kjbgz)   — /kjbgz/YYYYMM/tYYYYMMDD_XXXXXX.html
  2. 地方科技   (dfkj)    — /dfkj/XX/zxdt/YYYYMM/tYYYYMMDD_XXXXXX.html
  3. 新闻发布   (xwzx)    — /xwzx/ 下各子栏目
  4. 媒体聚焦   (mtjj)    — /mtjj/ 路径 + 外部链接（人民日报、新华社、央视等）
  5. 通知通告   (tztg)    — /tztg/YYYYMM/tYYYYMMDD_XXXXXX.html
  6. 科技政策   (fgzc)    — /xxgk/xinxifenlei/fdzdgknr/fgzc/...
  7. 政策解读   (zcjd)    — /xxgk/xinxifenlei/fdzdgknr/fgzc/zcjd/...
  8. 信息公开   (xxgk)    — /xxgk/xinxifenlei/fdzdgknr/ 其他子目录

新增来源提示
------------
如需在此模块增加更多板块，请参照 _SECTION_MAP 的映射方式，
在字典中添加新的路径前缀与 source_tag 对应关系即可。
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

# ── URL 路径 → 板块名称映射 ──────────────────────────────────────────────────

_SECTION_MAP = [
    ("/xxgk/xinxifenlei/fdzdgknr/fgzc/zcjd/", "科技部官网-政策解读"),
    ("/xxgk/xinxifenlei/fdzdgknr/fgzc/",       "科技部官网-科技政策"),
    ("/xxgk/xinxifenlei/fdzdgknr/",             "科技部官网-信息公开"),
    ("/kjbgz/",                                  "科技部官网-科技部工作"),
    ("/dfkj/",                                   "科技部官网-地方科技"),
    ("/tztg/",                                   "科技部官网-通知通告"),
    ("/xwzx/",                                   "科技部官网-新闻发布"),
    ("/mtjj/",                                   "科技部官网-媒体聚焦"),
]

# 外部媒体域名 → 板块标签（用于匹配首页"锐科技"等板块中的外部链接）
_EXTERNAL_MEDIA_DOMAINS = [
    ("paper.people.com.cn", "科技部官网-媒体聚焦"),
    ("people.com.cn",       "科技部官网-媒体聚焦"),
    ("news.cn",             "科技部官网-媒体聚焦"),
    ("xinhuanet.com",       "科技部官网-媒体聚焦"),
    ("news.cctv.com",       "科技部官网-媒体聚焦"),
    ("gov.cn/yaowen",       "科技部官网-要闻"),
]

# ── 日期提取辅助（most.gov.cn 专用） ─────────────────────────────────────────

# 精确日期：URL 文件名中的 tYYYYMMDD_ID.html（TRS CMS 标准格式）
_RE_MOST_YYYYMMDD = re.compile(r"/t(20\d{2})(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])_")
# 粗略日期：URL 路径中的 /YYYYMM/ 目录
_RE_MOST_YYYYMM = re.compile(r"/(20\d{2})(0[1-9]|1[0-2])/")


def _extract_date_from_url(url: str) -> Optional[str]:
    """从 most.gov.cn URL 路径中提取日期，优先精确到日，其次年月。"""
    # 优先：文件名中的 tYYYYMMDD_ 精确日期
    m = _RE_MOST_YYYYMMDD.search(url)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    # 回退：路径中的 YYYYMM 目录（日默认 01）
    m = _RE_MOST_YYYYMM.search(url)
    if m:
        return f"{m.group(1)}-{m.group(2)}-01"
    return None


def _classify_link(href: str) -> Optional[str]:
    """根据链接路径判断所属板块，返回 source_tag；无法识别时返回 None。"""
    for prefix, tag in _SECTION_MAP:
        if prefix in href:
            return tag
    # 外部媒体域名匹配（首页"锐科技"等板块的外部链接）
    for domain, tag in _EXTERNAL_MEDIA_DOMAINS:
        if domain in href:
            return tag
    return None


# ── parse_most_home ──────────────────────────────────────────────────────────

def parse_most_home(config: dict, now: datetime) -> List[Item]:
    """
    抓取科技部首页并返回符合条件的新闻列表。

    Args:
        config: 来自 config.yaml 的全量配置字典。
        now:    当前时间（带时区）。

    Returns:
        过滤并去重后的 Item 列表。
    """
    src = config["sources"]["most_home"]
    base_url = src["url"]
    html = http_get(base_url)
    soup = BeautifulSoup(html, "lxml")

    fetched_at = format_fetched_at(now)
    items: List[Item] = []

    keywords = config["keywords"]
    window_days = int(config["window_days"])
    hard_cap_days = int(config["hard_cap_days"])

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

    def extract_nearby_date(tag) -> Optional[str]:
        """
        从链接附近的文本中提取日期。

        实际页面结构：
          <span class="date mhide">2026-04-10</span>
          <a href="...">标题</a>
        日期 span 在 <a> 之前，需向前查找。
        """
        # 1) 前一个兄弟 <span>（MOST 实际结构：date span 在 a 之前）
        prev_span = tag.find_previous_sibling("span")
        if prev_span:
            text = prev_span.get_text(" ", strip=True)
            pub = extract_date(text)
            if pub:
                return pub

        # 2) 同级或父级中的所有 <span>
        parent = tag.parent
        if parent:
            for span in parent.find_all("span"):
                text = span.get_text(" ", strip=True)
                pub = extract_date(text)
                if pub:
                    return pub
            # 3) 父容器全文
            pub = extract_date(parent.get_text(" ", strip=True))
            if pub:
                return pub

        # 4) 紧邻的后续兄弟节点文本
        for sibling in (tag.next_sibling, tag.previous_sibling):
            if sibling and hasattr(sibling, "get_text"):
                pub = extract_date(sibling.get_text(" ", strip=True))
                if pub:
                    return pub
            elif sibling and isinstance(sibling, str):
                pub = extract_date(sibling.strip())
                if pub:
                    return pub
        return None

    # ── 板块抓取 ─────────────────────────────────────────────────────────────

    for a in soup.find_all("a", href=True):
        href = a["href"]

        source_tag = _classify_link(href)
        if source_tag is None:
            continue

        # 标题：优先取 title 属性，其次取链接文本
        title = (a.get("title") or "").strip()
        if not title:
            title = a.get_text(" ", strip=True)

        # 日期：优先从附近文本提取，其次从 URL 路径提取
        pub_date = extract_nearby_date(a)
        if not pub_date:
            url = normalize_url(base_url, href)
            pub_date = _extract_date_from_url(url)

        it = build_item(title, href, pub_date, source_tag)
        if it:
            items.append(it)

    # ── 过滤（关键词 + 时间窗口） ─────────────────────────────────────────────
    most_keywords = [k for k in keywords if k not in MIIT_ONLY_KEYWORDS]

    filtered: List[Item] = []
    for it in items:
        if not keyword_hit(it.title, most_keywords):
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
