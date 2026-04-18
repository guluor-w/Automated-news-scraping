"""
国家发展和改革委员会官网（ndrc.gov.cn）解析器。

入口函数
--------
parse_ndrc_home(config, now) -> List[Item]
    抓取 https://www.ndrc.gov.cn/ 首页，按关键词和时间窗口过滤后返回新闻列表。

页面结构说明
------------
首页包含以下板块：
  1. 新闻发布 — /xwdt/xwfb/ 路径，链接含 YYYYMM 日期
  2. 时政要闻 — 外部链接（news.cn、gov.cn、xinhuanet.com 等）
  3. 通知公告 — 日期以 YYYY/MM/DD 格式出现在链接旁
  4. 委领导动态 — /fzggw/wld/ 路径
  5. 司局动态 — /xwdt/dt/sjdt/ 及 /fzggw/jgsj/ 路径
  6. 地方动态 — /xwdt/dt/dfdt/ 路径
  7. 政策发布 — /xxgk/zcfb/ 路径
  8. 政策解读 — /xxgk/jd/ 路径
  9. 发展改革工作 — /fggz/ 路径
  10. 视频发改 — /xwdt/spfg/ 路径
  11. 委属单位话发改 — /wsdwhfz/ 路径
  12. 发改数据 — /fgsj/ 路径
  13. 互动交流 — /hdjl/ 路径（意见征集等）

页面/配置（config.yaml）
-------------------------
sources:
  ndrc_home:
    name: 国家发展和改革委员会
    url: https://www.ndrc.gov.cn/

注意：与 gov.py 一致，本解析器不使用"印发"和"体系建设"关键词进行过滤，
因为这类词在非 MIIT 官方/工信微报场景下噪声较大，且对发改委这类政策密集型来源而言，
往往会出现在核心政策信息标题中。

新增来源提示
------------
若需在此模块扩展更多板块，请参照 add_links 的调用方式，
在函数末尾添加新的 soup.select + 对应 source_tag 即可。
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

# ── 日期提取辅助（ndrc.gov.cn 专用） ─────────────────────────────────────────

# 精确日期：URL 文件名中的 tYYYYMMDD_ID.html（TRS CMS 标准格式）
_RE_NDRC_YYYYMMDD = re.compile(r"/t(20\d{2})(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])_")
# 粗略日期：URL 路径中的 /YYYYMM/ 目录
_RE_NDRC_YYYYMM = re.compile(r"/(20\d{2})(0[1-9]|1[0-2])/")
# 页面文本中的 YYYY/MM/DD 格式（<span>2026/04/16</span>）
_RE_DATE_SLASH = re.compile(
    r"(20\d{2})/(0?[1-9]|1[0-2])/(0?[1-9]|[12]\d|3[01])"
)


def _extract_date_from_url(url: str) -> Optional[str]:
    """从 ndrc.gov.cn URL 路径中提取日期，优先精确到日，其次年月。"""
    # 优先：文件名中的 tYYYYMMDD_ 精确日期
    m = _RE_NDRC_YYYYMMDD.search(url)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    # 回退：路径中的 YYYYMM 目录（日默认 01）
    m = _RE_NDRC_YYYYMM.search(url)
    if m:
        return f"{m.group(1)}-{m.group(2)}-01"
    return None


def _extract_date_from_text(text: str) -> Optional[str]:
    """从相邻文本中提取 YYYY/MM/DD 格式的日期。"""
    m = _RE_DATE_SLASH.search(text)
    if not m:
        return None
    yyyy = m.group(1)
    mm = m.group(2).zfill(2)
    dd = m.group(3).zfill(2)
    return f"{yyyy}-{mm}-{dd}"


# ── parse_ndrc_home ──────────────────────────────────────────────────────────

def parse_ndrc_home(config: dict, now: datetime) -> List[Item]:
    """
    抓取国家发展和改革委员会首页并返回符合条件的新闻列表。

    Args:
        config: 来自 config.yaml 的全量配置字典。
        now:    当前时间（带时区）。

    Returns:
        过滤并去重后的 Item 列表。
    """
    src = config["sources"]["ndrc_home"]
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

    def _resolve_pub_date(a_tag, href: str) -> Optional[str]:
        """
        尝试多种策略提取发布日期：
        1. <a> 标签的下一个兄弟 <span>（实际结构：<li><a>标题</a><span>2026/04/16</span></li>）
        2. 父容器文本中的 YYYY/MM/DD
        3. URL 文件名中的 tYYYYMMDD_ 或路径中的 YYYYMM
        4. 通用 extract_date 回退
        """
        # 1) 紧邻的兄弟 <span>（NDRC 实际页面结构）
        sibling_span = a_tag.find_next_sibling("span")
        if sibling_span:
            span_text = sibling_span.get_text(" ", strip=True)
            d = _extract_date_from_text(span_text)
            if d:
                return d

        # 2) 父容器的文本
        parent = a_tag.parent
        if parent:
            sibling_text = parent.get_text(" ", strip=True)
            d = _extract_date_from_text(sibling_text)
            if d:
                return d
            d = extract_date(sibling_text)
            if d:
                return d

        # 3) 从 URL 路径提取（优先 tYYYYMMDD_，其次 YYYYMM）
        url = normalize_url(base_url, href)
        d = _extract_date_from_url(url)
        if d:
            return d

        return None

    def add_links(selector: str, source_tag: str):
        """通用板块抓取：直接从选择器匹配的 <a> 标签中提取链接。"""
        for a in soup.select(selector):
            href = a.get("href")
            if not href:
                continue
            title = a.get("title") or a.get_text(" ", strip=True)
            pub_date = _resolve_pub_date(a, href)
            it = build_item(title, href, pub_date, source_tag)
            if it:
                items.append(it)

    # ── 板块抓取 ─────────────────────────────────────────────────────────────

    # 1) 新闻发布 — /xwdt/xwfb/ 路径
    add_links("a[href*='/xwdt/xwfb/']", "发改委官网-新闻发布")

    # 2) 时政要闻 — /xwdt/szyw/ 路径 + 外部链接
    add_links("a[href*='/xwdt/szyw/']", "发改委官网-时政要闻")
    add_links(
        "a[href*='news.cn'], a[href*='people.com.cn'], "
        "a[href*='xinhuanet.com'], a[href*='gov.cn/yaowen']",
        "发改委官网-时政要闻",
    )

    # 3) 通知公告 — 日期以 YYYY/MM/DD 格式出现
    add_links("a[href*='/xwdt/tzgg/']", "发改委官网-通知公告")

    # 4) 委领导动态 — /fzggw/wld/ 路径
    add_links("a[href*='/fzggw/wld/']", "发改委官网-委领导动态")

    # 5) 司局动态 — /xwdt/dt/sjdt/ 及 /fzggw/jgsj/ 下各司局动态路径
    add_links("a[href*='/xwdt/dt/sjdt/']", "发改委官网-司局动态")
    add_links("a[href*='/fzggw/jgsj/']", "发改委官网-司局动态")

    # 6) 地方动态 — /xwdt/dt/dfdt/ 路径
    add_links("a[href*='/xwdt/dt/dfdt/']", "发改委官网-地方动态")

    # 7) 政策发布 — /xxgk/zcfb/ 路径
    add_links("a[href*='/xxgk/zcfb/']", "发改委官网-政策发布")

    # 8) 政策解读 — /xxgk/jd/ 路径
    add_links("a[href*='/xxgk/jd/']", "发改委官网-政策解读")

    # 9) 发展改革工作 — /fggz/ 路径
    add_links("a[href*='/fggz/']", "发改委官网-发展改革工作")

    # 10) 视频发改 — /xwdt/spfg/ 路径
    add_links("a[href*='/xwdt/spfg/']", "发改委官网-视频发改")

    # 11) 委属单位话发改 — /wsdwhfz/ 路径
    add_links("a[href*='/wsdwhfz/']", "发改委官网-委属单位话发改")

    # 12) 发改数据 — /fgsj/ 路径
    add_links("a[href*='/fgsj/']", "发改委官网-发改数据")

    # 13) 互动交流 — /hdjl/ 路径（意见征集等）
    add_links("a[href*='/hdjl/']", "发改委官网-互动交流")

    # ── 过滤（不使用"印发"和"体系建设"关键词，与 gov.py 保持一致） ─────────────
    ndrc_keywords = [k for k in keywords if k not in MIIT_ONLY_KEYWORDS]

    filtered: List[Item] = []
    for it in items:
        if not keyword_hit(it.title, ndrc_keywords):
            continue
        if it.pub_date:
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
