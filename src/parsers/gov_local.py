"""
各省（自治区、直辖市）人民政府门户网站解析器。

入口函数
--------
parse_gov_local(config, now) -> List[Item]
    遍历全国 31 个省级政府门户网站（+ 新疆生产建设兵团），
    提取新闻链接，按关键词和时间窗口过滤后返回新闻列表。

数据来源
--------
各省份 URL 取自 https://www.gov.cn/home/2023-03/29/content_5748954.htm

配置（config.yaml）
-------------------
sources:
  gov_local:
    name: 地方政府门户
    enabled: true

实现策略
--------
复用 miit_local.py 中已经验证过的通用辅助函数（URL 识别、日期提取），
因为各省政府门户与工信部门网站使用相同的 CMS 平台（TRS、E-Gov 等）。
"""

import logging
from datetime import datetime
from typing import Dict, List

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

# 复用 miit_local.py 的通用辅助函数
from parsers.miit_local import (
    _is_news_like_url,
    _is_nav_text,
    _extract_date_from_url,
    _extract_date_from_context,
    _clean_title,
    DEFAULT_TIMEOUT,
)

logger = logging.getLogger(__name__)

# ── 省级政府门户源列表 ────────────────────────────────────────────────────────────

GOV_LOCAL_SOURCES = [
    {"province": "北京", "name": "北京市人民政府", "url": "https://www.beijing.gov.cn/"},
    {"province": "天津", "name": "天津市人民政府", "url": "https://www.tj.gov.cn/"},
    {"province": "河北", "name": "河北省人民政府", "url": "https://www.hebei.gov.cn/"},
    {"province": "山西", "name": "山西省人民政府", "url": "https://www.shanxi.gov.cn/"},
    {"province": "内蒙古", "name": "内蒙古自治区人民政府", "url": "https://www.nmg.gov.cn/"},
    {"province": "辽宁", "name": "辽宁省人民政府", "url": "https://www.ln.gov.cn/"},
    {"province": "吉林", "name": "吉林省人民政府", "url": "https://www.jl.gov.cn/"},
    {"province": "黑龙江", "name": "黑龙江省人民政府", "url": "https://www.hlj.gov.cn/"},
    {"province": "上海", "name": "上海市人民政府", "url": "https://www.shanghai.gov.cn/"},
    {"province": "江苏", "name": "江苏省人民政府", "url": "https://www.jiangsu.gov.cn/"},
    {"province": "浙江", "name": "浙江省人民政府", "url": "https://www.zj.gov.cn/"},
    {"province": "安徽", "name": "安徽省人民政府", "url": "https://www.ah.gov.cn/"},
    {"province": "福建", "name": "福建省人民政府", "url": "https://www.fujian.gov.cn/"},
    {"province": "江西", "name": "江西省人民政府", "url": "https://www.jiangxi.gov.cn/"},
    {"province": "山东", "name": "山东省人民政府", "url": "https://www.shandong.gov.cn/"},
    {"province": "河南", "name": "河南省人民政府", "url": "https://www.henan.gov.cn/"},
    {"province": "湖北", "name": "湖北省人民政府", "url": "https://www.hubei.gov.cn/"},
    {"province": "湖南", "name": "湖南省人民政府", "url": "https://www.hunan.gov.cn/"},
    {"province": "广东", "name": "广东省人民政府", "url": "https://www.gd.gov.cn/"},
    {"province": "广西", "name": "广西壮族自治区人民政府", "url": "https://www.gxzf.gov.cn/"},
    {"province": "海南", "name": "海南省人民政府", "url": "https://www.hainan.gov.cn/"},
    {"province": "重庆", "name": "重庆市人民政府", "url": "https://www.cq.gov.cn/"},
    {"province": "四川", "name": "四川省人民政府", "url": "https://www.sc.gov.cn/"},
    {"province": "贵州", "name": "贵州省人民政府", "url": "https://www.guizhou.gov.cn/"},
    {"province": "云南", "name": "云南省人民政府", "url": "https://www.yn.gov.cn/"},
    {"province": "西藏", "name": "西藏自治区人民政府", "url": "https://www.xizang.gov.cn/"},
    {"province": "陕西", "name": "陕西省人民政府", "url": "https://www.shaanxi.gov.cn/"},
    {"province": "甘肃", "name": "甘肃省人民政府", "url": "https://www.gansu.gov.cn/"},
    {"province": "青海", "name": "青海省人民政府", "url": "https://www.qinghai.gov.cn/"},
    {"province": "宁夏", "name": "宁夏回族自治区人民政府", "url": "https://www.nx.gov.cn/"},
    {"province": "新疆", "name": "新疆维吾尔自治区人民政府", "url": "https://www.xinjiang.gov.cn/"},
    {"province": "新疆兵团", "name": "新疆生产建设兵团", "url": "https://www.xjbt.gov.cn/"},
]

# ── 单站抓取 ──────────────────────────────────────────────────────────────────────


def _scrape_one_gov_site(
    source: dict,
    fetched_at: str,
    keywords: List[str],
    now: datetime,
    window_days: int,
    hard_cap_days: int,
    timeout: int,
) -> List[Item]:
    """
    抓取单个省级政府门户网站并返回符合条件的 Item 列表。

    Args:
        source:        包含 province / name / url 的字典。
        fetched_at:    格式化后的查询时间字符串。
        keywords:      关键词列表。
        now:           当前时间（带时区）。
        window_days:   时间窗口天数。
        hard_cap_days: 硬性截止天数。
        timeout:       HTTP 请求超时秒数。

    Returns:
        过滤后的 Item 列表。
    """
    province = source["province"]
    gov_name = source["name"]
    base_url = source["url"]
    source_tag = f"地方政府-{province}"

    html = http_get(base_url, timeout=timeout)
    soup = BeautifulSoup(html, "lxml")

    items: List[Item] = []

    for a_tag in soup.find_all("a", href=True):
        href = (a_tag.get("href") or "").strip()

        # 跳过空链接和锚点（在提取标题前先过滤，节省 clean_title 调用）
        if not href or href.startswith("#") or href.startswith("javascript"):
            continue

        # 先用 get_text 做快速导航/长度预过滤，再用 _clean_title 清洗
        raw_text = a_tag.get_text(" ", strip=True)

        # 跳过导航性文字
        if _is_nav_text(raw_text):
            continue

        # 跳过标题过短的链接（至少 6 个字符）
        if len(raw_text) < 6:
            continue

        url = normalize_url(base_url, href)

        # 仅保留看起来像新闻文章的链接
        if not _is_news_like_url(url):
            continue

        # 清洗标题：优先使用 title 属性，去除首尾日期，截断过长文本
        title = _clean_title(a_tag, raw_text)
        if len(title) < 6:
            continue

        # 提取发布日期：优先从 URL，其次从上下文文本
        pub_date = _extract_date_from_url(url)
        if not pub_date:
            pub_date = _extract_date_from_context(a_tag)
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
            publisher=gov_name,
            url=url,
            pub_date=pub_date,
            source=source_tag,
            fetched_at=fetched_at,
        ))

    return items

# ── 入口函数 ──────────────────────────────────────────────────────────────────────


def parse_gov_local(config: dict, now: datetime) -> List[Item]:
    """
    遍历全国省级政府门户网站，抓取并返回符合条件的新闻列表。

    Args:
        config: 来自 config.yaml 的全量配置字典，需包含：
                - sources.gov_local.name / enabled
                - keywords
                - window_days
                - hard_cap_days
        now:    当前时间（带时区）。

    Returns:
        过滤并去重后的 Item 列表。
    """
    src = config["sources"]["gov_local"]
    if not src.get("enabled", True):
        logger.info("gov_local 源已禁用，跳过")
        return []

    fetched_at = format_fetched_at(now)
    keywords = [k for k in config["keywords"] if k not in MIIT_ONLY_KEYWORDS]
    window_days = int(config["window_days"])
    hard_cap_days = int(config["hard_cap_days"])
    timeout = int(config.get("gov_local_timeout", DEFAULT_TIMEOUT))

    all_items: List[Item] = []

    for source in GOV_LOCAL_SOURCES:
        province = source["province"]
        try:
            site_items = _scrape_one_gov_site(
                source=source,
                fetched_at=fetched_at,
                keywords=keywords,
                now=now,
                window_days=window_days,
                hard_cap_days=hard_cap_days,
                timeout=timeout,
            )
            if site_items:
                logger.info("地方政府-%s: 获取 %d 条", province, len(site_items))
            all_items.extend(site_items)
        except Exception:
            logger.warning("地方政府-%s: 抓取失败", province, exc_info=True)
            continue

    # ── 去重（URL 规范化） ──────────────────────────────────────────────────────
    uniq: Dict[str, Item] = {}
    for it in all_items:
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

    result = list(uniq.values())
    logger.info("地方政府汇总: %d 条（去重后）", len(result))
    return result
