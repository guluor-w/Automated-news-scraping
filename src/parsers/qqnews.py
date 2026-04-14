"""
腾讯新闻搜索（qqnews）解析器。

入口函数
--------
parse_qqnews_search(config, now) -> List[Item]
    调用腾讯新闻 PC 搜索 API，搜索 config 中配置的关键词（支持多个），
    收集近 N 天的图文结果并返回 Item 列表。

工作机制
--------
- 仅保留 secList 中 component=pictext（图文）且含 newsList 的条目。
- 时间字段优先使用 API 返回的 time；无法解析则跳过（满足"近N天"约束）。
- 仅保留 publisher（发布单位）名称中包含查询词的结果，以避免混入无关媒体。
- 多个查询词之间随机休眠 10～20 秒，防止被封禁。
- 特殊处理：仅"工信微报"来源保留"印发"关键词匹配。

配置示例（config.yaml）
-----------------------
sources:
  qqnews_search:
    name: 腾讯新闻
    url: https://i.news.qq.com/gw/pc_search/result
    queries:
      - 工信微报
      - 微言教育
    max_pages: 5
    page_size: 20

新增来源提示
------------
在 queries 列表中追加新的公众号或关键词名称即可，无需修改代码。
"""

import random
import time
from datetime import datetime, timedelta
from typing import List, Optional

import requests
from dateutil import parser as dtparser

from models import Item, SG_TZ, USER_AGENT
from utils import format_fetched_at, keyword_hit

QQNEWS_API_URL = "https://i.news.qq.com/gw/pc_search/result"

QQNEWS_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "User-Agent": USER_AGENT,
    "Origin": "https://news.qq.com",
    "Referer": "https://news.qq.com/",
}


def _parse_qqnews_time_to_dt(s: str, now: datetime) -> Optional[datetime]:
    """
    将腾讯新闻 API 返回的 time 字段解析为带时区的 datetime。
    支持相对时间（X分钟前/X小时前/X天前）和绝对时间字符串。
    """
    s = (s or "").strip()
    if not s:
        return None
    try:
        if s.endswith("分钟前"):
            n = int(s.replace("分钟前", "").strip())
            return now - timedelta(minutes=n)
        if s.endswith("小时前"):
            n = int(s.replace("小时前", "").strip())
            return now - timedelta(hours=n)
        if s.endswith("天前"):
            n = int(s.replace("天前", "").strip())
            return now - timedelta(days=n)
    except Exception:
        pass

    try:
        dt = dtparser.parse(s, fuzzy=True)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=SG_TZ)
        else:
            dt = dt.astimezone(SG_TZ)
        return dt
    except Exception:
        return None


def _qqnews_search_fetch_page(session: requests.Session, api_url: str, query: str, page: int, limit: int) -> dict:
    """向腾讯新闻搜索 API 发起一次翻页请求并返回原始 JSON。"""
    payload = {
        "page": str(page),
        "query": query,
        "is_pc": "1",
        "hippy_custom_version": "24",
        "search_type": "all",
        "search_count_limit": str(limit),
        "appver": "15.5_qqnews_7.1.80",
    }
    resp = session.post(api_url, data=payload, headers=QQNEWS_HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.json()


def parse_qqnews_search(config: dict, now: datetime) -> List[Item]:
    """
    搜索腾讯新闻，返回近 N 天内符合条件的图文新闻列表。

    Args:
        config: 来自 config.yaml 的全量配置字典。
        now:    当前时间（带时区）。

    Returns:
        过滤后的 Item 列表；若来源未配置则返回空列表。
    """
    src = config["sources"].get("qqnews_search")
    if not src:
        return []

    queries = src.get("queries")
    if not queries:
        single_query = src.get("query")
        queries = [single_query] if single_query else ["工信微报"]

    if isinstance(queries, str):
        queries = [queries]

    window_days = int(config.get("window_days", 15))
    max_pages = int(src.get("max_pages") or 5)
    page_size = int(src.get("page_size") or 20)
    api_url = src.get("url") or QQNEWS_API_URL

    threshold = now - timedelta(days=window_days)
    fetched_at = format_fetched_at(now)

    all_items: List[Item] = []
    with requests.Session() as session:
        for i, query in enumerate(queries):
            query = str(query).strip()
            if not query:
                continue

            # 多关键词间随机休眠，防止高频访问被封禁
            if i > 0:
                sleep_sec = random.uniform(10.0, 20.0)
                time.sleep(sleep_sec)

            for page in range(max_pages):
                try:
                    raw = _qqnews_search_fetch_page(session, api_url=api_url, query=query, page=page, limit=page_size)
                except Exception:
                    continue

                sec_list = raw.get("secList") or []
                page_min_dt: Optional[datetime] = None

                for sec in sec_list:
                    try:
                        component = (sec.get("component") or "").strip()
                        if component and component != "pictext":
                            continue

                        for n in (sec.get("newsList") or []):
                            title = (n.get("title") or "").strip()
                            url = (n.get("surl") or n.get("url") or "").strip()
                            if not title or not url:
                                continue

                            t_raw = (n.get("time") or "").strip()
                            dt = _parse_qqnews_time_to_dt(t_raw, now=now)
                            if dt is None:
                                continue

                            if page_min_dt is None or dt < page_min_dt:
                                page_min_dt = dt

                            if dt < threshold:
                                continue

                            pub_date = dt.date().isoformat()
                            publisher = (n.get("source") or src.get("name") or "腾讯新闻").strip()

                            # 仅保留发布单位名称中包含查询词的结果
                            if query not in publisher:
                                continue

                            all_items.append(Item(
                                title=title,
                                publisher=publisher,
                                url=url,
                                pub_date=pub_date,
                                source=f"腾讯新闻-{query}",
                                fetched_at=fetched_at,
                            ))
                    except Exception:
                        continue

                if raw.get("hasMore") in (0, "0", False):
                    break

                if page_min_dt and page_min_dt < threshold:
                    break

    # ── 关键词过滤 ────────────────────────────────────────────────────────────
    base_keywords = config.get("keywords", [])
    keywords_full = base_keywords
    keywords_no_yinfa = [k for k in base_keywords if k != "印发"]

    filtered: List[Item] = []
    for it in all_items:
        # "工信微报"来源保留"印发"；其他来源不使用"印发"
        if "工信微报" in it.source:
            target_keywords = keywords_full
        else:
            target_keywords = keywords_no_yinfa

        if not keyword_hit(it.title, target_keywords):
            continue
        filtered.append(it)

    return filtered
