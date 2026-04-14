"""
微博 / 官网监控解析器（依赖 weibo_monitor.py）。

入口函数
--------
parse_weibo_monitor_sources(config, now) -> List[Item]
    通过 weibo_monitor.WeiboMonitor 抓取微博帖子和官网文章，
    按关键词和时间窗口过滤后返回 Item 列表。

依赖说明
--------
需要安装 Playwright 并下载 Chromium：
    pip install playwright
    python -m playwright install chromium

配置示例（config.yaml）
-----------------------
weibo_monitor:
  enabled: true
  mode: weibo_only       # all | weibo_only | website_only
  max_pages: 1           # 每个微博账号最多抓取的页数

微博账号和官网地址在 weibo_monitor.py 中的
MONITOR_ACCOUNTS 和 WEBSITE_SOURCES 字典里维护。

新增来源提示
------------
- 新增微博账号：在 weibo_monitor.MONITOR_ACCOUNTS 中添加 {账号名: UID}。
- 新增监控官网：在 weibo_monitor.WEBSITE_SOURCES 中添加对应条目。
- 本文件无需改动。
"""

import asyncio
import sys
from datetime import datetime
from pathlib import Path
from typing import List

from models import Item
from utils import format_fetched_at, keyword_hit, within_window


def parse_weibo_monitor_sources(config: dict, now: datetime) -> List[Item]:
    """
    从微博账号和/或官网信息源获取新闻，按关键词和时间窗口过滤后返回 Item 列表。

    Args:
        config: 来自 config.yaml 的全量配置字典。
        now:    当前时间（带时区）。

    Returns:
        过滤后的 Item 列表；若功能未启用或抓取失败则返回空列表。
    """
    weibo_cfg = config.get("weibo_monitor", {})
    if not weibo_cfg.get("enabled", False):
        return []

    # 确保 weibo_monitor 模块可被导入（与本文件同一 src 目录的上级）
    src_dir = str(Path(__file__).resolve().parents[1])
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)

    try:
        from weibo_monitor import (  # type: ignore[import]
            MONITOR_ACCOUNTS,
            WEBSITE_SOURCES,
            WeiboMonitor,
        )
    except ImportError as exc:
        print(f"[WARN] weibo_monitor 导入失败，跳过该数据源: {exc}")
        return []

    mode = weibo_cfg.get("mode", "all")
    max_pages = int(weibo_cfg.get("max_pages", 2))

    accounts = MONITOR_ACCOUNTS
    website_sources = WEBSITE_SOURCES
    if mode == "weibo_only":
        website_sources = {}
    elif mode == "website_only":
        accounts = {}

    keywords = config.get("keywords", [])
    weibo_keywords = [k for k in keywords if k != "印发"]
    window_days = int(config.get("window_days", 15))
    hard_cap_days = int(config.get("hard_cap_days", 15))
    fetched_at = format_fetched_at(now)

    async def _fetch() -> dict:
        monitor = WeiboMonitor(
            accounts=accounts,
            website_sources=website_sources,
            max_pages=max_pages,
        )
        return await monitor.fetch_all(include_seen=True)

    def _run_fetch() -> dict:
        return asyncio.run(_fetch())

    try:
        try:
            asyncio.get_running_loop()
            running = True
        except RuntimeError:
            running = False

        if running:
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                all_results: dict = pool.submit(_run_fetch).result()
        else:
            all_results = asyncio.run(_fetch())
    except Exception as exc:
        print(f"[WARN] weibo_monitor 抓取失败，跳过该数据源: {exc}")
        return []

    items: List[Item] = []
    for source_name, posts in all_results.items():
        for post in posts:
            if "mid" in post:
                # 微博帖子格式
                title = post.get("title", "").strip()
                url = post.get("article_url") or post.get("url", "")
                pub_date_str = post.get("parsed_time", "")
                pub_date = pub_date_str[:10] if pub_date_str and len(pub_date_str) >= 10 else None
                publisher = source_name
                source_tag = f"微博-{source_name}"
            else:
                # 官网文章格式
                title = post.get("title", "").strip()
                url = post.get("url", "")
                pub_date = None
                publisher = source_name
                source_tag = f"官网-{source_name}"

            if not title or not url:
                continue

            if not keyword_hit(title, weibo_keywords):
                continue

            if pub_date and not within_window(pub_date, now, window_days, hard_cap_days):
                continue

            items.append(Item(
                title=title,
                publisher=publisher,
                url=url,
                pub_date=pub_date,
                source=source_tag,
                fetched_at=fetched_at,
            ))

    return items
