"""
各部门官网解析器（依赖 weibo_monitor.py 中的 WebsiteNewsClient）。

入口函数
--------
parse_website_monitor(config, now) -> List[Item]
    通过 weibo_monitor.WeiboMonitor 抓取配置的官网文章列表，
    按关键词过滤后返回 Item 列表。

说明
----
本模块当前使用 weibo_monitor.WEBSITE_SOURCES 中定义的官网列表，
通过 Playwright 渲染页面并提取新闻链接。
官网解析逻辑将持续迭代优化。

依赖说明
--------
需要安装 Playwright 并下载 Chromium：
    pip install playwright
    python -m playwright install chromium

配置示例（config.yaml）
-----------------------
weibo_monitor:
  enabled: true
  mode: website_only   # all | weibo_only | website_only

官网地址在 weibo_monitor.py 的 WEBSITE_SOURCES 字典中维护：
    WEBSITE_SOURCES = {
        "国家数据局": {
            "url": "https://www.nda.gov.cn/sjj/swdt/list/index_pc_1.html",
            "org": "国家数据局",
        },
        "新来源名": {"url": "列表页URL", "org": "机构名"},
    }

新增来源提示
------------
在 weibo_monitor.WEBSITE_SOURCES 中添加对应条目即可，无需修改本文件。
"""

import asyncio
import sys
from datetime import datetime
from pathlib import Path
from typing import List

from models import Item
from utils import format_fetched_at, keyword_hit, within_window


def parse_website_monitor(config: dict, now: datetime) -> List[Item]:
    """
    抓取各部门官网新闻并返回符合条件的文章列表。

    仅处理官网文章（post 中不含 "mid" 字段）；微博帖子由 parse_weibo 负责。

    Args:
        config: 来自 config.yaml 的全量配置字典。
        now:    当前时间（带时区）。

    Returns:
        过滤后的 Item 列表；若功能未启用或 mode=weibo_only 则返回空列表。
    """
    weibo_cfg = config.get("weibo_monitor", {})
    if not weibo_cfg.get("enabled", False):
        return []

    mode = weibo_cfg.get("mode", "all")
    if mode == "weibo_only":
        return []

    # 确保 weibo_monitor 模块可被导入（src 目录）
    src_dir = str(Path(__file__).resolve().parents[1])
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)

    try:
        from weibo_monitor import WEBSITE_SOURCES, WeiboMonitor  # type: ignore[import]
    except ImportError as exc:
        print(f"[WARN] weibo_monitor 导入失败，跳过官网数据源: {exc}")
        return []

    keywords = config.get("keywords", [])
    website_keywords = [k for k in keywords if k != "印发"]
    window_days = int(config.get("window_days", 15))
    hard_cap_days = int(config.get("hard_cap_days", 15))
    fetched_at = format_fetched_at(now)

    async def _fetch() -> dict:
        monitor = WeiboMonitor(
            accounts={},                    # 仅官网，不抓微博
            website_sources=WEBSITE_SOURCES,
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
        print(f"[WARN] 官网抓取失败，跳过该数据源: {exc}")
        return []

    items: List[Item] = []
    for source_name, posts in all_results.items():
        for post in posts:
            if "mid" in post:
                continue  # 微博帖子，跳过

            title = post.get("title", "").strip()
            url = post.get("url", "")
            pub_date = None

            if not title or not url:
                continue
            if not keyword_hit(title, website_keywords):
                continue
            if pub_date and not within_window(pub_date, now, window_days, hard_cap_days):
                continue

            items.append(Item(
                title=title,
                publisher=source_name,
                url=url,
                pub_date=pub_date,
                source=f"官网-{source_name}",
                fetched_at=fetched_at,
            ))

    return items
