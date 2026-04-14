"""
工业和信息化部官网（miit.gov.cn）解析器。

入口函数
--------
parse_miit_home(config, now) -> List[Item]
    抓取 https://www.miit.gov.cn/ 首页，按关键词和时间窗口过滤后返回新闻列表。

页面结构说明
------------
首页包含多个 tabbox：
  1. tabbox-bds1 — 时政要闻 / 工信动态 / 最新政策 / 新闻发布（4 个 tab）
  2. tabbox-bds4 — 部领导活动 / 司局动态 / 地方动态 / 部属动态（4 个 tab）
  3. floor4 tabbox-bds2 — 政策文件 / 政策解读（含子链接）
  4. floor4 tabbox-bds3 — 文件公示 / 意见征集

新增来源提示
------------
如需在此模块增加更多板块，请参照 add_primary_link / add_related_links_from_policy_li
的调用方式，在函数末尾添加新的 soup.select + 对应 source_tag 即可。
"""

from datetime import datetime
from typing import Dict, List, Optional

from bs4 import BeautifulSoup

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

from dateutil import parser as dtparser


def parse_miit_home(config: dict, now: datetime) -> List[Item]:
    """
    抓取工信部首页并返回符合条件的新闻列表。

    Args:
        config: 来自 config.yaml 的全量配置字典。
        now:    当前时间（带时区）。

    Returns:
        过滤并去重后的 Item 列表。
    """
    src = config["sources"]["miit_home"]
    base_url = src["url"]
    html = http_get(base_url)
    soup = BeautifulSoup(html, "lxml")

    fetched_at = format_fetched_at(now)
    items: List[Item] = []

    keywords = config["keywords"]
    window_days = int(config["window_days"])
    hard_cap_days = int(config["hard_cap_days"])

    # ── 内部辅助函数 ──────────────────────────────────────────────────────────

    def build_item(title: str, href: str, pub_date: Optional[str], source_tag: str) -> Optional[Item]:
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

    def get_pub_date_from_container(container) -> Optional[str]:
        date_text = ""
        span1 = container.find("span")
        if span1:
            date_text = span1.get_text(" ", strip=True)

        p = container.find("p")
        if p:
            pspan = p.find("span")
            if pspan:
                date_text = pspan.get_text(" ", strip=True) or date_text

        pub_date = extract_date(date_text)
        if not pub_date:
            pub_date = extract_date(container.get_text(" ", strip=True))
        return pub_date

    def add_primary_link(container, source_tag: str):
        a = container.find("a", href=True)
        if not a:
            return
        pub_date = get_pub_date_from_container(container)
        it = build_item(a.get_text(" ", strip=True), a.get("href", ""), pub_date, source_tag)
        if it:
            items.append(it)

    def add_related_links_from_policy_li(li, source_tag: str):
        pub_date = get_pub_date_from_container(li)

        p = li.find("p")
        if p:
            a_main = p.find("a", href=True)
            if a_main:
                it = build_item(a_main.get_text(" ", strip=True), a_main["href"], pub_date, source_tag)
                if it:
                    items.append(it)

        for dl in li.select("dl.tslb-list"):
            dt = dl.find("dt")
            dt_text = dt.get_text(" ", strip=True) if dt else ""
            sub_tag = source_tag
            if dt_text:
                sub_tag = f"{source_tag}-{dt_text}"

            for a in dl.select("dd a[href]"):
                it = build_item(a.get_text(" ", strip=True), a["href"], pub_date, sub_tag)
                if it:
                    items.append(it)

    # ── 板块抓取 ─────────────────────────────────────────────────────────────

    # 1) 顶部四个 tab：时政要闻 / 工信动态 / 最新政策 / 新闻发布
    for idx, con in enumerate(soup.select("div.tabbox-bd.tabbox-bds1 div.tabbox-bd-con")):
        tab_names = ["时政要闻", "工信动态", "最新政策", "新闻发布"]
        tab = tab_names[idx] if idx < len(tab_names) else f"tab{idx}"
        for li in con.select("ul > li"):
            add_primary_link(li, f"工信部官网-{tab}")

    # 2) 中部四个 tab：部领导活动 / 司局动态 / 地方动态 / 部属动态
    for idx, con in enumerate(soup.select("div.tabbox-bd.tabbox-bds4 div.tabbox-bd-con")):
        tab_names = ["部领导活动", "司局动态", "地方动态", "部属动态"]
        tab = tab_names[idx] if idx < len(tab_names) else f"tab{idx}"
        for li in con.select("ul > li"):
            add_primary_link(li, f"工信部官网-{tab}")

    # 3) 政策文件 / 政策解读（floor4 左侧 tabbox-bds2）
    policy_cons = soup.select("div.floor4 div.tabbox-bd.tabbox-bds2 div.tabbox-bd-con")
    if policy_cons:
        names = ["政策文件", "政策解读"]
        for i, con in enumerate(policy_cons[:2]):
            tab = names[i] if i < len(names) else f"tab{i}"
            for li in con.select("ul > li"):
                add_related_links_from_policy_li(li, f"工信部官网-{tab}")

    # 4) 文件公示 / 意见征集（floor4 中间 tabbox-bds3）
    for idx, con in enumerate(soup.select("div.floor4 div.tabbox-bd.tabbox-bds3 div.tabbox-bd-con")):
        tab_names = ["文件公示", "意见征集"]
        tab = tab_names[idx] if idx < len(tab_names) else f"tab{idx}"
        for li in con.select("ul > li"):
            add_primary_link(li, f"工信部官网-{tab}")
        for p in con.select("p"):
            add_primary_link(p, f"工信部官网-{tab}")

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
