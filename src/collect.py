import csv
import hashlib
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Dict, Tuple
from pathlib import Path

import feedparser
import pandas as pd
import requests
import yaml
from bs4 import BeautifulSoup
from dateutil import parser as dtparser

SG_TZ = timezone(timedelta(hours=8))  # Asia/Singapore 固定 +08:00

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

DATE_PATTERNS = [
    # 2026-01-16 / 2026/01/16
    re.compile(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})"),
    # 2026年1月16日
    re.compile(r"(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日"),
]

SECTION_KEYWORDS = ["最新政策", "政策文件", "文件公示", "意见征集"]


@dataclass
class Item:
    title: str
    publisher: str
    url: str
    pub_date: Optional[str]  # YYYY-MM-DD
    source: str
    fetched_at: str          # ISO timestamp

    
def load_config(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def normalize_url(base: str, href: str) -> str:
    href = href.strip()
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("//"):
        return "https:" + href
    if href.startswith("/"):
        return base.rstrip("/") + href
    return base.rstrip("/") + "/" + href


def extract_date(text: str) -> Optional[str]:
    if not text:
        return None
    for pat in DATE_PATTERNS:
        m = pat.search(text)
        if m:
            y, mo, d = map(int, m.groups())
            try:
                return f"{y:04d}-{mo:02d}-{d:02d}"
            except Exception:
                return None
    # 尝试用 dateutil 宽松解析（避免误判，必须包含年份）
    try:
        dt = dtparser.parse(text, fuzzy=True)
        if dt.year >= 2000:
            return dt.date().isoformat()
    except Exception:
        pass
    return None


def keyword_hit(title: str, keywords: List[str]) -> bool:
    t = (title or "").lower()
    for k in keywords:
        if k.lower() in t:
            return True
    return False


def within_window(pub_date: Optional[str], now: datetime, window_days: int, hard_cap_days: int) -> bool:
    """
    逻辑：
    - 有发布日期：必须在 [now-hard_cap_days, now] 内
      且建议窗口为 window_days（默认 7 天）；如果你希望“宁可多也不要漏”，可放宽到 hard_cap_days。
    - 无发布日期：保守起见也收录，但建议你后续人工抽查（会在表里显示空日期）。
    """
    if pub_date is None:
        return True
    try:
        d = dtparser.parse(pub_date).date()
    except Exception:
        return True

    lower = (now - timedelta(days=hard_cap_days)).date()
    upper = now.date()
    if not (lower <= d <= upper):
        return False

    # 这里默认严格按近一周；如果你希望“近一周优先、但不足时补到两周”，可以改成：
    # return d >= (now - timedelta(days=window_days)).date() or True
    return d >= (now - timedelta(days=window_days)).date()


def http_get(url: str) -> str:
    resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=30)
    resp.raise_for_status()
    resp.encoding = resp.apparent_encoding or "utf-8"
    return resp.text


def parse_miit_home(config: dict, now: datetime) -> List[Item]:
    src = config["sources"]["miit_home"]
    base_url = src["url"]
    html = http_get(base_url)
    soup = BeautifulSoup(html, "lxml")

    # 由于官网结构可能调整，这里采用“板块关键词附近抓链接”的鲁棒策略：
    # 1) 在全文中找到“最新政策/政策文件/文件公示/意见征集”等板块附近的DOM
    # 2) 向下收集一定数量的链接
    text = soup.get_text(" ", strip=True)

    items: List[Item] = []
    fetched_at = now.astimezone(SG_TZ).isoformat(timespec="seconds")

    for sec in SECTION_KEYWORDS:
        # 找到包含该文字的元素
        nodes = soup.find_all(string=re.compile(sec))
        for node in nodes[:3]:
            container = node.parent
            # 向上找一个较大的容器
            for _ in range(4):
                if container is None:
                    break
                # 如果容器里链接足够多，就停止上探
                if len(container.find_all("a", href=True)) >= 5:
                    break
                container = container.parent

            if container is None:
                continue

            # 收集链接（过滤明显无关的导航链接）
            links = []
            for a in container.find_all("a", href=True):
                title = a.get_text(" ", strip=True)
                href = a.get("href", "")
                if not title or len(title) < 6:
                    continue
                if title in SECTION_KEYWORDS:
                    continue
                if href.startswith("javascript:"):
                    continue
                full = normalize_url(base_url, href)
                links.append((title, full, a))

            # 去重并截断（每个板块最多取 20）
            seen_local = set()
            for title, full, a in links:
                if full in seen_local:
                    continue
                seen_local.add(full)

                # 尝试在父节点附近找日期
                parent_text = a.parent.get_text(" ", strip=True) if a.parent else ""
                pub_date = extract_date(parent_text)

                items.append(Item(
                    title=title,
                    publisher=src["name"],
                    url=full,
                    pub_date=pub_date,
                    source=f"MIIT-首页-{sec}",
                    fetched_at=fetched_at,
                ))
                if len(seen_local) >= 20:
                    break

    # 基于时间窗口与关键词过滤
    keywords = config["keywords"]
    window_days = int(config["window_days"])
    hard_cap_days = int(config["hard_cap_days"])

    filtered = []
    for it in items:
        if not keyword_hit(it.title, keywords):
            continue
        if not within_window(it.pub_date, now, window_days, hard_cap_days):
            continue
        filtered.append(it)

    return filtered


def parse_gov_rss(config: dict, now: datetime) -> List[Item]:
    src = config["sources"]["gov_latest_policy_rss"]
    feed_url = src["rss"]
    d = feedparser.parse(feed_url)
    fetched_at = now.astimezone(SG_TZ).isoformat(timespec="seconds")

    items: List[Item] = []
    for e in d.entries[:50]:
        title = getattr(e, "title", "").strip()
        link = getattr(e, "link", "").strip()
        published = getattr(e, "published", "") or getattr(e, "updated", "")
        pub_date = extract_date(published)  # RSS 通常带日期

        if title and link:
            items.append(Item(
                title=title,
                publisher=src["name"],
                url=link,
                pub_date=pub_date,
                source="GOV-最新政策(RSSHub)",
                fetched_at=fetched_at,
            ))

    keywords = config["keywords"]
    window_days = int(config["window_days"])
    hard_cap_days = int(config["hard_cap_days"])

    filtered = []
    for it in items:
        if not keyword_hit(it.title, keywords):
            continue
        if not within_window(it.pub_date, now, window_days, hard_cap_days):
            continue
        filtered.append(it)

    return filtered


def load_existing(csv_path: str) -> pd.DataFrame:
    if not os.path.exists(csv_path):
        return pd.DataFrame(columns=["标题", "发布单位", "新闻URL", "发布日期", "来源", "抓取时间"])
    return pd.read_csv(csv_path)


def dedup_merge(existing: pd.DataFrame, new_items: List[Item]) -> Tuple[pd.DataFrame, int]:
    if existing.empty:
        existing_urls = set()
    else:
        existing_urls = set(existing["新闻URL"].astype(str).tolist())

    rows = []
    added = 0
    for it in new_items:
        if it.url in existing_urls:
            continue
        rows.append({
            "标题": it.title,
            "发布单位": it.publisher,
            "新闻URL": it.url,
            "发布日期": it.pub_date or "",
            "来源": it.source,
            "抓取时间": it.fetched_at,
        })
        existing_urls.add(it.url)
        added += 1

    if rows:
        new_df = pd.concat([existing, pd.DataFrame(rows)], ignore_index=True)
    else:
        new_df = existing.copy()

    # 按发布日期（空值排后）和抓取时间排序
    def sort_key(row):
        d = row.get("发布日期", "")
        return d if d else "0000-00-00"

    if not new_df.empty:
        new_df["__sortdate"] = new_df.apply(sort_key, axis=1)
        new_df = new_df.sort_values(by=["__sortdate", "抓取时间"], ascending=[False, False]).drop(columns=["__sortdate"])

    return new_df, added

def main():
    # collect.py 位于 src/，仓库根目录是它的上一级
    repo_root = Path(__file__).resolve().parents[1]
    config_path = repo_root / "config.yaml"

    config = load_config(config_path)
    # 后续所有相对路径也建议基于 repo_root
    now = datetime.now(tz=SG_TZ)

    all_items: List[Item] = []
    all_items.extend(parse_miit_home(config, now))
    all_items.extend(parse_gov_rss(config, now))

    out_csv = repo_root / config["output"]["csv_path"]
    existing = load_existing(str(out_csv))
    merged, added = dedup_merge(existing, all_items)

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(str(out_csv), index=False, encoding="utf-8-sig")

    added_path = repo_root / "added_count.txt"
    with open(added_path, "w", encoding="utf-8") as f:
        f.write(str(added))

if __name__ == "__main__":
    main()
