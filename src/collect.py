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
from typing import List, Dict, Optional
from urllib.parse import urlsplit, urlunsplit
from typing import List, Dict, Optional, Tuple
from urllib.parse import urlsplit, urlunsplit
import re

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
    # 2026.01.16
    re.compile(r"(\d{4})\.(\d{1,2})\.(\d{1,2})"),
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

    return d >= (now - timedelta(days=window_days)).date() or True



def http_get(url: str) -> str:
    resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=30)
    resp.raise_for_status()
    resp.encoding = resp.apparent_encoding or "utf-8"
    return resp.text



def canonicalize_url_for_dedup(url: str) -> str:
    """
    用于去重：忽略 http/https；统一 www；去掉 fragment；规范化末尾斜杠；
    保留 path 与 query（有些站点同路径不同 query 可能不同页面）。
    """
    try:
        u = url.strip()
        if u.startswith("//"):
            u = "https:" + u
        parts = urlsplit(u)
        netloc = parts.netloc.lower()
        if netloc.startswith("www."):
            netloc = netloc[4:]
        path = parts.path or "/"
        # 统一末尾斜杠（但保留根路径）
        if path != "/" and path.endswith("/"):
            path = path[:-1]
        # 忽略 scheme 与 fragment
        return urlunsplit(("", netloc, path, parts.query or "", ""))
    except Exception:
        return url  # 兜底


#---------------------------------------------------miit.gov.cn 相关解析代码---------------------------------------------------#
def parse_miit_home(config: dict, now: datetime) -> List[Item]:
    src = config["sources"]["miit_home"]
    base_url = src["url"]
    html = http_get(base_url)
    soup = BeautifulSoup(html, "lxml")

    fetched_at = now.astimezone(SG_TZ).isoformat(timespec="seconds")
    items: List[Item] = []

    keywords = config["keywords"]
    window_days = int(config["window_days"])
    hard_cap_days = int(config["hard_cap_days"])

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
        """
        统一从一条 li/p 容器里抽发布日期：
        - 优先取最近的 span 文本（例如 <li><span>2026-01-20</span>...）
        - 再取 p>span（政策文件那种）
        - 最后 fallback 全文扫描日期
        """
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
        """
        抓主链接（一般是 p>a 或 li 内第一个 a）
        """
        a = container.find("a", href=True)
        if not a:
            return
        pub_date = get_pub_date_from_container(container)
        it = build_item(a.get_text(" ", strip=True), a.get("href", ""), pub_date, source_tag)
        if it:
            items.append(it)

    def add_related_links_from_policy_li(li, source_tag: str):
        """
        关键：抓取政策文件 li 中 <dl class="tslb-list"> 下的 <dd><a> 相关解读/相关新闻
        并尽量继承该 li 的发布日期。
        """
        pub_date = get_pub_date_from_container(li)

        # 1) 主政策链接：通常在 <p><a ...></a><span>日期...</span></p>
        p = li.find("p")
        if p:
            a_main = p.find("a", href=True)
            if a_main:
                it = build_item(a_main.get_text(" ", strip=True), a_main["href"], pub_date, source_tag)
                if it:
                    items.append(it)

        # 2) dl.tslb-list 下的 dd>a：相关解读、相关新闻
        for dl in li.select("dl.tslb-list"):
            # dt 标题可作为子来源标签（可选）
            dt = dl.find("dt")
            dt_text = dt.get_text(" ", strip=True) if dt else ""
            sub_tag = source_tag
            if dt_text:
                sub_tag = f"{source_tag}-{dt_text}"

            for a in dl.select("dd a[href]"):
                it = build_item(a.get_text(" ", strip=True), a["href"], pub_date, sub_tag)
                if it:
                    items.append(it)

    # 1) 顶部四个 tab：时政要闻/工信动态/最新政策/新闻发布
    for idx, con in enumerate(soup.select("div.tabbox-bd.tabbox-bds1 div.tabbox-bd-con")):
        tab_names = ["时政要闻", "工信动态", "最新政策", "新闻发布"]
        tab = tab_names[idx] if idx < len(tab_names) else f"tab{idx}"
        for li in con.select("ul > li"):
            add_primary_link(li, f"MIIT-首页-{tab}")

    # 2) 中部四个 tab：部领导活动/司局动态/地方动态/部属动态（floornew）
    for idx, con in enumerate(soup.select("div.tabbox-bd.tabbox-bds4 div.tabbox-bd-con")):
        tab_names = ["部领导活动", "司局动态", "地方动态", "部属动态"]
        tab = tab_names[idx] if idx < len(tab_names) else f"tab{idx}"
        for li in con.select("ul > li"):
            add_primary_link(li, f"MIIT-首页-{tab}")

    # 3) 政策文件/政策解读（floor4 左侧 tabbox-bds2）
    policy_cons = soup.select("div.floor4 div.tabbox-bd.tabbox-bds2 div.tabbox-bd-con")
    if policy_cons:
        names = ["政策文件", "政策解读"]
        for i, con in enumerate(policy_cons[:2]):
            tab = names[i] if i < len(names) else f"tab{i}"
            # 这里的 li 可能包含 dl.tslb-list，需要抓主链接 + 相关解读/相关新闻
            for li in con.select("ul > li"):
                add_related_links_from_policy_li(li, f"MIIT-首页-{tab}")

    # 4) 文件公示/意见征集（floor4 中间 tabbox-bds3）
    for idx, con in enumerate(soup.select("div.floor4 div.tabbox-bd.tabbox-bds3 div.tabbox-bd-con")):
        tab_names = ["文件公示", "意见征集"]
        tab = tab_names[idx] if idx < len(tab_names) else f"tab{idx}"
        for li in con.select("ul > li"):
            add_primary_link(li, f"MIIT-首页-{tab}")
        for p in con.select("p"):
            add_primary_link(p, f"MIIT-首页-{tab}")

    # ===== 过滤（关键词 + 时间窗口）=====
    filtered: List[Item] = []
    for it in items:
        if not keyword_hit(it.title, keywords):
            continue
        if not within_window(it.pub_date, now, window_days, hard_cap_days):
            continue
        filtered.append(it)

    # ===== 去重（URL 归一化）=====
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
#---------------------------------------------------gov.cn 相关解析代码---------------------------------------------------#
# 去重用：忽略 http/https，统一 www，去掉 fragment，规范化 path
def canonicalize_url_for_dedup(url: str) -> str:
    try:
        u = url.strip()
        if u.startswith("//"):
            u = "https:" + u
        parts = urlsplit(u)
        netloc = (parts.netloc or "").lower()
        if netloc.startswith("www."):
            netloc = netloc[4:]
        path = parts.path or "/"
        if path != "/" and path.endswith("/"):
            path = path[:-1]
        # 忽略 scheme 与 fragment；保留 query（一般不影响，但保险）
        return urlunsplit(("", netloc, path, parts.query or "", ""))
    except Exception:
        return url


# 从 gov.cn 的 URL 粗略提取日期（弱兜底）
_RE_GOV_YYYYMM = re.compile(r"/(20\d{2})(0[1-9]|1[0-2])/(?:content_|index\.htm|)$")
_RE_GOV_ANY_YYYYMM = re.compile(r"/(20\d{2})(0[1-9]|1[0-2])/")
def extract_gov_date_from_url(url: str) -> Optional[str]:
    # 先用更严格的匹配，失败再用宽松的
    m = _RE_GOV_YYYYMM.search(url)
    if not m:
        m = _RE_GOV_ANY_YYYYMM.search(url)
    if not m:
        return None
    yyyy, mm = m.group(1), m.group(2)
    # 首页 URL 通常只有年月，没有日；这里用当月 01 日做“窗口过滤兜底”
    return f"{yyyy}-{mm}-01"



_RE_DATE_YMD = re.compile(r"(20\d{2})[.\-/年](0?[1-9]|1[0-2])[.\-/月](0?[1-9]|[12]\d|3[01])日?")
def extract_gov_pub_date_from_article_html(article_html: str) -> Optional[str]:
    # 1) 常见 meta（不同频道不一致，尽量多兜）
    # 例如：<meta name="others" content="页面生成时间 2026-01-21 08:52:30" />
    # 注意：这是页面生成时间，不等于发布时间；但文章页通常会有“发布时间/日期”字段。
    # 我们先对全文做日期扫描，结合“发布时间”等上下文更稳。
    text = article_html

    # 2) 优先：含“发布时间/日期/来源”等附近的日期
    # 例：发布时间：2026-01-20 19:30
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
            return f"{y}-{mo}-{d}"

    # 3) 次优：全文第一个“合法日期”
    m = _RE_DATE_YMD.search(text)
    if m:
        y, mo, d = m.group(1), m.group(2).zfill(2), m.group(3).zfill(2)
        return f"{y}-{mo}-{d}"

    return None


def parse_gov_home(config: dict, now: datetime) -> List[Item]:
    src = config["sources"]["gov_home"]
    base_url = src["url"]

    html = http_get(base_url)
    soup = BeautifulSoup(html, "lxml")

    fetched_at = now.astimezone(SG_TZ).isoformat(timespec="seconds")
    items: List[Item] = []

    keywords = config["keywords"]
    window_days = int(config["window_days"])
    hard_cap_days = int(config["hard_cap_days"])

    # 是否请求文章页补发布时间（强烈建议 True）
    resolve_pub_date = bool(config.get("resolve_pub_date", True))
    # 避免首页链接太多导致抓取过慢，可设置上限
    resolve_cap = int(config.get("resolve_pub_date_cap", 60))

    # 文章页日期缓存，避免同 URL 重复请求
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

        pub_date = extract_date(title)  # 一般不含
        if not pub_date:
            pub_date = extract_gov_date_from_url(url)

        it = Item(
            title=title,
            publisher=src["name"],
            url=url,
            pub_date=pub_date,
            source=source_tag,
            fetched_at=fetched_at,
        )
        return it

    def add_links(selector: str, source_tag: str):
        for a in soup.select(selector):
            href = a.get("href")
            if not href:
                continue
            title = a.get_text(" ", strip=True)
            it = build_item(title, href, source_tag)
            if it:
                items.append(it)

    add_links("#index_tpxw .slider_carousel .item h4 a[href]", "GOV-首页-焦点图片")
    add_links("#index_ywowen ul li a[href]", "GOV-首页-要闻")
    add_links("#index_zxzc ul li a[href]", "GOV-首页-最新政策")
    add_links("#index_zcjd ul li a[href]", "GOV-首页-政策解读")
    add_links("#index_gwygzjxs ul.ul1 li a[href]", "GOV-首页-国新办")
    add_links("#index_zwlb ul.ul2 li a[href]", "GOV-首页-政务联播")
    add_links("#index_jyzj ul.ul01 li a[href]", "GOV-首页-建言征集/回应关切")

    # 补齐真实发布时间：访问文章页提取日期
    if resolve_pub_date:
        # 先按 URL 去个粗重，减少请求量
        temp_uniq: Dict[str, Item] = {}
        for it in items:
            k = canonicalize_url_for_dedup(it.url)
            if k not in temp_uniq:
                temp_uniq[k] = it
        uniq_items = list(temp_uniq.values())

        # 只对前 N 条补日期（可通过配置调大）
        for it in uniq_items[:resolve_cap]:
            d = resolve_pub_date_for_url(it.url)
            if d:
                it.pub_date = d

        # 用补完日期的对象回填
        items = uniq_items

    # 过滤：关键词 + 时间窗口
    filtered: List[Item] = []
    for it in items:
        if not keyword_hit(it.title, keywords):
            continue
        if it.pub_date:
            if not within_window(it.pub_date, now, window_days, hard_cap_days):
                continue
        filtered.append(it)

    # 去重
    uniq: Dict[str, Item] = {}
    for it in filtered:
        key = canonicalize_url_for_dedup(it.url)
        if key not in uniq:
            uniq[key] = it
        else:
            old = uniq[key]
            # 优先保留“有 pub_date”的；都有则保留更晚的
            if (not old.pub_date) and it.pub_date:
                uniq[key] = it
            elif old.pub_date and it.pub_date:
                try:
                    if dtparser.parse(it.pub_date) > dtparser.parse(old.pub_date):
                        uniq[key] = it
                except Exception:
                    pass

    return list(uniq.values())


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
    now = datetime.now(tz=SG_TZ)

    all_items: List[Item] = []
    all_items.extend(parse_miit_home(config, now))
    all_items.extend(parse_gov_home(config, now))

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
