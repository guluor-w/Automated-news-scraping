import csv
import hashlib
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Dict, Tuple
from pathlib import Path

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
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

SG_TZ = timezone(timedelta(hours=8))  # Asia/Singapore 固定 +08:00

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

DATE_PATTERNS = [
    # 2026-01-16 或 2026/01/16
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
    pub_date: Optional[str]  
    source: str
    fetched_at: str          

    
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
    try:
        u = url.strip()
        if u.startswith("//"):
            u = "https:" + u
        parts = urlsplit(u)
        netloc = parts.netloc.lower()
        if netloc.startswith("www."):
            netloc = netloc[4:]
        path = parts.path or "/"
        if path != "/" and path.endswith("/"):
            path = path[:-1]
        return urlunsplit(("", netloc, path, parts.query or "", ""))
    except Exception:
        return url  


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

    # 过滤（关键词 + 时间窗口）
    filtered: List[Item] = []
    for it in items:
        if not keyword_hit(it.title, keywords):
            continue
        if not within_window(it.pub_date, now, window_days, hard_cap_days):
            continue
        filtered.append(it)

    # 去重（URL 归一化）
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
# 去重用：忽略 http/https
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
        return urlunsplit(("", netloc, path, parts.query or "", ""))
    except Exception:
        return url


# 从 gov.cn 的 URL 粗略提取日期（弱兜底）
_RE_GOV_YYYYMM = re.compile(r"/(20\d{2})(0[1-9]|1[0-2])/(?:content_|index\.htm|)$")
_RE_GOV_ANY_YYYYMM = re.compile(r"/(20\d{2})(0[1-9]|1[0-2])/")
def extract_gov_date_from_url(url: str) -> Optional[str]:
    m = _RE_GOV_YYYYMM.search(url)
    if not m:
        m = _RE_GOV_ANY_YYYYMM.search(url)
    if not m:
        return None
    yyyy, mm = m.group(1), m.group(2)
    return f"{yyyy}-{mm}-01"



_RE_DATE_YMD = re.compile(r"(20\d{2})[.\-/年](0?[1-9]|1[0-2])[.\-/月](0?[1-9]|[12]\d|3[01])日?")
def extract_gov_pub_date_from_article_html(article_html: str) -> Optional[str]:
    # 1) 常见 meta（不同频道不一致，尽量多兜）
    # 例如：<meta name="others" content="页面生成时间 2026-01-21 08:52:30" />
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

    # 是否请求文章页补发布时间
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

        # 只对前 N 条补日期（可调大）
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
            if (not old.pub_date) and it.pub_date:
                uniq[key] = it
            elif old.pub_date and it.pub_date:
                try:
                    if dtparser.parse(it.pub_date) > dtparser.parse(old.pub_date):
                        uniq[key] = it
                except Exception:
                    pass

    return list(uniq.values())

#-------------------------------------- qqnews search (腾讯新闻) --------------------------------------#
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
    解析腾讯新闻搜索结果里的 time 字段，尽量转成带时区的 datetime。
    """
    s = (s or "").strip()
    if not s:
        return None
    # 相对时间
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

    # 绝对时间
    try:
        dt = dtparser.parse(s, fuzzy=True)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=SG_TZ)
        else:
            dt = dt.astimezone(SG_TZ)
        return dt
    except Exception:
        return None


def _qqnews_search_fetch_page(session: requests.Session, query: str, page: int, limit: int) -> dict:
    payload = {
        "page": str(page),                 
        "query": query,
        "is_pc": "1",
        "hippy_custom_version": "24",
        "search_type": "all",
        "search_count_limit": str(limit),
        "appver": "15.5_qqnews_7.1.80",
    }
    resp = session.post(QQNEWS_API_URL, data=payload, headers=QQNEWS_HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.json()


def parse_qqnews_search(config: dict, now: datetime) -> List[Item]:
    """
    目标：调用腾讯新闻 PC 搜索 API，搜索指定关键词（默认：工信微报），收集近 15 天的结果写入统一 CSV。
    说明：
    - 仅保留 secList 中 component=pictext（图文）且含 newsList 的条目
    - 时间字段优先使用 API 返回的 time；无法解析则跳过（满足“近15天”约束）
    """
    src = config["sources"].get("qqnews_search")
    window_days = config.get("window_days", 15)
    query = (src.get("query") or "工信微报").strip()
    max_pages = int(src.get("max_pages") or 5)
    page_size = int(src.get("page_size") or 20)

    threshold = now - timedelta(days=window_days)
    fetched_at = now.astimezone(SG_TZ).isoformat(timespec="seconds")

    session = requests.Session()
    items: List[Item] = []

    for page in range(max_pages):
        raw = _qqnews_search_fetch_page(session, query=query, page=page, limit=page_size)
        sec_list = raw.get("secList") or []

        page_items: List[Item] = []
        page_min_dt: Optional[datetime] = None
        for sec in sec_list:
            try:
                # 图文：component=pictext；同时 secType 通常为 0
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
                        # 无法判断时间的条目直接跳过
                        continue

                    if page_min_dt is None or dt < page_min_dt:
                        page_min_dt = dt

                    if dt < threshold:
                        continue

                    pub_date = dt.date().isoformat()
                    publisher = (n.get("source") or src.get("name") or "腾讯新闻").strip()
                    page_items.append(Item(
                        title=title,
                        publisher=publisher,
                        url=url,
                        pub_date=pub_date,
                        source=f"腾讯新闻搜索-{query}",
                        fetched_at=fetched_at,
                    ))
            except Exception:
                continue

        items.extend(page_items)

        # 终止
        if raw.get("hasMore") in (0, "0", False):
            break
        if page_min_dt and page_min_dt < threshold:
            break
    
    keywords = config["keywords"]
    filtered: List[Item] = []
    for it in items:
        if not keyword_hit(it.title, keywords):
            continue
        filtered.append(it)    

    return filtered
#--------------------------------------------------------------------------------------------------------------——#


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
    repo_root = Path(__file__).resolve().parents[1]
    config_path = repo_root / "config.yaml"

    config = load_config(config_path)
    now = datetime.now(tz=SG_TZ)

    all_items: List[Item] = []
    all_items.extend(parse_miit_home(config, now))
    all_items.extend(parse_gov_home(config, now))
    all_items.extend(parse_qqnews_search(config, now))

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
