"""
公共工具函数。

提供日期提取、关键词匹配、HTTP 请求、URL 规范化等通用逻辑，
供各解析器模块（parsers/）复用。
"""

import re
from datetime import datetime, timedelta
from typing import List, Optional
from urllib.parse import urlsplit, urljoin, urlunsplit

import requests
from dateutil import parser as dtparser

from models import DATE_PATTERNS, SG_TZ, USER_AGENT


def sha1(s: str) -> str:
    """计算字符串的 SHA-1 十六进制摘要。"""
    import hashlib
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def normalize_url(base: str, href: str) -> str:
    """将相对 href 补全为绝对 URL（相对于 base）。"""
    href = href.strip()
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("//"):
        return "https:" + href
    if href.startswith("/"):
        return urljoin(base, href)
    return base.rstrip("/") + "/" + href


def normalize_pub_date(date_str: str) -> str:
    """将任意合法日期字符串统一转换为 YYYY-MM-DD 格式。

    支持输入格式包括 YYYY-MM-DD、YYYY/MM/DD、YYYY-MM-DDTHH:MM:SS+TZ 等。
    对空值或无法解析的字符串原样返回。
    """
    if not date_str or not str(date_str).strip():
        return date_str
    try:
        dt = dtparser.parse(str(date_str))
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return date_str


def extract_date(text: str) -> Optional[str]:
    """从任意文本中尝试提取 YYYY-MM-DD 格式的日期字符串。"""
    if not text:
        return None
    for pat in DATE_PATTERNS:
        m = pat.search(text)
        if m:
            y, mo, d = map(int, m.groups())
            try:
                return f"{y}-{mo:02d}-{d:02d}"
            except Exception:
                return None
    try:
        dt = dtparser.parse(text, fuzzy=True)
        if dt.year >= 2000:
            return dt.strftime("%Y-%m-%d")
    except Exception:
        pass
    return None


def keyword_hit(title: str, keywords: List[str]) -> bool:
    """判断标题是否包含任意关键词。英文关键词使用词边界匹配，中文使用子串匹配。"""
    t = (title or "").lower()
    for k in keywords:
        k_lower = k.lower()
        if k.isalpha() and k.isascii():
            # 英文关键词：使用词边界 \b 进行完整词匹配，避免误匹配
            pattern = r'\b' + re.escape(k_lower) + r'\b'
            if re.search(pattern, t):
                return True
        else:
            # 中文或混合关键词：使用子串匹配
            if k_lower in t:
                return True
    return False


def within_window(pub_date: Optional[str], now: datetime, window_days: int, hard_cap_days: int) -> bool:
    """
    判断 pub_date 是否在有效时间窗口内。
    - pub_date 为 None 时视为在窗口内（不过滤）。
    - 超过 hard_cap_days 的记录一律过滤。
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

    return d >= (now - timedelta(days=window_days)).date()


def http_get(url: str, timeout: int = 30) -> str:
    """发起 GET 请求并返回响应文本，自动处理编码。"""
    resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=timeout)
    resp.raise_for_status()
    resp.encoding = resp.apparent_encoding or "utf-8"
    return resp.text


def canonicalize_url_for_dedup(url: str) -> str:
    """规范化 URL 用于去重（去除协议、www 前缀和尾部斜杠）。"""
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


def format_fetched_at(now: datetime) -> str:
    """将 datetime 格式化为查询时间字符串 YYYY-MM-DD HH:MM:SS（UTC+8）。"""
    return now.astimezone(SG_TZ).strftime("%Y-%m-%d %H:%M:%S")
