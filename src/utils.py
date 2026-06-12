"""
公共工具函数。

提供日期提取、关键词匹配、HTTP 请求、URL 规范化等通用逻辑，
供各解析器模块（parsers/）复用。
"""

import posixpath
import re
from datetime import datetime, timedelta
from typing import List, Optional
from urllib.parse import parse_qsl, urlencode, urlsplit, urljoin, urlunsplit

import requests
from dateutil import parser as dtparser

from models import DATE_PATTERNS, SG_TZ, USER_AGENT

# URL 去重时应剥离的跟踪/会话类查询参数前缀或名称
# 注：from_ 前缀容易误伤政府站点 from_id/from_year 等业务参数，
#     此处改为显式列举已知的分享/会话参数名。
_TRACKING_PARAM_PREFIXES = ("utm_", "spm", "wt_")
_TRACKING_PARAM_NAMES = frozenset({
    "from", "spm", "share_source", "share_medium", "share_token", "share_from",
    "from_source", "from_share", "from_spm", "from_uid",
    "luicode", "lfid", "launchid", "sourceType", "sudaref",
    "_t", "_r", "_", "timestamp",
})

# 部分域名归一映射：value 为统一后的 netloc
_DOMAIN_ALIAS = {
    "m.weibo.cn": "weibo.com",
    "weibo.cn": "weibo.com",
    "www.weibo.com": "weibo.com",
    "video.weibo.com": "weibo.com",
    "live.media.weibo.com": "weibo.com",
}

# 标题中常见的零宽/格式控制字符
_ZERO_WIDTH_CHARS = re.compile(r"[\u200b\u200c\u200d\u2060\ufeff]")
# 末尾省略号（中英文）
_TRAILING_ELLIPSIS = re.compile(r"[\s]*(\.{3,}|…+)\s*$")


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
    对空值或无法解析的字符串返回空字符串。
    """
    if not date_str or not str(date_str).strip() or str(date_str).strip().lower() == "nan":
        return ""
    try:
        dt = dtparser.parse(str(date_str))
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return ""


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


def keyword_hit_first_sentence(text: str, keywords: List[str]) -> bool:
    """
    判断文本的前半部分是否包含任意关键词。

    前半部分优先取第一个中文句号'。'或英文句号'.'之前的内容；
    若不存在句号，则取文本的前 50%。

    英文关键词使用词边界匹配，中文使用子串匹配。
    """
    if not text:
        return False

    # 查找第一个中文句号或英文句号
    for i, ch in enumerate(text):
        if ch in '。.':
            prefix = text[:i]
            break
    else:
        # 无句号时取前 50%
        cut = max(1, len(text) // 2)
        prefix = text[:cut]

    return keyword_hit(prefix, keywords)


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


def _strip_tracking_params(query: str) -> str:
    """剥离跟踪/会话类查询参数，其余按字母序保留以保证稳定。"""
    if not query:
        return ""
    pairs = parse_qsl(query, keep_blank_values=True)
    kept = []
    for k, v in pairs:
        k_low = k.lower()
        if k_low in _TRACKING_PARAM_NAMES:
            continue
        if any(k_low.startswith(p) for p in _TRACKING_PARAM_PREFIXES):
            continue
        kept.append((k, v))
    if not kept:
        return ""
    kept.sort(key=lambda kv: kv[0])
    return urlencode(kept, doseq=True)


def _collapse_path(path: str) -> str:
    """折叠 URL 路径中的 /./ 和 /../，并去除多余斜杠。"""
    if not path:
        return "/"
    # posixpath.normpath 会把 // 折成 /，把 /./ 去掉，把 /a/b/../c 折成 /a/c
    collapsed = posixpath.normpath(path)
    # 保留以 / 开头
    if not collapsed.startswith("/"):
        collapsed = "/" + collapsed
    # 保留原始尾部斜杠语义（normpath 会去掉尾部斜杠）
    if path != "/" and path.endswith("/") and not collapsed.endswith("/"):
        collapsed += "/"
    return collapsed


def canonicalize_url_for_dedup(url: str) -> str:
    """规范化 URL 用于去重。

    处理：
    1. 补全协议、统一小写 scheme/host、去 www. 前缀；
    2. 通过域名别名表统一同源站点（如微博的 m./www./video. 子域）；
    3. 折叠路径中的 /./ 和 /../，去除重复斜杠与尾部斜杠；
    4. 去除 fragment；
    5. 剥离 utm_/spm/from 等跟踪参数，剩余 query 按字母序输出。
    """
    if not url:
        return url
    try:
        u = url.strip()
        if u.startswith("//"):
            u = "https:" + u
        parts = urlsplit(u)
        netloc = parts.netloc.lower()
        if netloc.startswith("www."):
            netloc = netloc[4:]
        netloc = _DOMAIN_ALIAS.get(netloc, netloc)
        path = _collapse_path(parts.path or "/")
        if path != "/" and path.endswith("/"):
            path = path[:-1]
        query = _strip_tracking_params(parts.query or "")
        return urlunsplit(("", netloc, path, query, ""))
    except Exception:
        return url


def clean_title(title: str) -> str:
    """清洗标题：去除零宽字符、首尾空白和末尾省略号。

    用于入库前的统一处理，避免视觉相同但二进制不同的标题被判为不同。
    """
    if not title:
        return ""
    t = _ZERO_WIDTH_CHARS.sub("", str(title))
    # 内部多余空白折叠为单个空格
    t = re.sub(r"[\t\r\n\u00a0]+", " ", t)
    t = re.sub(r" {2,}", " ", t)
    t = t.strip()
    # 去掉末尾省略号
    t = _TRAILING_ELLIPSIS.sub("", t).strip()
    return t


def format_fetched_at(now: datetime) -> str:
    """将 datetime 格式化为查询时间字符串 YYYY-MM-DD HH:MM:SS（UTC+8）。"""
    return now.astimezone(SG_TZ).strftime("%Y-%m-%d %H:%M:%S")


def normalize_fetched_at(value: str, fallback: Optional[datetime] = None) -> str:
    """将任意"查询时间"字符串统一为 YYYY-MM-DD HH:MM:SS（UTC+8）。

    - 空值/无法解析时返回 fallback 对应的字符串（若提供）或空串；
    - 已经是该格式的输入会被原样返回。
    """
    s = (value or "").strip()
    if s and s.lower() != "nan":
        try:
            dt = dtparser.parse(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=SG_TZ)
            return dt.astimezone(SG_TZ).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass
    if fallback is not None:
        return format_fetched_at(fallback)
    return ""


def normalize_publisher(name: str, alias_map: Optional[dict] = None) -> str:
    """根据 alias_map 把发布单位归一到标准名。

    - 输入会先剥离零宽字符、合并空白；
    - 命中映射时返回 alias_map[name]，否则返回清洗后的原值；
    - alias_map 为空/None 时仅做基础清洗。
    """
    if not name:
        return ""
    # 复用 clean_title 的轻量清洗逻辑（去零宽 + 合并空白），但不去末尾省略号
    s = _ZERO_WIDTH_CHARS.sub("", str(name))
    s = re.sub(r"[\t\r\n\u00a0]+", " ", s)
    s = re.sub(r" {2,}", " ", s).strip()
    if alias_map:
        return alias_map.get(s, s)
    return s
