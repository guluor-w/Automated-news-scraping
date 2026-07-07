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
    """发起 GET 请求并返回响应文本，自动处理编码。

    针对部分政府站点存在的老旧 SSL 套件（如山东省政府站
    SSLV3_ALERT_HANDSHAKE_FAILURE、湖南省政府站 BAD_ECPOINT，这些
    握手错误在新版 OpenSSL 中无法通过放宽 cipher 绕过），在首次 HTTPS
    握手失败时，自动降级到同域名的 ``http://`` 重试一次，确保解析
    流程不会被 SSL 兼容性问题阻塞。
    """
    headers = {"User-Agent": USER_AGENT}
    try:
        resp = requests.get(url, headers=headers, timeout=timeout)
    except requests.exceptions.SSLError:
        # SSL 握手失败：尝试同域名的 http:// 明文协议
        if url.startswith("https://"):
            http_url = "http://" + url[len("https://"):]
            resp = requests.get(http_url, headers=headers, timeout=timeout)
        else:
            raise
    resp.raise_for_status()
    resp.encoding = resp.apparent_encoding or "utf-8"
    return resp.text


# ---------------------------------------------------------------------------
# 详情页标题二次确认
# ---------------------------------------------------------------------------
#
# 背景：部分信源列表页的 <a> 标签把"标题 + 摘要 + 日期"拼在一起（如招商局
# 集团首页），或在 title 属性末尾紧贴日期（如山东工信厅），仅靠列表页清洗
# 规则难以完美还原标题。本工具在列表层判定"可疑"后，回源详情页用结构化
# 元数据字段提取权威标题。
#
# 字段优先级（基于对央企/政府站的实际探测结果）：
#   1. <meta name="ArticleTitle" content="..."> —— 国内政务/央企站普遍遵循，最纯净
#   2. <h1>                                       —— 标准主标题
#   3. <h2>                                       —— 招商局/山东工信厅等用作主标题
#   4. <title>                                    —— 末尾常含"- 站名"，需剥后缀
#
# 注：og:title / twitter:title 在国内政府/央企站普遍缺失，不作为主优先级。

import json
from pathlib import Path

# 详情页标题本地缓存：避免重复请求同一详情页
_DETAIL_TITLE_CACHE_PATH = Path(".cache") / "detail_titles.json"
_DETAIL_TITLE_CACHE: Optional[dict] = None  # 进程内单例

# 详情页 <title> 标签常见的"- 站名 / _ 站名"后缀分隔符
_RE_SITE_NAME_SUFFIX = re.compile(
    r"\s*[-_|–—]\s*[^-_|–—]{2,20}\s*$"
)


def _load_detail_title_cache() -> dict:
    """惰性加载详情页标题缓存（首次访问时读盘）。"""
    global _DETAIL_TITLE_CACHE
    if _DETAIL_TITLE_CACHE is not None:
        return _DETAIL_TITLE_CACHE
    try:
        if _DETAIL_TITLE_CACHE_PATH.exists():
            with _DETAIL_TITLE_CACHE_PATH.open("r", encoding="utf-8") as f:
                _DETAIL_TITLE_CACHE = json.load(f)
        else:
            _DETAIL_TITLE_CACHE = {}
    except Exception:
        _DETAIL_TITLE_CACHE = {}
    return _DETAIL_TITLE_CACHE


def _save_detail_title_cache() -> None:
    """将详情页标题缓存写盘。"""
    if _DETAIL_TITLE_CACHE is None:
        return
    try:
        _DETAIL_TITLE_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with _DETAIL_TITLE_CACHE_PATH.open("w", encoding="utf-8") as f:
            json.dump(_DETAIL_TITLE_CACHE, f, ensure_ascii=False, indent=2)
    except Exception:
        # 缓存写入失败不影响主流程
        pass


def _extract_title_from_html(html: str) -> Optional[str]:
    """从详情页 HTML 中按字段优先级提取标题，提取不到返回 None。"""
    if not html:
        return None
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return None
    soup = BeautifulSoup(html, "html.parser")

    # 优先级 1: <meta name="ArticleTitle">（国内政务站普遍遵循 GB/T 23287）
    for attr_name in ("ArticleTitle", "articletitle", "article:title", "article_title"):
        m = soup.find("meta", attrs={"name": attr_name})
        if m and m.get("content"):
            val = m.get("content").strip()
            if val:
                return val

    # 优先级 2: <h1>
    h1 = soup.find("h1")
    if h1:
        val = re.sub(r"\s+", " ", h1.get_text(strip=True))
        if val and 4 <= len(val) <= 120:
            return val

    # 优先级 3: <h2>（招商局、山东工信厅等用作主标题）
    h2 = soup.find("h2")
    if h2:
        val = re.sub(r"\s+", " ", h2.get_text(strip=True))
        if val and 4 <= len(val) <= 120:
            return val

    # 优先级 4: <title>，剥末尾"- 站名"后缀
    ttl = soup.find("title")
    if ttl:
        val = re.sub(r"\s+", " ", ttl.get_text(strip=True))
        # 反复剥末尾的"- 站名"段，直到稳定
        for _ in range(3):
            stripped = _RE_SITE_NAME_SUFFIX.sub("", val).strip()
            if stripped == val or len(stripped) < 4:
                break
            val = stripped
        if val and 4 <= len(val) <= 120:
            return val

    return None


def fetch_detail_title(url: str, timeout: int = 15) -> Optional[str]:
    """访问详情页，返回结构化字段提取的标题；带文件缓存与多重兜底。

    用法：仅在列表页标题"可疑"时调用，避免每条都回源造成请求风暴。

    返回 ``None`` 表示提取失败（HTTP 失败 / 无可用字段 / 长度异常），
    调用方应回退到列表页清洗后的标题。
    """
    if not url or not isinstance(url, str):
        return None

    cache = _load_detail_title_cache()
    if url in cache:
        val = cache[url]
        return val if val else None  # 缓存的失败结果（None / "")也会命中，避免重试

    title: Optional[str] = None
    try:
        html = http_get(url, timeout=timeout)
        title = _extract_title_from_html(html)
    except Exception:
        title = None

    # 写缓存（无论成功失败都缓存，失败缓存可避免反复请求异常页）
    cache[url] = title or ""
    _save_detail_title_cache()

    return title


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


# 标题指纹归一化：去除标点/空白/大小写差异后用于跨页同新闻识别。
_TITLE_FP_STRIP = re.compile(r"[\s\u3000\W_]+", flags=re.UNICODE)


def title_fingerprint(title: str) -> str:
    """生成用于跨入口去重比对的标题指纹（去除标点、空白、大小写差异）。

    主要用于：同一个信源的主页与二级新闻栏目同时收录同一篇新闻，
    但 URL 形式不同时（如相对路径 vs 绝对路径、列表页摘要链接 vs 详情页链接），
    通过标题指纹识别它们指向同一篇新闻。
    """
    if not title:
        return ""
    t = clean_title(title).lower()
    return _TITLE_FP_STRIP.sub("", t)


def _item_quality_score(item) -> int:
    """给 Item 打分以择优：发布日期和有效标题都非空的更优。

    用于"同一信源主页与二级页同时存在某新闻"的择优场景：
    以能正确采集标题和发布日期的为准。
    """
    score = 0
    if getattr(item, "pub_date", None):
        score += 10
    title = getattr(item, "title", "") or ""
    if title.strip():
        score += 1
        # 标题越完整（未被列表页截断）越好，但封顶避免长杂讯标题得高分
        score += min(len(title), 60) // 10
    return score


def dedup_items_keep_best(items):
    """对同一信源/同一解析器内的 Item 列表做两级去重，择优保留。

    去重策略（按用户需求："以能正确采集标题和发布日期等信息的为准"）：

    1. **一级 URL 去重**：按 ``canonicalize_url_for_dedup(item.url)`` 归一后比对，
       URL 等价者择优保留（先比较是否有 ``pub_date``，再比较 ``pub_date`` 较新者）。
    2. **二级标题去重**：在同一 ``publisher`` 内按 ``title_fingerprint(title)``
       归一后比对（用于主页/二级栏目页同时收录同一新闻但 URL 不同的场景），
       以 ``_item_quality_score`` 较高者（即标题+日期更完整者）为准。

    Args:
        items: Item 列表（需具备 ``url``、``title``、``publisher``、``pub_date`` 属性）。

    Returns:
        去重并择优后的 Item 列表，相对顺序保留首次出现位置。
    """
    if not items:
        return []

    # ── 1. 一级 URL 去重（保留首次出现顺序）
    url_index: dict = {}
    ordered: List = []
    for it in items:
        key = canonicalize_url_for_dedup(getattr(it, "url", "") or "")
        if not key:
            ordered.append(it)
            continue
        if key not in url_index:
            url_index[key] = len(ordered)
            ordered.append(it)
        else:
            idx = url_index[key]
            old = ordered[idx]
            # 优先选择有 pub_date 的；若都有，选 pub_date 较新的
            if (not getattr(old, "pub_date", None)) and getattr(it, "pub_date", None):
                ordered[idx] = it
            elif getattr(old, "pub_date", None) and getattr(it, "pub_date", None):
                try:
                    if dtparser.parse(it.pub_date) > dtparser.parse(old.pub_date):
                        ordered[idx] = it
                except (ValueError, TypeError):
                    pass

    # ── 2. 二级标题指纹去重（同一发布单位内）
    # 仅当标题指纹非空时合并，避免空标题误合并；
    # 跨 publisher 的同名新闻不在此处合并（交由 storage.dedup_merge 处理）。
    fp_index: dict = {}
    result: List = []
    for it in ordered:
        publisher = getattr(it, "publisher", "") or ""
        fp = title_fingerprint(getattr(it, "title", "") or "")
        if not fp or not publisher:
            result.append(it)
            continue
        composite_key = (publisher, fp)
        if composite_key not in fp_index:
            fp_index[composite_key] = len(result)
            result.append(it)
        else:
            idx = fp_index[composite_key]
            old = result[idx]
            if _item_quality_score(it) > _item_quality_score(old):
                result[idx] = it

    return result


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


# ── URL 域名提取与白名单比对（用于二次检验：丢弃跳转到外站的新闻） ────────────

# 常见的双段后缀（pseudo-eTLD），用于把 host 折叠到"注册域"（eTLD+1）层级。
# 仅覆盖项目实际涉及的中国常见后缀；不在此表中的按 host 末两段处理。
_MULTI_PART_TLDS = frozenset({
    "gov.cn", "com.cn", "net.cn", "org.cn", "edu.cn", "ac.cn",
    "com.hk", "org.hk", "gov.hk",
    "com.mo", "gov.mo",
    "com.tw", "org.tw",
})


def extract_host(url: str) -> str:
    """从 URL 中提取小写 host（不含端口与 www. 前缀）；解析失败时返回空串。"""
    if not url:
        return ""
    try:
        u = str(url).strip()
        if u.startswith("//"):
            u = "https:" + u
        host = urlsplit(u).netloc.lower()
        if "@" in host:  # 去掉 userinfo
            host = host.split("@", 1)[1]
        if ":" in host:  # 去掉端口
            host = host.split(":", 1)[0]
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return ""


def extract_registered_domain(url_or_host: str) -> str:
    """提取"注册域"（eTLD+1）。

    例：
        www.miit.gov.cn → miit.gov.cn
        wap.miit.gov.cn → miit.gov.cn
        wap.gov.cn      → wap.gov.cn   （仅 3 段，已是 eTLD+1，按原值返回）
        gov.cn          → gov.cn       （≤ 2 段，按 host 原样返回）
        example.com     → example.com

    注：双段后缀（如 ``gov.cn``）下，host 段数 < 3 时无法再向左折叠，
    本函数保持 host 原样返回，避免把 ``*.gov.cn`` 这类域名全部塌缩到
    ``gov.cn`` 而导致后续白名单比对过于宽松。无法识别则返回 host 本身
    （保守处理）。
    """
    if not url_or_host:
        return ""
    s = str(url_or_host).strip().lower()
    # 既支持完整 URL，也支持裸 host
    host = extract_host(s) if ("://" in s or s.startswith("//") or "/" in s) else s
    if not host:
        return ""
    parts = host.split(".")
    if len(parts) <= 2:
        return host
    # 检测末两段是否为双段后缀（如 gov.cn）
    tail2 = ".".join(parts[-2:])
    if tail2 in _MULTI_PART_TLDS and len(parts) >= 3:
        return ".".join(parts[-3:])
    return tail2


def host_matches_allowed(url: str, allowed_domains: List[str]) -> bool:
    """判断 URL 的 host 是否落在允许域名清单内（根源包含匹配）。

    匹配规则：
    - 取 URL 的 host（已去 www.）；
    - 若 host == allowed 或 host.endswith("." + allowed)，视为命中；
    - allowed_domains 为空或 host 无法解析时，按调用方策略另行处理；
      本函数仅返回布尔值，无法解析的 host 一律返回 False。
    """
    host = extract_host(url)
    if not host:
        return False
    for raw in allowed_domains or []:
        d = str(raw or "").strip().lower()
        if d.startswith("www."):
            d = d[4:]
        if not d:
            continue
        if host == d or host.endswith("." + d):
            return True
    return False
