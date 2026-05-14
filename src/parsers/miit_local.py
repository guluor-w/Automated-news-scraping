"""
工信部地方主管部门网站（省级工业和信息化厅/局/委）解析器。

入口函数
--------
parse_miit_local(config, now) -> List[Item]
    遍历全国 32 个省级工信主管部门官网，提取新闻链接，
    按关键词和时间窗口过滤后返回新闻列表。

数据来源
--------
各省份 URL 取自 https://www.miit.gov.cn/zzjg/index.html#dfzgbm

配置（config.yaml）
-------------------
sources:
  miit_local:
    name: 地方工信部门
    enabled: true

实现策略
--------
由于 32 个网站结构各异，采用通用抓取方式：
  1. HTTP GET 获取首页 HTML
  2. BeautifulSoup 提取所有 <a> 标签
  3. 通过 URL 路径中的日期模式识别新闻链接
  4. 从 URL 路径或相邻文本中提取发布日期
  5. 过滤导航链接、过短标题，按关键词和时间窗口筛选
"""

import logging
import re
from datetime import datetime
from typing import Dict, List, Optional

from bs4 import BeautifulSoup
from dateutil import parser as dtparser

from models import Item, MIIT_ONLY_KEYWORDS
from utils import (
    canonicalize_url_for_dedup,
    extract_date,
    format_fetched_at,
    http_get,
    keyword_hit,
    normalize_url,
    within_window,
)

logger = logging.getLogger(__name__)

# ── 地方工信部门源列表 ──────────────────────────────────────────────────────────

MIIT_LOCAL_SOURCES = [
    {"province": "北京", "name": "北京市经济和信息化局", "url": "https://jxj.beijing.gov.cn/jxdt/tzgg/"},
    {"province": "天津", "name": "天津市工业和信息化局", "url": "https://gyxxh.tj.gov.cn/"},
    {"province": "河北", "name": "河北省工业和信息化厅", "url": "http://gxt.hebei.gov.cn/"},
    {"province": "山西", "name": "山西省工业和信息化厅", "url": "http://gxt.shanxi.gov.cn/"},
    {"province": "内蒙古", "name": "内蒙古自治区工业和信息化厅", "url": "http://gxt.nmg.gov.cn/"},
    {"province": "辽宁", "name": "辽宁省工业和信息化厅", "url": "http://gxt.ln.gov.cn/"},
    {"province": "吉林", "name": "吉林省工业和信息化厅", "url": "http://gxt.jl.gov.cn/"},
    {"province": "黑龙江", "name": "黑龙江省工业和信息化厅", "url": "http://gxt.hlj.gov.cn/"},
    {"province": "上海", "name": "上海市经济和信息化委员会", "url": "http://www.sheitc.sh.gov.cn/"},
    {"province": "江苏", "name": "江苏省工业和信息化厅", "url": "http://gxt.jiangsu.gov.cn/"},
    {"province": "浙江", "name": "浙江省经济和信息化厅", "url": "http://jxt.zj.gov.cn/"},
    {"province": "安徽", "name": "安徽省工业和信息化厅", "url": "http://jx.ah.gov.cn/"},
    {"province": "福建", "name": "福建省工业和信息化厅", "url": "http://gxt.fujian.gov.cn/"},
    {"province": "江西", "name": "江西省工业和信息化厅", "url": "http://gxt.jiangxi.gov.cn/"},
    {"province": "山东", "name": "山东省工业和信息化厅", "url": "http://gxt.shandong.gov.cn/"},
    {"province": "河南", "name": "河南省工业和信息化厅", "url": "http://gxt.henan.gov.cn/"},
    {"province": "湖北", "name": "湖北省经济和信息化厅", "url": "http://jxt.hubei.gov.cn/"},
    {"province": "湖南", "name": "湖南省工业和信息化厅", "url": "http://gxt.hunan.gov.cn/"},
    {"province": "广东", "name": "广东省工业和信息化厅", "url": "https://gdii.gd.gov.cn/zwgk/tzgg1011/index.html"},
    {"province": "广西", "name": "广西壮族自治区工业和信息化厅", "url": "http://gxt.gxzf.gov.cn/wzsy/tzgg_6719901/tzgg/"},
    {"province": "海南", "name": "海南省工业和信息化厅", "url": "http://iitb.hainan.gov.cn/"},
    {"province": "重庆", "name": "重庆市经济和信息化委员会", "url": "http://jjxxw.cq.gov.cn/"},
    {"province": "四川", "name": "四川省经济和信息化厅", "url": "http://jxt.sc.gov.cn/"},
    {"province": "贵州", "name": "贵州省工业和信息化厅", "url": "http://gxt.guizhou.gov.cn/"},
    {"province": "云南", "name": "云南省工业和信息化厅", "url": "http://gxt.yn.gov.cn/"},
    {"province": "西藏", "name": "西藏自治区经济和信息化厅", "url": "http://jxt.xizang.gov.cn/"},
    {"province": "陕西", "name": "陕西省工业和信息化厅", "url": "http://gxt.shaanxi.gov.cn/"},
    {"province": "甘肃", "name": "甘肃省工业和信息化厅", "url": "http://gxt.gansu.gov.cn/"},
    {"province": "青海", "name": "青海省工业和信息化厅", "url": "https://gxt.qinghai.gov.cn/"},
    {"province": "宁夏", "name": "宁夏回族自治区工业和信息化厅", "url": "http://gxt.nx.gov.cn/"},
    {"province": "新疆", "name": "新疆维吾尔自治区工业和信息化厅", "url": "http://gxt.xinjiang.gov.cn/"},
    {"province": "新疆兵团", "name": "新疆生产建设兵团工业和信息化局", "url": "http://btgxj.xjbt.gov.cn/"},
]

# ── 常量 ────────────────────────────────────────────────────────────────────────

# 导航类链接常见文字，匹配到的 <a> 标签直接跳过
NAV_WORDS = frozenset([
    "首页", "关于", "联系", "网站地图", "English", "搜索",
    "更多", "下一页", "上一页", "登录", "注册",
])

# ── 新闻链接识别模式 ──────────────────────────────────────────────────────────
#
# 32 个省级网站使用多种 CMS，URL 格式各异（基于全量实测）：
#   A) TRS CMS:     /YYYYMM/tYYYYMMDD_ID.html      （北京、吉林、福建、湖北、湖南、重庆、贵州、宁夏等）
#   B) E-Gov CMS:   /art/YYYY/M/D/art_COL_ART.html  （江苏、山东，月/日不补零）
#   C) 自建 CMS:    /SECTION/content/post_ID.html    （广东，URL 无日期）
#   D) 日期目录:    /zxxx/YYYYMMDD/uuid.html         （上海，8位日期目录）
#   E) 带连字符:    /c/YYYY-MM-DD/ID.shtml           （新疆兵团）
#   F) 长时间戳:    /YYYYMMDDHHMMSS.../index.shtml   （辽宁，19位时间戳ID）
#   G) 序列 ID:     /section/tNNNNNNNN.shtml          （广西，无日期）
#   H) SPA 查询:    /xzweb/detail?id=N               （西藏）
#   I) Channel:     /cCHANNEL/YYYYMM/ID.shtml        （黑龙江、甘肃，路径含YYYYMM → 匹配 A）
#   J) UUID目录:    /section/YYYYMM/uuid.shtml        （新疆，UUID文件名 → 匹配 A）

# 模式 A/I/J：URL 路径中含 /YYYYMM/ 日期目录（零填充月份）
_RE_NEWS_DATE_SLASH = re.compile(
    r"/(20\d{2})(0[1-9]|1[0-2])/"           # /YYYYMM/
    r"|/(20\d{2})-(0[1-9]|1[0-2])/"          # /YYYY-MM/
    r"|/(20\d{2})/(0?[1-9]|1[0-2])/"         # /YYYY/MM/ （允许非零填充）
)

# 模式 D：/YYYYMMDD/ 完整8位日期目录（上海 /zxxx/20260417/uuid.html）
_RE_NEWS_DATE_FULL = re.compile(
    r"/(20\d{2})(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])/"
)

# 模式 E：/YYYY-MM-DD/ 带连字符完整日期目录（新疆兵团 /c/2026-04-17/ID.shtml）
_RE_NEWS_DATE_HYPHEN = re.compile(
    r"/(20\d{2})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])/"
)

# 模式 F：19位长时间戳（辽宁），路径段以 YYYYMMDD 开头后跟更多数字
_RE_NEWS_LONG_TS = re.compile(
    r"/(20\d{2})(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])\d{5,}/"
)

# 模式 B：E-Gov CMS 风格 /art/YYYY/M/D/（江苏、山东）
# 也包含 /art/YYYY/art_UUID.html 变体（miit.gov.cn 等）
_RE_ART_URL = re.compile(r"/art/20\d{2}/(?:\d{1,2}/\d{1,2}/|art_[0-9a-f]+\.html)")

# 模式 C/H：自建 CMS 内容页（URL 无日期，依赖 ID 标识或查询参数）
_RE_CONTENT_URL = re.compile(
    r"/content/post_\d+\.html"              # C: 广东 /content/post_ID.html
    r"|/detail\?id=\d+"                     # H: 西藏 /xzweb/detail?id=N
    r"|\.shtml\?id=[0-9a-f-]+"             # 黑龙江 /path.shtml?id=uuid
)

# 模式 G：广西序列 ID /section/tNNNNNNNN.shtml（t 后跟纯数字，无下划线）
_RE_GUANGXI_TRS = re.compile(r"/t\d{7,}\.shtml")

# 年份目录 + 文章文件名（避免将 /YYYY/ 栏目页误判为新闻）
_RE_YEAR_DIR_ARTICLE = re.compile(
    r"/(20\d{2})/(?:[^/?#]+/)*(?:[^/?#]*\d[^/?#]*|[0-9a-f-]{8,})\.(?:s?html?)$"
)

# ── 日期提取正则 ──────────────────────────────────────────────────────────────

# TRS CMS 文件名精确日期 tYYYYMMDD_ID.html/.htm/.shtml
_RE_TRS_FILENAME = re.compile(
    r"/t(20\d{2})(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])_\d+\.(?:html?|shtml)"
)

# 从 URL 路径中提取完整日期 (YYYYMMDD)
_RE_URL_YYYYMMDD = re.compile(r"/(20\d{2})(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])")
# 从 URL 路径中提取年月 (YYYYMM)
_RE_URL_YYYYMM = re.compile(r"/(20\d{2})(0[1-9]|1[0-2])/")
_RE_URL_YYYY_MM = re.compile(r"/(20\d{2})[-/](0[1-9]|1[0-2])/")

# E-Gov /art/YYYY/M/D/ 日期提取（月/日不补零）
_RE_ART_DATE = re.compile(r"/art/(20\d{2})/(\d{1,2})/(\d{1,2})/")

# 斜杠分隔完整日期：/YYYY/MM/DD/（青海 /system/2026/04/16/ID.shtml）
_RE_URL_SLASH_DATE = re.compile(
    r"/(20\d{2})/(0?[1-9]|1[0-2])/(0?[1-9]|[12]\d|3[01])/"
)

# 模式 E 日期提取：/YYYY-MM-DD/（新疆兵团）
_RE_URL_HYPHEN_DATE = re.compile(
    r"/(20\d{2})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])/"
)

# 模式 F 日期提取：长时间戳中的前8位 YYYYMMDD（辽宁）
_RE_URL_LONG_TS_DATE = re.compile(
    r"/(20\d{2})(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])\d{5,}/"
)

# 国药格式：/(YYYY-MM)/(DD)/ 如 /2026-05/07/c_20548.htm
_RE_URL_HYPHEN_YYYYMM_DD = re.compile(
    r"/(20\d{2})-(0[1-9]|1[0-2])/(0[1-9]|[12]\d|3[01])/"
)

# 默认请求超时（秒）
DEFAULT_TIMEOUT = 20

# ── 标题清理正则 ─────────────────────────────────────────────────────────────────

# 前缀日期（三种格式）：
#   "DD YYYY-MM "  例：16 2026-04 【标题…】
#   "YYYY-MM-DD "  例：2026-04-17 关于…
#   "YYYY.MM DD "  例：2026.04 14 袁野赴…（哈电集团格式）
_RE_TITLE_LEADING_DATE = re.compile(
    r"^(?:"
    r"\d{1,2}\s+20\d{2}-(?:0[1-9]|1[0-2])"              # DD YYYY-MM
    r"|20\d{2}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01])"  # YYYY-MM-DD
    r"|20\d{2}\.(?:0[1-9]|1[0-2])\s+\d{1,2}"            # YYYY.MM DD（哈电格式）
    r")\s+"
)

# 后缀日期（三种格式，方括号可选、内外空格可选）：
#   " 2026-04-17"  或  " [ 2026-04-17 ]"
#   " 04-16"       或  " [ 04-16 ]"
#   " 04/16 2026"  或  " 04/16"（招商局格式，斜杠分隔）
_RE_TITLE_TRAILING_DATE = re.compile(
    r"(?:"
    r"\s+\[?\s*20\d{2}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01])\s*\]?"  # YYYY-MM-DD
    r"|\s+\[?\s*(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\d|3[01])\s*\]?"          # MM-DD
    r"|\s+(?:0[1-9]|1[0-2])/(?:0[1-9]|[12]\d|3[01])(?:\s+20\d{2})?"       # MM/DD 或 MM/DD YYYY
    r")\s*$"
)

# 正文摘要起始标志：中文日期（N月N日，）出现在非标题开头位置
# 用于识别新闻卡片中标题后紧跟的正文摘要（如招商局集团网站格式）
# 示例："灵卫·智能巡检机器人亮相香港国际创科展 4月13日，招商局狮子山..."
#        ────────────── 标题 ─────────────────── ↑摘要起始
_RE_EXCERPT_START = re.compile(r"(?<=\S)\s+\d{1,2}月\d{1,2}日[，,、。 ]")

# 触发摘要截断所需的最小标题前置长度（确保截断点之前有足够的标题内容）
_MIN_TITLE_CHARS_BEFORE_EXCERPT = 5

# 标题最大保留长度（中文标题通常 ≤ 80 字，超出视为正文混入）
_TITLE_MAX_LEN = 120


# ── 辅助函数 ────────────────────────────────────────────────────────────────────

def _is_news_like_url(url: str) -> bool:
    """
    判断 URL 是否像新闻文章链接。

    覆盖 32 个省级网站实测发现的全部 URL 模式（A-J）。
    """
    if _RE_NEWS_DATE_SLASH.search(url):       # A/I/J: /YYYYMM/ 日期目录
        return True
    if _RE_NEWS_DATE_FULL.search(url):        # D: /YYYYMMDD/ 8位日期（上海）
        return True
    if _RE_NEWS_DATE_HYPHEN.search(url):      # E: /YYYY-MM-DD/（新疆兵团）
        return True
    if _RE_NEWS_LONG_TS.search(url):          # F: 长时间戳（辽宁）
        return True
    if _RE_ART_URL.search(url):               # B: /art/YYYY/M/D/（江苏/山东）
        return True
    if _RE_CONTENT_URL.search(url):           # C/H: content/post, detail?id=
        return True
    if _RE_GUANGXI_TRS.search(url):           # G: /tNNNNNNNN.shtml（广西）
        return True
    if _RE_YEAR_DIR_ARTICLE.search(url):      # /YYYY/.../article123.html
        return True
    return False


def _is_nav_text(text: str) -> bool:
    """判断链接文字是否为导航性文字（应跳过）。"""
    stripped = text.strip()
    if not stripped:
        return True
    if stripped in NAV_WORDS:
        return True
    # 纯数字（分页）、纯符号
    if re.fullmatch(r"[\d\s.…>»<«\[\]【】]+", stripped):
        return True
    return False


def _strip_date_affixes(text: str) -> str:
    """
    去除标题字符串首尾的日期标注。

    处理以下格式：
      前缀：「DD YYYY-MM 」（如 "16 2026-04 "）
            「YYYY-MM-DD 」（如 "2026-04-17 "）
            「YYYY.MM DD 」（如 "2026.04 14 "，哈电集团格式）
      后缀：「 YYYY-MM-DD」或「 [ YYYY-MM-DD ]」
            「 MM-DD」     或「 [ MM-DD ]」
            「 MM/DD YYYY」或「 MM/DD」（招商局格式，斜杠分隔）
    """
    text = _RE_TITLE_LEADING_DATE.sub("", text)
    text = _RE_TITLE_TRAILING_DATE.sub("", text)
    return text.strip()


def _clean_title(a_tag, raw_text: Optional[str] = None) -> str:
    """
    从 <a> 标签中提取干净的标题文本。

    策略（按优先级）：
      1. <a title="..."> 属性：CMS 常在此存放完整、无截断的标题文本，
         可避免以下两类问题：
           - 显示文字被 CSS 截断（含 "..."）；
           - <a> 内嵌正文摘要导致 get_text() 带入正文内容。
      2. get_text() 可见文字（兜底），同时：
           - 去除首尾日期标注（date span 混入 get_text 的情况）；
           - 在「标题 + N月N日，摘要…」模式下截断至摘要起始处
             （用于招商局等将标题、摘要和日期封装在同一 <a> 内的 CMS）；
           - 截断至 _TITLE_MAX_LEN，防止正文内容被误识为标题。

    可选参数 raw_text 允许调用方传入已计算的 get_text() 结果，避免重复解析。
    """
    # 1) title 属性优先
    attr_title = (a_tag.get("title") or "").strip()
    if attr_title and len(attr_title) >= 6:
        return _strip_date_affixes(attr_title)

    # 2) 可见文字兜底（复用调用方已计算的值，避免重复 get_text）
    text = raw_text if raw_text is not None else a_tag.get_text(" ", strip=True)
    text = _strip_date_affixes(text)

    # 2.5) 检测「标题 + 正文摘要」混合文本：若存在「空格 + N月N日，」模式，
    #      则截断至摘要起始，仅保留前面的标题部分。
    m = _RE_EXCERPT_START.search(text)
    if m and m.start() > _MIN_TITLE_CHARS_BEFORE_EXCERPT:
        text = text[:m.start()]

    # 3) 截断过长文本（防止正文混入）
    if len(text) > _TITLE_MAX_LEN:
        text = text[:_TITLE_MAX_LEN]

    return text.strip()


def _extract_date_from_url(url: str) -> Optional[str]:
    """
    从 URL 路径中提取日期。

    支持七种格式（按优先级排列）：
      1. TRS 文件名 tYYYYMMDD_ID.html/.shtml → 精确日期
      2. E-Gov /art/YYYY/M/D/               → 精确日期（月/日不补零）
      3. /YYYY/MM/DD/ 斜杠分隔目录           → 精确日期（青海）
      4. /YYYY-MM-DD/ 连字符目录             → 精确日期（新疆兵团）
      5. /YYYYMMDD.../ 长时间戳              → 精确日期（辽宁，取前8位）
      6. /YYYYMMDD/ 或 URL 路径中的连续8位   → 精确日期
      7. /YYYYMM/ 路径目录                   → 粗略日期（日默认 01）
    """
    # 1) TRS CMS 文件名精确日期
    m = _RE_TRS_FILENAME.search(url)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"

    # 2) E-Gov /art/YYYY/M/D/ 风格（江苏、山东，月/日不补零）
    m = _RE_ART_DATE.search(url)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"

    # 3) /YYYY/MM/DD/ 斜杠分隔完整日期（青海 /system/2026/04/16/）
    m = _RE_URL_SLASH_DATE.search(url)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"

    # 4) /YYYY-MM-DD/ 连字符目录（新疆兵团 /c/2026-04-17/）
    m = _RE_URL_HYPHEN_DATE.search(url)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"

    # 5) 长时间戳：前8位为 YYYYMMDD（辽宁 /2026031009183942113/）
    m = _RE_URL_LONG_TS_DATE.search(url)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"

    # 6) 路径中的 YYYYMMDD（8位连续数字，如上海 /20260417/）
    m = _RE_URL_YYYYMMDD.search(url)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"

    # 7) 国药格式 /(YYYY-MM)/(DD)/
    m = _RE_URL_HYPHEN_YYYYMM_DD.search(url)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"

    # 8) 路径中的 YYYYMM 目录（日默认 01）
    m = _RE_URL_YYYYMM.search(url)
    if not m:
        m = _RE_URL_YYYY_MM.search(url)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-01"

    # 9) /YYYY/.../article123.html 路径中的年份（月日默认 01-01，仅作为最后回退）
    m = _RE_YEAR_DIR_ARTICLE.search(url)
    if m:
        return f"{m.group(1)}-01-01"

    return None


def _extract_date_from_context(a_tag, now: Optional[datetime] = None) -> Optional[str]:
    """
    从 <a> 标签的相邻兄弟节点或父容器中提取日期文本。

    实际页面结构因省份/央企而异：
      - 北京/广东工信：<li><a>标题</a><span class="date">2026-04-15</span></li>
      - 江苏：<li>04-13 <a>标题</a></li>
      - 新疆：<span class="year">2026-04</span><span class="date">14</span>
      - 西藏：<span>2026-04-16</span><a>标题</a>
      - 哈电：<div><a>标题</a><div class="right">2026.05.14 ...</div></div>
      - 电气装备：<div class="title"><a>标题</a></div><div class="time">05/13</div>
      - 中建材：<h5><a>标题</a></h5><span>[05-06]</span>
      - 检验认证：<div class="news_list_date">2026.05</div><div class="news_list_l">08</div>
      - 表格：<tr><td><a>标题</a></td><td>2026-04-15</td></tr>
    """
    _DATE_TAG_NAMES = ["span", "div", "em", "i", "time", "p", "h6", "td"]
    ref_now = now or datetime.now()

    def _extract_date_by_text(text: str) -> Optional[str]:
        text = text.strip("[]【】")
        if not text:
            return None
        if re.search(r"20\d{2}", text):
            return extract_date(text)
        m = re.search(r'(?<![/\d])(\d{1,2})-(\d{1,2})(?![-\d])', text)
        if m:
            mm, dd = int(m.group(1)), int(m.group(2))
            if 1 <= mm <= 12 and 1 <= dd <= 31:
                return f"{ref_now.year}-{mm:02d}-{dd:02d}"
        m = re.search(r'(?<![/\d])(\d{1,2})/(\d{1,2})(?![/\d])', text)
        if m:
            mm, dd = int(m.group(1)), int(m.group(2))
            if 1 <= mm <= 12 and 1 <= dd <= 31:
                return f"{ref_now.year}-{mm:02d}-{dd:02d}"
        return None

    # 0) <a> 标签自身文本中的日期（哈电格式：2026.05 09）
    a_text = a_tag.get_text(" ", strip=True)
    m = re.search(r'(20\d{2})\.(\d{1,2})\s+(\d{1,2})', a_text)
    if m:
        mm, dd = int(m.group(2)), int(m.group(3))
        if 1 <= mm <= 12 and 1 <= dd <= 31:
            return f"{m.group(1)}-{mm:02d}-{dd:02d}"

    # 1) 直接兄弟节点（span/div/em/i/time/p/h6）
    for tag_name in _DATE_TAG_NAMES:
        sibling = a_tag.find_next_sibling(tag_name)
        if sibling:
            text = sibling.get_text(" ", strip=True).strip("[]【】")
            d = _extract_date_by_text(text)
            if d:
                return d
        prev = a_tag.find_previous_sibling(tag_name)
        if prev:
            text = prev.get_text(" ", strip=True).strip("[]【】")
            d = _extract_date_by_text(text)
            if d:
                return d

    parent = a_tag.parent
    if not parent:
        return None

    # 2) 父元素内所有候选元素（限制文本长度，避免匹配到正文摘要）
    for tag in parent.find_all(_DATE_TAG_NAMES):
        text = tag.get_text(" ", strip=True).strip("[]【】")
        if len(text) > 40:
            continue
        d = _extract_date_by_text(text)
        if d:
            return d

    # 3) 父元素的下一个兄弟（电气装备 div.time 等）
    for tag_name in _DATE_TAG_NAMES:
        sib = parent.find_next_sibling(tag_name)
        if sib:
            text = sib.get_text(" ", strip=True).strip("[]【】")
            d = _extract_date_by_text(text)
            if d:
                return d

    # 4) 新疆分段日期：<span class="year">2026-04</span><span class="date">14</span>
    year_span = parent.find("span", class_="year")
    date_span = parent.find("span", class_="date")
    if year_span and date_span:
        combined = year_span.get_text(strip=True) + "-" + date_span.get_text(strip=True)
        d = extract_date(combined)
        if d:
            return d

    # 5) 父元素全文（兜底）- 仅当父元素不是超大列表容器时
    if len(parent.find_all("a")) <= 5:
        d = _extract_date_by_text(parent.get_text(" ", strip=True))
        if d:
            return d

    # 6) 祖父元素搜索
    grandparent = parent.parent
    if grandparent:
        # 先搜索祖父元素内的候选日期元素（不受 <a> 数量限制，但受文本长度限制）
        for tag in grandparent.find_all(_DATE_TAG_NAMES):
            text = tag.get_text(" ", strip=True).strip("[]【】")
            if len(text) > 40:
                continue
            d = _extract_date_by_text(text)
            if d:
                return d
        # 祖父元素全文兜底 - 仅当不是超大列表容器时
        if len(grandparent.find_all("a")) <= 10:
            d = _extract_date_by_text(grandparent.get_text(" ", strip=True))
            if d:
                return d

    # 7) 表格行布局
    if parent.name == "td":
        row = parent.parent
        if row and row.name == "tr":
            for cell in row.find_all("td"):
                if cell is parent:
                    continue
                text = cell.get_text(" ", strip=True).strip("[]【】")
                d = _extract_date_by_text(text)
                if d:
                    return d

    # 8) 特殊格式处理：[MM-DD]、MM-DD、MM/DD、YYYY.MM.DD
    context_text = ""
    for ancestor in [parent, grandparent]:
        if ancestor and len(ancestor.find_all("a")) <= 10:
            context_text = ancestor.get_text(" ", strip=True)
            if context_text:
                break

    if context_text:
        # [MM-DD]（中建材）
        m = re.search(r'\[(\d{1,2})-(\d{1,2})\]', context_text)
        if m:
            year = now.year if now else datetime.now().year
            return f"{year}-{int(m.group(1)):02d}-{int(m.group(2)):02d}"

        # MM-DD 纯文本（中国电建 bt_time 等，结合当前年份）
        m = re.search(r'(?<![/\d])(\d{1,2})-(\d{1,2})(?![-\d])', context_text)
        if m:
            mm, dd = int(m.group(1)), int(m.group(2))
            if 1 <= mm <= 12 and 1 <= dd <= 31:
                year = now.year if now else datetime.now().year
                return f"{year}-{mm:02d}-{dd:02d}"

        # MM/DD（电气装备等，结合当前年份）
        # 避免匹配 URL 路径中的数字，优先在紧邻的短文本中匹配
        m = re.search(r'(?<![/\d])(\d{1,2})/(\d{1,2})(?![/\d])', context_text)
        if m:
            mm, dd = int(m.group(1)), int(m.group(2))
            if 1 <= mm <= 12 and 1 <= dd <= 31:
                year = now.year if now else datetime.now().year
                return f"{year}-{mm:02d}-{dd:02d}"

        # YYYY.MM.DD（哈电格式）
        m = re.search(r'(20\d{2})\.(\d{1,2})\.(\d{1,2})', context_text)
        if m:
            return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"

        # 检验认证格式：DDYYYY.MM 或 DD YYYY.MM（如 "082026.05"）
        m = re.search(r'(\d{2})(20\d{2})\.(\d{1,2})', context_text)
        if m:
            dd, year, mm = int(m.group(1)), m.group(2), int(m.group(3))
            if 1 <= mm <= 12 and 1 <= dd <= 31:
                return f"{year}-{mm:02d}-{dd:02d}"

    return None


def _scrape_one_site(
    source: dict,
    fetched_at: str,
    keywords: List[str],
    now: datetime,
    window_days: int,
    hard_cap_days: int,
    timeout: int,
) -> List[Item]:
    """
    抓取单个地方工信部门网站并返回符合条件的 Item 列表。

    Args:
        source:        包含 province / name / url 的字典。
        fetched_at:    格式化后的查询时间字符串。
        keywords:      关键词列表。
        now:           当前时间（带时区）。
        window_days:   时间窗口天数。
        hard_cap_days: 硬性截止天数。
        timeout:       HTTP 请求超时秒数。

    Returns:
        过滤后的 Item 列表。
    """
    province = source["province"]
    dept_name = source["name"]
    base_url = source["url"]
    source_tag = f"地方工信-{province}"

    html = http_get(base_url, timeout=timeout)
    soup = BeautifulSoup(html, "lxml")

    items: List[Item] = []

    for a_tag in soup.find_all("a", href=True):
        href = (a_tag.get("href") or "").strip()

        # 跳过空链接和锚点（在提取标题前先过滤，节省 clean_title 调用）
        if not href or href.startswith("#") or href.startswith("javascript"):
            continue

        # 先用 get_text 做快速导航/长度预过滤，再用 _clean_title 清洗
        raw_text = a_tag.get_text(" ", strip=True)

        # 跳过导航性文字
        if _is_nav_text(raw_text):
            continue

        # 跳过标题过短的链接（至少 6 个字符）
        if len(raw_text) < 6:
            continue

        url = normalize_url(base_url, href)

        # 仅保留看起来像新闻文章的链接
        if not _is_news_like_url(url):
            continue

        # 清洗标题：优先使用 title 属性，去除首尾日期，截断过长文本
        title = _clean_title(a_tag, raw_text)
        if len(title) < 6:
            continue

        # 提取发布日期：优先从 URL，其次从上下文文本，再其次从原始链接文本
        pub_date = _extract_date_from_url(url)
        if not pub_date:
            pub_date = _extract_date_from_context(a_tag, now)
        if not pub_date:
            pub_date = extract_date(raw_text)
        if not pub_date:
            pub_date = extract_date(title)

        # 关键词过滤
        if not keyword_hit(title, keywords):
            continue

        # 时间窗口过滤
        if not within_window(pub_date, now, window_days, hard_cap_days):
            continue

        items.append(Item(
            title=title,
            publisher=dept_name,
            url=url,
            pub_date=pub_date,
            source=source_tag,
            fetched_at=fetched_at,
        ))

    return items


# ── 入口函数 ────────────────────────────────────────────────────────────────────

def parse_miit_local(config: dict, now: datetime) -> List[Item]:
    """
    遍历全国地方工信主管部门网站，抓取并返回符合条件的新闻列表。

    Args:
        config: 来自 config.yaml 的全量配置字典，需包含：
                - sources.miit_local.name / enabled
                - keywords
                - window_days
                - hard_cap_days
        now:    当前时间（带时区）。

    Returns:
        过滤并去重后的 Item 列表。
    """
    src = config["sources"]["miit_local"]
    if not src.get("enabled", True):
        logger.info("miit_local 源已禁用，跳过")
        return []

    fetched_at = format_fetched_at(now)
    keywords = [k for k in config["keywords"] if k not in MIIT_ONLY_KEYWORDS]
    window_days = int(config["window_days"])
    hard_cap_days = int(config["hard_cap_days"])
    timeout = int(config.get("miit_local_timeout", DEFAULT_TIMEOUT))

    all_items: List[Item] = []

    for source in MIIT_LOCAL_SOURCES:
        province = source["province"]
        try:
            site_items = _scrape_one_site(
                source=source,
                fetched_at=fetched_at,
                keywords=keywords,
                now=now,
                window_days=window_days,
                hard_cap_days=hard_cap_days,
                timeout=timeout,
            )
            if site_items:
                logger.info("地方工信-%s: 获取 %d 条", province, len(site_items))
            all_items.extend(site_items)
        except Exception:
            logger.warning("地方工信-%s: 抓取失败", province, exc_info=True)
            continue

    # ── 去重（URL 规范化） ──────────────────────────────────────────────────────
    uniq: Dict[str, Item] = {}
    for it in all_items:
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

    result = list(uniq.values())
    logger.info("地方工信汇总: %d 条（去重后）", len(result))
    return result
