"""
中央企业（央企）官网解析器。

入口函数
--------
parse_soe(config, now) -> List[Item]
    遍历国务院国资委公布的央企名录（约 95 家），逐站抓取首页新闻链接，
    按关键词和时间窗口过滤后返回新闻列表。

数据来源
--------
央企名录取自 http://www.sasac.gov.cn/n2588045/n27271785/n27271792/c14159097/content.html

实现策略
--------
95 家央企官网使用至少 8 种 CMS 平台，URL 格式高度多样：
  - TRS CMS:          /YYYYMM/tYYYYMMDD_ID.html
  - E-Gov /art/:      /art/YYYY/M/D/art_COL_ART.html
  - Channel-Content:  /nNNN/cNNN/content.html
  - Date-directory:   /path/YYYYMM/tYYYYMMDD_ID.html
  - Timestamp-file:   /path/MMDD/YYYYMMDDHHMMSS_pc.html
  - Numeric-ID:       /path/ID.html (6+ digit)
  - Gone-suffix CMS:  /path/YYYY/M/IDNNN.html
  - 新闻中心 /xwzx/:  通用新闻路径前缀

采用复合正则模式匹配，复用 miit_local 中已验证的核心函数。

配置（config.yaml）
-------------------
sources:
  soe:
    name: 中央企业
    enabled: true
"""

import logging
import re
from datetime import datetime
from typing import Dict, List, Optional

from bs4 import BeautifulSoup
from dateutil import parser as dtparser

from models import Item, MIIT_ONLY_KEYWORDS, SOE_EXCLUDED_KEYWORDS
from utils import (
    canonicalize_url_for_dedup,
    extract_date,
    format_fetched_at,
    http_get,
    keyword_hit,
    normalize_url,
    within_window,
)

# 复用 miit_local 中已验证的 URL 模式识别和日期提取函数
from parsers.miit_local import (
    _clean_title,
    _extract_date_from_context,
    _extract_date_from_url,
    _is_nav_text,
    _is_news_like_url,
    DEFAULT_TIMEOUT,
)

logger = logging.getLogger(__name__)

# SOE_SOURCES 定义在文件末尾（央企名录约 95 家）

# ── SOE 额外 URL 模式（补充 miit_local 未覆盖的央企特有格式） ────────────────

# Channel-Content TRS: /nNNN/cNNN/content.html（国资委系 CMS）
_RE_CHANNEL_CONTENT = re.compile(r"/n\d+/c\d+/content\.html")

# 纯数字 ID 文件：/path/NNNNN.html（5位及以上）
# 覆盖中石化 /group/.../71062.shtml、中建材 /CNBM/.../69538.html 等
_RE_NUMERIC_ID = re.compile(r"/\d{5,}\.s?html?$")

# Gone-suffix CMS: /path/YYYY/M/IDNNNNN.html（大唐、中化等）
_RE_GONE_SUFFIX = re.compile(r"/(20\d{2})/(\d{1,2})/[A-Za-z]\d{10,}\.html")

# 新闻中心路径前缀（大量央企使用 /xwzx/ 或 /news/ 等）
_RE_NEWS_CENTER_PATH = re.compile(
    r"/(?:xwzx|news|xwdt|jtxw|zhxw|gsxw|jtdt|qyxw|xwzx_?\d*)"
    r"/[^/]+\.(?:html?|shtml)$"
)

# UUID 文件名（矿冶科技/中国盐业等）：/xwzx/kydt/{32hex-with-dashes}.htm
_RE_UUID_FILENAME = re.compile(r"/[0-9a-f]{8,}[0-9a-f-]*\.html?$")

# NDA 风格时间戳文件名（中化、部分央企二级站点）
_RE_TIMESTAMP_FILENAME = re.compile(
    r"/(20\d{2})(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])\d{5,}_?\w*\.(?:html?|shtml)"
)

# 南航等：/YYYYMMDDHHMMSS.../0/index.html
_RE_LONG_DIR_ID = re.compile(
    r"/(20\d{2})(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])\d{8,}/\d+/index\.html"
)

# /col/colNNN/art/YYYY/art_UUID.html（中远海运 E-Gov 变体）
_RE_COL_ART = re.compile(r"/col/col\d+/art/20\d{2}/art_[0-9a-f]+\.html")

# /info/NNNN/NNNNN.htm（东方电气 info 路径）
_RE_INFO_PATH = re.compile(r"/info/\d+/\d+\.htm")

# /article/NNNNN（中国旅游集团/中国林业等，无扩展名）
_RE_ARTICLE_PATH = re.compile(r"/article/\d{3,}$")

# /contents/NNN/NNNNN.html（中国机械总院等）
_RE_CONTENTS_PATH = re.compile(r"/contents/\d+/\d+\.html$")

# /s/NNNN-NNNN-NNNNNNN.html（中国有研科技等）
_RE_DASH_ID_PATH = re.compile(r"/s/\d+-\d+-\d+\.html$")

# 层级数字路径 + index.html（中国电科 /zgdk/NNN/NNN/NNNNN/index.html）
_RE_HIERARCHICAL_INDEX = re.compile(r"/\d{5,}/index\.html$")

# /webfront/webpage/web/contentPage/id/{UUID}（中国华电）
# 注：仅匹配 contentPage（文章页），不匹配 contentList（列表/栏目页）
_RE_WEBFRONT_CONTENT = re.compile(
    r"/webfront/webpage/web/contentPage/id/[0-9a-f-]+"
)

# 雪花 ID 双段文件名（招商局 /content/{snowflake}_{snowflake}.html）
_RE_SNOWFLAKE_CONTENT = re.compile(r"/content/\d{15,}_\d{15,}\.html$")

# 根路径短数字 ID（中国电子 /NNNN.html，限制最少 4 位）
_RE_ROOT_SHORT_ID = re.compile(r"^https?://[^/]+/\d{4,}\.html$")

# 中国华能 hash 路径：/list_{section}/-/article/{hash}/list/{page}/
_RE_HUANENG_ARTICLE = re.compile(r"/article/[A-Za-z0-9]{8,}/list/\d+/")

# 中国中车日期-article 路径：/crrcgc/YYYY-MM/DD/article_NNNNN.html
_RE_CRRC_ARTICLE = re.compile(
    r"/\d{4}-\d{2}/\d{2}/article_\d+\.html$"
)


def _is_soe_news_url(url: str) -> bool:
    """
    判断 URL 是否像央企新闻文章链接。
    先用 miit_local 的通用模式，再补充央企特有模式。
    """
    if _is_news_like_url(url):
        return True
    if _RE_CHANNEL_CONTENT.search(url):
        return True
    if _RE_NUMERIC_ID.search(url):
        return True
    if _RE_GONE_SUFFIX.search(url):
        return True
    if _RE_NEWS_CENTER_PATH.search(url):
        return True
    if _RE_UUID_FILENAME.search(url):
        return True
    if _RE_TIMESTAMP_FILENAME.search(url):
        return True
    if _RE_LONG_DIR_ID.search(url):
        return True
    if _RE_COL_ART.search(url):
        return True
    if _RE_INFO_PATH.search(url):
        return True
    if _RE_ARTICLE_PATH.search(url):
        return True
    if _RE_CONTENTS_PATH.search(url):
        return True
    if _RE_DASH_ID_PATH.search(url):
        return True
    if _RE_HIERARCHICAL_INDEX.search(url):
        return True
    if _RE_WEBFRONT_CONTENT.search(url):
        return True
    if _RE_SNOWFLAKE_CONTENT.search(url):
        return True
    if _RE_ROOT_SHORT_ID.search(url):
        return True
    if _RE_HUANENG_ARTICLE.search(url):
        return True
    if _RE_CRRC_ARTICLE.search(url):
        return True
    return False


def _extract_soe_date(a_tag, url: str) -> Optional[str]:
    """
    从央企新闻链接中提取发布日期。
    优先 URL，其次上下文文本。
    """
    # 1) miit_local 的 URL 日期提取
    d = _extract_date_from_url(url)
    if d:
        return d

    # 2) Gone-suffix CMS: /YYYY/M/ID.html
    m = _RE_GONE_SUFFIX.search(url)
    if m:
        yyyy = m.group(1)
        mm = int(m.group(2))
        return f"{yyyy}-{int(mm):02d}-01"

    # 3) 时间戳文件名
    m = _RE_TIMESTAMP_FILENAME.search(url)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"

    # 4) 南航等长目录 ID
    m = _RE_LONG_DIR_ID.search(url)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"

    # 5) miit_local 的上下文日期提取
    d = _extract_date_from_context(a_tag)
    if d:
        return d

    return None


# ── 单站抓取 ──────────────────────────────────────────────────────────────────

def _scrape_soe_site(
    source: dict,
    fetched_at: str,
    keywords: List[str],
    now: datetime,
    window_days: int,
    hard_cap_days: int,
    timeout: int,
) -> List[Item]:
    """抓取单个央企官网并返回符合条件的 Item 列表。"""
    name = source["name"]
    base_url = source["url"]
    source_tag = f"央企-{name}"

    html = http_get(base_url, timeout=timeout)
    soup = BeautifulSoup(html, "lxml")

    items: List[Item] = []

    for a_tag in soup.find_all("a", href=True):
        href = (a_tag.get("href") or "").strip()
        if not href or href.startswith("#") or href.startswith("javascript"):
            continue

        raw_text = a_tag.get_text(" ", strip=True)
        if _is_nav_text(raw_text):
            continue
        if len(raw_text) < 6:
            continue

        url = normalize_url(base_url, href)

        # 仅保留新闻文章链接
        if not _is_soe_news_url(url):
            continue

        # 跳过各站点明确配置的非新闻栏目路径（如业务领域、服务目录等）
        exclude_paths = source.get("exclude_paths", [])
        if any(ep in url for ep in exclude_paths):
            continue

        title = _clean_title(a_tag, raw_text)
        if len(title) < 6:
            continue

        # 日期提取
        pub_date = _extract_soe_date(a_tag, url)
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
            publisher=name,
            url=url,
            pub_date=pub_date,
            source=source_tag,
            fetched_at=fetched_at,
        ))

    return items


# ── 主入口 ────────────────────────────────────────────────────────────────────

def parse_soe(config: dict, now: datetime) -> List[Item]:
    """
    遍历央企名录，逐站抓取首页新闻并返回符合条件的列表。
    """
    src = config["sources"]["soe"]
    if not src.get("enabled", True):
        logger.info("soe 源已禁用，跳过")
        return []

    fetched_at = format_fetched_at(now)
    keywords = [k for k in config["keywords"] if k not in MIIT_ONLY_KEYWORDS and k not in SOE_EXCLUDED_KEYWORDS]
    window_days = int(config["window_days"])
    hard_cap_days = int(config["hard_cap_days"])
    timeout = int(config.get("soe_timeout", DEFAULT_TIMEOUT))

    all_items: List[Item] = []

    for source in SOE_SOURCES:
        name = source["name"]
        try:
            site_items = _scrape_soe_site(
                source=source,
                fetched_at=fetched_at,
                keywords=keywords,
                now=now,
                window_days=window_days,
                hard_cap_days=hard_cap_days,
                timeout=timeout,
            )
            if site_items:
                logger.info("央企-%s: 获取 %d 条", name, len(site_items))
            all_items.extend(site_items)
        except Exception:
            logger.warning("央企-%s: 抓取失败", name, exc_info=True)
            continue

    # ── 去重 ──────────────────────────────────────────────────────────────────
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
    logger.info("央企汇总: %d 条（去重后）", len(result))
    return result


# ── 央企名录（95 家，来自国资委官网） ────────────────────────────────────────

SOE_SOURCES = [
    {"name": "中国核工业集团有限公司", "url": "http://www.cnnc.com.cn/"},
    {"name": "中国航天科技集团有限公司", "url": "http://www.spacechina.com/n25/index.html"},
    {"name": "中国航天科工集团有限公司", "url": "http://www.casic.com.cn/"},
    {"name": "中国航空工业集团有限公司", "url": "http://www.avic.com.cn/"},
    {"name": "中国船舶集团有限公司", "url": "http://www.csic.com.cn/"},
    {"name": "中国兵器工业集团有限公司", "url": "http://www.norincogroup.com.cn/"},
    {"name": "中国兵器装备集团有限公司", "url": "https://www.csgc.com.cn/"},
    {"name": "中国电子科技集团有限公司", "url": "http://www.cetc.com.cn/",
     # /zgdk/1592960/1592986/ 为"业务领域"栏目，链接均为行业介绍，不是新闻
     "exclude_paths": ["/1592960/1592986/"]},
    {"name": "中国航空发动机集团有限公司", "url": "http://www.aecc.cn/"},
    {"name": "中国融通资产管理集团有限公司", "url": "https://www.crtamg.com.cn/"},
    {"name": "中国石油天然气集团有限公司", "url": "http://www.cnpc.com.cn/"},
    {"name": "中国石油化工集团有限公司", "url": "http://www.sinopecgroup.com/"},
    {"name": "中国海洋石油集团有限公司", "url": "http://www.cnooc.com.cn/"},
    {"name": "国家石油天然气管网集团有限公司", "url": "http://www.pipechina.com.cn/"},
    {"name": "国家电网有限公司", "url": "http://www.sgcc.com.cn/"},
    {"name": "中国南方电网有限责任公司", "url": "http://www.csg.cn/"},
    {"name": "中国华能集团有限公司", "url": "http://www.chng.com.cn/"},
    {"name": "中国大唐集团有限公司", "url": "http://www.china-cdt.com/"},
    {"name": "中国华电集团有限公司", "url": "http://www.chd.com.cn/"},
    {"name": "国家电力投资集团有限公司", "url": "http://www.spic.com.cn/"},
    {"name": "中国长江三峡集团有限公司", "url": "http://www.ctg.com.cn/"},
    {"name": "国家能源投资集团有限责任公司", "url": "http://www.ceic.com/"},
    {"name": "中国电信集团有限公司", "url": "http://www.chinatelecom.com.cn/"},
    {"name": "中国联合网络通信集团有限公司", "url": "http://www.chinaunicom.com.cn/"},
    {"name": "中国移动通信集团有限公司", "url": "http://www.10086.cn/"},
    {"name": "中国电子信息产业集团有限公司", "url": "http://www.cec.com.cn/"},
    {"name": "中国第一汽车集团有限公司", "url": "http://www.faw.com.cn/"},
    {"name": "东风汽车集团有限公司", "url": "http://www.dfmc.com.cn/"},
    {"name": "中国一重集团有限公司", "url": "http://www.cfhi.com/"},
    {"name": "中国机械工业集团有限公司", "url": "http://www.sinomach.com.cn/"},
    {"name": "哈尔滨电气集团有限公司", "url": "http://www.harbin-electric.com/"},
    {"name": "中国东方电气集团有限公司", "url": "http://www.dongfang.com/"},
    {"name": "鞍钢集团有限公司", "url": "http://www.ansteel.cn/"},
    {"name": "中国宝武钢铁集团有限公司", "url": "http://www.baowugroup.com/"},
    {"name": "中国铝业集团有限公司", "url": "https://www.chinalco.com.cn/"},
    {"name": "中国远洋海运集团有限公司", "url": "http://www.coscoshipping.com/"},
    {"name": "中国航空集团有限公司", "url": "http://www.airchinagroup.com/"},
    {"name": "中国东方航空集团有限公司", "url": "http://www.ceairgroup.com/"},
    {"name": "中国南方航空集团有限公司", "url": "https://www.csairgroup.cn/cn/"},
    {"name": "中国中车集团有限公司", "url": "http://www.crrcgc.cc/"},
    {"name": "中国铁路通信信号集团有限公司", "url": "http://www.crsc.cn/"},
    {"name": "中国铁路工程集团有限公司", "url": "http://www.crecg.com/"},
    {"name": "中国铁道建筑集团有限公司", "url": "http://www.crcc.cn/"},
    {"name": "中国交通建设集团有限公司", "url": "http://www.ccccltd.cn/"},
    {"name": "中国电力建设集团有限公司", "url": "http://www.powerchina.cn/"},
    {"name": "中国能源建设集团有限公司", "url": "http://www.ceec.net.cn/"},
    {"name": "中国安能建设集团有限公司", "url": "https://www.china-an.cn/"},
    {"name": "中国中化控股有限责任公司", "url": "http://www.sinochem.com/"},
    {"name": "中粮集团有限公司", "url": "http://www.cofco.com/"},
    {"name": "中国五矿集团有限公司", "url": "http://www.minmetals.com.cn/"},
    {"name": "中国通用技术（集团）控股有限责任公司", "url": "http://www.gt.cn/"},
    {"name": "中国建筑集团有限公司", "url": "http://www.cscec.com/"},
    {"name": "中国储备粮管理集团有限公司", "url": "https://www.sinograin.com.cn/"},
    {"name": "国家开发投资集团有限公司", "url": "http://www.sdic.com.cn/"},
    {"name": "招商局集团有限公司", "url": "https://www.cmhk.com/main/"},
    {"name": "华润（集团）有限公司", "url": "http://www.crc.com.hk/"},
    {"name": "中国旅游集团有限公司", "url": "https://www.ctg.cn/"},
    {"name": "中国商用飞机有限责任公司", "url": "http://www.comac.cc/"},
    {"name": "中国节能环保集团有限公司", "url": "http://www.cecep.cn/"},
    {"name": "中国国际工程咨询有限公司", "url": "http://www.ciecc.com.cn/"},
    {"name": "中国诚通控股集团有限公司", "url": "http://www.cctgroup.com.cn/",
     # /736493/736498/ 为"战略性新兴产业"等业务板块介绍，不是新闻
     "exclude_paths": ["/736493/736498/"]},
    {"name": "中国中煤能源集团有限公司", "url": "http://www.chinacoal.com/"},
    {"name": "中国煤炭科工集团有限公司", "url": "http://www.ccteg.cn/"},
    {"name": "中国机械科学研究总院集团有限公司", "url": "http://www.cam.com.cn/"},
    {"name": "中国钢研科技集团有限公司", "url": "http://www.cisri.com.cn/"},
    {"name": "中国化学工程集团有限公司", "url": "http://www.cncec.cn/"},
    {"name": "中国盐业集团有限公司", "url": "http://www.chinasalt.com.cn/"},
    {"name": "中国建材集团有限公司", "url": "http://www.cnbm.com.cn/"},
    {"name": "中国有色矿业集团有限公司", "url": "http://www.cnmc.com.cn/"},
    {"name": "中国稀土集团有限公司", "url": "https://www.regcc.cn/"},
    {"name": "中国资源循环集团有限公司", "url": "http://www.crrg.com.cn/"},
    {"name": "中国有研科技集团有限公司", "url": "http://www.grinm.com/"},
    {"name": "矿冶科技集团有限公司", "url": "http://www.bgrimm.com/"},
    {"name": "中国国际技术智力合作集团有限公司", "url": "http://www.ciic.com.cn/",
     # /1039027/1039029/ 为"服务领域"栏目（如数字化转型咨询），不是新闻
     "exclude_paths": ["/1039027/1039029/"]},
    {"name": "中国建筑科学研究院有限公司", "url": "http://www.cabr.com.cn/"},
    {"name": "中国信息通信科技集团有限公司", "url": "http://www.cict.com/"},
    {"name": "中国农业发展集团有限公司", "url": "http://www.cnadc.com.cn/"},
    {"name": "中国林业集团有限公司", "url": "http://www.cfgc.cn/"},
    {"name": "中国医药集团有限公司", "url": "http://www.sinopharm.com/"},
    {"name": "中国保利集团有限公司", "url": "http://www.poly.com.cn/"},
    {"name": "中国建设科技有限公司", "url": "https://www.cctc.cn/"},
    {"name": "新兴际华集团有限公司", "url": "http://www.xxcig.com/"},
    {"name": "中国矿产资源集团有限公司", "url": "http://www.cmr-co.com/"},
    {"name": "中国民航信息集团有限公司", "url": "http://www.travelsky.cn/"},
    {"name": "中国航空油料集团有限公司", "url": "http://www.cnaf.com/"},
    {"name": "中国航空器材集团有限公司", "url": "http://www.casc.com.cn/"},
    {"name": "中国电气装备集团有限公司", "url": "http://www.cee-group.cn/"},
    {"name": "中国物流集团有限公司", "url": "https://www.chinalogisticsgroup.com.cn/"},
    {"name": "中国南水北调集团有限公司", "url": "http://www.csnwd.com.cn/"},
    {"name": "中国国新控股有限责任公司", "url": "http://www.crhc.cn/"},
    {"name": "华侨城集团有限公司", "url": "http://www.chinaoct.com/"},
    {"name": "南光（集团）有限公司", "url": "http://www.namkwong.com.mo/"},
    {"name": "中国广核集团有限公司", "url": "http://www.cgnpc.com.cn/"},
    {"name": "中国黄金集团有限公司", "url": "https://www.chinagoldgroup.com/"},
    {"name": "中国检验认证（集团）有限公司", "url": "http://www.ccic.com/"},
]
