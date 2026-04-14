"""
各部门官网解析器。

入口函数
--------
parse_website_monitor(config, now) -> List[Item]
    通过 Playwright 渲染页面，抓取配置的官网文章列表，
    按关键词过滤后返回 Item 列表。

说明
----
本模块维护 WEBSITE_SOURCES 字典，列出所有待监控的官网地址。
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

新增来源提示
------------
在本文件的 WEBSITE_SOURCES 中添加对应条目即可：
    WEBSITE_SOURCES = {
        "国家数据局": {
            "url": "https://www.nda.gov.cn/sjj/swdt/list/index_pc_1.html",
            "org": "国家数据局",
        },
        "新来源名": {"url": "列表页URL", "org": "机构名"},
    }
"""

import asyncio
import concurrent.futures
import logging
import random
from datetime import datetime
from typing import List
from urllib.parse import urlparse

from models import Item
from utils import format_fetched_at, keyword_hit

logger = logging.getLogger(__name__)

# ─────────────── 官网新闻源 ──────────────────────────────────────────────────
# 格式: {显示名: {"url": 新闻列表页URL, "org": 全称}}
# 系统会自动解析页面中的文章标题和链接
# 可选字段:
#   "slow": True     — 超时时间延长至 60 秒（默认 30 秒）
#   "disabled": True — 跳过该源（保留配置但暂停抓取）
WEBSITE_SOURCES = {
    # --- 中央部委 ---
    "国家数据局":           {"url": "https://www.nda.gov.cn/sjj/swdt/list/index_pc_1.html",    "org": "国家数据局"},
    "国家信访局":           {"url": "https://www.gjxfj.gov.cn/gjxfj/news/index.htm",           "org": "国家信访局"},
    "财政部":               {"url": "https://www.mof.gov.cn/zhengwuxinxi/caizhengxinwen/",     "org": "财政部"},
    "审计署":               {"url": "https://www.audit.gov.cn/n4/n19/index.html",              "org": "审计署"},
    "国家能源局":           {"url": "http://www.nea.gov.cn/xwzx/index.htm",                    "org": "国家能源局"},
    "国家国际发展合作署":   {"url": "http://www.cidca.gov.cn/hzdt2.htm",                       "org": "国家国际发展合作署"},
    "国家核安全局":         {"url": "https://nnsa.mee.gov.cn/ywdt/hyzx/",                      "org": "国家核安全局"},
    "国家档案局":           {"url": "https://www.saac.gov.cn/daj/xwdt/xwdt.shtml",             "org": "国家档案局"},
    "首都之窗":             {"url": "https://www.beijing.gov.cn/ywdt/",                        "org": "北京市人民政府门户网站"},
    "人力资源和社会保障部": {"url": "https://www.mohrss.gov.cn/SYrlzyhshbzb/dongtaixinwen/buneiyaowen/", "org": "人力资源和社会保障部"},

    # --- 央企 ---
    "中国融通集团":     {"url": "https://www.crtamg.com.cn/xwzx/jtdt/",             "org": "中国融通资产管理集团有限公司"},
    "哈电集团":         {"url": "https://www.harbin-electric.com/xwzx.htm",          "org": "哈尔滨电气集团有限公司"},
    "中铝集团":         {"url": "https://www.chinalco.com.cn/xwzx/",                "org": "中国铝业集团有限公司"},
    "中国航空集团":     {"url": "https://www.airchinagroup.com/cnah/include/xwzxindex.shtml", "org": "中国航空集团有限公司"},
    "招商局集团":       {"url": "https://www.cmhk.com/main/xwzx/jtyw/index.html",  "org": "招商局集团有限公司"},
    "华润集团":         {"url": "https://winfo.crc.com.cn/news/crc_dynamic/",       "org": "华润（集团）有限公司"},
    "中国节能":         {"url": "https://www.cecep.cn/cecep/news/jtxw/",             "org": "中国节能环保集团有限公司"},
    "中国有色集团":     {"url": "https://www.cnmc.com.cn/cnmc/xwzx/jtxw/",          "org": "中国有色矿业集团有限公司"},
    "中国稀土集团":     {"url": "https://www.regcc.cn/zgxtjt/xwzx/news.shtml",      "org": "中国稀土集团有限公司"},
    "国药集团":         {"url": "https://www.sinopharm.com/mediacenter.html",        "org": "中国医药集团有限公司"},
    # 注: 以下几个源暂不可用，保留配置以便后续恢复; disabled=True 时自动跳过
    #   - 中国中煤: 境外网络超时
    #   - 中国矿产资源集团: 纯SPA无服务端渲染
    #   - 中咨公司: 官网502不可达
    "中国中煤":         {"url": "https://www.chinacoal.com/col/col3/index.html",     "org": "中国中煤能源集团有限公司", "slow": True, "disabled": True},
    "中国矿产资源集团": {"url": "https://www.cmr-co.com/news",                      "org": "中国矿产资源集团有限公司", "disabled": True},
    "中国资源循环集团": {"url": "http://www.crrg.com.cn/crrg/xwzx/jtyw/index.html", "org": "中国资源循环集团有限公司"},
    "中国有研":         {"url": "https://www.grinm.com/1332.html",                  "org": "中国有研科技集团有限公司", "slow": True},
    "中国建科":         {"url": "https://www.cctc.cn/xwzx/jtyw/index.shtml",        "org": "中国建设科技有限公司"},
    "中盐集团":         {"url": "http://www.chinasalt.com.cn/xwzx",                 "org": "中国盐业集团有限公司"},
    "矿冶科技集团":     {"url": "https://www.bgrimm.com/xwzx/kydt/index1.htm",      "org": "矿冶科技集团有限公司"},
    "南光集团":         {"url": "http://www.namkwong.com.mo/col/col1816/index.html", "org": "南光（集团）有限公司"},
    "中咨公司":         {"url": "https://www.ciecc.com.cn/col/col1595/index.html",  "org": "中国国际工程咨询有限公司", "disabled": True},
    "中国机械总院":     {"url": "https://www.cam.com.cn/channels/169.html",          "org": "中国机械科学研究总院集团有限公司"},
}


# ─────────────── 官网新闻客户端 ──────────────────────────────────────────────

class WebsiteNewsClient:
    """
    通用官网新闻列表页解析器。
    使用 Playwright 渲染页面（支持 JS 动态加载），
    然后提取 <a> 标签中的文章标题和链接。
    """

    # 常见新闻列表页中需要排除的导航/功能链接关键词
    EXCLUDE_KEYWORDS = [
        "首页", "关于我们", "联系我们", "网站地图", "版权", "隐私",
        "登录", "注册", "English", "搜索", "更多", "下一页", "上一页",
        "javascript:", "void(0)", "#", "mailto:", "返回顶部",
    ]

    def __init__(self, browser=None):
        self._playwright = None
        self._browser = browser  # 可复用已有浏览器实例
        self._owns_browser = browser is None

    async def _ensure_browser(self):
        if self._browser is None:
            from playwright.async_api import async_playwright
            self._playwright = await async_playwright().start()
            self._browser = await self._playwright.chromium.launch(headless=True)

    async def close(self):
        if self._owns_browser:
            if self._browser:
                await self._browser.close()
            if self._playwright:
                await self._playwright.stop()
        self._browser = None
        self._playwright = None

    async def fetch_news(self, name: str, url: str, max_items: int = 20,
                         timeout: int = 30000) -> list:
        """
        从指定 URL 抓取新闻列表。
        返回: [{"title": ..., "url": ..., "source": name}, ...]
        """
        await self._ensure_browser()
        articles = []
        context = None

        try:
            context = await self._browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 900},
                locale="zh-CN",
                ignore_https_errors=True,
            )
            page = await context.new_page()

            logger.info(f"    加载官网: {url}")
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
                await page.wait_for_timeout(3000)
            except Exception as nav_err:
                logger.warning(f"    导航超时，尝试降级加载: {nav_err}")
                try:
                    await page.goto(url, wait_until="commit", timeout=15000)
                    await page.wait_for_timeout(5000)
                except Exception:
                    raise nav_err

            links = await page.evaluate("""() => {
                const anchors = document.querySelectorAll('a');
                return Array.from(anchors).map(a => ({
                    text: (a.textContent || '').trim(),
                    href: a.href || '',
                    title: a.getAttribute('title') || '',
                }));
            }""")

            base_url = url.rsplit("/", 1)[0] if "/" in url else url
            seen_urls: set = set()

            for link in links:
                text = link.get("title") or link.get("text", "")
                href = link.get("href", "")

                if not text or len(text) < 6:
                    continue
                if not href or href == url:
                    continue
                if any(kw in text for kw in self.EXCLUDE_KEYWORDS):
                    continue
                if any(kw in href for kw in ["javascript:", "void(0)", "mailto:"]):
                    continue
                if href in seen_urls:
                    continue

                if href.startswith("//"):
                    href = "https:" + href
                elif href.startswith("./"):
                    href = base_url + "/" + href[2:]
                elif href.startswith("/"):
                    parsed = urlparse(url)
                    href = f"{parsed.scheme}://{parsed.netloc}{href}"

                seen_urls.add(href)
                articles.append({
                    "title": text.strip()[:120],
                    "url": href,
                    "source": name,
                })

                if len(articles) >= max_items:
                    break

        except Exception as e:
            logger.error(f"    官网抓取异常 ({name}): {e}")
        finally:
            if context is not None:
                await context.close()

        return articles


# ─────────────── 内部抓取函数（可在测试中 patch） ────────────────────────────

async def _fetch_website_raw(sources: dict) -> dict:
    """
    使用 WebsiteNewsClient 抓取所有官网新闻。

    Args:
        sources: WEBSITE_SOURCES 格式的字典。

    Returns:
        {显示名: [article, ...]} 字典（已跳过 disabled 源）。
    """
    client = WebsiteNewsClient()
    all_results: dict = {}
    try:
        for name, cfg in sources.items():
            if cfg.get("disabled"):
                continue
            url = cfg["url"]
            timeout = 60000 if cfg.get("slow") else 30000
            logger.info(f"[官网] {name}")
            articles = await client.fetch_news(name, url, timeout=timeout)
            all_results[name] = articles
            await asyncio.sleep(random.uniform(1, 3))
    finally:
        await client.close()
    return all_results


# ─────────────── 解析器入口 ──────────────────────────────────────────────────

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

    keywords = config.get("keywords", [])
    website_keywords = [k for k in keywords if k != "印发"]
    fetched_at = format_fetched_at(now)

    async def _fetch() -> dict:
        return await _fetch_website_raw(WEBSITE_SOURCES)

    def _run_fetch() -> dict:
        return asyncio.run(_fetch())

    try:
        try:
            asyncio.get_running_loop()
            running = True
        except RuntimeError:
            running = False

        if running:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                all_results: dict = pool.submit(_run_fetch).result()
        else:
            all_results = _run_fetch()
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
            pub_date = None  # TODO: extract pub_date from article page

            if not title or not url:
                continue
            if not keyword_hit(title, website_keywords):
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
