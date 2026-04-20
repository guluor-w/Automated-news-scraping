"""
微博账号解析器。

入口函数
--------
parse_weibo(config, now) -> List[Item]
    通过 Playwright 无头浏览器抓取配置的微博账号帖子，
    按关键词和时间窗口过滤后返回 Item 列表。

依赖说明
--------
需要安装 Playwright 并下载 Chromium：
    pip install playwright
    python -m playwright install chromium

配置示例（config.yaml）
-----------------------
weibo_monitor:
  enabled: true
  mode: weibo_only   # all | weibo_only | website_only
  max_pages: 1       # 每个微博账号最多抓取的页数

新增来源提示
------------
在本文件的 MONITOR_ACCOUNTS 中添加 {账号名: UID} 即可。
UID 可在微博主页地址栏中获取（URL 格式：https://weibo.com/u/<UID>）。
"""

import asyncio
import concurrent.futures
import logging
import random
import re
from datetime import datetime, timedelta
from html import unescape
from typing import List, Optional

from dateutil import parser as dtparser

from models import Item, MIIT_ONLY_KEYWORDS
from utils import format_fetched_at, keyword_hit, within_window

logger = logging.getLogger(__name__)

# ─────────────── 监控账号列表 ────────────────────────────────────────────────
# 格式: {显示名: uid}
# UID 可在微博主页地址栏中获取
MONITOR_ACCOUNTS = {
    # ==================== 中央部委 ====================
    # "工信微报":       "5149608258",  # 工业和信息化部新闻宣传中心
    "锐科技":         "5356414944",  # 科学技术部官方微博
    "国家发展改革委": "5663214224",  # 国家发展和改革委员会政策研究室
    "国资小新":       "2752396553",  # 国务院国资委新闻中心
    "中国政府网":     "5000609535",  # 国务院办公厅中国政府网运行中心
    "中国统计":       "3919628624",  # 国家统计局新闻办公室
    "微言教育":       "2737798435",  # 教育部新闻办公室
    "央行微播":       "3921015143",  # 中国人民银行办公厅
    "健康中国":       "2834480301",  # 国家卫生健康委员会
    "国密局网站":     "5994847966",  # 国家密码管理局
    "国家矿山安全监察局":   "7293871891",  # 国家矿山安全监察局
    "国家药监局":     "1335661387",  # 国家药品监督管理局
    "中国消防":       "3549916270",  # 国家消防救援局
    "应急管理部":     "5342220662",  # 应急管理部
    "国家税务总局":   "5120551209",  # 国家税务总局新闻宣传办公室
    "海关发布":       "5832321505",  # 海关总署办公厅
    "司法部":         "6199038235",  # 司法部
    "国家移民管理局": "6929716472",  # 国家移民管理局
    "证监会发布":     "3802136340",  # 中国证监会办公厅新闻办
    "外交部":         "1938330147",  # 中华人民共和国外交部
    "生态环境部":     "6059162597",  # 生态环境部
    "自然资源部":     "5000764997",  # 自然资源部
    "国防部发布":     "5611549371",  # 国防部新闻局
    "中国交通":       "7073634525",  # 交通运输部
    "中国水利":       "7819214109",  # 中国水利报社
    "市说新语":       "6535805862",  # 国家市场监督管理总局
    "中国气象局":     "2117508734",  # 中国气象局
    "中科院之声":     "3494982177",  # 中国科学院
    "文旅之声":       "5713450386",  # 文化和旅游部
    "国家知识产权局": "7209873791",  # 国家知识产权局
    "民政微语":       "2565811051",  # 民政部新闻办
    "中国文博":       "3896555376",  # 国家文物局
    "视听中国":       "7408066931",  # 国家广电总局信息中心
    "国家粮食和物资储备局": "6142709212",  # 国家粮食和物资储备局
    "商务微新闻":     "2848929290",  # 商务部新闻办
    "国家邮政局":     "6067873008",  # 国家邮政局
    "外汇局发布":     "5263752045",  # 国家外汇管理局
    "道中华":         "7921810443",  # 国家民委融媒体中心
    "国家版权局":     "5286924878",  # 国家版权局

    # ==================== 省级政务 ====================
    "北京发布":       "2418724427",  # 北京市政府新闻办公室
    "上海发布":       "2539961154",  # 上海市政府新闻办公室
    "广东发布":       "2775872784",  # 广东省人民政府新闻办公室
    "浙江发布":       "5131766197",  # 浙江省人民政府新闻办公室
    "微博江苏":       "2784361770",  # 江苏省人民政府新闻办公室
    "山东发布":       "2993099575",  # 山东省人民政府新闻办公室
    "四川发布":       "1905843503",  # 四川省人民政府新闻办公室
    "湖北发布":       "2607972104",  # 湖北省人民政府新闻办公室
    "湖南微政务":     "3499010272",  # 湖南省互联网信息办公室
    "河北发布":       "2634384567",  # 河北省人民政府新闻办公室
    "河南政府网":     "2339634231",  # 河南省人民政府门户网站
    "安徽发布":       "3011694992",  # 安徽省互联网信息办公室
    "重庆发布":       "1988438334",  # 重庆市人民政府新闻办公室
    "江西发布":       "3687019147",  # 江西省人民政府新闻办公室
    "辽宁发布":       "5537781788",  # 辽宁省政府门户网站
    "黑龙江发布":     "3950759014",  # 黑龙江省人民政府新闻办公室
    "新疆发布":       "2541592687",  # 新疆维吾尔自治区人民政府新闻办公室
    "云南发布":       "1662558237",  # 中共云南省委宣传部
    "贵州发布":       "2207702064",  # 贵州省人民政府新闻办公室
    "西藏发布":       "2620622835",  # 西藏发布官方微博
    "山西发布":       "2726922721",  # 山西省人民政府新闻办公室
    "甘肃发布":       "1937187173",  # 甘肃省政府新闻办
    "吉林发布":       "3229450293",  # 吉林省人民政府新闻办公室
    "陕西发布":       "3097688767",  # 陕西省人民政府门户网站
    "广西发布":       "7921790417",  # 广西壮族自治区人民政府新闻办公室
    "福建发布":       "5033508400",  # 福建省政府新闻办
    "海南发布":       "5245236250",  # 海南省新闻办公室
    "活力内蒙古":     "2270636837",  # 内蒙古自治区互联网信息办公室
    "天津发布":       "2489610225",  # 天津市人民政府新闻办公室
    "青海发布":       "2782520515",  # 青海省人民政府新闻办公室
    "宁夏政务发布":   "3949984662",  # 宁夏回族自治区人民政府

    # # ==================== 央企（100家） ====================
    # # --- 军工/航天/航空 ---
    # "中核集团":           "2884530251",
    # "中国航天科技集团":   "5386897742",
    # "中国航天科工":       "2459025125",
    # "中国航空工业集团":   "3061210763",
    # "中国船舶":           "6861836076",
    # "兵工之声":           "5616642069",
    # "中国兵器装备":       "6510003802",
    # "中国电科":           "6086357399",
    # "中国航发":           "7854615254",
    # "中国商飞":           "5120831098",

    # # --- 能源/电力 ---
    # "中国石油":           "5655420911",
    # "中国石化":           "3429300952",
    # "海油螺号":           "5306774965",
    # "国家电网":           "1730306175",
    # "南网50Hz":           "2053782235",
    # "中国华能":           "5702759490",
    # "中国大唐":           "3872312979",
    # "中国华电":           "6915122349",
    # "国家电投":           "5663505560",
    # "中国三峡集团":       "6053241815",
    # "国家能源集团之声":   "3012462187",
    # "中国煤炭科工集团":   "5751944981",
    # "中国广核集团":       "1901762782",

    # # --- 通信/电子/IT ---
    # "中国电信":           "1975415803",
    # "中国联通":           "2002148123",
    # "中国移动":           "2001627641",
    # "CEC中国电子":        "3117177915",
    # "信科视界":           "6664247586",

    # # --- 汽车/装备制造 ---
    # "中国一汽":           "5143653913",
    # "东风汽车":           "5229898329",
    # "中国长安汽车集团":   "8009156401",
    # "中国一重官微":       "5209116401",
    # "国机集团":           "5248542234",
    # "东方电气":           "7439217558",
    # "中国中车":           "5618105325",
    # "中国电气装备":       "7870336560",

    # # --- 钢铁/矿业/有色 ---
    # "鞍钢集团":           "2625024707",
    # "友爱的宝武":         "2696345163",
    # "中国五矿":           "5120239186",
    # "中国黄金ChinaGold":  "2315762592",
    # "中国钢研":           "2907265074",
    # "中国冶金地质总局":   "7623411122",
    # "中国煤炭地质总局":   "2299126247",

    # # --- 交通/物流/航空 ---
    # "中远海运":           "7912578026",
    # "中国东方航空":       "1647310954",
    # "中国南方航空":       "1647687670",
    # "中国中铁":           "5667381614",
    # "中国铁建":           "5669279258",
    # "中国交建":           "3912086680",
    # "中国物流集团":       "7787705472",
    # "中国航油":           "2670112415",

    # # --- 建筑/建材/化工 ---
    # "中国建筑":           "6147164852",
    # "中国化学":           "7258465916",
    # "中国建材集团":       "5622948974",
    # "中国电建":           "7784996775",
    # "中国能建":           "7688462735",
    # "中国安能_水电铁军":  "7739126144",
    # "中国建研院":         "2383385807",

    # # --- 粮食/农业/消费 ---
    # "中粮COFCO":          "1752161437",
    # "中储粮集团":         "5042945896",
    # "中国农发集团":       "5335999195",
    # "中国旅游集团":       "7504088595",
    # "OCT华侨城":          "1964212803",
    # "中国中化":           "3763680854",

    # # --- 投资/金融/综合 ---
    # "国投集团":           "6088143954",
    # "中国诚通":           "7383209742",
    # "中国国新":           "7794174596",
    # "中智集团":           "5982866191",
    # "保利发展控股":       "1770996052",
    # "通用技术":           "5997112495",
    # "新兴际华集团":       "7459689956",
    # "中国中检":           "7907329117",

    # # --- 水利/林业/盐业/其他 ---
    # "博言南水北调":       "6135422374",
    # "中林集团":           "2674830355",
}

# ─────────────── 常量 ─────────────────────────────────────────────────────────
# 每个账号最多翻页数（每页约 10 条微博）
MAX_PAGES = 3

# 请求间随机延迟范围（秒），避免触发反爬
REQUEST_DELAY = (10, 20)

# CAPTCHA 触发后的冷却等待时间（秒）
CAPTCHA_COOLDOWN = 60

# 单个账号最大重试次数
MAX_RETRIES = 2


# ─────────────── 微博数据解析工具 ─────────────────────────────────────────────

def clean_html(html_text: str) -> str:
    """清理 HTML 标签，保留纯文本"""
    if not html_text:
        return ""
    text = re.sub(r"<[^>]+>", " ", html_text)
    text = unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def parse_weibo_time(time_str: str, now: Optional[datetime] = None) -> str:
    """将微博的各种时间格式统一转换为 YYYY-MM-DD HH:MM"""
    if not time_str:
        return ""
    try:
        reference_now = now if now is not None else datetime.now().astimezone()
        if reference_now.tzinfo is None:
            reference_now = reference_now.astimezone()

        if "刚刚" in time_str:
            return reference_now.strftime("%Y-%m-%d %H:%M")
        m = re.search(r"(\d+)分钟前", time_str)
        if m:
            return (reference_now - timedelta(minutes=int(m.group(1)))).strftime("%Y-%m-%d %H:%M")
        m = re.search(r"(\d+)小时前", time_str)
        if m:
            return (reference_now - timedelta(hours=int(m.group(1)))).strftime("%Y-%m-%d %H:%M")
        m = re.search(r"昨天\s*(\d{2}:\d{2})", time_str)
        if m:
            return f"{(reference_now - timedelta(days=1)).strftime('%Y-%m-%d')} {m.group(1)}"
        m = re.match(r"^(\d{2})-(\d{2})$", time_str.strip())
        if m:
            return f"{reference_now.year}-{m.group(1)}-{m.group(2)}"
        # 标准格式 "Fri Apr 03 12:22:54 +0800 2026"
        try:
            dt = datetime.strptime(time_str, "%a %b %d %H:%M:%S %z %Y")
            return dt.strftime("%Y-%m-%d %H:%M")
        except ValueError:
            pass
        return time_str
    except Exception:
        return time_str


def parse_mblog(mblog: dict) -> Optional[dict]:
    """解析单条微博数据，提取标题、链接等关键信息"""
    mid = str(mblog.get("mid", "") or mblog.get("id", ""))
    if not mid:
        return None

    raw_text = mblog.get("text", "")
    clean_text = clean_html(raw_text)

    # 检查是否为头条文章或外链
    page_info = mblog.get("page_info", {})
    is_article = False
    article_url = ""
    article_title = ""

    if page_info:
        page_type = page_info.get("type", "")
        if page_type == "article":
            is_article = True
            article_url = page_info.get("page_url", "")
            article_title = (
                page_info.get("page_title", "")
                or page_info.get("content1", "")
            )
        elif page_type in ("webpage", "video"):
            article_url = page_info.get("page_url", "")
            article_title = (
                page_info.get("page_title", "")
                or page_info.get("content1", "")
            )

    # 标题：优先文章标题，否则截取微博文本
    title = article_title if article_title else clean_text[:80]
    if len(clean_text) > 80 and not article_title:
        title += "..."

    return {
        "mid": mid,
        "title": title.strip(),
        "text": clean_text.strip(),
        "url": f"https://m.weibo.cn/detail/{mid}",
        "article_url": article_url,
        "is_article": is_article,
        "created_at": mblog.get("created_at", ""),
        "parsed_time": parse_weibo_time(mblog.get("created_at", "")),
        "source": clean_html(mblog.get("source", "")),
        "reposts_count": mblog.get("reposts_count", 0),
        "comments_count": mblog.get("comments_count", 0),
        "attitudes_count": mblog.get("attitudes_count", 0),
    }


# ─────────────── Playwright 微博客户端 ───────────────────────────────────────

class PlaywrightWeiboClient:
    """
    使用 Playwright 无头浏览器访问微博。
    核心思路：让浏览器正常渲染页面（自动完成访客验证），
    同时拦截浏览器发出的 API 请求获取结构化 JSON 数据。
    支持 CAPTCHA 检测与自动重试。
    """

    # 多个 User-Agent 轮换，降低指纹识别风险
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    ]

    def __init__(self):
        self._playwright = None
        self._browser = None
        self._captcha_hit = False
        self._request_count = 0

    async def _ensure_browser(self):
        """懒加载浏览器实例"""
        if self._browser is None:
            from playwright.async_api import async_playwright
            self._playwright = await async_playwright().start()
            self._browser = await self._playwright.chromium.launch(headless=True)
            logger.info("Playwright 浏览器已启动")

    async def close(self):
        """关闭浏览器"""
        if self._browser:
            await self._browser.close()
            self._browser = None
        if self._playwright:
            await self._playwright.stop()
            self._playwright = None

    async def _new_context(self):
        """创建新的浏览器上下文，随机 User-Agent"""
        await self._ensure_browser()
        ua = random.choice(self.USER_AGENTS)
        return await self._browser.new_context(
            user_agent=ua,
            viewport={"width": 1280, "height": 800},
            locale="zh-CN",
        )

    async def _random_delay(self):
        """随机延迟，模拟人类行为"""
        delay = random.uniform(*REQUEST_DELAY)
        await asyncio.sleep(delay)

    async def _check_captcha(self, page) -> bool:
        """检测页面是否跳转到了验证码页面"""
        url = page.url
        return "captcha" in url or "verify" in url

    async def _handle_captcha(self):
        """处理 CAPTCHA：等待冷却后重置浏览器"""
        if self._captcha_hit:
            return
        self._captcha_hit = True
        logger.warning(f"检测到微博验证码拦截，等待 {CAPTCHA_COOLDOWN} 秒后重试...")
        logger.warning("提示：如频繁触发，建议降低抓取频率或使用代理IP")
        await asyncio.sleep(CAPTCHA_COOLDOWN)
        await self.close()
        self._captcha_hit = False
        self._request_count = 0

    async def get_user_timeline(self, uid: str, max_pages: int = 1) -> Optional[list]:
        """
        获取指定用户的微博时间线。
        通过拦截浏览器加载页面时的 API 响应来获取数据。
        支持 CAPTCHA 检测与自动重试。
        返回 None 表示 CAPTCHA 导致重试耗尽。
        """
        for attempt in range(1, MAX_RETRIES + 1):
            result = await self._fetch_timeline_once(uid, max_pages)
            if result is not None:
                return result
            if attempt < MAX_RETRIES:
                logger.info(f"    第 {attempt} 次重试...")
                await self._handle_captcha()
            else:
                logger.warning(f"    已达最大重试次数，跳过此账号")
        return None

    async def _fetch_timeline_once(self, uid: str, max_pages: int) -> Optional[list]:
        """单次尝试获取时间线，CAPTCHA 时返回 None"""
        await self._random_delay()
        context = await self._new_context()
        all_posts = []

        try:
            page = await context.new_page()
            captured_cards = []

            async def on_response(response):
                url = response.url
                if "api/container/getIndex" in url and f"107603{uid}" in url:
                    try:
                        body = await response.json()
                        if body.get("ok") == 1:
                            cards = body.get("data", {}).get("cards", [])
                            captured_cards.extend(cards)
                    except Exception:
                        pass

            page.on("response", on_response)

            logger.info(f"    加载用户主页 (UID: {uid})...")
            await page.goto(
                f"https://m.weibo.cn/u/{uid}",
                wait_until="networkidle",
                timeout=30000,
            )
            await page.wait_for_timeout(2000)

            if await self._check_captcha(page):
                logger.warning(f"    UID {uid} 触发验证码拦截")
                return None

            self._request_count += 1

            for pg in range(2, max_pages + 1):
                logger.info(f"    滚动加载第 {pg} 页...")
                prev_count = len(captured_cards)
                for _ in range(3):
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await page.wait_for_timeout(1500)
                if len(captured_cards) == prev_count:
                    logger.info(f"    无更多数据，停止翻页")
                    break

            for card in captured_cards:
                if card.get("card_type") != 9:
                    continue
                mblog = card.get("mblog")
                if mblog:
                    parsed = parse_mblog(mblog)
                    if parsed:
                        all_posts.append(parsed)

        except Exception as e:
            logger.error(f"    抓取异常: {e}")
        finally:
            await context.close()

        return all_posts


# ─────────────── 内部抓取函数（可在测试中 patch） ────────────────────────────

async def _fetch_weibo_raw(accounts: dict, max_pages: int) -> dict:
    """
    使用 PlaywrightWeiboClient 抓取所有微博账号数据。

    Args:
        accounts:  {显示名: uid} 字典。
        max_pages: 每个账号最多翻页数。

    Returns:
        {显示名: [post, ...]} 字典。
    """
    client = PlaywrightWeiboClient()
    all_results: dict = {}
    try:
        for name, uid in accounts.items():
            logger.info(f"[微博] {name} (UID: {uid})")
            posts = await client.get_user_timeline(uid, max_pages)
            all_results[name] = posts if posts is not None else []
    finally:
        await client.close()
    return all_results


# ─────────────── 解析器入口 ──────────────────────────────────────────────────

def parse_weibo(config: dict, now: datetime) -> List[Item]:
    """
    抓取微博账号帖子并返回符合条件的新闻列表。

    仅处理微博帖子（post 中含 "mid" 字段）；官网文章由 parse_website_monitor 负责。

    Args:
        config: 来自 config.yaml 的全量配置字典。
        now:    当前时间（带时区）。

    Returns:
        过滤后的 Item 列表；若功能未启用或 mode=website_only 则返回空列表。
    """
    weibo_cfg = config.get("weibo_monitor", {})
    if not weibo_cfg.get("enabled", False):
        return []

    mode = weibo_cfg.get("mode", "all")
    if mode == "website_only":
        return []

    max_pages = int(weibo_cfg.get("max_pages", MAX_PAGES))
    keywords = config.get("keywords", [])
    weibo_keywords = [k for k in keywords if k not in MIIT_ONLY_KEYWORDS]
    window_days = int(config.get("window_days", 15))
    hard_cap_days = int(config.get("hard_cap_days", 15))
    fetched_at = format_fetched_at(now)

    async def _fetch() -> dict:
        return await _fetch_weibo_raw(MONITOR_ACCOUNTS, max_pages)

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
        print(f"[WARN] 微博抓取失败，跳过该数据源: {exc}")
        return []

    items: List[Item] = []
    for source_name, posts in all_results.items():
        for post in posts:
            if "mid" not in post:
                continue  # 非微博帖子，跳过

            title = post.get("title", "").strip()
            url = post.get("article_url") or post.get("url", "")
            pub_date_str = post.get("parsed_time", "")
            pub_date = None
            if pub_date_str and len(pub_date_str) >= 10:
                try:
                    _d = dtparser.parse(pub_date_str[:10])
                    pub_date = f"{_d.year}/{_d.month}/{_d.day}"
                except Exception:
                    pub_date = pub_date_str[:10]

            if not title or not url:
                continue
            if not keyword_hit(title, weibo_keywords):
                continue
            if pub_date and not within_window(pub_date, now, window_days, hard_cap_days):
                continue

            items.append(Item(
                title=title,
                publisher=source_name,
                url=url,
                pub_date=pub_date,
                source=f"微博-{source_name}",
                fetched_at=fetched_at,
            ))

    return items
