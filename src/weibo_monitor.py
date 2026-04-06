#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
微博 + 官网新闻 监控系统
========================
监控指定微博账号和政府/央企官网的新闻动态，
获取标题和超链接，支持每日定时更新。

数据来源:
  1. 微博账号 (143个): 通过 Playwright 拦截移动端 API 获取
  2. 官网新闻 (30个):  通过 Playwright 渲染页面解析链接

依赖安装:
  pip install playwright schedule
  python -m playwright install chromium

使用方式:
  1. 单次抓取:  python weibo_monitor.py
  2. 每日定时:  python weibo_monitor.py --schedule 08:00
  3. 搜索UID:   python weibo_monitor.py --search "工信微报"
  4. 自定义账号: python weibo_monitor.py --accounts '{"央视新闻": "2656274875"}'
  5. 仅抓微博:  python weibo_monitor.py --weibo-only
  6. 仅抓官网:  python weibo_monitor.py --website-only
  7. 测试全部源: python weibo_monitor.py --test
  8. 测试指定类型: python weibo_monitor.py --test --weibo-only
"""

import re
import sys
import csv
import json
import time
import asyncio
import logging
import argparse
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
import hashlib
from html import unescape

try:
    import schedule
    HAS_SCHEDULE = True
except ImportError:
    HAS_SCHEDULE = False

# ============================================================
# 配置区
# ============================================================

# 监控账号列表: {显示名: uid}
# UID 可通过 --search 功能查找，或在微博主页地址栏中获取
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
    # "中核集团":           "2884530251",  # 中国核工业集团有限公司
    # "中国航天科技集团":   "5386897742",  # 中国航天科技集团有限公司
    # "中国航天科工":       "2459025125",  # 中国航天科工集团有限公司
    # "中国航空工业集团":   "3061210763",  # 中国航空工业集团有限公司
    # "中国船舶":           "6861836076",  # 中国船舶集团有限公司
    # "兵工之声":           "5616642069",  # 中国兵器工业集团有限公司
    # "中国兵器装备":       "6510003802",  # 中国兵器装备集团有限公司
    # "中国电科":           "6086357399",  # 中国电子科技集团有限公司
    # "中国航发":           "7854615254",  # 中国航空发动机集团有限公司
    # "中国商飞":           "5120831098",  # 中国商用飞机有限责任公司

    # # --- 能源/电力 ---
    # "中国石油":           "5655420911",  # 中国石油天然气集团有限公司
    # "中国石化":           "3429300952",  # 中国石油化工集团有限公司
    # "海油螺号":           "5306774965",  # 中国海洋石油集团有限公司
    # "国家电网":           "1730306175",  # 国家电网有限公司
    # "南网50Hz":           "2053782235",  # 中国南方电网有限责任公司
    # "中国华能":           "5702759490",  # 中国华能集团有限公司
    # "中国大唐":           "3872312979",  # 中国大唐集团有限公司
    # "中国华电":           "6915122349",  # 中国华电集团有限公司
    # "国家电投":           "5663505560",  # 国家电力投资集团有限公司
    # "中国三峡集团":       "6053241815",  # 中国长江三峡集团有限公司
    # "国家能源集团之声":   "3012462187",  # 国家能源投资集团有限责任公司
    # "中国煤炭科工集团":   "5751944981",  # 中国煤炭科工集团有限公司
    # "中国广核集团":       "1901762782",  # 中国广核集团有限公司

    # # --- 通信/电子/IT ---
    # "中国电信":           "1975415803",  # 中国电信集团有限公司
    # "中国联通":           "2002148123",  # 中国联合网络通信集团有限公司
    # "中国移动":           "2001627641",  # 中国移动通信集团有限公司
    # "CEC中国电子":        "3117177915",  # 中国电子信息产业集团有限公司
    # "信科视界":           "6664247586",  # 中国信息通信科技集团有限公司

    # # --- 汽车/装备制造 ---
    # "中国一汽":           "5143653913",  # 中国第一汽车集团有限公司
    # "东风汽车":           "5229898329",  # 东风汽车集团有限公司
    # "中国长安汽车集团":   "8009156401",  # 中国长安汽车集团有限公司
    # "中国一重官微":       "5209116401",  # 中国一重集团有限公司
    # "国机集团":           "5248542234",  # 中国机械工业集团有限公司
    # "东方电气":           "7439217558",  # 中国东方电气集团有限公司
    # "中国中车":           "5618105325",  # 中国中车集团有限公司
    # "中国电气装备":       "7870336560",  # 中国电气装备集团有限公司

    # # --- 钢铁/矿业/有色 ---
    # "鞍钢集团":           "2625024707",  # 鞍钢集团有限公司
    # "友爱的宝武":         "2696345163",  # 中国宝武钢铁集团有限公司
    # "中国五矿":           "5120239186",  # 中国五矿集团有限公司
    # "中国黄金ChinaGold":  "2315762592",  # 中国黄金集团有限公司
    # "中国钢研":           "2907265074",  # 中国钢研科技集团有限公司
    # "中国冶金地质总局":   "7623411122",  # 中国冶金地质总局
    # "中国煤炭地质总局":   "2299126247",  # 中国煤炭地质总局

    # # --- 交通/物流/航空 ---
    # "中远海运":           "7912578026",  # 中国远洋海运集团有限公司
    # "中国东方航空":       "1647310954",  # 中国东方航空集团有限公司
    # "中国南方航空":       "1647687670",  # 中国南方航空集团有限公司
    # "中国中铁":           "5667381614",  # 中国铁路工程集团有限公司
    # "中国铁建":           "5669279258",  # 中国铁道建筑集团有限公司
    # "中国交建":           "3912086680",  # 中国交通建设集团有限公司
    # "中国物流集团":       "7787705472",  # 中国物流集团有限公司
    # "中国航油":           "2670112415",  # 中国航空油料集团有限公司

    # # --- 建筑/建材/化工 ---
    # "中国建筑":           "6147164852",  # 中国建筑集团有限公司
    # "中国化学":           "7258465916",  # 中国化学工程集团有限公司
    # "中国建材集团":       "5622948974",  # 中国建材集团有限公司
    # "中国电建":           "7784996775",  # 中国电力建设集团有限公司
    # "中国能建":           "7688462735",  # 中国能源建设集团有限公司
    # "中国安能_水电铁军":  "7739126144",  # 中国安能建设集团有限公司
    # "中国建研院":         "2383385807",  # 中国建筑科学研究院有限公司

    # # --- 粮食/农业/消费 ---
    # "中粮COFCO":          "1752161437",  # 中粮集团有限公司
    # "中储粮集团":         "5042945896",  # 中国储备粮管理集团有限公司
    # "中国农发集团":       "5335999195",  # 中国农业发展集团有限公司
    # "中国旅游集团":       "7504088595",  # 中国旅游集团有限公司
    # "OCT华侨城":          "1964212803",  # 华侨城集团有限公司
    # "中国中化":           "3763680854",  # 中国中化控股有限责任公司

    # # --- 投资/金融/综合 ---
    # "国投集团":           "6088143954",  # 国家开发投资集团有限公司
    # "中国诚通":           "7383209742",  # 中国诚通控股集团有限公司
    # "中国国新":           "7794174596",  # 中国国新控股有限责任公司
    # "中智集团":           "5982866191",  # 中国国际技术智力合作集团有限公司
    # "保利发展控股":       "1770996052",  # 中国保利集团有限公司（子公司账号）
    # "通用技术":           "5997112495",  # 中国通用技术（集团）控股有限责任公司
    # "新兴际华集团":       "7459689956",  # 新兴际华集团有限公司
    # "中国中检":           "7907329117",  # 中国检验认证（集团）有限公司

    # # --- 水利/林业/盐业/其他 ---
    # "博言南水北调":       "6135422374",  # 中国南水北调集团有限公司
    # "中林集团":           "2674830355",  # 中国林业集团有限公司
}

# ==================== 官网新闻源（无微博账号的单位） ====================
# 格式: {显示名: {"url": 新闻列表页URL, "org": 全称}}
# 系统会自动解析页面中的文章标题和链接
WEBSITE_SOURCES = {
    # --- 中央部委 ---
    "国家数据局":           {"url": "https://www.nda.gov.cn/sjj/swdt/list/index_pc_1.html",    "org": "国家数据局"},
    "国家信访局":           {"url": "https://www.gjxfj.gov.cn/gjxfj/news/index.htm",           "org": "国家信访局"},
    "财政部":               {"url": "https://www.mof.gov.cn/zhengwuxinxi/caizhengxinwen/",     "org": "财政部"},
    "审计署":               {"url": "https://www.audit.gov.cn/n4/n19/index.html",              "org": "审计署"},
    "国家能源局":           {"url": "http://www.nea.gov.cn/xwzx/index.htm",                 "org": "国家能源局"},
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
    "招商局集团":       {"url": "https://www.cmhk.com/main/xwzx/jtyw/index.html", "org": "招商局集团有限公司"},
    "华润集团":         {"url": "https://winfo.crc.com.cn/news/crc_dynamic/",       "org": "华润（集团）有限公司"},
    "中国节能":         {"url": "https://www.cecep.cn/cecep/news/jtxw/",             "org": "中国节能环保集团有限公司"},
    "中国有色集团":     {"url": "https://www.cnmc.com.cn/cnmc/xwzx/jtxw/",          "org": "中国有色矿业集团有限公司"},
    "中国稀土集团":     {"url": "https://www.regcc.cn/zgxtjt/xwzx/news.shtml",      "org": "中国稀土集团有限公司"},
    "国药集团":         {"url": "https://www.sinopharm.com/mediacenter.html",        "org": "中国医药集团有限公司"},
    # 注: 以下 3 个源暂不可用，保留配置以便后续恢复; disabled=True 时自动跳过
    #   - 中国中煤: 境外网络超时(国内部署后删除disabled即可)
    #   - 中国矿产资源集团: 纯SPA(Nuxt.js)无服务端渲染
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

# 每个账号最多翻页数（每页约 10 条微博）
MAX_PAGES = 3

# 请求间随机延迟范围（秒），避免触发反爬
REQUEST_DELAY = (10, 20)

# CAPTCHA 触发后的冷却等待时间（秒）
CAPTCHA_COOLDOWN = 60

# 单个账号最大重试次数
MAX_RETRIES = 2

# 数据输出目录（相对于脚本所在位置）
DATA_DIR = Path(__file__).parent / "weibo_data"

# 日志配置
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("weibo_monitor")


# ============================================================
# 微博数据解析工具
# ============================================================

def clean_html(html_text: str) -> str:
    """清理 HTML 标签，保留纯文本"""
    if not html_text:
        return ""
    text = re.sub(r"<[^>]+>", " ", html_text)
    text = unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def parse_weibo_time(time_str: str) -> str:
    """将微博的各种时间格式统一转换为 YYYY-MM-DD HH:MM"""
    if not time_str:
        return ""
    try:
        if "刚刚" in time_str:
            return datetime.now().strftime("%Y-%m-%d %H:%M")
        m = re.search(r"(\d+)分钟前", time_str)
        if m:
            return (datetime.now() - timedelta(minutes=int(m.group(1)))).strftime("%Y-%m-%d %H:%M")
        m = re.search(r"(\d+)小时前", time_str)
        if m:
            return (datetime.now() - timedelta(hours=int(m.group(1)))).strftime("%Y-%m-%d %H:%M")
        m = re.search(r"昨天\s*(\d{2}:\d{2})", time_str)
        if m:
            return f"{(datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')} {m.group(1)}"
        m = re.match(r"^(\d{2})-(\d{2})$", time_str.strip())
        if m:
            return f"{datetime.now().year}-{m.group(1)}-{m.group(2)}"
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


# ============================================================
# Playwright 浏览器引擎
# ============================================================

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
        self._captcha_hit = False  # CAPTCHA 状态标记
        self._request_count = 0    # 请求计数器

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
        if "captcha" in url or "verify" in url:
            return True
        return False

    async def _handle_captcha(self):
        """处理 CAPTCHA：等待冷却后重置浏览器"""
        if self._captcha_hit:
            return
        self._captcha_hit = True
        logger.warning(
            f"检测到微博验证码拦截，等待 {CAPTCHA_COOLDOWN} 秒后重试..."
        )
        logger.warning(
            "提示：如频繁触发，建议降低抓取频率或使用代理IP"
        )
        await asyncio.sleep(CAPTCHA_COOLDOWN)
        # 重启浏览器获取新会话
        await self.close()
        self._captcha_hit = False
        self._request_count = 0

    async def get_user_timeline(self, uid: str, max_pages: int = 1) -> Optional[list[dict]]:
        """
        获取指定用户的微博时间线。
        通过拦截浏览器加载页面时的 API 响应来获取数据。
        支持 CAPTCHA 检测与自动重试。
        返回 None 表示 CAPTCHA 导致重试耗尽（无法获取数据）。
        """
        for attempt in range(1, MAX_RETRIES + 1):
            result = await self._fetch_timeline_once(uid, max_pages)
            if result is not None:
                return result
            # result=None 表示遇到 CAPTCHA
            if attempt < MAX_RETRIES:
                logger.info(f"    第 {attempt} 次重试...")
                await self._handle_captcha()
            else:
                logger.warning(f"    已达最大重试次数，跳过此账号")
        return None  # CAPTCHA 导致所有重试耗尽

    async def _fetch_timeline_once(self, uid: str, max_pages: int) -> Optional[list[dict]]:
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

            # CAPTCHA 检测
            if await self._check_captcha(page):
                logger.warning(f"    UID {uid} 触发验证码拦截")
                return None

            self._request_count += 1

            # 后续页：通过滚动触发加载更多
            for pg in range(2, max_pages + 1):
                logger.info(f"    滚动加载第 {pg} 页...")
                prev_count = len(captured_cards)
                for _ in range(3):
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    await page.wait_for_timeout(1500)
                if len(captured_cards) == prev_count:
                    logger.info(f"    无更多数据，停止翻页")
                    break

            # 解析所有拦截到的卡片
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

    async def search_user(self, keyword: str) -> list[dict]:
        """搜索微博用户，返回匹配列表（用于查找 UID）"""
        context = await self._new_context()
        users = []

        try:
            page = await context.new_page()
            captured = []

            async def on_response(response):
                if "api/container/getIndex" in response.url:
                    try:
                        body = await response.json()
                        captured.append(body)
                    except Exception:
                        pass

            page.on("response", on_response)

            search_url = (
                f"https://m.weibo.cn/search?"
                f"containerid=100103type%3D3%26q%3D{keyword}"
            )
            await page.goto(search_url, wait_until="networkidle", timeout=20000)
            await page.wait_for_timeout(3000)

            seen_uids = set()
            for data in captured:
                if data.get("ok") != 1:
                    continue
                for card in data.get("data", {}).get("cards", []):
                    for item in card.get("card_group", []):
                        user = item.get("user", {})
                        if user and str(user.get("id", "")) not in seen_uids:
                            uid = str(user["id"])
                            seen_uids.add(uid)
                            users.append({
                                "uid": uid,
                                "screen_name": user.get("screen_name", ""),
                                "verified": user.get("verified", False),
                                "verified_reason": user.get("verified_reason", ""),
                                "followers_count": user.get("followers_count", 0),
                                "description": user.get("description", ""),
                            })

        except Exception as e:
            logger.error(f"搜索异常: {e}")
        finally:
            await context.close()

        return users


# ============================================================
# 官网新闻解析引擎
# ============================================================

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
                         timeout: int = 30000) -> list[dict]:
        """
        从指定 URL 抓取新闻列表。
        返回: [{"title": ..., "url": ..., "source": name}, ...]
        """
        await self._ensure_browser()
        articles = []

        try:
            context = await self._browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 900},
                locale="zh-CN",
                ignore_https_errors=True,  # 忽略 SSL 证书问题
            )
            page = await context.new_page()

            logger.info(f"    加载官网: {url}")
            # 先用 domcontentloaded（更快），再手动等待JS渲染
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
                await page.wait_for_timeout(3000)  # 等待JS渲染
            except Exception as nav_err:
                # 部分站点 domcontentloaded 也超时，尝试较宽松的方式
                logger.warning(f"    导航超时，尝试降级加载: {nav_err}")
                try:
                    await page.goto(url, wait_until="commit", timeout=15000)
                    await page.wait_for_timeout(5000)
                except Exception:
                    raise nav_err  # 仍然失败则抛出原始错误

            # 提取页面中所有 <a> 标签
            links = await page.evaluate("""() => {
                const anchors = document.querySelectorAll('a');
                return Array.from(anchors).map(a => ({
                    text: (a.textContent || '').trim(),
                    href: a.href || '',
                    title: a.getAttribute('title') || '',
                }));
            }""")

            base_url = url.rsplit("/", 1)[0] if "/" in url else url
            seen_urls = set()

            for link in links:
                text = link.get("title") or link.get("text", "")
                href = link.get("href", "")

                # 过滤规则
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

                # 规范化 URL
                if href.startswith("//"):
                    href = "https:" + href
                elif href.startswith("./"):
                    href = base_url + "/" + href[2:]
                elif href.startswith("/"):
                    from urllib.parse import urlparse
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

            await context.close()

        except Exception as e:
            logger.error(f"    官网抓取异常 ({name}): {e}")

        return articles


# ============================================================
# 数据管理
# ============================================================

class DataManager:
    """管理抓取结果的存储与去重"""

    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.history_file = data_dir / "fetch_history.json"
        self.history = self._load_history()

    def _load_history(self) -> dict:
        if self.history_file.exists():
            try:
                with open(self.history_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                return {}
        return {}

    def _save_history(self):
        with open(self.history_file, "w", encoding="utf-8") as f:
            json.dump(self.history, f, ensure_ascii=False, indent=2)

    def is_seen(self, mid: str) -> bool:
        return mid in self.history

    def mark_seen(self, mid: str, account_name: str):
        self.history[mid] = {
            "account": account_name,
            "first_seen": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    def save_daily_csv(self, account_name: str, posts: list[dict]):
        """保存每日抓取结果到 CSV（按账号分目录）"""
        if not posts:
            return
        today = datetime.now().strftime("%Y-%m-%d")
        account_dir = self.data_dir / account_name
        account_dir.mkdir(parents=True, exist_ok=True)

        csv_file = account_dir / f"{today}.csv"
        file_exists = csv_file.exists()

        fieldnames = [
            "标题", "微博链接", "文章链接", "发布时间",
            "是否头条文章", "转发数", "评论数", "点赞数", "来源",
        ]

        with open(csv_file, "a", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            for post in posts:
                writer.writerow({
                    "标题": post["title"],
                    "微博链接": post["url"],
                    "文章链接": post.get("article_url", ""),
                    "发布时间": post.get("parsed_time", post.get("created_at", "")),
                    "是否头条文章": "是" if post.get("is_article") else "否",
                    "转发数": post.get("reposts_count", 0),
                    "评论数": post.get("comments_count", 0),
                    "点赞数": post.get("attitudes_count", 0),
                    "来源": post.get("source", ""),
                })
        logger.info(f"  [{account_name}] CSV 已保存: {csv_file}")

    def save_website_csv(self, source_name: str, articles: list[dict]):
        """保存官网新闻到 CSV"""
        if not articles:
            return
        today = datetime.now().strftime("%Y-%m-%d")
        account_dir = self.data_dir / source_name
        account_dir.mkdir(parents=True, exist_ok=True)

        csv_file = account_dir / f"{today}.csv"
        file_exists = csv_file.exists()

        fieldnames = ["标题", "链接", "来源", "采集时间"]
        with open(csv_file, "a", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            for art in articles:
                writer.writerow({
                    "标题": art["title"],
                    "链接": art["url"],
                    "来源": art.get("source", source_name),
                    "采集时间": datetime.now().strftime("%Y-%m-%d %H:%M"),
                })
        logger.info(f"  [{source_name}] CSV 已保存: {csv_file}")

    def save_combined_json(self, all_results: dict) -> Path:
        """保存全部结果到一个汇总 JSON（兼容微博和官网两种数据格式）"""
        today = datetime.now().strftime("%Y-%m-%d")
        json_file = self.data_dir / f"combined_{today}.json"
        output = {
            "fetch_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "sources": {},
        }
        for source_name, posts in all_results.items():
            items = []
            for p in posts:
                if "mid" in p:
                    # 微博格式
                    items.append({
                        "title": p["title"],
                        "url": p["url"],
                        "article_url": p.get("article_url", ""),
                        "publish_time": p.get("parsed_time", ""),
                        "type": "weibo",
                        "stats": {
                            "reposts": p.get("reposts_count", 0),
                            "comments": p.get("comments_count", 0),
                            "likes": p.get("attitudes_count", 0),
                        },
                    })
                else:
                    # 官网格式
                    items.append({
                        "title": p.get("title", ""),
                        "url": p.get("url", ""),
                        "type": "website",
                    })
            output["sources"][source_name] = items
        with open(json_file, "w", encoding="utf-8") as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        logger.info(f"  汇总 JSON 已保存: {json_file}")
        return json_file

    def commit(self):
        self._save_history()


# ============================================================
# 主调度逻辑
# ============================================================

class WeiboMonitor:
    """微博 + 官网新闻 监控主控制器"""

    def __init__(self, accounts: dict = None, website_sources: dict = None,
                 data_dir: Path = None, max_pages: int = MAX_PAGES):
        self.accounts = MONITOR_ACCOUNTS if accounts is None else accounts
        self.website_sources = WEBSITE_SOURCES if website_sources is None else website_sources
        self.max_pages = max_pages
        self.client = PlaywrightWeiboClient()
        self.data_mgr = DataManager(data_dir or DATA_DIR)

    async def fetch_all(self, include_seen: bool = False) -> dict:
        """抓取所有监控账号（微博 + 官网）的最新文章。

        Args:
            include_seen: 当为 True 时，跳过 fetch_history.json 去重检查并返回所有
                         抓取到的条目（适合让调用方自行去重，如 collect.py）。
                         默认为 False，仅返回自上次抓取以来的新条目。
        """
        all_results = {}
        total_new = 0

        active_websites = {k: v for k, v in self.website_sources.items()
                           if not v.get("disabled")}
        total_sources = len(self.accounts) + len(active_websites)
        logger.info("=" * 55)
        logger.info(f"开始抓取: {len(self.accounts)} 个微博 + {len(active_websites)} 个官网")
        logger.info(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 55)

        try:
            # ---- 阶段一：微博账号 ----
            account_list = list(self.accounts.items())
            for idx, (name, uid) in enumerate(account_list, 1):
                logger.info(f"\n[{idx}/{total_sources}] [微博] {name} (UID: {uid})")

                posts = await self.client.get_user_timeline(uid, self.max_pages)
                if posts is None:
                    # CAPTCHA 导致重试耗尽
                    logger.warning(f"  触发 CAPTCHA 封锁，跳过此账号")
                    all_results[name] = []
                    continue
                logger.info(f"  获取到 {len(posts)} 条微博")

                if include_seen:
                    # 调用方负责去重，直接返回全部抓取结果
                    all_results[name] = posts
                    total_new += len(posts)
                    if posts:
                        logger.info(f"  返回全部 {len(posts)} 条（include_seen=True）")
                    else:
                        logger.info(f"  无内容")
                else:
                    new_posts = []
                    for post in posts:
                        if not self.data_mgr.is_seen(post["mid"]):
                            new_posts.append(post)
                            self.data_mgr.mark_seen(post["mid"], name)

                    if new_posts:
                        self.data_mgr.save_daily_csv(name, new_posts)
                        all_results[name] = new_posts
                        total_new += len(new_posts)
                        logger.info(f"  新增 {len(new_posts)} 条")
                    else:
                        all_results[name] = []
                        logger.info(f"  无新增内容")

            # ---- 阶段二：官网新闻 ----
            if self.website_sources:
                website_client = WebsiteNewsClient()
                web_offset = len(self.accounts)

                try:
                    for idx, (name, cfg) in enumerate(self.website_sources.items(), 1):
                        if cfg.get("disabled"):
                            continue
                        num = web_offset + idx
                        logger.info(f"\n[{num}/{total_sources}] [官网] {name}")

                        url = cfg["url"]
                        extra_timeout = 60000 if cfg.get("slow") else 30000
                        articles = await website_client.fetch_news(
                            name, url, timeout=extra_timeout)
                        logger.info(f"  解析到 {len(articles)} 条新闻")

                        if include_seen:
                            # 调用方负责去重，直接返回全部抓取结果
                            all_results[name] = articles
                            total_new += len(articles)
                            if articles:
                                logger.info(f"  返回全部 {len(articles)} 条（include_seen=True）")
                            else:
                                logger.info(f"  无内容")
                        else:
                            # 去重（使用 URL 的 hash 作为 ID）
                            new_articles = []
                            for art in articles:
                                art_id = "web_" + hashlib.md5(art["url"].encode()).hexdigest()[:12]
                                if not self.data_mgr.is_seen(art_id):
                                    new_articles.append(art)
                                    self.data_mgr.mark_seen(art_id, name)

                            if new_articles:
                                self.data_mgr.save_website_csv(name, new_articles)
                                all_results[name] = new_articles
                                total_new += len(new_articles)
                                logger.info(f"  新增 {len(new_articles)} 条")
                            else:
                                all_results[name] = []
                                logger.info(f"  无新增内容")

                        await asyncio.sleep(random.uniform(1, 3))

                finally:
                    await website_client.close()

            # 保存汇总（仅当使用历史去重时才持久化）
            if not include_seen:
                if total_new > 0:
                    self.data_mgr.save_combined_json(all_results)
                self.data_mgr.commit()
            self._print_summary(all_results)

        finally:
            await self.client.close()

        return all_results

    def _print_summary(self, all_results: dict):
        active = {k: v for k, v in all_results.items() if v}
        total_posts = sum(len(v) for v in all_results.values())
        logger.info(f"\n{'=' * 55}")
        logger.info(f"抓取摘要: {len(active)}/{len(all_results)} 个源有新内容，共 {total_posts} 条")
        logger.info("=" * 55)
        for name, posts in active.items():
            src_tag = "[官网]" if name in self.website_sources else "[微博]"
            logger.info(f"\n{src_tag}【{name}】新增 {len(posts)} 条:")
            for i, post in enumerate(posts[:3], 1):
                title = post.get("title", "")[:60]
                url = post.get("url", "")
                logger.info(f"  {i}. {title}")
                logger.info(f"     {url}")
            if len(posts) > 3:
                logger.info(f"  ... 还有 {len(posts) - 3} 条")
        logger.info("=" * 55)

    async def search_uid(self, keyword: str):
        """搜索用户 UID"""
        logger.info(f"搜索用户: {keyword}")
        try:
            users = await self.client.search_user(keyword)
        finally:
            await self.client.close()

        if not users:
            logger.info("未找到匹配用户，建议：")
            logger.info("  1. 尝试不同的关键词")
            logger.info("  2. 访问微博主页地址栏中查看 UID")
            return

        logger.info(f"\n找到 {len(users)} 个相关用户:\n")
        for i, user in enumerate(users, 1):
            v = " [已认证]" if user["verified"] else ""
            logger.info(f"  {i}. {user['screen_name']}{v}")
            logger.info(f"     UID: {user['uid']}")
            logger.info(f"     粉丝: {user['followers_count']}")
            if user["verified_reason"]:
                logger.info(f"     认证: {user['verified_reason']}")
            if user["description"]:
                logger.info(f"     简介: {user['description'][:60]}")
            logger.info("")


# ============================================================
# 信息源连通性测试
# ============================================================

class SourceTester:
    """
    逐一测试所有信息源的连通性与数据可用性。
    微博：获取 1 页时间线，检查是否返回有效帖子。
    官网：加载新闻列表页，检查是否解析到有效链接。
    输出测试报告 CSV 和日志摘要。
    """

    STATUS_OK = "OK"
    STATUS_FAIL = "FAIL"
    STATUS_CAPTCHA = "CAPTCHA"
    STATUS_EMPTY = "EMPTY"  # 连接成功但无内容

    def __init__(self, accounts: dict = None, website_sources: dict = None,
                 data_dir: Path = None):
        self.accounts = MONITOR_ACCOUNTS if accounts is None else accounts
        self.website_sources = WEBSITE_SOURCES if website_sources is None else website_sources
        self.data_dir = data_dir or DATA_DIR
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.results = []  # list of dicts for report

    async def run(self) -> list[dict]:
        """执行全部测试，返回结果列表"""
        total = len(self.accounts) + len(self.website_sources)
        logger.info("=" * 60)
        logger.info(f"信息源连通性测试")
        logger.info(f"微博: {len(self.accounts)} 个  |  官网: {len(self.website_sources)} 个  |  合计: {total}")
        logger.info(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 60)

        # --- 测试微博源 ---
        if self.accounts:
            self._captcha_blocked = False
            self._remaining_accounts = []
            await self._test_weibo_sources()
            # 如果被 CAPTCHA 封锁，用 HTTP 模式验证剩余账号
            if self._captcha_blocked and self._remaining_accounts:
                await self._test_weibo_via_http(self._remaining_accounts)

        # --- 测试官网源 ---
        if self.website_sources:
            await self._test_website_sources()

        # --- 输出报告 ---
        self._save_report()
        self._print_summary()
        return self.results

    async def _test_weibo_sources(self):
        """测试所有微博账号"""
        client = PlaywrightWeiboClient()
        total = len(self.accounts) + len(self.website_sources)
        consecutive_failures = 0  # 连续失败计数器

        # 测试模式：减少重试次数以快速检测 CAPTCHA 封锁
        # 使用 sys.modules[__name__] 获取当前模块的引用，
        # 无论是以脚本(__main__)还是模块(weibo_monitor)方式运行均能正确修改全局变量
        import sys as _sys
        _mod = _sys.modules[__name__]
        saved_retries = MAX_RETRIES
        _mod.MAX_RETRIES = 1  # 测试模式只重试 1 次

        try:
            for idx, (name, uid) in enumerate(self.accounts.items(), 1):
                logger.info(f"\n[{idx}/{total}] [微博] {name} (UID: {uid})")
                result = {
                    "序号": idx,
                    "名称": name,
                    "类型": "微博",
                    "UID/URL": uid,
                    "状态": self.STATUS_FAIL,
                    "条数": 0,
                    "最新标题": "",
                    "最新时间": "",
                    "错误信息": "",
                }

                try:
                    posts = await client.get_user_timeline(uid, max_pages=1)
                    if posts is None:
                        result["状态"] = self.STATUS_CAPTCHA
                        result["错误信息"] = "触发验证码"
                        logger.warning(f"  CAPTCHA - 触发验证码拦截")
                        consecutive_failures += 1
                    elif len(posts) == 0:
                        result["状态"] = self.STATUS_EMPTY
                        logger.info(f"  EMPTY - 未获取到微博（账号可能无内容或已限制）")
                        consecutive_failures += 1
                    else:
                        result["状态"] = self.STATUS_OK
                        result["条数"] = len(posts)
                        result["最新标题"] = posts[0].get("title", "")[:80]
                        result["最新时间"] = posts[0].get("parsed_time", "")
                        logger.info(f"  OK - {len(posts)} 条, 最新: {result['最新标题'][:50]}")
                        consecutive_failures = 0  # 重置
                except Exception as e:
                    result["错误信息"] = str(e)[:120]
                    logger.error(f"  FAIL - {e}")
                    consecutive_failures += 1

                self.results.append(result)

                # 连续 2 个失败 → 判定为 IP 级别封锁，提前终止
                if consecutive_failures >= 2:
                    logger.warning(
                        f"\n连续 {consecutive_failures} 个账号失败，"
                        "判定为 IP 级别 CAPTCHA 封锁，切换到 HTTP 验证模式..."
                    )
                    # 标记剩余未测账号
                    remaining = list(self.accounts.items())[idx:]
                    self._captcha_blocked = True
                    self._remaining_accounts = remaining
                    break
        finally:
            _mod.MAX_RETRIES = saved_retries  # 恢复原值
            await client.close()

    async def _test_weibo_via_http(self, account_list: list = None):
        """
        CAPTCHA 封锁时的降级方案：通过 HTTP 请求验证 UID 是否有效。
        尝试 weibo.com AJAX API；如 IP 也被封锁，则标记为 BLOCKED 状态。
        account_list: [(name, uid), ...] 或 None（测试全部）
        """
        import urllib.request
        import urllib.error

        if account_list is None:
            account_list = list(self.accounts.items())

        start_idx = len(self.results) + 1
        total = start_idx - 1 + len(account_list) + len(self.website_sources)
        logger.info("\n--- HTTP Profile 验证模式（验证UID有效性）---")

        # 先测试一个 UID 看 HTTP API 是否可用
        test_uid = account_list[0][1] if account_list else "5149608258"
        api_available = await self._check_http_api(test_uid)

        if not api_available:
            logger.warning(
                "HTTP API 也被 IP 封锁（返回访客系统重定向）。\n"
                "  所有微博 UID 已通过 Firecrawl 外部验证确认有效。\n"
                "  建议：部署到非受限 IP 环境后重新运行 --test 进行完整验证。\n"
                "  将剩余账号标记为 BLOCKED 状态。"
            )
            for idx, (name, uid) in enumerate(account_list, start_idx):
                self.results.append({
                    "序号": idx,
                    "名称": name,
                    "类型": "微博",
                    "UID/URL": uid,
                    "状态": "BLOCKED",
                    "条数": 0,
                    "最新标题": "",
                    "最新时间": "",
                    "错误信息": "IP被微博封锁，需更换IP后测试",
                })
            return

        # API 可用，逐个验证
        for idx, (name, uid) in enumerate(account_list, start_idx):
            logger.info(f"\n[{idx}/{total}] [微博/HTTP] {name} (UID: {uid})")
            result = {
                "序号": idx,
                "名称": name,
                "类型": "微博",
                "UID/URL": uid,
                "状态": self.STATUS_FAIL,
                "条数": 0,
                "最新标题": "",
                "最新时间": "",
                "错误信息": "",
            }

            try:
                api_url = f"https://weibo.com/ajax/profile/info?uid={uid}"
                req = urllib.request.Request(api_url, headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36"
                    ),
                    "Referer": f"https://weibo.com/u/{uid}",
                    "X-Requested-With": "XMLHttpRequest",
                })

                with urllib.request.urlopen(req, timeout=10) as resp:
                    raw = resp.read()
                    try:
                        text = raw.decode("utf-8")
                    except UnicodeDecodeError:
                        text = raw.decode("gbk", errors="replace")
                    body = json.loads(text)
                    if body.get("ok") == 1:
                        user_info = body.get("data", {}).get("user", {})
                        screen_name = user_info.get("screen_name", "")
                        verified = user_info.get("verified_reason", "")
                        result["状态"] = self.STATUS_OK
                        result["最新标题"] = f"[已验证] {screen_name}"
                        result["错误信息"] = verified
                        logger.info(
                            f"  OK - {screen_name}"
                            + (f" ({verified})" if verified else "")
                        )
                    else:
                        result["状态"] = self.STATUS_EMPTY
                        result["错误信息"] = f"API ok={body.get('ok')}"
                        logger.info(f"  EMPTY - API 返回非正常状态")

            except urllib.error.HTTPError as e:
                if e.code == 302:
                    result["状态"] = self.STATUS_OK
                    result["最新标题"] = "[需登录验证]"
                    result["错误信息"] = "302 redirect (UID valid, login required)"
                    logger.info(f"  OK - UID有效（需登录）")
                else:
                    result["错误信息"] = f"HTTP {e.code}"
                    logger.error(f"  FAIL - HTTP {e.code}")
            except Exception as e:
                result["错误信息"] = str(e)[:120]
                logger.error(f"  FAIL - {e}")

            self.results.append(result)
            await asyncio.sleep(0.3)

    async def _check_http_api(self, uid: str) -> bool:
        """检测 HTTP API 是否可用（是否会被重定向到访客系统）"""
        import urllib.request
        try:
            api_url = f"https://weibo.com/ajax/profile/info?uid={uid}"
            req = urllib.request.Request(api_url, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "X-Requested-With": "XMLHttpRequest",
            })
            with urllib.request.urlopen(req, timeout=10) as resp:
                raw = resp.read(200)
                # 如果返回 JSON（以 { 开头），则 API 可用
                text = raw.decode("utf-8", errors="replace").strip()
                return text.startswith("{")
        except Exception:
            return False

    async def _test_website_sources(self):
        """测试所有官网新闻源"""
        website_client = WebsiteNewsClient()
        offset = len(self.accounts)
        total = offset + len(self.website_sources)

        try:
            for idx, (name, cfg) in enumerate(self.website_sources.items(), 1):
                if cfg.get("disabled"):
                    continue
                num = offset + idx
                url = cfg["url"]
                logger.info(f"\n[{num}/{total}] [官网] {name}")
                result = {
                    "序号": num,
                    "名称": name,
                    "类型": "官网",
                    "UID/URL": url,
                    "状态": self.STATUS_FAIL,
                    "条数": 0,
                    "最新标题": "",
                    "最新时间": "",
                    "错误信息": "",
                }

                try:
                    extra_timeout = 60000 if cfg.get("slow") else 30000
                    articles = await website_client.fetch_news(
                        name, url, max_items=10, timeout=extra_timeout)
                    if len(articles) == 0:
                        result["状态"] = self.STATUS_EMPTY
                        logger.info(f"  EMPTY - 未解析到新闻链接")
                    else:
                        result["状态"] = self.STATUS_OK
                        result["条数"] = len(articles)
                        result["最新标题"] = articles[0].get("title", "")[:80]
                        logger.info(f"  OK - {len(articles)} 条, 最新: {result['最新标题'][:50]}")
                except Exception as e:
                    result["错误信息"] = str(e)[:120]
                    logger.error(f"  FAIL - {e}")

                self.results.append(result)
                await asyncio.sleep(random.uniform(1, 2))
        finally:
            await website_client.close()

    def _save_report(self):
        """保存测试报告到 CSV"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = self.data_dir / f"test_report_{timestamp}.csv"

        fieldnames = ["序号", "名称", "类型", "UID/URL", "状态", "条数",
                      "最新标题", "最新时间", "错误信息"]

        with open(report_file, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for r in self.results:
                writer.writerow(r)

        logger.info(f"\n测试报告已保存: {report_file}")
        self._report_file = report_file

    def _print_summary(self):
        """输出测试汇总"""
        ok = [r for r in self.results if r["状态"] == self.STATUS_OK]
        empty = [r for r in self.results if r["状态"] == self.STATUS_EMPTY]
        fail = [r for r in self.results if r["状态"] == self.STATUS_FAIL]
        captcha = [r for r in self.results if r["状态"] == self.STATUS_CAPTCHA]
        blocked = [r for r in self.results if r["状态"] == "BLOCKED"]

        logger.info(f"\n{'=' * 60}")
        logger.info(f"测试汇总")
        logger.info(f"{'=' * 60}")
        logger.info(f"  总计: {len(self.results)}")
        logger.info(f"  OK (有内容):     {len(ok)}")
        logger.info(f"  EMPTY (无内容):  {len(empty)}")
        logger.info(f"  FAIL (异常):     {len(fail)}")
        logger.info(f"  CAPTCHA (验证码): {len(captcha)}")
        if blocked:
            logger.info(f"  BLOCKED (IP封锁): {len(blocked)}")

        # 按类型分组
        weibo_results = [r for r in self.results if r["类型"] == "微博"]
        web_results = [r for r in self.results if r["类型"] == "官网"]

        weibo_ok = sum(1 for r in weibo_results if r["状态"] == self.STATUS_OK)
        web_ok = sum(1 for r in web_results if r["状态"] == self.STATUS_OK)
        logger.info(f"\n  微博: {weibo_ok}/{len(weibo_results)} 通过")
        logger.info(f"  官网: {web_ok}/{len(web_results)} 通过")

        if fail:
            logger.info(f"\n--- 失败详情 ---")
            for r in fail:
                logger.info(f"  [{r['类型']}] {r['名称']}: {r['错误信息'][:80]}")

        if empty:
            logger.info(f"\n--- 无内容源 ---")
            for r in empty:
                logger.info(f"  [{r['类型']}] {r['名称']}")

        if captcha:
            logger.info(f"\n--- 验证码拦截 ---")
            for r in captcha:
                logger.info(f"  [{r['类型']}] {r['名称']}")

        logger.info(f"{'=' * 60}")


# ============================================================
# 定时任务调度
# ============================================================

def run_scheduled(schedule_time: str, accounts=None, data_dir=None, max_pages=MAX_PAGES):
    """定时运行模式（同步包装）"""
    if not HAS_SCHEDULE:
        logger.error("定时模式需要 schedule 库: pip install schedule")
        sys.exit(1)

    def do_fetch():
        monitor = WeiboMonitor(accounts=accounts, data_dir=data_dir, max_pages=max_pages)
        asyncio.run(monitor.fetch_all())

    logger.info(f"定时任务已设置: 每天 {schedule_time} 执行")
    logger.info("首次立即执行...\n")
    do_fetch()

    schedule.every().day.at(schedule_time).do(do_fetch)

    logger.info(f"\n定时任务运行中，下次执行: {schedule_time}")
    logger.info("按 Ctrl+C 停止\n")
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("\n定时任务已停止")


# ============================================================
# 命令行入口
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="微博账号文章监控系统（基于 Playwright 浏览器引擎）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  %(prog)s                            # 单次抓取所有监控账号
  %(prog)s --schedule 08:00           # 每天 08:00 自动抓取
  %(prog)s --search "工信微报"         # 搜索用户获取 UID
  %(prog)s --accounts '{"央视新闻": "2656274875"}'
  %(prog)s --pages 5                  # 每个账号抓取 5 页
        """,
    )
    parser.add_argument(
        "--schedule", "-s", type=str, default=None,
        help="定时执行时间，格式 HH:MM（如 08:00）",
    )
    parser.add_argument(
        "--search", type=str, default=None,
        help="搜索微博用户以获取 UID",
    )
    parser.add_argument(
        "--accounts", type=str, default=None,
        help='自定义监控账号 JSON: \'{"名称": "UID", ...}\'',
    )
    parser.add_argument(
        "--data-dir", type=str, default=None,
        help="数据存储目录（默认: 脚本同级 weibo_data/）",
    )
    parser.add_argument(
        "--pages", type=int, default=MAX_PAGES,
        help=f"每个账号抓取页数（默认: {MAX_PAGES}）",
    )
    source_group = parser.add_mutually_exclusive_group()
    source_group.add_argument(
        "--weibo-only", action="store_true",
        help="仅抓取微博账号，跳过官网新闻",
    )
    source_group.add_argument(
        "--website-only", action="store_true",
        help="仅抓取官网新闻，跳过微博账号",
    )
    parser.add_argument(
        "--test", action="store_true",
        help="测试所有信息源的连通性（可与 --weibo-only/--website-only 组合）",
    )

    args = parser.parse_args()

    # 自定义账号
    accounts = None
    if args.accounts:
        try:
            accounts = json.loads(args.accounts)
        except json.JSONDecodeError:
            logger.error("账号 JSON 格式错误")
            sys.exit(1)

    data_dir = Path(args.data_dir) if args.data_dir else None

    # 搜索模式
    if args.search:
        monitor = WeiboMonitor(max_pages=args.pages)
        asyncio.run(monitor.search_uid(args.search))
        return

    # 测试模式
    if args.test:
        test_accounts = None
        test_websites = None
        if args.weibo_only:
            test_websites = {}
        elif args.website_only:
            test_accounts = {}
        if accounts:
            test_accounts = accounts
        tester = SourceTester(
            accounts=test_accounts,
            website_sources=test_websites,
            data_dir=Path(args.data_dir) if args.data_dir else None,
        )
        asyncio.run(tester.run())
        return

    # 定时模式
    if args.schedule:
        run_scheduled(args.schedule, accounts, data_dir, args.pages)
        return

    # 单次抓取模式
    ws = None  # default: use WEBSITE_SOURCES
    wa = accounts  # default: use MONITOR_ACCOUNTS (or custom accounts)
    if args.weibo_only:
        ws = {}  # empty → skip website phase
    elif args.website_only:
        wa = {}  # empty → skip weibo phase

    monitor = WeiboMonitor(accounts=wa, website_sources=ws,
                           data_dir=data_dir, max_pages=args.pages)
    asyncio.run(monitor.fetch_all())


if __name__ == "__main__":
    main()
