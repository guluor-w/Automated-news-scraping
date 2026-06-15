"""
parsers — 各信息来源的解析器包。

每个模块对应一个数据来源，对外暴露一个 parse_*(config, now) 函数，
签名统一为::

    def parse_xxx(config: dict, now: datetime) -> List[Item]:
        ...

新增信源时，在此目录下创建新模块，并在本 __init__.py 中导入即可。

目前已实现的解析器
------------------
miit             工业和信息化部官网   parse_miit_home
gov              中国政府网           parse_gov_home, parse_gov_rss
ndrc             国家发展和改革委员会 parse_ndrc_home
most             科学技术部           parse_most_home
moe              教育部               parse_moe_news
miit_local       工信部地方主管部门   parse_miit_local
gov_local        各省级政府门户       parse_gov_local
sasac            国务院国有资产监督管理委员会 parse_sasac_home
nda              国家数据局           parse_nda_home
soe              中央企业             parse_soe
qqnews           腾讯新闻搜索         parse_qqnews_search
weibo            微博账号             parse_weibo
website_monitor  各部门官网           parse_website_monitor
"""

from datetime import datetime
from typing import List

from .gov import parse_gov_home, parse_gov_rss
from .miit import parse_miit_home
from .ndrc import parse_ndrc_home
from .most import parse_most_home
from .moe import parse_moe_news
from .miit_local import parse_miit_local, MIIT_LOCAL_SOURCES
from .gov_local import parse_gov_local, GOV_LOCAL_SOURCES
from .qqnews import parse_qqnews_search
from .sasac import parse_sasac_home
from .nda import parse_nda_home
from .soe import parse_soe, SOE_SOURCES
from .weibo import parse_weibo
from .website_monitor import parse_website_monitor, WEBSITE_SOURCES

from models import Item


def parse_weibo_monitor_sources(config: dict, now: datetime) -> List[Item]:
    """
    向后兼容包装器：同时调用 parse_weibo 和 parse_website_monitor 并合并结果。

    新代码应直接调用 parse_weibo / parse_website_monitor。
    """
    items: List[Item] = []
    items.extend(parse_weibo(config, now))
    items.extend(parse_website_monitor(config, now))
    return items


__all__ = [
    "parse_miit_home",
    "parse_gov_home",
    "parse_gov_rss",
    "parse_ndrc_home",
    "parse_most_home",
    "parse_moe_news",
    "parse_miit_local",
    "parse_gov_local",
    "parse_qqnews_search",
    "parse_sasac_home",
    "parse_nda_home",
    "parse_soe",
    "parse_weibo",
    "parse_website_monitor",
    # 多源信源的来源清单（供二次检验等使用）
    "SOE_SOURCES",
    "MIIT_LOCAL_SOURCES",
    "GOV_LOCAL_SOURCES",
    "WEBSITE_SOURCES",
    # 向后兼容
    "parse_weibo_monitor_sources",
]
