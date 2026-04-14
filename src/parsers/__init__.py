"""
parsers — 各信息来源的解析器包。

每个模块对应一个数据来源，对外暴露一个 parse_*(config, now) 函数，
签名统一为::

    def parse_xxx(config: dict, now: datetime) -> List[Item]:
        ...

新增信源时，在此目录下创建新模块，并在本 __init__.py 中导入即可。

目前已实现的解析器
------------------
miit      工业和信息化部官网   parse_miit_home
gov       中国政府网           parse_gov_home, parse_gov_rss
qqnews    腾讯新闻搜索         parse_qqnews_search
weibo     微博 / 官网监控      parse_weibo_monitor_sources
"""

from parsers.gov import parse_gov_home, parse_gov_rss
from parsers.miit import parse_miit_home
from parsers.qqnews import parse_qqnews_search
from parsers.weibo import parse_weibo_monitor_sources

__all__ = [
    "parse_miit_home",
    "parse_gov_home",
    "parse_gov_rss",
    "parse_qqnews_search",
    "parse_weibo_monitor_sources",
]
