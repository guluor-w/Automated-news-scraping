"""
共享数据模型与常量。

所有解析器和工具函数均从此模块导入 Item、SG_TZ 等公共定义，
避免在多个文件中重复声明。
"""

import re
from dataclasses import dataclass
from datetime import timedelta, timezone
from typing import Optional

# 时区：UTC+8（Asia/Shanghai / Asia/Singapore）
SG_TZ = timezone(timedelta(hours=8))

# 仅用于工业和信息化部官网和腾讯新闻-工信微报的专属关键词
# 其他所有信源在过滤时必须排除这些关键词
MIIT_ONLY_KEYWORDS = frozenset({"印发", "体系建设"})

# 央企信源需要排除的关键词（央企信源不使用这些词作为匹配关键词）
SOE_EXCLUDED_KEYWORDS = frozenset({"装备"})

# 通用 HTTP 请求头 User-Agent
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# 日期正则模式列表（按优先级排列）
DATE_PATTERNS = [
    # 2026-01-16 或 2026/01/16
    re.compile(r"(\d{4})[-/](\d{1,2})[-/](\d{1,2})"),
    # 2026年1月16日
    re.compile(r"(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日"),
    # 2026.01.16
    re.compile(r"(\d{4})\.(\d{1,2})\.(\d{1,2})"),
]


@dataclass
class Item:
    """单条新闻条目，由各解析器生成，最终写入 CSV。"""
    title: str
    publisher: str
    url: str
    pub_date: Optional[str]   # YYYY-MM-DD 或 None
    source: str               # 来源标签，如 "工信部官网-时政要闻"
    fetched_at: str           # 查询时间，格式 "YYYY-MM-DD HH:MM:SS"
