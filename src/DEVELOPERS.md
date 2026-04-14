# 开发指南

本文档面向需要维护或扩展本项目的开发者，包含项目结构说明和新增信息来源的操作步骤。

---

## 项目结构

```
.
├── config.yaml                  # 关键词、时间窗口、来源配置
├── requirements.txt
├── .github/workflows/weekly.yml # CI/CD：每天两次自动抓取并提交
├── docs/data/
│   ├── policy_news.csv          # 新闻数据（持续追增）
│   ├── added_count.txt          # 最近一次新增条数
│   ├── rss_full.xml             # 全量 RSS
│   └── rss_miit.xml             # 专项 RSS
└── src/
    ├── collect.py               # 主入口：读取配置、调用各解析器、写入输出
    ├── models.py                # 数据模型（Item）和公共常量
    ├── utils.py                 # 通用工具函数（日期解析、关键词匹配、HTTP 等）
    ├── storage.py               # 持久化：CSV 读写 + RSS 生成
    ├── test_collect.py          # 单元测试
    ├── DEVELOPERS.md            # 本文件
    └── parsers/                 # 各信源解析器（每个来源一个文件）
        ├── __init__.py          # 统一导出所有 parse_* 函数
        ├── miit.py              # 官网首页解析
        ├── gov.py               # 官网首页 + RSS 解析
        ├── qqnews.py            # 新闻平台搜索 API
        ├── weibo.py             # 社交媒体账号监控（Playwright）
        └── website_monitor.py   # 多官网轮询监控（Playwright）
```

各解析器均暴露模块级 `_fetch_*_raw()` 协程作为内部 fetch 函数，可在测试中直接 patch，无需 mock 外部模块。

---

## 当前数据来源

| 解析器 | 类型 | 配置节点 |
|--------|------|----------|
| `parsers/miit.py` | 官网首页 | `sources.miit_home` |
| `parsers/gov.py` | 官网首页 + RSS | `sources.gov_home` / `sources.gov_latest_policy_rss` |
| `parsers/qqnews.py` | 新闻平台搜索 API | `sources.qqnews_search` |
| `parsers/weibo.py` | 社交媒体账号（需 Playwright） | `weibo_monitor` |
| `parsers/website_monitor.py` | 多官网轮询（需 Playwright） | `weibo_monitor` |

---

## 新增信息来源

### 方式一：新闻平台追加公众号查询

只需在 `config.yaml` 的 `sources.qqnews_search.queries` 列表中添加公众号名称，**无需修改代码**：

```yaml
qqnews_search:
  queries:
    - 已有账号名
    - 新账号名   # ← 新增
```

### 方式二：新增社交媒体账号

在 `src/parsers/weibo.py` 的 `MONITOR_ACCOUNTS` 字典中添加条目：

```python
MONITOR_ACCOUNTS = {
    "已有账号": "已有UID",
    "新账号名": "对应UID",  # ← 新增；UID 可从主页 URL 获取：https://weibo.com/u/<UID>
}
```

### 方式三：新增官网监控

在 `src/parsers/website_monitor.py` 的 `WEBSITE_SOURCES` 字典中添加条目：

```python
WEBSITE_SOURCES = {
    "已有来源": {"url": "列表页URL", "org": "机构名"},
    "新来源名": {"url": "列表页URL", "org": "机构名"},  # ← 新增
}
```

### 方式四：接入全新类型的信息来源

1. 在 `src/parsers/` 下新建文件，例如 `src/parsers/xinhua.py`。
2. 实现标准签名的入口函数：

   ```python
   from datetime import datetime
   from typing import List
   from models import Item
   from utils import http_get, keyword_hit, within_window, format_fetched_at

   def parse_xinhua(config: dict, now: datetime) -> List[Item]:
       src = config["sources"]["xinhua"]
       ...
       return items
   ```

3. 在 `src/parsers/__init__.py` 中导出新函数：

   ```python
   from .xinhua import parse_xinhua
   __all__ = [..., "parse_xinhua"]
   ```

4. 在 `src/collect.py` 的 `main()` 中调用：

   ```python
   all_items.extend(parse_xinhua(config, now))
   ```

5. 在 `config.yaml` 的 `sources` 下添加对应配置（可选）：

   ```yaml
   sources:
     xinhua:
       name: 新华社
       url: https://www.xinhuanet.com/politics/
   ```

6. 运行 `python src/test_collect.py` 确认现有测试不受影响。

---

## 运行与测试

```bash
# 安装依赖
pip install -r requirements.txt
python -m playwright install chromium   # 微博/官网监控需要

# 手动执行一次抓取
python src/collect.py

# 运行单元测试
python src/test_collect.py
```
