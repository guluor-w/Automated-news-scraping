# Automated-news-scraping

自动化轮询人工智能、制造业、工业化相关新闻，并以 CSV / RSS 形式持久化存储。

---

## 当前数据来源

| 来源 | 类型 | 解析器文件 |
|------|------|------------|
| 工业和信息化部官网 https://www.miit.gov.cn/ | 官网首页 | `src/parsers/miit.py` |
| 中国政府网 https://www.gov.cn/ | 官网首页 | `src/parsers/gov.py` |
| 中国政府网最新政策 RSS | RSS Feed | `src/parsers/gov.py` |
| 腾讯新闻（政务公众号搜索，如：工信微报） | 搜索 API | `src/parsers/qqnews.py` |
| 微博账号监控（需 Playwright） | Weibo | `src/parsers/weibo.py` |
| 各部门官网监控（需 Playwright） | 官网 | `src/parsers/website_monitor.py` |

---

## 执行效果

`.github/workflows/weekly.yml` 每天两次自动运行 `python src/collect.py`，
从以上来源收集相关新闻，经比对去重后追增到 `docs/data/policy_news.csv`，
并按发布时间降序排序（时间缺失时按查询时间排）。

每次运行还会输出：
- `docs/data/added_count.txt` — 本次新增条数（仅记录最新一次）
- `docs/data/rss_full.xml` — 全部新闻 RSS 2.0 订阅
- `docs/data/rss_miit.xml` — 工信相关新闻 RSS 2.0 订阅

---

## 参数配置（config.yaml）

```yaml
keywords:           # 关键词列表，命中任意一个即保留
  - 智能
  - AI
  - ...

window_days: 15     # 时间窗口：近 N 天
hard_cap_days: 15   # 最早时间上限（过滤更早的新闻）

sources:
  miit_home:
    name: 工业和信息化部
    url: https://www.miit.gov.cn/

  gov_latest_policy_rss:
    name: 中国政府网
    rss: https://rsshub.app/gov/zhengce/zuixin

  gov_home:
    name: 中国政府网
    url: https://www.gov.cn/

  qqnews_search:
    name: 腾讯新闻
    url: https://i.news.qq.com/gw/pc_search/result
    queries:
      - 工信微报      # 可追加更多公众号名称
    max_pages: 5
    page_size: 20

weibo_monitor:
  enabled: true
  mode: weibo_only  # all | weibo_only | website_only
  max_pages: 1

resolve_pub_date: true
resolve_pub_date_cap: 30

output:
  csv_path: docs/data/policy_news.csv
```

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
│   └── rss_miit.xml             # 工信专项 RSS
└── src/
    ├── collect.py               # 主入口：读取配置、调用各解析器、写入输出
    ├── models.py                # 数据模型（Item）和公共常量
    ├── utils.py                 # 通用工具函数（日期解析、关键词匹配、HTTP 等）
    ├── storage.py               # 持久化：CSV 读写 + RSS 生成
    ├── weibo_monitor.py         # 微博/官网抓取核心（Playwright）
    ├── test_collect.py          # 单元测试
    └── parsers/                 # 各信源解析器（每个来源一个文件）
        ├── __init__.py          # 统一导出所有 parse_* 函数
        ├── miit.py              # 工信部官网
        ├── gov.py               # 中国政府网（首页 + RSS）
        ├── qqnews.py            # 腾讯新闻搜索
        ├── weibo.py             # 微博账号监控（调用 weibo_monitor.py）
        └── website_monitor.py   # 各部门官网监控（调用 weibo_monitor.py）
```

---

## 新增信息来源（开发指南）

### 方式一：腾讯新闻追加公众号

只需在 `config.yaml` 的 `sources.qqnews_search.queries` 列表中添加公众号名称，**无需修改代码**：

```yaml
qqnews_search:
  queries:
    - 工信微报
    - 微言教育   # ← 新增
    - 中国网信    # ← 新增
```

### 方式二：新增微博账号或官网

在 `src/weibo_monitor.py` 中维护两个字典：

```python
MONITOR_ACCOUNTS = {
    "工信微报": "5149608258",   # 账号名: 微博 UID
    "新账号名": "对应UID",      # ← 新增微博账号
}

WEBSITE_SOURCES = {
    "国家数据局": {
        "url": "https://www.nda.gov.cn/sjj/swdt/list/index_pc_1.html",
        "org": "国家数据局",
    },
    "新来源名": {"url": "列表页URL", "org": "机构名"},  # ← 新增官网
}
```

### 方式三：接入全新类型的信息来源

1. 在 `src/parsers/` 下新建文件，例如 `src/parsers/xinhua.py`。
2. 在文件中实现标准签名的入口函数：

   ```python
   from datetime import datetime
   from typing import List
   from models import Item
   from utils import http_get, keyword_hit, within_window, format_fetched_at

   def parse_xinhua(config: dict, now: datetime) -> List[Item]:
       """
       抓取新华社新闻并返回过滤后的 Item 列表。
       config 中可读取 config["sources"]["xinhua"] 的自定义字段。
       """
       ...
       return items
   ```

3. 在 `src/parsers/__init__.py` 中导出新函数：

   ```python
   from parsers.xinhua import parse_xinhua
   __all__ = [..., "parse_xinhua"]
   ```

4. 在 `src/collect.py` 的 `main()` 中调用：

   ```python
   from parsers import ..., parse_xinhua
   ...
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
python -m playwright install chromium  # 仅 weibo_monitor 需要

# 手动执行一次抓取
python src/collect.py

# 运行单元测试
python src/test_collect.py
```
