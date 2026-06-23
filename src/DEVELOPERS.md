# 开发指南

本文档面向需要维护或扩展本项目的开发者，包含项目结构说明和新增信息来源的操作步骤。

---

## 项目结构

```
.
├── config.yaml                  # 关键词、时间窗口、来源、别名等全部配置
├── requirements.txt
├── .github/workflows/weekly.yml # CI/CD：cron "0 4,17 * * *"（UTC），每天两次
├── docs/data/
│   ├── policy_news.csv          # 新闻数据（持续追增，UTF-8-SIG）
│   ├── added_count.txt          # 最近一次新增条数（供 CI 判断是否提交）
│   ├── rss_full.xml             # 全量 RSS 2.0
│   └── rss_miit.xml             # 来源名称包含"工信"的子集 RSS 2.0
├── .cache/                      # 本地缓存目录（已在 .gitignore 中排除）
│   └── detail_titles.json       # 详情页标题二次确认结果缓存
└── src/
    ├── collect.py               # 主入口：加载配置、调用各 parser、二次检验、去重、写 CSV/RSS
    ├── models.py                # 数据模型（Item）和公共常量（SG_TZ 等）
    ├── utils.py                 # 通用工具（日期解析、关键词匹配、HTTP、域名比对、详情页标题提取等）
    ├── storage.py               # 持久化：CSV 读写 + RSS 生成 + 去重合并
    ├── test_collect.py          # collect/storage 相关单元测试
    ├── test_parsers.py          # 各 parser 单元测试
    ├── verify_existing_csv.py   # 对存量 CSV 回溯执行外站跳转二次检验
    ├── DEVELOPERS.md            # 本文件
    └── parsers/                 # 各信源解析器（每个来源一个文件）
        ├── __init__.py          # 统一导出 parse_* 与各 *_SOURCES 常量
        ├── miit.py              # 工信部官网
        ├── miit_local.py        # 地方工信门户（多源）
        ├── gov.py               # 中国政府网（首页 + RSS）
        ├── gov_local.py         # 地方政府门户（多源）
        ├── ndrc.py              # 国家发展改革委
        ├── most.py              # 科学技术部
        ├── moe.py               # 教育部
        ├── sasac.py             # 国资委
        ├── nda.py               # 国家数据局
        ├── soe.py               # 中央企业（多源）
        ├── qqnews.py            # 腾讯新闻关键词搜索 API
        ├── weibo.py             # 微博账号监控（Playwright）
        └── website_monitor.py   # 多官网轮询监控（Playwright）
```

各解析器均暴露模块级 `_fetch_*_raw()` 协程作为内部 fetch 函数，可在测试中直接 patch，无需 mock 外部模块。

---

## 当前数据来源

| 解析器 | 类型 | 配置节点 |
|--------|------|----------|
| `parsers/miit.py` | 官网首页 | `sources.miit_home` |
| `parsers/miit_local.py` | 多省工信门户 | `sources.miit_local` |
| `parsers/gov.py` | 官网首页 + RSS | `sources.gov_home` / `sources.gov_latest_policy_rss` |
| `parsers/gov_local.py` | 多省政府门户 | `sources.gov_local` |
| `parsers/ndrc.py` | 官网首页 | `sources.ndrc_home` |
| `parsers/most.py` | 官网首页 | `sources.most_home` |
| `parsers/moe.py` | 官网新闻列表 | `sources.moe_news` |
| `parsers/sasac.py` | 官网首页 | `sources.sasac_home` |
| `parsers/nda.py` | 官网首页 | `sources.nda_home` |
| `parsers/soe.py` | 中央企业（多源） | `sources.soe` |
| `parsers/qqnews.py` | 新闻平台搜索 API | `sources.qqnews_search` |
| `parsers/weibo.py` | 微博账号（需 Playwright） | `weibo_monitor` |
| `parsers/website_monitor.py` | 多官网轮询（需 Playwright） | `weibo_monitor` |

---

## 标题质量保障

部分政务/央企门户在列表页同时渲染"标题 + 摘要片段 + 日期后缀"，直接取链接文本会得到混合串。本项目通过**三层机制**逐级净化，确保入库标题尽量接近源站官方标题。

### 第一层：列表页结构识别（多源入口）

`parsers/miit_local.py`、`parsers/soe.py`、`parsers/gov_local.py` 的源定义支持 `urls` 列表与 `exclude_paths` 字段：

```python
{
  "name": "示例机构",
  "urls": [
    "https://example.com/news/",          # 新闻动态
    "https://example.com/notice/",        # 通知公告
  ],
  "exclude_paths": ["/gzwdt/", "/sasac.gov.cn/"],  # 过滤上级单位等无关板块
}
```

对应解析函数（如 `_scrape_soe_site`）会逐 URL 抓取并合并；遇到字符串型 `url` 字段自动兼容旧配置。

### 第二层：文本清洗（`_clean_title`）

`parsers/miit_local.py` 中的 `_clean_title` / `_strip_date_affixes` / `_post_process_title` 负责：

- 剥离末尾日期后缀（`YYYY-MM-DD` / `MM-DD` / `[YYYY年MM月DD日]` 等多种格式，支持可选空格）
- 去除 `[详细]` `[更多]` 等多余标记
- 识别正文摘要起始点（`……当一位` `。一是` `，背景是` 等连接词），截取摘要前的标题主体
- 拒绝纯导航文本（`_is_nav_text`）

### 第三层：详情页按需二次确认（新）

对于经第二层清洗后仍"可疑"的标题（典型场景：列表页 `<a>` 同时承载标题与摘要片段，结构上无法分离），按需访问详情页提取规范标题。

**判定条件**（`_is_suspicious_title`，位于 `parsers/miit_local.py`）：

- 长度 ≥ 60 字（普通新闻标题极少超过此长度）
- 中部出现典型摘要标志：标点后跟 ≥15 字连续叙述、`……`、`...`、`一是/二是` 等枚举

**字段提取优先级**（`_extract_title_from_html`，位于 `utils.py`）：

1. `<meta name="ArticleTitle">`（国内政务站普遍遵循 GB/T 23287）
2. `<h1>`
3. `<h2>`（部分央企/省厅站用作主标题）
4. `<title>`（自动反复剥离末尾的 `- 站名` / `_ 站名` 后缀）

**安全兜底**（`_refine_title_by_detail`）：

- 详情页请求失败、解析失败、或返回长度 ≥ 列表页清洗结果时 → 保留原标题
- 同一 URL 请求结果（含失败）写入 `.cache/detail_titles.json`，进程内单例 + 跨次复用，避免请求风暴

**调用位置**：三个解析器（`miit_local.py` / `soe.py` / `gov_local.py`）的主循环中，**在关键词与时间窗口过滤通过之后、`items.append` 之前**调用，确保被丢弃的条目不产生额外请求。

### HTTP 容错（`http_get`）

`utils.py:http_get` 在遇到 HTTPS 握手失败（部分省厅站证书链不全/SSL 协议过旧）时自动降级为 HTTP 重试一次，保障可达性。

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

6. 运行 `python src/test_collect.py` 与 `python src/test_parsers.py` 确认现有测试不受影响。

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
python src/test_parsers.py

# 对存量 CSV 回溯执行外站跳转二次检验（按 collect.judge_offsite 同一语义）
python src/verify_existing_csv.py
```
