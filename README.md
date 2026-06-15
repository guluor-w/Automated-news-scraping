# Automated-news-scraping

自动收集政策领域相关新闻，经关键词筛选和去重后，以 CSV 和 RSS 格式持久化存储。

---

## 功能概述

- **自动抓取**：定时从多类政策信息渠道（政府官网、社交媒体账号、新闻平台）采集新闻
- **关键词过滤**：仅保留命中预设关键词的条目（可在 `config.yaml` 中配置）
- **去重合并**：与历史记录比对，仅追加新增内容
- **多格式输出**：同步生成 CSV 数据文件和 RSS 订阅源

---

## 配置（config.yaml）

```yaml
keywords:                 # 关键词列表，命中任意一个即保留
  - 智能
  - AI
  - ...

sources:                  # 各信源配置块（URL / RSS / 查询关键词等）
  gov_home: { ... }
  ndrc_home: { ... }
  most_home: { ... }
  moe_news: { ... }
  miit_local: { enabled: true }       # 地方工信门户（多源）
  gov_local:  { enabled: true }       # 地方政府门户（多源）
  qqnews_search:                      # 腾讯新闻关键词搜索
    queries: [工信微报]
    max_pages: 5
    page_size: 20
  sasac_home: { ... }
  nda_home:   { ... }
  soe: { enabled: true, soe_timeout: 5 }   # 中央企业（多源）

weibo_monitor:            # 微博 / 官网监控（基于 Playwright）
  enabled: true
  mode: all               # all | weibo_only | website_only
  max_pages: 1            # 每个微博账号最多抓取的页数（mode=all/weibo_only 时有效）

resolve_pub_date: true    # 是否对缺失发布时间的条目回源抓取页面解析
resolve_pub_date_cap: 30  # 单次运行最多回源解析的条目数

output:
  csv_path: docs/data/policy_news.csv
```

> 完整字段请直接参阅仓库根目录的 [`config.yaml`](config.yaml)。

---

## 输出文件

| 文件 | 说明 |
|------|------|
| `docs/data/policy_news.csv` | 新闻数据（持续追增，按发布时间降序，UTF-8-SIG 编码） |
| `docs/data/added_count.txt` | 最近一次运行新增条数（供 CI 判断是否提交） |
| `docs/data/rss_full.xml` | 全量新闻 RSS 2.0 |
| `docs/data/rss_miit.xml` | 来源名称包含"工信"的子集 RSS 2.0 |

---

## 运行

```bash
# 安装依赖
pip install -r requirements.txt
python -m playwright install chromium

# 手动执行一次抓取
python src/collect.py

# 运行单元测试
python src/test_collect.py
python src/test_parsers.py
```

CI/CD 由 `.github/workflows/weekly.yml` 自动执行：cron `0 4,17 * * *`（UTC），即每天两次定时运行，也支持 `workflow_dispatch` 手动触发。仅当 `added_count.txt` 不为 `0` 时才会提交 `docs/data/` 下的 CSV 与 RSS。

---

技术实现细节及开发指南请参阅 [`src/DEVELOPERS.md`](src/DEVELOPERS.md)。
