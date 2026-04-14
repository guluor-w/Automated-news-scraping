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
keywords:           # 关键词列表，命中任意一个即保留
  - 智能
  - AI
  - ...

window_days: 15     # 时间窗口：近 N 天内的新闻
hard_cap_days: 15   # 过滤时间上限

weibo_monitor:
  enabled: true
  mode: weibo_only  # all | weibo_only | website_only
  max_pages: 1      # 每个账号最多抓取的页数

output:
  csv_path: docs/data/policy_news.csv
```

---

## 输出文件

| 文件 | 说明 |
|------|------|
| `docs/data/policy_news.csv` | 新闻数据（持续追增，按发布时间降序） |
| `docs/data/added_count.txt` | 最近一次运行新增条数 |
| `docs/data/rss_full.xml` | 全量新闻 RSS 2.0 |
| `docs/data/rss_miit.xml` | 专项新闻 RSS 2.0 |

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
```

CI/CD 由 `.github/workflows/weekly.yml` 自动执行，每天运行两次。

---

技术实现细节及开发指南请参阅 [`src/DEVELOPERS.md`](src/DEVELOPERS.md)。
