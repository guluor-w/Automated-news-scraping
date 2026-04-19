"""
collect.py — 新闻抓取主入口。

职责
----
1. 读取 config.yaml。
2. 依次调用各信源解析器，汇总所有 Item。
3. 与已有 CSV 合并去重后写回磁盘。
4. 生成 RSS 2.0 feed 文件。
5. 将本次新增条数写入 added_count.txt（供 CI/CD 判断是否提交）。

运行方式
--------
    python src/collect.py

各解析器位于 src/parsers/ 目录，
数据读写逻辑位于 src/storage.py，
共享工具函数位于 src/utils.py，
数据模型位于 src/models.py。
"""

import sys
from datetime import datetime
from pathlib import Path
from typing import List

import yaml

try:
    from models import Item, SG_TZ
    from parsers import (
        parse_gov_home,
        parse_gov_rss,
        parse_miit_home,
        parse_ndrc_home,
        parse_most_home,
        parse_moe_news,
        parse_miit_local,
        parse_gov_local,
        parse_qqnews_search,
        parse_sasac_home,
        parse_nda_home,
        parse_soe,
        parse_weibo,
        parse_website_monitor,
        parse_weibo_monitor_sources,  # 向后兼容
    )
    from storage import dedup_merge, generate_rss, load_existing
except ImportError:
    # 仅在 src/ 目录未在模块搜索路径中时补救一次（脚本直接运行场景）
    _SRC_DIR = Path(__file__).resolve().parent
    if str(_SRC_DIR) not in sys.path:
        sys.path.insert(0, str(_SRC_DIR))
    from models import Item, SG_TZ  # type: ignore[no-redef]
    from parsers import (  # type: ignore[no-redef]
        parse_gov_home,
        parse_gov_rss,
        parse_miit_home,
        parse_ndrc_home,
        parse_most_home,
        parse_moe_news,
        parse_miit_local,
        parse_gov_local,
        parse_qqnews_search,
        parse_sasac_home,
        parse_nda_home,
        parse_soe,
        parse_weibo,
        parse_website_monitor,
        parse_weibo_monitor_sources,
    )
    from storage import dedup_merge, generate_rss, load_existing  # type: ignore[no-redef]

# ── 为向后兼容保留的再导出（test_collect.py 直接 import collect） ──────────────
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
    "parse_weibo_monitor_sources",
    "load_existing",
    "dedup_merge",
    "generate_rss",
    "load_config",
    "main",
]


def load_config(path: Path) -> dict:
    """读取并返回 config.yaml 配置字典。"""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    config_path = repo_root / "config.yaml"

    config = load_config(config_path)
    now = datetime.now(tz=SG_TZ)

    # ── 各信源抓取 ────────────────────────────────────────────────────────────
    all_items: List[Item] = []
    all_items.extend(parse_miit_home(config, now))
    all_items.extend(parse_gov_home(config, now))
    all_items.extend(parse_gov_rss(config, now))
    all_items.extend(parse_ndrc_home(config, now))
    all_items.extend(parse_most_home(config, now))
    all_items.extend(parse_moe_news(config, now))
    all_items.extend(parse_miit_local(config, now))
    all_items.extend(parse_gov_local(config, now))
    all_items.extend(parse_qqnews_search(config, now))
    all_items.extend(parse_sasac_home(config, now))
    all_items.extend(parse_nda_home(config, now))
    all_items.extend(parse_soe(config, now))
    all_items.extend(parse_weibo(config, now))
    all_items.extend(parse_website_monitor(config, now))

    # ── 去重合并并写 CSV ──────────────────────────────────────────────────────
    out_csv = repo_root / config["output"]["csv_path"]
    existing = load_existing(str(out_csv))
    merged, added = dedup_merge(existing, all_items)

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(str(out_csv), index=False, encoding="utf-8-sig")

    # ── 生成 RSS Feeds ────────────────────────────────────────────────────────
    rss_full_path = repo_root / "docs/data/rss_full.xml"
    rss_miit_path = repo_root / "docs/data/rss_miit.xml"

    generate_rss(
        merged,
        rss_full_path,
        title="新闻完整清单",
        description="新闻完整清单 RSS 订阅",
    )

    miit_df = merged[merged["来源"].str.contains("工信", na=False)]
    generate_rss(
        miit_df,
        rss_miit_path,
        title="工信新闻清单",
        description='来源包含"工信"的新闻 RSS 订阅',
    )

    # ── 记录新增条数（供 CI/CD 判断是否需要提交） ─────────────────────────────
    added_path = repo_root / "docs/data/added_count.txt"
    with open(added_path, "w", encoding="utf-8") as f:
        f.write(str(added))


if __name__ == "__main__":
    main()
