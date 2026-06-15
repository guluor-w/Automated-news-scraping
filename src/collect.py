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
from typing import List, Optional

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
        SOE_SOURCES,
        MIIT_LOCAL_SOURCES,
        GOV_LOCAL_SOURCES,
        WEBSITE_SOURCES,
    )
    from storage import dedup_merge, generate_rss, load_existing
    from utils import extract_registered_domain, host_matches_allowed, extract_host
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
        SOE_SOURCES,
        MIIT_LOCAL_SOURCES,
        GOV_LOCAL_SOURCES,
        WEBSITE_SOURCES,
    )
    from storage import dedup_merge, generate_rss, load_existing  # type: ignore[no-redef]
    from utils import extract_registered_domain, host_matches_allowed, extract_host  # type: ignore[no-redef]

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
    "verify_offsite_redirect",
    "main",
]


# ─────────────────────────────────────────────────────────────────────────────
# 二次检验：丢弃跳转到外站的新闻
#
# 设计要点：
# 1. 仅按 URL 字符串做域名比对（零网络开销）；
# 2. 微博渠道（source 以"微博-"开头）按用户要求跳过本流程；
# 3. 单一域信源通过 _SOURCE_TAG_TO_CONFIG_KEYS 把 source 前缀映射到 config.yaml
#    中对应的 source 配置块，再从其 url 字段动态推断 allowed_domain（注册域）；
# 4. 多源信源（央企/地方政府/地方工信/官网）通过 _MULTI_SOURCE_INDEX 按
#    "完整 source 标签 → 该源官方 URL 的注册域"做精确反查，每条新闻都用其
#    所属源的官方域名做白名单比对；
# 5. host 无法解析（如纯文本或异常 URL）也保留，避免误伤；
# 6. 未命中任何映射的前缀（如新增 source 但未登记）默认保留，避免静默丢弃。
# ─────────────────────────────────────────────────────────────────────────────

# source 标签前缀 → config["sources"] 中的 key（用其 url 字段推断 allowed_domain）。
# 排序按前缀长度降序，避免短前缀抢占长前缀的匹配（如"国家数据局-"应优先于无此项）。
_SOURCE_TAG_TO_CONFIG_KEYS = (
    ("工信部官网-",      ["miit_home"]),
    ("中国政府网-",      ["gov_home"]),
    ("GOV-",            ["gov_home"]),
    ("发改委官网-",      ["ndrc_home"]),
    ("科技部官网-",      ["most_home"]),
    ("教育部官网-",      ["moe_news"]),
    ("国资委官网",       ["sasac_home"]),
    ("国家数据局-",      ["nda_home"]),
    ("腾讯新闻-",        ["qqnews_search"]),
)

# 不参与二次检验的来源前缀（按用户要求保留）。
_SECONDARY_CHECK_SKIP_PREFIXES = (
    "微博-",       # 用户明确要求跳过
)


def _build_multi_source_domain_index() -> dict:
    """构建多源信源的"完整 source 标签 → 注册域列表"精确反查表。

    覆盖：央企/地方政府/地方工信/官网 四类多源信源。每条新闻在生成时
    source 字段为 ``"<前缀>-<标识>"`` 形式（标识来自各 parser 的源清单），
    因此可按"标识"精确反查到该源的官方 URL，进而得到允许域名。

    返回值结构::

        {
            "央企-中国核工业集团有限公司":      ["cnnc.com.cn"],
            "地方工信-广东":                    ["gd.gov.cn"],
            "地方政府-北京":                    ["beijing.gov.cn"],
            "官网-财政部":                      ["mof.gov.cn"],
            ...
        }
    """
    index: dict = {}

    # 央企：source = f"央企-{name}"，name 为公司全称
    for src in SOE_SOURCES:
        name = src.get("name") or ""
        url = src.get("url") or ""
        d = extract_registered_domain(url)
        if name and d:
            index.setdefault(f"央企-{name}", []).append(d)

    # 地方工信：source = f"地方工信-{province}"
    for src in MIIT_LOCAL_SOURCES:
        province = src.get("province") or ""
        url = src.get("url") or ""
        d = extract_registered_domain(url)
        if province and d:
            index.setdefault(f"地方工信-{province}", []).append(d)

    # 地方政府：source = f"地方政府-{province}"
    for src in GOV_LOCAL_SOURCES:
        province = src.get("province") or ""
        url = src.get("url") or ""
        d = extract_registered_domain(url)
        if province and d:
            index.setdefault(f"地方政府-{province}", []).append(d)

    # 官网（website_monitor）：source = f"官网-{name}"，name 为 WEBSITE_SOURCES 的 key
    for name, meta in (WEBSITE_SOURCES or {}).items():
        url = (meta or {}).get("url") or ""
        d = extract_registered_domain(url)
        if name and d:
            index.setdefault(f"官网-{name}", []).append(d)

    return index


# 模块加载时一次性构建（常量集合静态不变，无需每条新闻重复构建）。
_MULTI_SOURCE_INDEX = _build_multi_source_domain_index()


def _allowed_domains_for_source(source_tag: str, sources_cfg: dict) -> Optional[List[str]]:
    """根据 source 标签返回允许的域名列表。

    返回值语义：
    - None  → 该来源不参与二次检验（跳过，保留）；
    - []    → 无法从配置中推断出有效域名（同样跳过，避免误删）；
    - 非空  → 用于白名单比对。
    """
    if not source_tag:
        return None

    # 1) 跳过名单：明确不做白名单过滤（目前仅微博）
    for skip in _SECONDARY_CHECK_SKIP_PREFIXES:
        if source_tag.startswith(skip):
            return None

    # 2) 多源信源精确反查（央企/地方政府/地方工信/官网）
    #    使用完整 source 标签做 key，命中即返回该源的官方注册域。
    if source_tag in _MULTI_SOURCE_INDEX:
        return list(_MULTI_SOURCE_INDEX[source_tag])

    # 3) 单一域信源：按前缀长度降序匹配，命中后从 config 中提取注册域
    for prefix, cfg_keys in sorted(
        _SOURCE_TAG_TO_CONFIG_KEYS, key=lambda kv: -len(kv[0])
    ):
        if source_tag.startswith(prefix):
            domains: List[str] = []
            for k in cfg_keys:
                src = (sources_cfg or {}).get(k) or {}
                url = src.get("url") or src.get("rss") or ""
                d = extract_registered_domain(url)
                if d:
                    domains.append(d)
            return domains  # 即使为空也返回（语义上"找到了映射但取不到域"）

    # 4) 未命中任何映射：默认不过滤（保守保留），避免新增 source 时静默丢弃
    #    注：对于已知前缀但具体标识未在索引中的多源（如 WEBSITE_SOURCES
    #    新增条目却未重启服务），此处也会保守保留。
    return None


def judge_offsite(source_tag: str, url: str, sources_cfg: dict) -> str:
    """对单条新闻按"source 标签 + URL"做同域判定，返回判定状态。

    本函数是二次检验的最小判定单元，被 :func:`verify_offsite_redirect`（用于
    新抓取批次）与 ``verify_existing_csv.py``（用于存量 CSV 回溯）共享，确保
    两条调用路径的判定语义完全一致。

    返回值（字符串状态码，便于上层做统计/分类）::

        "skip-source"  → 来源在跳过名单（如微博），不做判定
        "skip-no-rule" → 来源未命中任何映射，保守保留
        "skip-no-rule-host" → 命中映射但未取到允许域名，保守保留
        "skip-bad-url" → URL 为空或 host 无法解析，保守保留
        "keep-match"   → host 命中白名单，保留
        "drop-offsite" → host 可解析且明确不在白名单中，应丢弃（外站跳转）
    """
    if not source_tag:
        return "skip-no-rule"

    allowed = _allowed_domains_for_source(source_tag, sources_cfg)
    if allowed is None:
        # 来源在跳过名单中（目前仅微博）
        for skip in _SECONDARY_CHECK_SKIP_PREFIXES:
            if source_tag.startswith(skip):
                return "skip-source"
        return "skip-no-rule"
    if not allowed:
        # 命中映射但配置中查不到 URL → 保守保留
        return "skip-no-rule-host"

    host = extract_host(url or "")
    if not host:
        return "skip-bad-url"

    if host_matches_allowed(url, allowed):
        return "keep-match"
    return "drop-offsite"


def verify_offsite_redirect(items: List[Item], config: dict) -> List[Item]:
    """对非微博渠道的 items 执行二次检验，剔除跳转到外站的新闻。

    仅按 URL 域名做字符串比对（不发起任何网络请求）。对于无法判定的项（如
    配置中找不到 allowed_domain、host 解析失败、属于跳过名单的来源等），
    按保守保留原则不做删除。

    Args:
        items:  各 parser 汇总后的 Item 列表。
        config: 来自 config.yaml 的全量配置字典（用于查询各 source.url）。

    Returns:
        过滤后的 Item 列表（保持原顺序）。
    """
    sources_cfg = (config or {}).get("sources") or {}
    kept: List[Item] = []
    for it in items:
        source_tag = getattr(it, "source", "") or ""
        url = getattr(it, "url", "") or ""
        if judge_offsite(source_tag, url, sources_cfg) != "drop-offsite":
            kept.append(it)
        # 否则：host 可解析且明确不在白名单中 → 视为跳转外站，丢弃

    return kept


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

    # ── 二次检验：丢弃跳转到外站的新闻（微博渠道按用户要求跳过） ──────────────
    before_verify = len(all_items)
    all_items = verify_offsite_redirect(all_items, config)
    dropped = before_verify - len(all_items)
    if dropped > 0:
        print(f"[INFO] 二次检验丢弃跳转外站的新闻 {dropped} 条（剩余 {len(all_items)} 条）")

    # ── 去重合并并写 CSV ──────────────────────────────────────────────────────
    out_csv = repo_root / config["output"]["csv_path"]
    existing = load_existing(str(out_csv))
    merged, added = dedup_merge(
        existing,
        all_items,
        publisher_alias=config.get("publisher_alias") or {},
    )

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
