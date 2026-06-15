"""
verify_existing_csv.py — 对存量 CSV 做一次性来源同域回溯检验。

用途
----
``collect.py`` 在新抓取流程末尾会调用 :func:`collect.verify_offsite_redirect`
对当批新闻执行"来源同域"二次检验。但在该机制上线之前已经入库的存量数据
（``docs/data/policy_news.csv``）从未被检验过。本脚本以**相同的判定规则**
对存量 CSV 做一次回溯，输出/清理跳转外站的历史条目。

判定规则
--------
完全复用 ``collect.judge_offsite``（即同一函数、同一份多源索引、同一份配置），
保证存量回溯与日常抓取的判定结果一致。

运行方式
--------
默认 ``dry-run``，仅打印统计与疑似外站列表，不修改任何文件::

    python src/verify_existing_csv.py

可选参数::

    --csv PATH      存量 CSV 路径（默认从 config.yaml 的 output.csv_path 读取）
    --report PATH   将疑似外站行写入审计 CSV（不修改原文件）
    --apply         真正写回 CSV（自动备份为 <原名>.bak.<时间戳>.csv）

示例::

    # 1) 先 dry-run + 生成审计报告人工核查
    python src/verify_existing_csv.py --report docs/data/offsite_audit.csv

    # 2) 确认无误后写回（自动备份原 CSV）
    python src/verify_existing_csv.py --apply
"""

import argparse
import shutil
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

import pandas as pd

# 允许直接以脚本方式运行（python src/verify_existing_csv.py）
sys.path.insert(0, str(Path(__file__).resolve().parent))

from collect import judge_offsite, load_config  # noqa: E402


# CSV 必备列（与 storage.load_existing 的列名约定保持一致）
_COL_TITLE = "标题"
_COL_PUBLISHER = "发布单位"
_COL_URL = "新闻URL"
_COL_PUB_DATE = "发布日期"
_COL_SOURCE = "来源"
_COL_FETCHED_AT = "查询时间"


def _resolve_csv_path(arg_path: str | None, config: dict) -> Path:
    """优先使用 --csv 参数；否则从 config.output.csv_path 读取。"""
    if arg_path:
        return Path(arg_path)
    out_cfg = (config or {}).get("output") or {}
    csv_path = out_cfg.get("csv_path") or "docs/data/policy_news.csv"
    return Path(csv_path)


def _load_csv(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV 文件不存在：{csv_path}")
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    # 去除潜在的 BOM
    df.columns = [str(c).lstrip("\ufeff") for c in df.columns]
    required = {_COL_URL, _COL_SOURCE}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"CSV 缺少必需列：{missing}；现有列：{list(df.columns)}")
    # 缺列容错：补全后续展示需要的列
    for col in (_COL_TITLE, _COL_PUBLISHER, _COL_PUB_DATE, _COL_FETCHED_AT):
        if col not in df.columns:
            df[col] = ""
    return df


def _judge_dataframe(df: pd.DataFrame, sources_cfg: dict) -> Tuple[pd.Series, Counter]:
    """对 DataFrame 每一行调用 judge_offsite，返回状态 Series 与计数器。"""
    statuses: List[str] = []
    counter: Counter = Counter()
    for _, row in df.iterrows():
        source_tag = str(row.get(_COL_SOURCE) or "").strip()
        url = str(row.get(_COL_URL) or "").strip()
        st = judge_offsite(source_tag, url, sources_cfg)
        statuses.append(st)
        counter[st] += 1
    return pd.Series(statuses, index=df.index, name="_judge"), counter


def _print_summary(total: int, counter: Counter) -> None:
    """以表格形式打印各判定状态的计数。"""
    print(f"\n[汇总] 共 {total} 行")
    label = {
        "keep-match":         "保留（同域命中白名单）",
        "skip-source":        "跳过（来源在跳过名单，如微博）",
        "skip-no-rule":       "跳过（未配置同域规则的来源）",
        "skip-no-rule-host":  "跳过（命中规则但无可用域名）",
        "skip-bad-url":       "跳过（URL 为空或无法解析 host）",
        "drop-offsite":       "丢弃（跳转外站）",
    }
    width = max(len(v) for v in label.values())
    order = [
        "drop-offsite",
        "keep-match",
        "skip-source",
        "skip-no-rule",
        "skip-no-rule-host",
        "skip-bad-url",
    ]
    for key in order:
        n = counter.get(key, 0)
        bar = "■" * min(50, n // max(1, total // 100 or 1))
        print(f"  {label[key].ljust(width)} : {n:>6}  {bar}")


def _print_offsite_detail(df: pd.DataFrame, statuses: pd.Series, limit: int = 50) -> None:
    """打印疑似外站行的明细（按来源前缀分组）。"""
    offsite_df = df.loc[statuses == "drop-offsite"].copy()
    if offsite_df.empty:
        print("\n[明细] 无 drop-offsite 行。")
        return

    # 按来源分组计数
    print(f"\n[明细] 共 {len(offsite_df)} 行被判定为外站跳转，按来源分组：")
    grp = offsite_df[_COL_SOURCE].value_counts()
    for src, n in grp.items():
        print(f"  {n:>4}  {src}")

    print(f"\n[明细] 前 {min(limit, len(offsite_df))} 行示例：")
    for idx, row in offsite_df.head(limit).iterrows():
        # idx 是 0-based DataFrame 行号；CSV 第 1 行是表头 → 文件行号 = idx + 2
        file_row = int(idx) + 2
        src = str(row.get(_COL_SOURCE) or "")
        url = str(row.get(_COL_URL) or "")
        title = str(row.get(_COL_TITLE) or "")
        title_show = title if len(title) <= 40 else title[:39] + "…"
        print(f"  L{file_row:<5} | {src:<22} | {title_show:<41} | {url}")


def _write_report(df: pd.DataFrame, statuses: pd.Series, report_path: Path) -> None:
    offsite_df = df.loc[statuses == "drop-offsite"].copy()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    # 用 utf-8-sig 与项目主 CSV 保持一致（便于 Excel 打开）
    offsite_df.to_csv(report_path, index=False, encoding="utf-8-sig")
    print(f"\n[报告] 已写入审计报告：{report_path}（共 {len(offsite_df)} 行）")


def _apply_changes(df: pd.DataFrame, statuses: pd.Series, csv_path: Path) -> int:
    """备份原 CSV 后，将非 drop-offsite 行写回。返回被删除的行数。"""
    drop_mask = statuses == "drop-offsite"
    dropped = int(drop_mask.sum())
    if dropped == 0:
        print("\n[写回] 无需写回（drop-offsite 为 0）。")
        return 0

    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = csv_path.with_suffix(f".bak.{ts}.csv")
    shutil.copy2(csv_path, backup_path)
    print(f"\n[写回] 已备份原文件 → {backup_path}")

    kept_df = df.loc[~drop_mask].copy()
    kept_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"[写回] 已删除 {dropped} 行外站跳转记录，原文件已更新：{csv_path}")
    return dropped


def main() -> int:
    parser = argparse.ArgumentParser(
        description="对存量 policy_news.csv 做一次性来源同域回溯检验。"
    )
    parser.add_argument(
        "--config", default="config.yaml",
        help="config.yaml 路径（默认：config.yaml）",
    )
    parser.add_argument(
        "--csv", default=None,
        help="存量 CSV 路径（默认从 config.output.csv_path 读取）",
    )
    parser.add_argument(
        "--report", default=None,
        help="将 drop-offsite 行写入审计 CSV（不会修改原文件）",
    )
    parser.add_argument(
        "--apply", action="store_true",
        help="真正写回 CSV（删除 drop-offsite 行，自动备份原文件）",
    )
    parser.add_argument(
        "--detail-limit", type=int, default=50,
        help="终端最多打印多少条外站跳转明细（默认 50）",
    )
    args = parser.parse_args()

    config_path = Path(args.config)
    config = load_config(config_path)
    sources_cfg = (config or {}).get("sources") or {}

    csv_path = _resolve_csv_path(args.csv, config)
    print(f"[读取] CSV 路径：{csv_path}")
    df = _load_csv(csv_path)
    print(f"[读取] 共 {len(df)} 行")

    statuses, counter = _judge_dataframe(df, sources_cfg)
    _print_summary(len(df), counter)
    _print_offsite_detail(df, statuses, limit=max(0, args.detail_limit))

    if args.report:
        _write_report(df, statuses, Path(args.report))

    if args.apply:
        _apply_changes(df, statuses, csv_path)
    else:
        print("\n[提示] 当前为 dry-run。如需真正写回 CSV，请加 --apply；")
        print("       如需生成审计报告但不写回，请加 --report <path.csv>。")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
