"""定向修复非微博历史新闻的标题和发布日期。"""

import argparse
import re
import shutil
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from parsers.miit_local import (  # noqa: E402
    _detail_title_matches,
    _has_coarse_date_in_url,
    _has_precise_date_in_url,
    _is_suspicious_title,
    _looks_truncated_title,
    _post_process_title,
    _title_needs_replacement,
)
from storage import generate_rss  # noqa: E402
from utils import fetch_detail_metadata  # noqa: E402

_RE_ADDITIONAL_COARSE_URL = re.compile(r"/20\d{2}/\d{1,2}/[A-Za-z]\d{10,}\.html")


def _is_weibo(source: str) -> bool:
    return str(source or "").strip().startswith("微博-")


def _needs_repair(row: pd.Series) -> bool:
    if _is_weibo(row.get("来源", "")):
        return False
    title = str(row.get("标题", "") or "").strip()
    url = str(row.get("新闻URL", "") or "").strip()
    pub_date = str(row.get("发布日期", "") or "").strip()
    return bool(
        url
        and (
            not pub_date
            or _is_suspicious_title(title)
            or _looks_truncated_title(title)
            or _has_coarse_date_in_url(url)
            or _RE_ADDITIONAL_COARSE_URL.search(url)
        )
    )


def repair_dataframe(df: pd.DataFrame, limit: int | None = None):
    """返回修复后的副本、变更列表和无法取得可靠详情的行数。"""
    repaired = df.copy(deep=True)
    candidates = [idx for idx, row in df.iterrows() if _needs_repair(row)]
    if limit is not None:
        candidates = candidates[:max(0, limit)]

    changes = []
    unresolved = 0
    for number, idx in enumerate(candidates, 1):
        row = repaired.loc[idx]
        url = str(row.get("新闻URL", "") or "").strip()
        metadata = fetch_detail_metadata(url, timeout=20)
        if not metadata.get("title") and not metadata.get("pub_date"):
            unresolved += 1
            continue

        old_title = str(row.get("标题", "") or "").strip()
        old_date = str(row.get("发布日期", "") or "").strip()
        cleaned_old_title = _post_process_title(old_title)
        new_title = cleaned_old_title if len(cleaned_old_title) >= 6 else old_title
        new_date = old_date

        title_needed = _is_suspicious_title(old_title) or _looks_truncated_title(old_title)
        detail_title = _post_process_title(metadata.get("title") or "")
        if (
            title_needed
            and len(detail_title) >= 6
            and _title_needs_replacement(old_title)
            and _detail_title_matches(old_title, detail_title)
            and (
                len(detail_title) < len(old_title)
                or _looks_truncated_title(old_title)
            )
        ):
            new_title = detail_title

        detail_date = metadata.get("pub_date")
        fetched_date = str(row.get("查询时间", "") or "")[:10]
        date_is_plausible = not fetched_date or not detail_date or detail_date <= fetched_date
        coarse_date = _has_coarse_date_in_url(url) or _RE_ADDITIONAL_COARSE_URL.search(url)
        if (
            detail_date
            and date_is_plausible
            and (not old_date or coarse_date or not _has_precise_date_in_url(url))
        ):
            new_date = detail_date

        if new_title != old_title:
            repaired.at[idx, "标题"] = new_title
        if new_date != old_date:
            repaired.at[idx, "发布日期"] = new_date
        if new_title != old_title or new_date != old_date:
            changes.append({
                "index": idx,
                "source": str(row.get("来源", "") or ""),
                "url": url,
                "old_title": old_title,
                "new_title": new_title,
                "old_date": old_date,
                "new_date": new_date,
            })

        if number % 20 == 0:
            print(f"[进度] 已检查 {number}/{len(candidates)} 条候选", flush=True)

    return repaired, changes, unresolved, len(candidates)


def _print_summary(changes: list, unresolved: int, candidates: int) -> None:
    by_source = Counter(change["source"] for change in changes)
    title_changes = [c for c in changes if c["old_title"] != c["new_title"]]
    date_changes = [c for c in changes if c["old_date"] != c["new_date"]]
    print(f"\n[汇总] 检查 {candidates} 条候选，修复 {len(changes)} 条，无法回源 {unresolved} 条")
    print(f"  标题变更 {len(title_changes)} 条，日期变更 {len(date_changes)} 条")
    for source, count in by_source.most_common():
        print(f"  {count:>4}  {source}")
    samples = title_changes[:15]
    samples += [c for c in changes if c not in title_changes][:15]
    for change in samples:
        print(f"\n  {change['source']} | {change['url']}")
        if change["old_title"] != change["new_title"]:
            print(f"    标题: {change['old_title']} -> {change['new_title']}")
        if change["old_date"] != change["new_date"]:
            print(f"    日期: {change['old_date'] or '(空)'} -> {change['new_date']}")
    if len(changes) > len(samples):
        print(f"\n  其余 {len(changes) - len(samples)} 条变更未展开。")


def _write_outputs(df: pd.DataFrame, csv_path: Path) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = csv_path.with_suffix(f".bak.{timestamp}.csv")
    shutil.copy2(csv_path, backup_path)
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    data_dir = csv_path.parent
    generate_rss(
        df,
        data_dir / "rss_full.xml",
        title="新闻完整清单",
        description="新闻完整清单 RSS 订阅",
    )
    miit_df = df[df["来源"].str.contains("工信", na=False)]
    generate_rss(
        miit_df,
        data_dir / "rss_miit.xml",
        title="工信新闻清单",
        description='来源包含"工信"的新闻 RSS 订阅',
    )
    return backup_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", default="docs/data/policy_news.csv")
    parser.add_argument("--apply", action="store_true", help="写回 CSV 并重建 RSS")
    parser.add_argument("--limit", type=int, default=None, help="仅检查前 N 条候选")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    df = pd.read_csv(csv_path, encoding="utf-8-sig", dtype=str).fillna("")
    repaired, changes, unresolved, candidates = repair_dataframe(df, limit=args.limit)
    _print_summary(changes, unresolved, candidates)

    weibo_before = df[df["来源"].str.startswith("微博-")].reset_index(drop=True)
    weibo_after = repaired[repaired["来源"].str.startswith("微博-")].reset_index(drop=True)
    if not weibo_before.equals(weibo_after):
        raise RuntimeError("微博记录发生变化，已停止写回")

    if args.apply:
        backup = _write_outputs(repaired, csv_path)
        print(f"\n[写回] 已更新 CSV/RSS，原 CSV 备份：{backup}")
    else:
        print("\n[提示] 当前为 dry-run，未修改 CSV/RSS；确认样本后使用 --apply。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
