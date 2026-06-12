"""
针对 collect.py 中 parse_weibo_monitor_sources 集成逻辑的单元测试。
运行方式：python src/test_collect.py
"""
import sys
import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

# 将 src 目录加入模块搜索路径
sys.path.insert(0, str(Path(__file__).parent))
import collect

SG_TZ = timezone(timedelta(hours=8))

# ─────────────── 辅助工具 ───────────────

def make_config(enabled=True, mode="all", extra_keywords=None):
    keywords = ["智能", "数字化", "AI", "机器人", "印发"]
    if extra_keywords:
        keywords += extra_keywords
    return {
        "keywords": keywords,
        "window_days": 15,
        "hard_cap_days": 15,
        "weibo_monitor": {
            "enabled": enabled,
            "mode": mode,
            "max_pages": 1,
        },
    }


def make_weibo_post(title, hours_ago=1, has_article_url=False):
    """构造一个模拟的微博帖子字典"""
    now = datetime.now(SG_TZ)
    parsed_time = (now - timedelta(hours=hours_ago)).strftime("%Y-%m-%d %H:%M")
    mid = "100001"
    return {
        "mid": mid,
        "title": title,
        "text": title,
        "url": f"https://m.weibo.cn/detail/{mid}",
        "article_url": "https://example.com/article/1" if has_article_url else "",
        "is_article": has_article_url,
        "parsed_time": parsed_time,
        "reposts_count": 0,
        "comments_count": 0,
        "attitudes_count": 0,
    }


def make_website_article(title, url="https://www.nda.gov.cn/news/1.html"):
    """构造一个模拟的官网文章字典"""
    return {"title": title, "url": url, "source": "国家数据局"}


def run_parse_weibo_with_mock(config, mock_results: dict):
    """
    用 mock_results 替换微博和官网的实际抓取，
    运行 parse_weibo_monitor_sources 并返回 Item 列表。

    mock_results 格式: {来源名称: [post_or_article, ...]}
    含 "mid" 字段的条目被视为微博帖子，其余视为官网文章。
    """
    # 区分微博帖子（含 "mid"）与官网文章（不含 "mid"）
    weibo_results = {
        name: posts for name, posts in mock_results.items()
        if posts and "mid" in posts[0]
    }
    website_results = {
        name: posts for name, posts in mock_results.items()
        if not (posts and "mid" in posts[0])
    }

    async def mock_fetch_weibo(accounts, max_pages):
        return weibo_results

    async def mock_fetch_website(sources, now=None):
        return website_results

    with patch("parsers.weibo._fetch_weibo_raw", mock_fetch_weibo), \
         patch("parsers.website_monitor._fetch_website_raw", mock_fetch_website):
        now = datetime.now(tz=SG_TZ)
        return collect.parse_weibo_monitor_sources(config, now)


# ─────────────── 测试用例 ───────────────

def test_disabled_returns_empty():
    """enabled=False 时应立即返回空列表"""
    config = make_config(enabled=False)
    now = datetime.now(tz=SG_TZ)
    result = collect.parse_weibo_monitor_sources(config, now)
    assert result == [], f"期望空列表，实际: {result}"
    print("✓ test_disabled_returns_empty")


def test_weibo_post_keyword_hit():
    """关键词命中的微博帖子应被保留"""
    config = make_config()
    post = make_weibo_post("关于人工智能AI发展的重要通知")
    result = run_parse_weibo_with_mock(config, {"工信微报": [post]})
    assert len(result) == 1, f"期望 1 条，实际: {len(result)}"
    assert result[0].source == "微博-工信微报"
    assert result[0].publisher == "工信微报"
    print("✓ test_weibo_post_keyword_hit")


def test_weibo_post_keyword_miss():
    """不含关键词的微博帖子应被过滤掉"""
    config = make_config()
    post = make_weibo_post("今天天气很好，出去散步")
    result = run_parse_weibo_with_mock(config, {"工信微报": [post]})
    assert len(result) == 0, f"期望 0 条，实际: {len(result)}"
    print("✓ test_weibo_post_keyword_miss")


def test_weibo_post_article_url_preferred():
    """有 article_url 时应优先使用 article_url 而非微博详情 URL"""
    config = make_config()
    post = make_weibo_post("AI智能机器人重磅发布", has_article_url=True)
    result = run_parse_weibo_with_mock(config, {"工信微报": [post]})
    assert len(result) == 1
    assert result[0].url == "https://example.com/article/1", f"URL不正确: {result[0].url}"
    print("✓ test_weibo_post_article_url_preferred")


def test_website_article_keyword_hit():
    """关键词命中的官网文章应被保留"""
    config = make_config()
    article = make_website_article("数字化转型白皮书发布")
    result = run_parse_weibo_with_mock(config, {"国家数据局": [article]})
    assert len(result) == 1
    assert result[0].source == "官网-国家数据局"
    assert result[0].pub_date is None  # 官网文章无日期
    print("✓ test_website_article_keyword_hit")


def test_website_article_keyword_miss():
    """不含关键词的官网文章应被过滤掉"""
    config = make_config()
    article = make_website_article("关于开展窗口服务满意度调查的通知")
    result = run_parse_weibo_with_mock(config, {"国家数据局": [article]})
    assert len(result) == 0
    print("✓ test_website_article_keyword_miss")


def test_weibo_time_window_filter():
    """超出时间窗口的微博帖子应被过滤"""
    config = make_config()
    # 发布于 20 天前（超出 window_days=15）
    post = make_weibo_post("AI机器人政策发布", hours_ago=20 * 24)
    result = run_parse_weibo_with_mock(config, {"工信微报": [post]})
    assert len(result) == 0, f"期望 0 条（超出时间窗口），实际: {len(result)}"
    print("✓ test_weibo_time_window_filter")


def test_weibo_time_window_within():
    """时间窗口内的微博帖子应被保留"""
    config = make_config()
    # 发布于 5 天前（在 window_days=15 内）
    post = make_weibo_post("智能制造新政策发布", hours_ago=5 * 24)
    result = run_parse_weibo_with_mock(config, {"工信微报": [post]})
    assert len(result) == 1, f"期望 1 条，实际: {len(result)}"
    print("✓ test_weibo_time_window_within")


def test_mode_website_only():
    """mode=website_only 时应传入空 accounts，只抓官网"""
    config = make_config(mode="website_only")
    article = make_website_article("数字化经济新政策")
    # website_only 模式下 accounts={}, 只会产生官网来源的条目
    result = run_parse_weibo_with_mock(config, {"国家数据局": [article]})
    assert len(result) == 1
    assert result[0].source.startswith("官网-")
    print("✓ test_mode_website_only")


def test_empty_title_filtered():
    """标题为空的条目应被跳过"""
    config = make_config()
    article = make_website_article("", url="https://www.nda.gov.cn/2.html")
    result = run_parse_weibo_with_mock(config, {"国家数据局": [article]})
    assert len(result) == 0
    print("✓ test_empty_title_filtered")


def test_mixed_sources():
    """同时有微博和官网来源时，两者都应被正确处理"""
    config = make_config()
    weibo_post = make_weibo_post("AI机器人政策印发")
    web_article = make_website_article("智能制造产业发展规划")
    result = run_parse_weibo_with_mock(config, {
        "工信微报": [weibo_post],
        "国家数据局": [web_article],
    })
    assert len(result) == 2
    sources = {r.source for r in result}
    assert "微博-工信微报" in sources
    assert "官网-国家数据局" in sources
    print("✓ test_mixed_sources")


def test_item_fields():
    """转换后的 Item 对象应包含所有必要字段"""
    config = make_config()
    post = make_weibo_post("AI智能发展白皮书")
    result = run_parse_weibo_with_mock(config, {"工信微报": [post]})
    assert len(result) == 1
    item = result[0]
    assert item.title
    assert item.publisher == "工信微报"
    assert item.url.startswith("https://")
    assert item.source == "微博-工信微报"
    assert item.fetched_at
    print("✓ test_item_fields")


# ─────────────── 运行所有测试 ───────────────

if __name__ == "__main__":
    tests = [
        test_disabled_returns_empty,
        test_weibo_post_keyword_hit,
        test_weibo_post_keyword_miss,
        test_weibo_post_article_url_preferred,
        test_website_article_keyword_hit,
        test_website_article_keyword_miss,
        test_weibo_time_window_filter,
        test_weibo_time_window_within,
        test_mode_website_only,
        test_empty_title_filtered,
        test_mixed_sources,
        test_item_fields,
    ]

    passed = 0
    failed = 0
    for test in tests:
        try:
            test()
            passed += 1
        except AssertionError as e:
            print(f"✗ {test.__name__}: {e}")
            failed += 1
        except Exception as e:
            print(f"✗ {test.__name__} (异常): {e}")
            failed += 1

    print(f"\n结果: {passed} 通过, {failed} 失败")
    if failed:
        sys.exit(1)
