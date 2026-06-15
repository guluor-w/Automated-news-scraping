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


# ─────────────── 二次检验（verify_offsite_redirect）测试 ───────────────

from models import Item  # noqa: E402  （延后导入：与测试块靠近以便阅读）


def _make_item(url: str, source: str, title: str = "测试新闻") -> Item:
    """构造一个最小化的 Item 实例用于二次检验测试。"""
    return Item(
        title=title,
        publisher="测试单位",
        url=url,
        pub_date="2026-06-15",
        source=source,
        fetched_at="2026-06-15 10:00:00",
    )


def _verify_config() -> dict:
    """构造一份包含各信源 URL 的最小化 config，供二次检验测试使用。"""
    return {
        "sources": {
            "miit_home":     {"url": "https://www.miit.gov.cn/"},
            "gov_home":      {"url": "https://www.gov.cn/"},
            "ndrc_home":     {"url": "https://www.ndrc.gov.cn/"},
            "most_home":     {"url": "https://www.most.gov.cn/"},
            "moe_news":      {"url": "http://www.moe.gov.cn/jyb_xwfb/"},
            "sasac_home":    {"url": "http://www.sasac.gov.cn/"},
            "nda_home":      {"url": "https://www.nda.gov.cn/sjj/index_pc.html"},
            "qqnews_search": {"url": "https://i.news.qq.com/gw/pc_search/result"},
        }
    }


def test_verify_drops_offsite_for_most():
    """科技部"媒体聚焦"指向 people.com.cn 等外站时应被丢弃。"""
    cfg = _verify_config()
    items = [
        _make_item("http://paper.people.com.cn/rmrb/html/2026-06/15/x.htm",
                   "科技部官网-媒体聚焦"),
        _make_item("https://www.most.gov.cn/kjbgz/202606/t20260615_xxx.html",
                   "科技部官网-科技部工作"),
    ]
    kept = collect.verify_offsite_redirect(items, cfg)
    assert len(kept) == 1, f"期望 1 条（外站被丢弃），实际: {len(kept)}"
    assert kept[0].source == "科技部官网-科技部工作"
    print("✓ test_verify_drops_offsite_for_most")


def test_verify_keeps_subdomain_of_same_registered_domain():
    """同一注册域的子域（如 wap.miit.gov.cn）应保留。"""
    cfg = _verify_config()
    items = [
        _make_item("https://wap.miit.gov.cn/jgsj/notice/art/2026/x.html",
                   "工信部官网-时政要闻"),
        _make_item("https://www.miit.gov.cn/zwgk/zcwj/wjfb/yj/x.html",
                   "工信部官网-最新政策"),
    ]
    kept = collect.verify_offsite_redirect(items, cfg)
    assert len(kept) == 2, f"期望 2 条（同注册域均保留），实际: {len(kept)}"
    print("✓ test_verify_keeps_subdomain_of_same_registered_domain")


def test_verify_skips_weibo_source():
    """微博渠道按用户要求跳过二次检验，即便 host 与任何配置不符也应保留。"""
    cfg = _verify_config()
    items = [
        _make_item("https://m.weibo.cn/detail/123", "微博-工信微报"),
        _make_item("https://example.com/article/1", "微博-工信微报"),
    ]
    kept = collect.verify_offsite_redirect(items, cfg)
    assert len(kept) == 2, f"期望 2 条（微博跳过检验），实际: {len(kept)}"
    print("✓ test_verify_skips_weibo_source")


def test_verify_soe_same_domain_kept_offsite_dropped():
    """央企渠道：同域（含子域）保留，跳外站丢弃；按 source 标签精确反查。"""
    cfg = _verify_config()
    items = [
        # 中国核工业集团（cnnc.com.cn）：同域保留 + 子域保留 + 外站丢弃
        _make_item("http://www.cnnc.com.cn/cnnc/300557/news/x.html",
                   "央企-中国核工业集团有限公司"),
        _make_item("http://media.cnnc.com.cn/cnnc/zxbd/x.html",
                   "央企-中国核工业集团有限公司"),
        _make_item("https://example.com/redirected/article",
                   "央企-中国核工业集团有限公司"),
        # 华润集团（crc.com.hk）：注意华润 SOE 配置是 .com.hk
        _make_item("http://www.crc.com.hk/news/1.html", "央企-华润（集团）有限公司"),
        _make_item("https://news.qq.com/external/1", "央企-华润（集团）有限公司"),
    ]
    kept = collect.verify_offsite_redirect(items, cfg)
    kept_urls = [it.url for it in kept]
    assert len(kept) == 3, f"期望 3 条（保留同域、丢弃外站），实际: {len(kept)}; kept={kept_urls}"
    # 验证三条同域被保留
    assert any("www.cnnc.com.cn" in u for u in kept_urls)
    assert any("media.cnnc.com.cn" in u for u in kept_urls)
    assert any("crc.com.hk" in u for u in kept_urls)
    print("✓ test_verify_soe_same_domain_kept_offsite_dropped")


def test_verify_miit_local_same_domain_kept_offsite_dropped():
    """地方工信渠道：按 source=地方工信-<省> 反查省厅 URL 做白名单。"""
    cfg = _verify_config()
    items = [
        # 广东工信厅 URL 为 gdii.gd.gov.cn → 注册域 gd.gov.cn
        _make_item("https://gdii.gd.gov.cn/zwgk/tzgg/x.html", "地方工信-广东"),
        _make_item("https://www.gd.gov.cn/some/x.html",       "地方工信-广东"),  # 同 gd.gov.cn 注册域
        _make_item("https://gxt.fujian.gov.cn/x/y.html",      "地方工信-广东"),  # 福建域→外站
        # 北京工信局 URL 为 jxj.beijing.gov.cn → 注册域 beijing.gov.cn
        _make_item("https://jxj.beijing.gov.cn/jxdt/tzgg/x.html", "地方工信-北京"),
        _make_item("https://news.sina.com.cn/外站/x.html",        "地方工信-北京"),
    ]
    kept = collect.verify_offsite_redirect(items, cfg)
    kept_urls = [it.url for it in kept]
    assert len(kept) == 3, f"期望 3 条，实际: {len(kept)}; kept={kept_urls}"
    assert any("gdii.gd.gov.cn" in u for u in kept_urls)
    assert any("www.gd.gov.cn" in u for u in kept_urls)
    assert any("jxj.beijing.gov.cn" in u for u in kept_urls)
    print("✓ test_verify_miit_local_same_domain_kept_offsite_dropped")


def test_verify_gov_local_same_domain_kept_offsite_dropped():
    """地方政府渠道：按 source=地方政府-<省> 反查省政府 URL 做白名单。"""
    cfg = _verify_config()
    items = [
        # 北京市政府 URL 为 beijing.gov.cn
        _make_item("https://www.beijing.gov.cn/ywdt/x.html",       "地方政府-北京"),
        _make_item("https://wb.beijing.gov.cn/sub/x.html",         "地方政府-北京"),
        _make_item("https://www.shanghai.gov.cn/外省/x.html",       "地方政府-北京"),  # 外省→丢弃
        # 广东省政府 URL 为 gd.gov.cn
        _make_item("https://www.gd.gov.cn/gdywdt/gdyw/x.html",     "地方政府-广东"),
        _make_item("https://www.example.org/外站/x",                "地方政府-广东"),
    ]
    kept = collect.verify_offsite_redirect(items, cfg)
    kept_urls = [it.url for it in kept]
    assert len(kept) == 3, f"期望 3 条，实际: {len(kept)}; kept={kept_urls}"
    assert any("www.beijing.gov.cn" in u for u in kept_urls)
    assert any("wb.beijing.gov.cn" in u for u in kept_urls)
    assert any("www.gd.gov.cn" in u for u in kept_urls)
    print("✓ test_verify_gov_local_same_domain_kept_offsite_dropped")


def test_verify_website_monitor_same_domain_kept_offsite_dropped():
    """官网（website_monitor）渠道：按 source=官网-<名> 反查 WEBSITE_SOURCES 做白名单。"""
    cfg = _verify_config()
    items = [
        # 财政部 mof.gov.cn → 同域保留
        _make_item("https://www.mof.gov.cn/zhengwuxinxi/x.html",  "官网-财政部"),
        # 财政部外站（被引用到人民日报）→ 丢弃
        _make_item("http://paper.people.com.cn/x.htm",            "官网-财政部"),
        # 国家信访局 gjxfj.gov.cn → 同域保留
        _make_item("https://www.gjxfj.gov.cn/gjxfj/news/x.htm",   "官网-国家信访局"),
        # 国家信访局外站
        _make_item("https://www.xinhuanet.com/外站/x",            "官网-国家信访局"),
    ]
    kept = collect.verify_offsite_redirect(items, cfg)
    kept_urls = [it.url for it in kept]
    assert len(kept) == 2, f"期望 2 条，实际: {len(kept)}; kept={kept_urls}"
    assert any("www.mof.gov.cn" in u for u in kept_urls)
    assert any("www.gjxfj.gov.cn" in u for u in kept_urls)
    print("✓ test_verify_website_monitor_same_domain_kept_offsite_dropped")


def test_verify_multi_source_unknown_identifier_kept():
    """多源前缀但标识未登记（如新增源未重启）应保守保留，不静默丢弃。"""
    cfg = _verify_config()
    items = [
        _make_item("https://random.example.org/x", "央企-不存在的公司"),
        _make_item("https://random.example.org/y", "地方工信-不存在的省"),
        _make_item("https://random.example.org/z", "官网-不存在的部门"),
    ]
    kept = collect.verify_offsite_redirect(items, cfg)
    assert len(kept) == 3, f"期望 3 条（未登记标识保留），实际: {len(kept)}"
    print("✓ test_verify_multi_source_unknown_identifier_kept")


def test_verify_keeps_when_host_unresolvable():
    """URL 为空或异常无法解析 host 时应保守保留。"""
    cfg = _verify_config()
    items = [
        _make_item("", "工信部官网-时政要闻"),
        _make_item("not-a-url", "工信部官网-时政要闻"),
    ]
    kept = collect.verify_offsite_redirect(items, cfg)
    assert len(kept) == 2, f"期望 2 条（host 不可解析时保留），实际: {len(kept)}"
    print("✓ test_verify_keeps_when_host_unresolvable")


def test_verify_drops_qqnews_offsite():
    """腾讯新闻条目若 surl 指向非 qq.com 域名（外站）应被丢弃。"""
    cfg = _verify_config()
    items = [
        _make_item("https://news.qq.com/rain/a/20260615A0XYZ00", "腾讯新闻-工信微报"),
        _make_item("https://mp.weixin.qq.com/s/abc",            "腾讯新闻-工信微报"),
        _make_item("https://example.com/external/article",       "腾讯新闻-工信微报"),
    ]
    kept = collect.verify_offsite_redirect(items, cfg)
    kept_hosts = sorted(it.url for it in kept)
    # qq.com 与 weixin.qq.com 都属于 qq.com 注册域 → 保留；example.com → 丢弃
    assert len(kept) == 2, f"期望 2 条，实际: {len(kept)}; kept={kept_hosts}"
    print("✓ test_verify_drops_qqnews_offsite")


def test_verify_unknown_source_prefix_kept():
    """未在映射表中的来源前缀应保守保留，避免新增 source 时静默丢弃。"""
    cfg = _verify_config()
    items = [
        _make_item("https://random.example.org/x", "新来源-未配置"),
    ]
    kept = collect.verify_offsite_redirect(items, cfg)
    assert len(kept) == 1, f"期望 1 条（未知来源保留），实际: {len(kept)}"
    print("✓ test_verify_unknown_source_prefix_kept")


def test_verify_gov_rss_uses_gov_home_domain():
    '''"GOV-" 前缀（来自 gov RSS）应使用 gov_home 的域名做白名单。'''
    cfg = _verify_config()
    items = [
        _make_item("https://www.gov.cn/zhengce/zhengceku/x.htm", "GOV-最新政策-RSS"),
        _make_item("https://rsshub.app/some/article",            "GOV-最新政策-RSS"),
    ]
    kept = collect.verify_offsite_redirect(items, cfg)
    assert len(kept) == 1, f"期望 1 条（rsshub 域名被丢弃），实际: {len(kept)}"
    assert "gov.cn" in kept[0].url
    print("✓ test_verify_gov_rss_uses_gov_home_domain")


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
        # 二次检验
        test_verify_drops_offsite_for_most,
        test_verify_keeps_subdomain_of_same_registered_domain,
        test_verify_skips_weibo_source,
        test_verify_soe_same_domain_kept_offsite_dropped,
        test_verify_miit_local_same_domain_kept_offsite_dropped,
        test_verify_gov_local_same_domain_kept_offsite_dropped,
        test_verify_website_monitor_same_domain_kept_offsite_dropped,
        test_verify_multi_source_unknown_identifier_kept,
        test_verify_keeps_when_host_unresolvable,
        test_verify_drops_qqnews_offsite,
        test_verify_unknown_source_prefix_kept,
        test_verify_gov_rss_uses_gov_home_domain,
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
