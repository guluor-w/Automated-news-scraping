"""
各信源解析器的单元测试。

使用本地保存的 HTML 文件进行离线测试，通过 mock http_get 避免实际网络请求。

运行方式：
    若使用 pytest 运行，请先安装：pip install pytest
    python -m pytest src/test_parsers.py -v

    也可直接运行：
    python src/test_parsers.py

    直接运行时，若未安装 pytest，会启用兼容降级逻辑；
    遇到依赖 pytest.skip 的场景时会给出清晰提示并退出。
"""
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from models import MIIT_ONLY_KEYWORDS, SOE_EXCLUDED_KEYWORDS

try:
    import pytest
except ImportError:
    class _PytestCompat:
        """未安装 pytest 时提供最小兼容接口。"""

        @staticmethod
        def skip(reason=""):
            message = "Skipping test"
            if reason:
                message = f"{message}: {reason}"
            print(message)
            raise SystemExit(0)

    pytest = _PytestCompat()
# 将 src 目录加入模块搜索路径
sys.path.insert(0, str(Path(__file__).parent))

SG_TZ = timezone(timedelta(hours=8))
NOW = datetime(2026, 4, 18, 12, 0, 0, tzinfo=SG_TZ)

# 宽松关键词：空字符串可匹配所有标题（keyword_hit 中 "" in title 恒为 True）
MATCH_ALL_KEYWORDS = [""]

# 测试 HTML 文件目录
TEST_HTML_ROOT = Path(__file__).resolve().parents[1] / "test_html"

# 共用配置
BASE_CONFIG = {
    "keywords": [
        "印发", "智能", "智慧", "AI", "模型", "制造业", "新型工业化",
        "工业互联网", "产业互联网", "数字化", "数智化", "算力", "具身",
        "机器人", "芯片", "装备", "体系建设", "新兴产业", "未来产业",
        "数据集", "数据要素", "工业数据", "高质量数据", "词元", "token",
    ],
    "window_days": 15,
    "hard_cap_days": 15,
    "sources": {
        "miit_home": {"name": "工业和信息化部", "url": "https://www.miit.gov.cn/"},
        "gov_latest_policy_rss": {"name": "中国政府网", "rss": "https://rsshub.app/gov/zhengce/zuixin"},
        "gov_home": {"name": "中国政府网", "url": "https://www.gov.cn/"},
        "ndrc_home": {"name": "国家发展和改革委员会", "url": "https://www.ndrc.gov.cn/"},
        "most_home": {"name": "科学技术部", "url": "https://www.most.gov.cn/"},
        "moe_news": {"name": "教育部", "url": "http://www.moe.gov.cn/jyb_xwfb/"},
        "miit_local": {"name": "地方工信部门", "enabled": True},
        "gov_local": {"name": "地方政府门户", "enabled": True},
    },
}


def _load_html(relpath: str) -> str:
    """从测试 HTML 目录加载文件。"""
    fpath = TEST_HTML_ROOT / relpath
    if not fpath.exists():
        return ""
    return fpath.read_text(encoding="utf-8", errors="replace")


# ═══════════════════════════════════════════════════════════════════════════════
# 1. miit_local 辅助函数测试
# ═══════════════════════════════════════════════════════════════════════════════

from parsers.miit_local import (
    _is_news_like_url,
    _is_nav_text,
    _extract_date_from_url,
    _extract_date_from_context,
    _clean_title,
    _strip_date_affixes,
    _TITLE_MAX_LEN,
    _MIN_TITLE_CHARS_BEFORE_EXCERPT,
)


class TestUrlPatternRecognition:
    """测试 _is_news_like_url 对 10 种 CMS URL 模式的识别能力。"""

    def test_pattern_a_trs_yyyymm(self):
        """模式 A: TRS CMS /YYYYMM/tYYYYMMDD_ID.html"""
        url = "https://jxj.beijing.gov.cn/jxdt/tzgg/202604/t20260418_4591280.html"
        assert _is_news_like_url(url)

    def test_pattern_b_egov_art(self):
        """模式 B: E-Gov /art/YYYY/M/D/art_COL_ART.html"""
        url = "http://gxt.jiangsu.gov.cn/art/2026/4/13/art_73259_11524668.html"
        assert _is_news_like_url(url)

    def test_pattern_b_miit_art_uuid(self):
        """模式 B 变体: miit.gov.cn /art/YYYY/art_UUID.html"""
        url = "https://www.miit.gov.cn/zwgk/zcwj/wjfb/tz/art/2026/art_561068ed19ea491a9c6bd0a0909c1466.html"
        assert _is_news_like_url(url)

    def test_pattern_c_content_post(self):
        """模式 C: 广东 /content/post_ID.html"""
        url = "http://gdii.gd.gov.cn/gdywdt/ttxw/content/post_4886189.html"
        assert _is_news_like_url(url)

    def test_pattern_d_yyyymmdd_dir(self):
        """模式 D: 上海 /zxxx/YYYYMMDD/uuid.html"""
        url = "http://www.sheitc.sh.gov.cn/zxxx/20260417/abc123.html"
        assert _is_news_like_url(url)

    def test_pattern_e_hyphenated(self):
        """模式 E: 新疆兵团 /c/YYYY-MM-DD/ID.shtml"""
        url = "http://btgxj.xjbt.gov.cn/c/2026-04-17/8480381.shtml"
        assert _is_news_like_url(url)

    def test_pattern_f_long_timestamp(self):
        """模式 F: 辽宁 /YYYYMMDDHHMMSS.../index.shtml"""
        url = "http://gxt.ln.gov.cn/2026031009183942113/index.shtml"
        assert _is_news_like_url(url)

    def test_pattern_g_guangxi_trs(self):
        """模式 G: 广西 /tNNNNNNN.shtml"""
        url = "http://gxt.gxzf.gov.cn/ydpt/t1232694.shtml"
        assert _is_news_like_url(url)

    def test_pattern_h_spa_query(self):
        """模式 H: 西藏 /detail?id=N"""
        url = "http://jxt.xizang.gov.cn/xzweb/detail?id=12345"
        assert _is_news_like_url(url)

    def test_pattern_qinghai_system(self):
        """青海 /system/YYYY/MM/DD/ID.shtml"""
        url = "http://www.qinghai.gov.cn/zwgk/system/2026/04/16/030097355.shtml"
        assert _is_news_like_url(url)

    def test_year_directory_index_rejected(self):
        """仅 /YYYY/ 栏目页（无文章文件名）不应被误判为新闻。"""
        url = "https://example.com/news/2026/index.html"
        assert not _is_news_like_url(url)

    def test_year_directory_article_with_id_accepted(self):
        """仅 /YYYY/ 路径但文件名含 ID 的文章页应被识别。"""
        url = "https://example.com/news/2026/article_987654.html"
        assert _is_news_like_url(url)

    def test_non_news_rejected(self):
        """非新闻链接（导航、索引页等）应被拒绝。"""
        urls = [
            "https://www.miit.gov.cn/",
            "https://www.miit.gov.cn/zwgk/",
            "https://example.com/about.html",
            "https://www.miit.gov.cn/col/col1234/index.html",
        ]
        for url in urls:
            assert not _is_news_like_url(url), f"应被拒绝: {url}"


class TestDateExtractionFromUrl:
    """测试 _extract_date_from_url 对各种 URL 格式的日期提取。"""

    def test_trs_filename(self):
        """TRS 文件名 tYYYYMMDD_ID.html → 精确日期"""
        url = "https://jxj.beijing.gov.cn/jxdt/tzgg/202604/t20260418_4591280.html"
        assert _extract_date_from_url(url) == "2026-04-18"

    def test_trs_shtml(self):
        """TRS 文件名 .shtml 扩展名"""
        url = "http://gxt.hunan.gov.cn/xxgk/202604/t20260415_12345678.shtml"
        assert _extract_date_from_url(url) == "2026-04-15"

    def test_egov_art_date(self):
        """E-Gov /art/YYYY/M/D/ 非零填充日期"""
        url = "http://gxt.jiangsu.gov.cn/art/2026/4/3/art_73259_11524668.html"
        assert _extract_date_from_url(url) == "2026-04-03"

    def test_slash_separated_date(self):
        """斜杠分隔 /YYYY/MM/DD/ 日期（青海）"""
        url = "http://www.qinghai.gov.cn/zwgk/system/2026/04/16/030097355.shtml"
        assert _extract_date_from_url(url) == "2026-04-16"

    def test_hyphen_date(self):
        """连字符 /YYYY-MM-DD/ 日期（新疆兵团）"""
        url = "http://btgxj.xjbt.gov.cn/c/2026-04-17/8480381.shtml"
        assert _extract_date_from_url(url) == "2026-04-17"

    def test_long_timestamp(self):
        """长时间戳前 8 位（辽宁）"""
        url = "http://gxt.ln.gov.cn/2026031009183942113/index.shtml"
        assert _extract_date_from_url(url) == "2026-03-10"

    def test_yyyymmdd_dir(self):
        """8 位日期目录（上海）"""
        url = "http://www.sheitc.sh.gov.cn/zxxx/20260417/abc.html"
        assert _extract_date_from_url(url) == "2026-04-17"

    def test_yyyymm_fallback(self):
        """仅 YYYYMM 目录时日默认 01"""
        url = "http://gxt.example.gov.cn/news/202604/content.html"
        assert _extract_date_from_url(url) == "2026-04-01"

    def test_no_date(self):
        """无日期的 URL 返回 None"""
        url = "http://gdii.gd.gov.cn/content/post_4886189.html"
        assert _extract_date_from_url(url) is None


class TestNavTextFilter:
    """测试导航文字过滤。"""

    def test_nav_words_rejected(self):
        for text in ["首页", "更多", "English", "搜索", "下一页"]:
            assert _is_nav_text(text), f"应为导航文字: {text}"

    def test_pagination_rejected(self):
        for text in ["1", "2 3 4", ">>", "..."]:
            assert _is_nav_text(text), f"应为分页: {text}"

    def test_real_title_accepted(self):
        assert not _is_nav_text("关于印发数字化转型实施方案的通知")


class TestCleanTitle:
    """
    测试 _clean_title / _strip_date_affixes 标题清理功能。

    对应问题案例：
      案例1/5 - 正文内容混入标题（body content bleed）
      案例2   - 标题截断（...）+ 方括号日期后缀
      案例3   - YYYY-MM-DD 日期后缀
      案例4   - MM-DD 短日期后缀
      案例5   - DD YYYY-MM 日期前缀 + 正文混入
    """

    def _make_a(self, inner_html="", title_attr=""):
        """构造用于测试的 <a> BeautifulSoup 元素。"""
        from bs4 import BeautifulSoup
        attrs = f' title="{title_attr}"' if title_attr else ""
        soup = BeautifulSoup(f'<a href="/test"{attrs}>{inner_html}</a>', "lxml")
        return soup.find("a")

    # ── _strip_date_affixes 单元测试 ─────────────────────────────────────────

    def test_strip_trailing_full_date(self):
        """案例3：后缀 YYYY-MM-DD 被去除。"""
        result = _strip_date_affixes(
            "湖北省智能体公共服务平台启动建设 AI产业有了\u201c公共底座\u201d！ 2026-04-17"
        )
        assert result == "湖北省智能体公共服务平台启动建设 AI产业有了\u201c公共底座\u201d！"

    def test_strip_trailing_short_date(self):
        """案例4：后缀 MM-DD 被去除。"""
        result = _strip_date_affixes("关于举办2026年智能制造专题培训班的通知 04-16")
        assert result == "关于举办2026年智能制造专题培训班的通知"

    def test_strip_trailing_bracketed_date(self):
        """案例2：后缀 [ YYYY-MM-DD ]（含空格方括号）被去除。"""
        result = _strip_date_affixes(
            "一图读懂《关于印发重庆市加快构建开源鸿蒙应用创新生态工作方案... [ 2026-04-15 ]"
        )
        assert result == "一图读懂《关于印发重庆市加快构建开源鸿蒙应用创新生态工作方案..."

    def test_strip_leading_date_prefix(self):
        """案例5：前缀 DD YYYY-MM 被去除。"""
        result = _strip_date_affixes(
            "16 2026-04 【关于加快推进新型工业化全面落实在重庆大地上】全链发力"
        )
        assert result == "【关于加快推进新型工业化全面落实在重庆大地上】全链发力"

    def test_strip_leading_full_date_prefix(self):
        """前缀 YYYY-MM-DD（完整日期）被去除。"""
        result = _strip_date_affixes("2026-04-17 关于印发数字化转型实施方案的通知")
        assert result == "关于印发数字化转型实施方案的通知"

    def test_clean_text_unchanged(self):
        """干净标题不被修改。"""
        title = "关于印发数字化转型实施方案的通知"
        assert _strip_date_affixes(title) == title

    def test_year_in_title_not_stripped(self):
        """标题中间的年份数字不被误删（如"2026年"不含连字符）。"""
        title = "关于举办2026年智能制造专题培训班的通知"
        assert _strip_date_affixes(title) == title

    # ── _clean_title 集成测试 ────────────────────────────────────────────────

    def test_title_attr_preferred_over_gettext(self):
        """案例1/5：有 title 属性时优先使用，忽略含正文的 get_text 结果。"""
        a = self._make_a(
            inner_html=(
                "<span>重庆市加快构建开源鸿蒙应用创新生态工作方案印发</span>"
                "<span>近日，重庆市经济和信息化委员会印发《重庆市加快构建开源鸿蒙应用创新生态工作方案》…</span>"
            ),
            title_attr="重庆市加快构建开源鸿蒙应用创新生态工作方案印发",
        )
        assert _clean_title(a) == "重庆市加快构建开源鸿蒙应用创新生态工作方案印发"

    def test_date_stripped_when_no_title_attr(self):
        """案例3：无 title 属性时，后缀日期从 get_text 结果中去除。"""
        a = self._make_a(
            "湖北省智能体公共服务平台启动建设 AI产业有了\u201c公共底座\u201d！ 2026-04-17"
        )
        assert _clean_title(a) == "湖北省智能体公共服务平台启动建设 AI产业有了\u201c公共底座\u201d！"

    def test_short_date_stripped(self):
        """案例4：后缀 MM-DD 从 get_text 结果中去除。"""
        a = self._make_a("关于举办2026年智能制造专题培训班的通知 04-16")
        assert _clean_title(a) == "关于举办2026年智能制造专题培训班的通知"

    def test_bracketed_date_stripped(self):
        """案例2：后缀 [ YYYY-MM-DD ] 从 get_text 结果中去除。"""
        a = self._make_a(
            "一图读懂《关于印发重庆市加快构建开源鸿蒙应用创新生态工作方案... [ 2026-04-15 ]"
        )
        assert _clean_title(a) == "一图读懂《关于印发重庆市加快构建开源鸿蒙应用创新生态工作方案..."

    def test_leading_date_stripped(self):
        """案例5：前缀 DD YYYY-MM 从 get_text 结果中去除。"""
        a = self._make_a(
            "16 2026-04 【关于加快推进新型工业化全面落实在重庆大地上】全链发力"
        )
        assert _clean_title(a) == "【关于加快推进新型工业化全面落实在重庆大地上】全链发力"

    def test_max_length_truncation(self):
        """正文混入时超过 _TITLE_MAX_LEN 的文本被截断。"""
        long_body = "A" * (_TITLE_MAX_LEN + 50)
        a = self._make_a(long_body)
        result = _clean_title(a)
        assert len(result) <= _TITLE_MAX_LEN

    def test_normal_title_preserved(self):
        """正常标题不被修改（无前后缀日期，长度合适）。"""
        a = self._make_a("关于印发数字化转型实施方案的通知")
        assert _clean_title(a) == "关于印发数字化转型实施方案的通知"

    def test_truncated_visible_text_prefers_full_title_attr(self):
        """可见文本被截断为含 ... 时，应返回 title 属性中的完整标题。"""
        a = self._make_a(
            inner_html="<span>一图读懂《关于印发重庆市加快构建开源鸿蒙应用创新生态工作方案...</span>",
            title_attr="一图读懂《关于印发重庆市加快构建开源鸿蒙应用创新生态工作方案》的通知",
        )
        assert _clean_title(a) == "一图读懂《关于印发重庆市加快构建开源鸿蒙应用创新生态工作方案》的通知"

    def test_truncated_visible_text_uses_title_attr_and_strips_bracketed_date(self):
        """title 属性提供完整标题时，仍应清理其中附带的方括号日期。"""
        a = self._make_a(
            inner_html="<span>一图读懂《关于印发重庆市加快构建开源鸿蒙应用创新生态工作方案...</span>",
            title_attr="一图读懂《关于印发重庆市加快构建开源鸿蒙应用创新生态工作方案》的通知 [ 2026-04-15 ]",
        )
        assert _clean_title(a) == "一图读懂《关于印发重庆市加快构建开源鸿蒙应用创新生态工作方案》的通知"

    # ── 新增：哈电集团 YYYY.MM DD 前缀格式 ──────────────────────────────────────

    def test_strip_leading_date_dot_format(self):
        """哈电集团格式前缀 YYYY.MM DD 被去除。"""
        result = _strip_date_affixes("2026.04 14 袁野赴中国电信调研财务数智化转型工作进展")
        assert result == "袁野赴中国电信调研财务数智化转型工作进展"

    def test_clean_title_harbin_electric_prefix(self):
        """_clean_title 应去除哈电集团格式的 YYYY.MM DD 前缀日期。"""
        a = self._make_a("2026.04 14 袁野赴中国电信调研财务数智化转型工作进展")
        assert _clean_title(a) == "袁野赴中国电信调研财务数智化转型工作进展"

    # ── 新增：招商局集团 MM/DD YYYY 后缀格式 ─────────────────────────────────────

    def test_strip_trailing_slash_date_with_year(self):
        """招商局格式后缀 MM/DD YYYY 被去除。"""
        result = _strip_date_affixes("灵卫·智能巡检机器人亮相香港国际创科展 04/16 2026")
        assert result == "灵卫·智能巡检机器人亮相香港国际创科展"

    def test_strip_trailing_slash_date_without_year(self):
        """招商局格式后缀 MM/DD（无年份）被去除。"""
        result = _strip_date_affixes("灵卫·智能巡检机器人亮相河套AI社区产业生态大会 04/05 ")
        assert result == "灵卫·智能巡检机器人亮相河套AI社区产业生态大会"

    # ── 新增：招商局 标题+摘要 混合文本截断 ──────────────────────────────────────

    def test_excerpt_body_stripped_from_title(self):
        """招商局格式：标题后的正文摘要（以 N月N日， 起始）应被截断。"""
        mixed = (
            "灵卫 · 智能巡检机器人亮相香港国际创科展"
            " 4月13日，招商局狮子山人工智能实验室携灵卫智能巡检机器人亮相香港国际创科展。"
            "展会期间，香港特别行政区政府财政司司长陈茂波，香港特别行政区政府创新科技... 04/16 2026"
        )
        a = self._make_a(mixed)
        result = _clean_title(a)
        assert result == "灵卫 · 智能巡检机器人亮相香港国际创科展"

    def test_excerpt_body_stripped_case2(self):
        """招商局格式（案例2）：标题+摘要混合文本截断。"""
        mixed = (
            "灵卫 · 智能巡检机器人亮相河套AI社区产业生态大会"
            " 4月1日，河套模力福地 AI 社区产业生态大会在深圳市举行，"
            "招商局狮子山人工智能实验室携灵卫智能巡检机器人亮相本次大会，"
            "重点展示了具身智能创新产品在智慧社... 04/05 "
        )
        a = self._make_a(mixed)
        result = _clean_title(a)
        assert result == "灵卫 · 智能巡检机器人亮相河套AI社区产业生态大会"

    def test_title_with_date_middle_not_stripped(self):
        """标题中含有年份数字但不在摘要起始位置的不被误截断。"""
        # 标题中的"4月19日"后面没有逗号/顿号，不触发截断
        title = "关于2026年4月19日发布数字化政策的通知"
        a = self._make_a(title)
        assert _clean_title(a) == title


# ─── 新增：_extract_date_from_context 扩展测试 ─────────────────────────────────

class TestExtractDateFromContext:
    """测试 _extract_date_from_context 对多种 HTML 布局的日期提取。"""

    def _make_context(self, html: str):
        """构造带上下文的 BeautifulSoup 元素，返回其中的 <a> 标签。"""
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "lxml")
        return soup.find("a")

    def test_date_in_em_element(self):
        """日期在 <em> 元素中应被提取。"""
        a = self._make_context(
            "<li><a href='/info/123.htm'>智能制造新政发布</a><em>2026-04-15</em></li>"
        )
        result = _extract_date_from_context(a)
        assert result == "2026-04-15"

    def test_date_in_i_element(self):
        """日期在 <i> 元素中应被提取。"""
        a = self._make_context(
            "<li><a href='/info/123.htm'>数字化转型通知</a><i>2026-04-14</i></li>"
        )
        result = _extract_date_from_context(a)
        assert result == "2026-04-14"

    def test_date_in_table_sibling_td(self):
        """表格布局：日期在兄弟 <td> 中应被提取。"""
        a = self._make_context(
            "<tr><td><a href='/news/123.htm'>人工智能赋能制造业</a></td>"
            "<td>2026-04-15</td></tr>"
        )
        result = _extract_date_from_context(a)
        assert result == "2026-04-15"

    def test_existing_span_still_works(self):
        """原有 <span> 日期提取逻辑仍然正常工作。"""
        a = self._make_context(
            "<li><a href='/art/2026/4/13/art_100.html'>工信部印发方案</a>"
            "<span>[2026-04-13]</span></li>"
        )
        result = _extract_date_from_context(a)
        assert result == "2026-04-13"

    def test_mm_dd_uses_passed_now_year(self):
        """无年份 MM-DD 日期应使用调用方传入 now 的年份。"""
        a = self._make_context(
            "<li><a href='/news/123.html'>智能制造新政发布</a><span>[04-16]</span></li>"
        )
        custom_now = datetime(2024, 5, 1, tzinfo=SG_TZ)
        result = _extract_date_from_context(a, now=custom_now)
        assert result == "2024-04-16"


# ═══════════════════════════════════════════════════════════════════════════════
# 2. NDRC 解析器测试
# ═══════════════════════════════════════════════════════════════════════════════

class TestNdrcParser:
    """测试 NDRC（发改委）解析器，使用保存的 HTML。"""

    def _run(self):
        from parsers.ndrc import parse_ndrc_home
        html = _load_html("ndrc_home.html")
        if not html:
            pytest.skip("缺少测试 HTML 文件: ndrc_home.html")
        with patch("parsers.ndrc.http_get", return_value=html):
            return parse_ndrc_home(BASE_CONFIG, NOW)

    def test_returns_items(self):
        """应返回非空 Item 列表"""
        items = self._run()
        assert len(items) > 0, "NDRC 应返回至少 1 条新闻"

    def test_items_have_dates(self):
        """绝大多数 Item 应有发布日期"""
        items = self._run()
        with_date = sum(1 for it in items if it.pub_date)
        ratio = with_date / len(items) if items else 0
        assert ratio >= 0.8, f"日期覆盖率 {ratio:.0%} 低于 80%"

    def test_item_fields_valid(self):
        """每个 Item 的必填字段应非空"""
        items = self._run()
        for it in items:
            assert it.title and len(it.title) >= 6
            assert it.url.startswith("http")
            assert it.publisher == "国家发展和改革委员会"
            assert it.source.startswith("发改委官网-")
            assert it.fetched_at

    def test_covers_multiple_sections(self):
        """应覆盖多个板块"""
        items = self._run()
        sections = {it.source for it in items}
        assert len(sections) >= 3, f"仅覆盖 {len(sections)} 个板块: {sections}"

    def test_new_sections_covered(self):
        """验证新增的板块（视频发改、委属单位、发改数据、互动交流）"""
        from parsers.ndrc import parse_ndrc_home
        html = _load_html("ndrc_home.html")
        if not html:
            pytest.skip("缺少测试 HTML 文件: ndrc_home.html")
        # 使用超宽松配置（不过滤关键词）来检查板块覆盖
        wide_config = {**BASE_CONFIG, "keywords": MATCH_ALL_KEYWORDS, "window_days": 365, "hard_cap_days": 365}
        with patch("parsers.ndrc.http_get", return_value=html):
            items = parse_ndrc_home(wide_config, NOW)
        sources = {it.source for it in items}
        expected_sections = {"视频发改", "委属单位", "发改数据", "互动交流"}
        missing_sections = {name for name in expected_sections if not any(name in source for source in sources)}
        assert not missing_sections, (
            f"解析结果未覆盖新增板块: {sorted(missing_sections)}；"
            f"当前 source: {sorted(sources)}"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# 3. MOST 解析器测试
# ═══════════════════════════════════════════════════════════════════════════════

class TestMostParser:
    """测试 MOST（科技部）解析器。"""

    def _run(self):
        from parsers.most import parse_most_home
        html = _load_html("most_home.html")
        if not html:
            pytest.skip("缺少测试 HTML 文件: most_home.html")
        with patch("parsers.most.http_get", return_value=html):
            return parse_most_home(BASE_CONFIG, NOW)

    def test_returns_items(self):
        items = self._run()
        assert len(items) > 0, "MOST 应返回至少 1 条新闻"

    def test_items_have_dates(self):
        items = self._run()
        with_date = sum(1 for it in items if it.pub_date)
        ratio = with_date / len(items) if items else 0
        assert ratio >= 0.7, f"日期覆盖率 {ratio:.0%} 低于 70%"

    def test_external_media_classified(self):
        """外部媒体链接（人民日报、新华社等）应被正确分类"""
        from parsers.most import _classify_link
        test_hrefs = [
            ("https://paper.people.com.cn/rmrb/html/2026/0414/test.htm", "科技部官网-媒体聚焦"),
            ("https://www.news.cn/fortune/20260416/test.htm", "科技部官网-媒体聚焦"),
            ("https://news.cctv.com/2026/04/14/test.shtml", "科技部官网-媒体聚焦"),
            ("https://www.gov.cn/yaowen/liebiao/202604/content_7066001.htm", "科技部官网-要闻"),
        ]
        for href, expected_tag in test_hrefs:
            tag = _classify_link(href)
            assert tag == expected_tag, f"{href} → {tag}，期望 {expected_tag}"

    def test_item_fields_valid(self):
        items = self._run()
        for it in items:
            assert it.title and len(it.title) >= 6
            assert it.url.startswith("http")
            assert it.publisher == "科学技术部"


# ═══════════════════════════════════════════════════════════════════════════════
# 4. MOE 解析器测试
# ═══════════════════════════════════════════════════════════════════════════════

class TestMoeParser:
    """测试 MOE（教育部）解析器。"""

    def _run(self):
        from parsers.moe import parse_moe_news
        html = _load_html("moe_news.html")
        if not html:
            pytest.skip("缺少测试 HTML 文件: moe_news.html")
        with patch("parsers.moe.http_get", return_value=html):
            return parse_moe_news(BASE_CONFIG, NOW)

    def test_returns_items(self):
        items = self._run()
        assert len(items) > 0, "MOE 应返回至少 1 条新闻"

    def test_new_sections_classified(self):
        """新增板块（发布会、图解教育、图说新闻）应被正确分类"""
        from parsers.moe import _classify_section
        test_hrefs = [
            ("http://www.moe.gov.cn/jyb_xwfb/xw_fbh/moe_2069/test.html", "发布会/通气会"),
            ("http://www.moe.gov.cn/jyb_xwfb/s7600/202604/test.html", "图解教育"),
            ("http://www.moe.gov.cn/jyb_xwfb/s5984/xw_tsxwft/202604/test.html", "图说新闻"),
        ]
        for href, expected_label in test_hrefs:
            label = _classify_section(href)
            assert label == expected_label, f"{href} → {label}，期望 {expected_label}"

    def test_date_extraction_precise_url(self):
        """URL 含 tYYYYMMDD_ 时应提取精确日期"""
        from parsers.moe import _extract_precise_date_from_url
        url = "http://www.moe.gov.cn/jyb_xwfb/gzdt_gzdt/202604/t20260418_1234567.html"
        assert _extract_precise_date_from_url(url) == "2026-04-18"

    def test_date_extraction_yyyymm_only_url(self):
        """URL 仅含 /YYYYMM/ 时，精确提取返回 None，回退返回 YYYY/M/1"""
        from parsers.moe import _extract_precise_date_from_url, _extract_yyyymm_from_url
        url = "http://www.moe.gov.cn/jyb_xwfb/gzdt_gzdt/202604/content_abc.html"
        assert _extract_precise_date_from_url(url) is None
        assert _extract_yyyymm_from_url(url) == "2026-04-01"

    def test_date_extraction_prefers_text_over_yyyymm(self):
        """对于仅有 YYYYMM 的 URL，附近文本的 MM-DD 应优先于 YYYY/M/1 回退"""
        from parsers.moe import parse_moe_news
        # 构造一段包含 /YYYYMM/ URL 但页面文本含精确 MM-DD 的 HTML
        html = """<html><body>
            <ul>
              <li><a href="/jyb_xwfb/gzdt_gzdt/202604/content_abc.html">关于推进人工智能教育工作的通知</a>
                  <span>04-15</span></li>
            </ul>
        </body></html>"""
        with patch("parsers.moe.http_get", return_value=html):
            items = parse_moe_news(
                {**BASE_CONFIG, "keywords": MATCH_ALL_KEYWORDS, "window_days": 365, "hard_cap_days": 365},
                NOW,
            )
        assert items, "应解析出至少 1 条"
        # 日期应来自附近文本 04-15，而非 URL 回退的 04-01
        assert items[0].pub_date == "2026-04-15", (
            f"日期应为 2026-04-15（来自附近文本），实际为 {items[0].pub_date}"
        )

    def test_item_fields_valid(self):
        items = self._run()
        for it in items:
            assert it.title and len(it.title) >= 6
            assert it.url.startswith("http")
            assert it.publisher == "教育部"
            assert it.source.startswith("教育部官网-")


# ═══════════════════════════════════════════════════════════════════════════════
# 5. miit_local 端到端测试
# ═══════════════════════════════════════════════════════════════════════════════

class TestMiitLocal:
    """测试 miit_local 解析器（使用单个省份的保存 HTML）。"""

    def _run_single(self, province_file, source_dict, keywords=None):
        """对单个省份文件运行抓取逻辑。"""
        from parsers.miit_local import _scrape_one_site
        from utils import format_fetched_at
        html = _load_html(f"miit_local/{province_file}")
        if not html or len(html) < 1000:
            pytest.skip(f"缺少测试 HTML 文件: miit_local/{province_file}")
        fetched_at = format_fetched_at(NOW)
        kw = keywords if keywords is not None else BASE_CONFIG["keywords"]
        with patch("parsers.miit_local.http_get", return_value=html):
            return _scrape_one_site(
                source=source_dict,
                fetched_at=fetched_at,
                keywords=kw,
                now=NOW,
                window_days=BASE_CONFIG["window_days"],
                hard_cap_days=BASE_CONFIG["hard_cap_days"],
                timeout=20,
            )

    def test_beijing_has_items(self):
        items = self._run_single(
            "beijing.html",
            {
                "province": "北京", "name": "北京市经济和信息化局",
                "url": "https://jxj.beijing.gov.cn/jxdt/tzgg/",
            },
            keywords=MATCH_ALL_KEYWORDS,  # 宽松关键词：匹配所有标题
        )
        assert isinstance(items, list), "应返回列表"
        assert len(items) > 0, "使用宽松关键词时北京工信应返回至少 1 条链接"

    def test_jiangsu_art_pattern(self):
        """江苏使用 /art/YYYY/M/D/ 模式，应正确识别。"""
        from parsers.miit_local import _is_news_like_url, _extract_date_from_url
        url = "http://gxt.jiangsu.gov.cn/art/2026/4/13/art_73259_11524668.html"
        assert _is_news_like_url(url)
        assert _extract_date_from_url(url) == "2026-04-13"

    def test_shanghai_yyyymmdd(self):
        """上海使用 /YYYYMMDD/ 目录，应正确提取日期。"""
        from parsers.miit_local import _extract_date_from_url
        url = "http://www.sheitc.sh.gov.cn/zxxx/20260417/uuid.html"
        assert _extract_date_from_url(url) == "2026-04-17"

    def test_xizang_spa(self):
        """西藏使用 /detail?id=N SPA 链接，应正确识别。"""
        url = "http://jxt.xizang.gov.cn/xzweb/detail?id=12345"
        assert _is_news_like_url(url)

    def test_miit_art_uuid_variant(self):
        """miit.gov.cn 的 /art/YYYY/art_UUID.html 变体应被识别。"""
        url = "https://www.miit.gov.cn/jgsj/zfs/gzdt/art/2023/art_c1247f6f0eca440883ed83b3f97fd716.html"
        assert _is_news_like_url(url)


# ═══════════════════════════════════════════════════════════════════════════════
# 6. gov_local 端到端测试
# ═══════════════════════════════════════════════════════════════════════════════

class TestGovLocal:
    """测试 gov_local 解析器（使用单个省份的保存 HTML）。"""

    def _run_single(self, province_file, source_dict):
        from parsers.gov_local import _scrape_one_gov_site
        from utils import format_fetched_at
        html = _load_html(f"gov_local/{province_file}")
        if not html or len(html) < 1000:
            pytest.skip(f"缺少测试 HTML 文件: gov_local/{province_file}")
        fetched_at = format_fetched_at(NOW)
        with patch("parsers.gov_local.http_get", return_value=html):
            return _scrape_one_gov_site(
                source=source_dict,
                fetched_at=fetched_at,
                keywords=BASE_CONFIG["keywords"],
                now=NOW,
                window_days=BASE_CONFIG["window_days"],
                hard_cap_days=BASE_CONFIG["hard_cap_days"],
                timeout=20,
            )

    def test_beijing_gov(self):
        items = self._run_single("beijing.html", {
            "province": "北京", "name": "北京市人民政府",
            "url": "https://www.beijing.gov.cn/",
        })
        assert isinstance(items, list)
        for it in items:
            assert it.source == "地方政府-北京"
            assert it.publisher == "北京市人民政府"

    def test_qinghai_slash_date(self):
        """青海 /system/YYYY/MM/DD/ 日期应精确到日。"""
        items = self._run_single("qinghai.html", {
            "province": "青海", "name": "青海省人民政府",
            "url": "https://www.qinghai.gov.cn/",
        })
        # 检查日期是否精确到日（不是 YYYY/M/1 的 YYYYMM 回退）
        for it in items:
            if it.pub_date and it.pub_date.endswith("/1"):
                # 对于 YYYYMM 回退的情况，可以接受
                pass
            elif it.pub_date:
                parts = it.pub_date.split("/")
                assert len(parts) == 3 and parts[2] != "0"

    def test_shandong_egov(self):
        """山东使用 E-Gov /art/ 模式。"""
        items = self._run_single("shandong.html", {
            "province": "山东", "name": "山东省人民政府",
            "url": "https://www.shandong.gov.cn/",
        })
        assert isinstance(items, list)

    def test_xinjiang_bt_hyphen(self):
        """新疆兵团使用 /c/YYYY-MM-DD/ 模式。"""
        items = self._run_single("xinjiang_bt.html", {
            "province": "新疆兵团", "name": "新疆生产建设兵团",
            "url": "https://www.xjbt.gov.cn/",
        })
        assert isinstance(items, list)

    def test_disabled_returns_empty(self):
        """disabled 时应返回空列表。"""
        from parsers.gov_local import parse_gov_local
        config = {**BASE_CONFIG}
        config["sources"] = {**config["sources"]}
        config["sources"]["gov_local"] = {"name": "地方政府门户", "enabled": False}
        result = parse_gov_local(config, NOW)
        assert result == []


# ═══════════════════════════════════════════════════════════════════════════════
# 7. SASAC 解析器测试
# ═══════════════════════════════════════════════════════════════════════════════

class TestSasacParser:
    """测试 SASAC（国资委）解析器。"""

    _SASAC_HTML = """<html><body>
        <ul>
          <li>
            <a href="/n2588025/n2588129/c35407052/content.html"
               title="关于推进智能制造数字化转型的通知">
              关于推进智能制造数字化转型的通知
            </a>
            <span>[04-15]</span>
          </li>
          <li>
            <a href="/n2588025/n2588129/c35407053/content.html"
               title="国资委印发工作方案（仅含印发关键词）">
              国资委印发工作方案（仅含印发关键词）
            </a>
            <span>[04-14]</span>
          </li>
          <li>
            <a href="/n2588025/c35407054/content.html"
               title="关于数字化转型的新闻">
              关于数字化转型的新闻
            </a>
            <span>[12-31]</span>
          </li>
        </ul>
    </body></html>"""

    _SASAC_CONFIG = {
        **BASE_CONFIG,
        "sources": {
            **BASE_CONFIG["sources"],
            "sasac_home": {"name": "国务院国有资产监督管理委员会", "url": "http://www.sasac.gov.cn/"},
        },
        "window_days": 365,
        "hard_cap_days": 365,
    }

    def _run(self, keywords=None):
        from parsers.sasac import parse_sasac_home
        config = dict(self._SASAC_CONFIG)
        if keywords is not None:
            config = {**config, "keywords": keywords}
        with patch("parsers.sasac.http_get", return_value=self._SASAC_HTML):
            return parse_sasac_home(config, NOW)

    def test_returns_items(self):
        """基本解析：宽松关键词应返回至少 1 条新闻。"""
        items = self._run(keywords=MATCH_ALL_KEYWORDS)
        assert len(items) > 0, "SASAC 应返回至少 1 条新闻"

    def test_item_fields_valid(self):
        """每个 Item 的必填字段应非空。"""
        items = self._run(keywords=MATCH_ALL_KEYWORDS)
        for it in items:
            assert it.title and len(it.title) >= 6
            assert it.url.startswith("http")
            assert it.publisher == "国务院国有资产监督管理委员会"
            assert it.source == "国资委官网"
            assert it.fetched_at

    def test_miit_only_keywords_excluded(self):
        """仅含 MIIT_ONLY_KEYWORDS 的标题不应命中。"""
        # 标题"国资委印发工作方案（仅含印发关键词）"只含"印发"，应被排除
        items = self._run(keywords=list(MIIT_ONLY_KEYWORDS))
        titles = [it.title for it in items]
        assert not any("印发" in t and "智能" not in t and "数字" not in t for t in titles), (
            "非 MIIT 信源不应通过 MIIT_ONLY_KEYWORDS 过滤"
        )

    def test_cross_year_date_fix(self):
        """MM-DD 跨年修正：若补全后日期晚于今日，应回退 1 年。"""
        from parsers.sasac import _extract_date_from_context
        from bs4 import BeautifulSoup
        # 构造含 12-31 的 <a> + <span> 结构
        html = '<a href="/n1/c1/content.html">标题</a><span>[12-31]</span>'
        soup = BeautifulSoup(html, "lxml")
        a_tag = soup.find("a")
        # NOW 为 2026-04-18，补 2026 后 2026-12-31 > 今日，应回退到 2025
        result = _extract_date_from_context(a_tag, NOW.year, NOW.date())
        assert result == "2025-12-31", f"跨年修正失败：期望 2025-12-31，得到 {result}"

    def test_url_filter_rejects_non_channel_content(self):
        """非 Channel-Content 格式的 URL 应被过滤。"""
        from parsers.sasac import parse_sasac_home
        html = """<html><body>
            <a href="/n2588025/index.html" title="首页导航">首页导航导航导航</a>
            <a href="/n2588025/c99/content.html" title="数字化转型通知链接">数字化转型通知链接</a>
        </body></html>"""
        config = {
            **self._SASAC_CONFIG,
            "keywords": MATCH_ALL_KEYWORDS,
            "window_days": 365, "hard_cap_days": 365,
        }
        with patch("parsers.sasac.http_get", return_value=html):
            items = parse_sasac_home(config, NOW)
        urls = [it.url for it in items]
        assert not any("/index.html" in u for u in urls), "非新闻链接不应被返回"


# ═══════════════════════════════════════════════════════════════════════════════
# 8. NDA 解析器测试
# ═══════════════════════════════════════════════════════════════════════════════

class TestNdaParser:
    """测试 NDA（国家数据局）解析器。"""

    _NDA_HTML = """<html><body>
        <ul>
          <li>
            <a href="/sjj/swdt/xwfb/0415/20260415190239098954885_pc.html">
              关于推进数字化转型的通知
            </a>
            <span>2026.04.15</span>
          </li>
          <li>
            <a href="/sjj/zwgk/zcfb/0410/20260410120000000000000_pc.html">
              印发体系建设专项计划（仅含印发关键词）
            </a>
            <span>2026.04.10</span>
          </li>
          <li>
            <a href="/sjj/swdt/xwfb/0416/20260416100000000000001_pc.html">
              数据要素市场化配置改革进展综述
            </a>
            <span>2026.04.16</span>
          </li>
        </ul>
    </body></html>"""

    _NDA_CONFIG = {
        **BASE_CONFIG,
        "sources": {
            **BASE_CONFIG["sources"],
            "nda_home": {"name": "国家数据局", "url": "https://www.nda.gov.cn/sjj/index_pc.html"},
        },
        "window_days": 365,
        "hard_cap_days": 365,
    }

    def _run(self, keywords=None):
        from parsers.nda import parse_nda_home
        config = dict(self._NDA_CONFIG)
        if keywords is not None:
            config = {**config, "keywords": keywords}
        with patch("parsers.nda.http_get", return_value=self._NDA_HTML):
            return parse_nda_home(config, NOW)

    def test_returns_items(self):
        """基本解析：宽松关键词应返回至少 1 条新闻。"""
        items = self._run(keywords=MATCH_ALL_KEYWORDS)
        assert len(items) > 0, "NDA 应返回至少 1 条新闻"

    def test_item_fields_valid(self):
        """每个 Item 的必填字段应非空。"""
        items = self._run(keywords=MATCH_ALL_KEYWORDS)
        for it in items:
            assert it.title and len(it.title) >= 6
            assert it.url.startswith("http")
            assert it.publisher == "国家数据局"
            assert it.fetched_at

    def test_date_extracted_from_url(self):
        """日期应从 URL 文件名（YYYYMMDD 前缀）中正确提取。"""
        items = self._run(keywords=MATCH_ALL_KEYWORDS)
        with_date = [it for it in items if it.pub_date]
        assert len(with_date) > 0, "至少应有 1 条带日期的新闻"
        for it in with_date:
            assert it.pub_date.startswith("2026-04-"), f"日期格式异常: {it.pub_date}"

    def test_section_classified(self):
        """source 字段应包含板块名称。"""
        items = self._run(keywords=MATCH_ALL_KEYWORDS)
        for it in items:
            assert it.source.startswith("国家数据局-"), f"source 字段异常: {it.source}"

    def test_miit_only_keywords_excluded(self):
        """仅含 MIIT_ONLY_KEYWORDS（'印发'/'体系建设'）的标题不应命中。"""
        items = self._run(keywords=list(MIIT_ONLY_KEYWORDS))
        assert len(items) == 0, (
            f"非 MIIT 信源不应通过 MIIT_ONLY_KEYWORDS 匹配，实际返回: {[it.title for it in items]}"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# 9. SOE 解析器测试
# ═══════════════════════════════════════════════════════════════════════════════

class TestSoeParser:
    """测试 SOE（央企）解析器。"""

    _SOE_HTML = """<html><body>
        <ul>
          <li>
            <a href="/n2588025/c35407052/content.html">
              关于推进智能制造数字化转型的通知
            </a>
            <span>2026-04-15</span>
          </li>
          <li>
            <a href="/art/2026/4/15/art_100_200.html">
              人工智能赋能工业数字化转型
            </a>
          </li>
          <li>
            <a href="/">首页</a>
          </li>
          <li>
            <a href="/about.html">关于我们</a>
          </li>
        </ul>
    </body></html>"""

    _SOE_CONFIG = {
        **BASE_CONFIG,
        "sources": {
            **BASE_CONFIG["sources"],
            "soe": {"name": "中央企业", "enabled": True},
        },
        "window_days": 365,
        "hard_cap_days": 365,
        "soe_timeout": 5,
    }

    def test_channel_content_url_recognized(self):
        """Channel-Content TRS URL 应被识别为新闻链接。"""
        from parsers.soe import _is_soe_news_url
        assert _is_soe_news_url("http://www.example.com/n2588025/c35407052/content.html")

    def test_egov_art_url_recognized(self):
        """E-Gov /art/ URL 应被识别为新闻链接。"""
        from parsers.soe import _is_soe_news_url
        assert _is_soe_news_url("http://www.example.com/art/2026/4/15/art_100_200.html")

    def test_numeric_id_url_recognized(self):
        """纯数字 ID 文件名 URL 应被识别为新闻链接。"""
        from parsers.soe import _is_soe_news_url
        assert _is_soe_news_url("http://www.example.com/group/news/71062.shtml")

    def test_homepage_url_rejected(self):
        """主页 URL 不应被识别为新闻链接。"""
        from parsers.soe import _is_soe_news_url
        assert not _is_soe_news_url("http://www.example.com/")

    def test_about_url_rejected(self):
        """about.html 不应被识别为新闻链接。"""
        from parsers.soe import _is_soe_news_url
        assert not _is_soe_news_url("http://www.example.com/about.html")

    def test_disabled_returns_empty(self):
        """enabled: false 时应返回空列表。"""
        from parsers.soe import parse_soe
        config = {
            **self._SOE_CONFIG,
            "sources": {
                **self._SOE_CONFIG["sources"],
                "soe": {"name": "中央企业", "enabled": False},
            },
        }
        result = parse_soe(config, NOW)
        assert result == []

    def test_with_mock_html(self):
        """注入单个测试站点，使用 mock HTML，应能解析出新闻条目。"""
        from parsers.soe import parse_soe
        test_sources = [{"name": "测试央企", "url": "http://test.example.com/"}]
        with patch("parsers.soe.SOE_SOURCES", test_sources), \
             patch("parsers.soe.http_get", return_value=self._SOE_HTML):
            items = parse_soe(
                {**self._SOE_CONFIG, "keywords": MATCH_ALL_KEYWORDS},
                NOW,
            )
        assert len(items) > 0, "mock HTML 应解析出至少 1 条新闻"
        for it in items:
            assert it.publisher == "测试央企"
            assert it.source == "央企-测试央企"

    def test_miit_only_keywords_excluded(self):
        """仅含 MIIT_ONLY_KEYWORDS 的标题不应命中。"""
        from parsers.soe import parse_soe
        # 构造仅含"印发"/"体系建设"关键词的 HTML
        html_miit_only = """<html><body>
            <a href="/n1/c1/content.html">印发体系建设工作方案通知</a>
        </body></html>"""
        test_sources = [{"name": "测试央企", "url": "http://test.example.com/"}]
        with patch("parsers.soe.SOE_SOURCES", test_sources), \
             patch("parsers.soe.http_get", return_value=html_miit_only):
            items = parse_soe(
                {**self._SOE_CONFIG, "keywords": list(MIIT_ONLY_KEYWORDS)},
                NOW,
            )
        assert len(items) == 0, (
            f"非 MIIT 信源不应通过 MIIT_ONLY_KEYWORDS 匹配，实际返回: {[it.title for it in items]}"
        )

    def test_soe_excluded_keywords(self):
        """仅含 SOE_EXCLUDED_KEYWORDS 的标题不应命中。"""
        from parsers.soe import parse_soe
        # 构造仅含"装备"关键词的 HTML
        html_soe_excluded = """<html><body>
            <a href="/n1/c1/content.html">新型储能装备制造项目开工</a>
        </body></html>"""
        test_sources = [{"name": "测试央企", "url": "http://test.example.com/"}]
        with patch("parsers.soe.SOE_SOURCES", test_sources), \
             patch("parsers.soe.http_get", return_value=html_soe_excluded):
            items = parse_soe(
                {**self._SOE_CONFIG, "keywords": list(SOE_EXCLUDED_KEYWORDS)},
                NOW,
            )
        assert len(items) == 0, (
            f"央企信源不应通过 SOE_EXCLUDED_KEYWORDS 匹配，实际返回: {[it.title for it in items]}"
        )

    def test_exclude_paths_filters_column_sections(self):
        """per-source exclude_paths 应过滤掉指定栏目路径（如 CETC 业务领域）。"""
        from parsers.soe import parse_soe
        # 模拟中国电子科技集团首页：业务领域链接 + 真实新闻链接
        html_mixed = """<html><body>
            <a href="/zgdk/1592960/1592986/1651587/index.html">
              聚焦智慧城市、行业数字化、工业互联网及智能制造，推动国家治理能力提升和产业数字化，从需求牵引到牵引需求
            </a>
            <a href="/zgdk/1592571/1592909/2119043/index.html">
              国务院国资委党委与中央企业党委开展专题联学 为深化拓展"人工智能+"贡献力量
            </a>
        </body></html>"""
        # 注入包含 exclude_paths 的测试源，模拟实际 CETC 配置
        test_sources = [{
            "name": "测试央企",
            "url": "http://test.example.com/",
            "exclude_paths": ["/1592960/1592986/"],
        }]
        with patch("parsers.soe.SOE_SOURCES", test_sources), \
             patch("parsers.soe.http_get", return_value=html_mixed):
            items = parse_soe(
                {**self._SOE_CONFIG, "keywords": MATCH_ALL_KEYWORDS},
                NOW,
            )
        urls = [it.url for it in items]
        # 业务领域路径应被排除
        assert not any("/1592960/1592986/" in u for u in urls), (
            f"exclude_paths 路径不应出现：{[u for u in urls if '/1592960/1592986/' in u]}"
        )
        # 真正的新闻链接应被保留
        assert any("/1592571/" in u for u in urls), "新闻路径应被保留"

    def test_source_without_exclude_paths(self):
        """未配置 exclude_paths 的站点不受影响，所有新闻链接正常抓取。"""
        from parsers.soe import parse_soe
        test_sources = [{"name": "测试央企无排除", "url": "http://test.example.com/"}]
        with patch("parsers.soe.SOE_SOURCES", test_sources), \
             patch("parsers.soe.http_get", return_value=self._SOE_HTML):
            items = parse_soe(
                {**self._SOE_CONFIG, "keywords": MATCH_ALL_KEYWORDS},
                NOW,
            )
        assert len(items) > 0, "无 exclude_paths 时应正常返回新闻"

    def test_contentlist_url_rejected(self):
        """华电 contentList URL（栏目列表页）不应被识别为新闻链接。"""
        from parsers.soe import _is_soe_news_url
        url = "http://www.chd.com.cn/webfront/webpage/web/contentList/channelId/b87052da8ac94affad09200ecceb0279/pageNo/1"
        assert not _is_soe_news_url(url), "contentList URL 应被拒绝"

    def test_contentpage_url_accepted(self):
        """华电 contentPage URL（文章页）应被识别为新闻链接。"""
        from parsers.soe import _is_soe_news_url
        url = "http://www.chd.com.cn/webfront/webpage/web/contentPage/id/50f401fb37024262b88ed7f31ec91d25"
        assert _is_soe_news_url(url), "contentPage URL 应被接受"


# ═══════════════════════════════════════════════════════════════════════════════
# 10. 微博解析器测试
# ═══════════════════════════════════════════════════════════════════════════════

class TestWeiboParser:
    """测试微博解析器的标题提取逻辑。"""

    def _make_mblog(self, text, page_info=None):
        """构造简化的 mblog 字典。"""
        d = {
            "mid": "1234567890",
            "text": text,
            "created_at": "Thu May 14 10:00:00 +0800 2026",
            "source": "微博 weibo.com",
        }
        if page_info:
            d["page_info"] = page_info
        return d

    def test_video_with_real_title(self):
        """视频类型应优先使用 page_info.title 作为标题。"""
        from parsers.weibo import parse_mblog
        mblog = self._make_mblog(
            text='【#街头天降蜜汁竟是蚜虫排泄物#】近日，南京街边...',
            page_info={
                "type": "video",
                "page_url": "https://video.weibo.com/show?fid=1034:xxx",
                "page_title": "南京发布的微博视频",
                "title": "街头天降蜜汁竟是蚜虫排泄物",
                "content1": "南京发布的微博视频",
            },
        )
        result = parse_mblog(mblog)
        assert result is not None
        assert result["title"] == "街头天降蜜汁竟是蚜虫排泄物"
        assert "video.weibo.com" in result["article_url"]

    def test_video_fallback_to_text_when_title_empty(self):
        """视频类型 title 为空时应回退到微博正文，并去除末尾视频标识。"""
        from parsers.weibo import parse_mblog
        mblog = self._make_mblog(
            text='2026重庆国际友好城市合作大会新闻发布会今日下午举行... 重庆发布的微博视频',
            page_info={
                "type": "video",
                "page_url": "https://video.weibo.com/show?fid=1042211:yyy",
                "page_title": "重庆发布的微博视频",
                "title": "",
                "content1": "重庆发布的微博视频",
            },
        )
        result = parse_mblog(mblog)
        assert result is not None
        assert "重庆发布的微博视频" not in result["title"]
        assert "新闻发布会" in result["title"]

    def test_article_uses_content1_not_page_title(self):
        """文章类型应优先使用 content1 而非 page_title（后者通常是发布者名称）。"""
        from parsers.weibo import parse_mblog
        mblog = self._make_mblog(
            text='香蕉是全球最重要的经济作物之一... 香蕉采后病害发生机制研究取得进展',
            page_info={
                "type": "article",
                "page_url": "https://weibo.com/ttarticle/p/show?id=230935xxx",
                "page_title": "中科院之声",
                "content1": "香蕉采后病害发生机制研究取得进展",
            },
        )
        result = parse_mblog(mblog)
        assert result is not None
        assert result["title"] == "香蕉采后病害发生机制研究取得进展"
        assert result["is_article"] is True

    def test_article_fallback_to_page_title(self):
        """文章类型 content1 为空时可回退到 page_title。"""
        from parsers.weibo import parse_mblog
        mblog = self._make_mblog(
            text=' some text ',
            page_info={
                "type": "article",
                "page_url": "https://weibo.com/ttarticle/p/show?id=230935xxx",
                "page_title": "应急管理部",
                "content1": "",
            },
        )
        result = parse_mblog(mblog)
        assert result is not None
        assert result["title"] == "应急管理部"

    def test_plain_weibo_no_page_info(self):
        """普通微博（无 page_info）应截取正文作为标题。"""
        from parsers.weibo import parse_mblog
        mblog = self._make_mblog(
            text='【15日起售！老年旅客可享淡季火车票优惠】为更好服务...'
        )
        result = parse_mblog(mblog)
        assert result is not None
        assert "火车票优惠" in result["title"]
        assert result["article_url"] == ""
        assert result["is_article"] is False

    def test_video_meaningless_title_filtered(self):
        """page_title 为无意义文本时应被过滤，回退到正文。"""
        from parsers.weibo import parse_mblog
        mblog = self._make_mblog(
            text='#第18个全国防灾减灾日# 【人人讲安全 个个会应急】 @安徽 应急管理部的微博视频',
            page_info={
                "type": "video",
                "page_url": "https://video.weibo.com/show?fid=1034:zzz",
                "page_title": "应急管理部的微博视频",
                "title": "",
                "content1": "应急管理部的微博视频",
            },
        )
        result = parse_mblog(mblog)
        assert result is not None
        assert result["title"] != "应急管理部的微博视频"
        assert "应急管理部的微博视频" not in result["title"]
        assert "防灾减灾" in result["title"]

    def test_webpage_uses_page_title(self):
        """网页类型应正常使用 page_title。"""
        from parsers.weibo import parse_mblog
        mblog = self._make_mblog(
            text=' some text ',
            page_info={
                "type": "webpage",
                "page_url": "https://example.com/article",
                "page_title": "关于数字化转型的通知",
                "content1": "",
            },
        )
        result = parse_mblog(mblog)
        assert result is not None
        assert result["title"] == "关于数字化转型的通知"


# ═══════════════════════════════════════════════════════════════════════════════
# 11. 集成与回归测试
# ═══════════════════════════════════════════════════════════════════════════════

class TestIntegration:
    """集成和回归测试。"""

    def test_all_parsers_importable(self):
        """所有解析器函数应可从 parsers 包正常导入。"""
        from parsers import (
            parse_miit_home,
            parse_gov_home,
            parse_gov_rss,
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
        # 确认都是可调用对象
        for fn in [
            parse_miit_home, parse_gov_home, parse_gov_rss,
            parse_ndrc_home, parse_most_home, parse_moe_news,
            parse_miit_local, parse_gov_local, parse_qqnews_search,
            parse_sasac_home, parse_nda_home, parse_soe,
            parse_weibo, parse_website_monitor, parse_weibo_monitor_sources,
        ]:
            assert callable(fn), f"{fn} 不可调用"

    def test_collect_imports(self):
        """collect.py 应能正常导入所有解析器。"""
        import collect
        assert hasattr(collect, "parse_gov_local")
        assert "parse_gov_local" in collect.__all__

    def test_gov_local_sources_count(self):
        """GOV_LOCAL_SOURCES 应包含 32 个省级政府。"""
        from parsers.gov_local import GOV_LOCAL_SOURCES
        assert len(GOV_LOCAL_SOURCES) == 32

    def test_miit_local_sources_count(self):
        """MIIT_LOCAL_SOURCES 应包含 32 个地方工信部门。"""
        from parsers.miit_local import MIIT_LOCAL_SOURCES
        assert len(MIIT_LOCAL_SOURCES) == 32

    def test_no_duplicate_sources(self):
        """gov_local 和 miit_local 的源列表不应有重复省份。"""
        from parsers.gov_local import GOV_LOCAL_SOURCES
        from parsers.miit_local import MIIT_LOCAL_SOURCES
        gov_provinces = [s["province"] for s in GOV_LOCAL_SOURCES]
        miit_provinces = [s["province"] for s in MIIT_LOCAL_SOURCES]
        assert len(gov_provinces) == len(set(gov_provinces)), "gov_local 有重复省份"
        assert len(miit_provinces) == len(set(miit_provinces)), "miit_local 有重复省份"


# ═══════════════════════════════════════════════════════════════════════════════
# 运行入口
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import pytest
    sys.exit(pytest.main([__file__, "-v", "--tb=short"]))
