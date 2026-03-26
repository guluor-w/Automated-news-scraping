import os
import sys
import yaml
import requests
import time
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

# 添加 src 目录到路径以导入项目模块
sys.path.append(os.path.join(os.getcwd(), 'src'))
try:
    from collect import parse_qqnews_search, SG_TZ, USER_AGENT, QQNEWS_API_URL, QQNEWS_HEADERS, _parse_qqnews_time_to_dt, keyword_hit
except ImportError:
    print("无法导入 src.collect，请确保在项目根目录下运行此脚本。")
    sys.exit(1)

def load_config_local():
    if os.path.exists("config.yaml"):
        with open("config.yaml", "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    return None

def debug_qqnews_search():
    config = load_config_local()
    if not config:
        print("未找到 config.yaml")
        return

    src = config["sources"].get("qqnews_search")
    if not src:
        print("config.yaml 中未配置 qqnews_search")
        return

    queries = src.get("queries", [])
    if isinstance(queries, str):
        queries = [queries]
    
    if not queries:
        print("未配置查询关键词 (queries)")
        return

    print(f"当前配置的查询词: {queries}")
    
    # 模拟 collect.py 中的参数
    now = datetime.now(tz=SG_TZ)
    window_days = int(config.get("window_days", 15))
    keywords = config.get("keywords", [])
    
    # 我们只通过 API 抓取少量页面进行调试
    max_pages = 1 
    page_size = 10 
    
    print(f"\n开始调试抓取 (Time Window: {window_days} days)...")
    print(f"全局过滤关键词 (Keywords): {keywords[:5]}... (共{len(keywords)}个)")
    
    session = requests.Session()

    for query in queries:
        print(f"\n{'='*50}")
        print(f"正在测试查询词: [{query}]")
        print(f"{'='*50}")

        try:
            # 构造请求 (复制自 collect.py)
            payload = {
                "page": "0",  # 只测第一页               
                "query": query,
                "is_pc": "1",
                "hippy_custom_version": "24",
                "search_type": "all",
                "search_count_limit": str(page_size),
                "appver": "15.5_qqnews_7.1.80",
            }
            resp = session.post(QQNEWS_API_URL, data=payload, headers=QQNEWS_HEADERS, timeout=10)
            data = resp.json()
            
            sec_list = data.get("secList") or []
            print(f"API 返回 secList 数量: {len(sec_list)}")
            
            found_count = 0
            
            for sec in sec_list:
                # 检查 component
                component = sec.get("component")
                if component and component != "pictext":
                    continue # 忽略非图文
                
                news_list = sec.get("newsList") or []
                for n in news_list:
                    title = n.get("title", "").strip()
                    url = n.get("surl") or n.get("url")
                    source_name = n.get("source", "").strip() # 这是 API 返回的发布者
                    time_str = n.get("time", "").strip()
                    
                    found_count += 1
                    
                    print(f"\n  📝 [新闻 #{found_count}]")
                    print(f"     标题: {title}")
                    print(f"     发布单位 (API): '{source_name}'")
                    print(f"     时间字符串: {time_str}")

                    # 1. 模拟时间解析检查
                    dt = _parse_qqnews_time_to_dt(time_str, now)
                    if dt:
                        threshold = now - timedelta(days=window_days)
                        is_time_valid = dt >= threshold
                        print(f"     ✅ 时间解析: {dt} (是否在 {window_days} 天内: {is_time_valid})")
                    else:
                        print(f"     ❌ 时间解析失败")
                        is_time_valid = False

                    # 2. 模拟发布单位匹配检查 (原代码逻辑)
                    # 原逻辑: if query not in publisher: continue
                    # 我们不仅检查是否包含，还打印出来
                    is_publisher_valid = (query in source_name)
                    print(f"     🔍 发布单位检查: query='{query}' in publisher='{source_name}'? -> {is_publisher_valid}")
                    if source_name and (query not in source_name) and (source_name in query):
                         print(f"         (注: publisher 是 query 的子集，如果代码未放宽此条件则会被过滤)")

                    # 3. 模拟关键词匹配检查
                    # 只有 query="工信微报" 才包含 "印发"
                    if "工信微报" in str(query) or "工信微报" in f"腾讯新闻搜索-{query}":
                         target_keywords = keywords
                    else:
                         target_keywords = [k for k in keywords if k != "印发"]
                    
                    hit = keyword_hit(title, target_keywords)
                    print(f"     🔑 关键词匹配: {hit}")

                    # 总结
                    if is_time_valid and is_publisher_valid and hit:
                         print(f"     ✅✅✅ 最终结果: [保留]")
                    else:
                         reasons = []
                         if not is_time_valid: reasons.append("时间过期或无效")
                         if not is_publisher_valid: reasons.append("发布单位不匹配")
                         if not hit: reasons.append("标题关键词不匹配")
                         print(f"     🚫🚫🚫 最终结果: [丢弃] 原因: {', '.join(reasons)}")

        except Exception as e:
            print(f"请求失败: {e}")
            
        time.sleep(1) # 简单休眠

if __name__ == "__main__":
    debug_qqnews_search()
