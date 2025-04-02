#!/usr/bin/env python3
"""
CCTV URL 自动发现模块
自动检测和验证 CCTV 频道的当前可用直播源
"""

import re
import time
import json
import logging
import requests
import sqlite3
from urllib.parse import urlparse

logger = logging.getLogger("cctv-discovery")

# CCTV 已知的 CDN 模式
CCTV_URL_PATTERNS = [
    "https://ldocctvwbcdtxy.liveplay.myqcloud.com/ldocctvwbcd/cdrmld{channel}_1_td.m3u8",
    "https://ldncctvwbcdbyte.volcfcdn.com/ldncctvwbcd/cdrmld{channel}_1/index.m3u8",
    "https://cctvalih5ca.v.myalicdn.com/live/{channel}_2/index.m3u8",
    "https://cctvwbndks.v.kcdnvip.com/cctvwbnd/{channel}_2/index.m3u8",
    "https://cctvwbndtxy.liveplay.myqcloud.com/cctvwbnd/{channel}_2/index.m3u8",
    "https://cctvcnch5ca.v.wscdns.com/live/{channel}_2/index.m3u8"
]

# 用于检测的频道列表
TEST_CHANNELS = ["cctv1", "cctv2", "cctv13"]

# 请求头
CCTV_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
    'Accept': '*/*',
    'Accept-Language': 'en,zh-CN;q=0.9,zh;q=0.8',
    'Origin': 'https://tv.cctv.com',
    'Referer': 'https://tv.cctv.com/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'cross-site'
}

def verify_url(url, timeout=5):
    """验证URL是否可访问"""
    try:
        response = requests.get(url, headers=CCTV_HEADERS, timeout=timeout, stream=True)
        # 只读取前几个字节验证
        content = next(response.iter_content(chunk_size=1024), None)
        return response.status_code == 200 and content
    except:
        return False

def discover_cctv_sources():
    """发现当前有效的CCTV URL模式"""
    valid_patterns = []
    
    for pattern in CCTV_URL_PATTERNS:
        valid_for_channels = []
        
        for channel in TEST_CHANNELS:
            url = pattern.format(channel=channel)
            if verify_url(url):
                valid_for_channels.append(channel)
        
        if valid_for_channels:
            valid_patterns.append({
                "pattern": pattern,
                "valid_channels": valid_for_channels,
                "example_url": pattern.format(channel=valid_for_channels[0])
            })
    
    return valid_patterns

def extract_from_webpage(channel_id):
    """从CCTV网页提取直播流URL"""
    url = f"https://tv.cctv.com/live/{channel_id}/"
    
    try:
        response = requests.get(url, headers=CCTV_HEADERS, timeout=10)
        if response.status_code != 200:
            return None
        
        html = response.text
        
        # 查找所有可能的m3u8 URL
        m3u8_urls = re.findall(r'(https?://[^"\']+\.m3u8[^"\']*)', html)
        
        for url in m3u8_urls:
            if verify_url(url):
                return url
    except:
        pass
    
    return None

def update_channel_sources(db_path, channel_id, url, priority=10):
    """更新数据库中的频道源"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
        INSERT INTO channel_sources (channel_id, url, priority, is_active)
        VALUES (?, ?, ?, 1)
        ON CONFLICT(channel_id, url) DO UPDATE SET
        priority = ?, is_active = 1, last_checked = CURRENT_TIMESTAMP
        """, (channel_id, url, priority, priority))
        
        conn.commit()
        status = True
    except Exception as e:
        logger.error(f"更新源失败: {str(e)}")
        status = False
    
    conn.close()
    return status

def discover_and_update(db_path):
    """发现并更新CCTV源"""
    # 首先尝试从模式中发现
    patterns = discover_cctv_sources()
    
    if patterns:
        logger.info(f"发现 {len(patterns)} 个有效的CCTV URL模式")
        
        # 获取所有CCTV频道
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT channel_id FROM channel_info WHERE channel_id LIKE 'cctv%'")
        channels = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        for channel_id in channels:
            # 尝试从每个有效模式生成URL
            for pattern_info in patterns:
                pattern = pattern_info["pattern"]
                url = pattern.format(channel=channel_id)
                
                if verify_url(url):
                    logger.info(f"为 {channel_id} 找到有效URL: {url}")
                    update_channel_sources(db_path, channel_id, url)
                    break
            else:
                # 如果所有模式都失败，尝试从网页提取
                url = extract_from_webpage(channel_id)
                if url:
                    logger.info(f"从网页为 {channel_id} 提取URL: {url}")
                    update_channel_sources(db_path, channel_id, url)
    else:
        logger.warning("未找到有效的CCTV URL模式")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    valid_patterns = discover_cctv_sources()
    print(f"找到 {len(valid_patterns)} 个有效的CCTV URL模式:")
    
    for i, pattern_info in enumerate(valid_patterns):
        pattern = pattern_info["pattern"]
        channels = pattern_info["valid_channels"]
        example = pattern_info["example_url"]
        
        print(f"\n{i+1}. 模式: {pattern}")
        print(f"   有效频道: {', '.join(channels)}")
        print(f"   示例URL: {example}")