#!/usr/bin/env python3
"""
MyTV Super MPD支持模块
处理MyTV Super的MPD格式流媒体

用法:
    python mytv_super.py add-token YOUR-TOKEN
    python mytv_super.py test-mpd URL-TO-TEST
"""

import os
import sys
import re
import json
import time
import base64
import logging
import sqlite3
import argparse
import xml.etree.ElementTree as ET
import requests
from urllib.parse import urlparse, parse_qs, quote

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("mytv-super")

# 数据库路径
DB_PATH = "iptv_proxy.db"

def init_db():
    """初始化数据库"""
    if not os.path.exists(DB_PATH):
        logger.warning("数据库文件不存在，将创建新数据库")
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 创建MyTV Super token表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS mytv_tokens (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        token TEXT UNIQUE NOT NULL,       -- MyTV Super token
        expiry TIMESTAMP,                 -- 过期时间
        added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    conn.commit()
    conn.close()

def add_token(token, expiry=None):
    """添加MyTV Super token到数据库"""
    init_db()
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        if expiry:
            cursor.execute(
                "INSERT OR REPLACE INTO mytv_tokens (token, expiry) VALUES (?, ?)",
                (token, expiry)
            )
        else:
            # 默认30天有效期
            expiry_date = time.strftime('%Y-%m-%d %H:%M:%S', 
                                       time.localtime(time.time() + 30*24*60*60))
            cursor.execute(
                "INSERT OR REPLACE INTO mytv_tokens (token, expiry) VALUES (?, ?)",
                (token, expiry_date)
            )
        
        conn.commit()
        logger.info(f"Token添加成功，过期时间: {expiry or expiry_date}")
        return True
    except Exception as e:
        logger.error(f"添加Token失败: {str(e)}")
        return False
    finally:
        conn.close()

def get_token():
    """获取最新的token"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute("""
        SELECT token FROM mytv_tokens 
        WHERE expiry > datetime('now') 
        ORDER BY expiry DESC LIMIT 1
        """)
        
        result = cursor.fetchone()
        conn.close()
        
        if result:
            return result[0]
        else:
            logger.warning("未找到有效的token")
            return None
    except Exception as e:
        logger.error(f"获取token失败: {str(e)}")
        return None

def analyze_mpd(mpd_url):
    """分析MPD文件结构"""
    token = get_token()
    
    if not token:
        logger.error("未找到有效的token，无法分析MPD")
        return False
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Authorization': f'Bearer {token}'
    }
    
    try:
        # 添加token到URL
        if '?' not in mpd_url:
            url_with_token = f"{mpd_url}?token={token}"
        else:
            url_with_token = f"{mpd_url}&token={token}"
        
        response = requests.get(url_with_token, headers=headers)
        
        if response.status_code != 200:
            logger.error(f"获取MPD失败，状态码: {response.status_code}")
            return False
        
        # 解析XML
        root = ET.fromstring(response.text)
        
        print("\n=== MPD文件信息 ===")
        
        # 基本信息
        print(f"MPD类型: {root.get('type', 'unknown')}")
        print(f"最小缓冲时间: {root.get('minBufferTime', 'unknown')}")
        
        # 时长
        if 'mediaPresentationDuration' in root.attrib:
            print(f"总时长: {root.get('mediaPresentationDuration')}")
        
        # Period信息
        periods = root.findall('.//{*}Period')
        print(f"Period数量: {len(periods)}")
        
        # 适配集信息
        adaptation_sets = root.findall('.//{*}AdaptationSet')
        print(f"AdaptationSet数量: {len(adaptation_sets)}")
        
        for i, adaptation_set in enumerate(adaptation_sets):
            mime_type = adaptation_set.get('mimeType', 'unknown')
            content_type = adaptation_set.get('contentType', '未知')
            
            print(f"\nAdaptationSet #{i+1}:")
            print(f"  内容类型: {content_type}")
            print(f"  MIME类型: {mime_type}")
            
            representations = adaptation_set.findall('.//{*}Representation')
            print(f"  Representation数量: {len(representations)}")
            
            for j, representation in enumerate(representations):
                rep_id = representation.get('id', 'unknown')
                bandwidth = representation.get('bandwidth', 'unknown')
                width = representation.get('width', 'N/A')
                height = representation.get('height', 'N/A')
                
                print(f"    Representation #{j+1}:")
                print(f"      ID: {rep_id}")
                print(f"      带宽: {bandwidth}")
                if width != 'N/A' and height != 'N/A':
                    print(f"      分辨率: {width}x{height}")
        
        # 分段信息
        segments = root.findall('.//{*}SegmentTemplate')
        if segments:
            print("\n分段模板信息:")
            for segment in segments:
                init = segment.get('initialization', 'N/A')
                media = segment.get('media', 'N/A')
                
                print(f"  初始化: {init}")
                print(f"  媒体模板: {media}")
        
        print("\n测试成功! MPD文件有效并且可以正确解析。")
        return True
    
    except ET.ParseError as e:
        logger.error(f"XML解析错误: {str(e)}")
        print("MPD文件格式错误，无法解析XML")
        return False
    
    except Exception as e:
        logger.error(f"分析MPD出错: {str(e)}")
        print(f"Error: {str(e)}")
        return False

def test_mpd_segments(mpd_url):
    """测试MPD文件中的分段是否可访问"""
    token = get_token()
    
    if not token:
        logger.error("未找到有效的token，无法测试分段")
        return False
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Authorization': f'Bearer {token}'
    }
    
    try:
        # 添加token到URL
        if '?' not in mpd_url:
            url_with_token = f"{mpd_url}?token={token}"
        else:
            url_with_token = f"{mpd_url}&token={token}"
        
        # 获取MPD内容
        response = requests.get(url_with_token, headers=headers)
        
        if response.status_code != 200:
            logger.error(f"获取MPD失败，状态码: {response.status_code}")
            return False
        
        # 解析XML
        root = ET.fromstring(response.text)
        
        # 获取基础URL
        base_url = mpd_url.rsplit('/', 1)[0] + '/'
        
        # 查找分段URL
        segment_templates = root.findall('.//{*}SegmentTemplate')
        
        if not segment_templates:
            print("未找到分段模板信息")
            return False
        
        # 测试初始化分段
        init_urls = []
        for template in segment_templates:
            if 'initialization' in template.attrib:
                init_template = template.get('initialization')
                # 替换变量
                init_url = init_template.replace('$RepresentationID$', template.find('../{*}Representation').get('id', '1'))
                
                if not init_url.startswith('http'):
                    init_url = f"{base_url}{init_url}"
                
                init_urls.append(init_url)
        
        print("\n=== 测试分段可访问性 ===")
        
        # 测试初始化分段
        for i, url in enumerate(init_urls):
            print(f"\n测试初始化分段 #{i+1}:")
            
            # 添加token
            if '?' not in url:
                test_url = f"{url}?token={token}"
            else:
                test_url = f"{url}&token={token}"
            
            try:
                seg_response = requests.get(test_url, headers=headers)
                
                if seg_response.status_code == 200:
                    print(f"✓ 成功! 状态码: {seg_response.status_code}")
                    print(f"  大小: {len(seg_response.content)} 字节")
                else:
                    print(f"✗ 失败! 状态码: {seg_response.status_code}")
            except Exception as e:
                print(f"✗ 请求错误: {str(e)}")
        
        print("\n分段测试完成")
        return True
    
    except Exception as e:
        logger.error(f"测试分段出错: {str(e)}")
        print(f"错误: {str(e)}")
        return False

def main():
    parser = argparse.ArgumentParser(description='MyTV Super MPD支持工具')
    subparsers = parser.add_subparsers(dest='command', help='命令')
    
    # 添加token命令
    add_token_parser = subparsers.add_parser('add-token', help='添加MyTV Super token')
    add_token_parser.add_argument('token', help='Token字符串')
    add_token_parser.add_argument('--expiry', help='过期时间 (YYYY-MM-DD HH:MM:SS)')
    
    # 测试MPD命令
    test_mpd_parser = subparsers.add_parser('test-mpd', help='测试MPD URL')
    test_mpd_parser.add_argument('url', help='MPD文件URL')
    test_mpd_parser.add_argument('--segments', action='store_true', help='测试分段可访问性')
    
    args = parser.parse_args()
    
    if args.command == 'add-token':
        if add_token(args.token, args.expiry):
            print("Token添加成功!")
        else:
            print("Token添加失败")
            return 1
    
    elif args.command == 'test-mpd':
        analyze_result = analyze_mpd(args.url)
        
        if analyze_result and args.segments:
            test_mpd_segments(args.url)
    
    else:
        parser.print_help()
    
    return 0

if __name__ == "__main__":
    sys.exit(main())