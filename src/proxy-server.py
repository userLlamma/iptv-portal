#!/usr/bin/env python3
"""
IPTV代理服务器
- 支持多种流媒体格式(HLS/MPEG-TS/MPD)
- 特别支持CCTV流和MyTV Super MPD流
- 配合m3u-to-proxy.py使用
"""

import os
import re
import time
import json
import logging
import sqlite3
import requests
import threading
import xml.etree.ElementTree as ET
from urllib.parse import urlparse, quote, unquote
from flask import Flask, request, Response, stream_with_context, jsonify, send_file

# 配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("iptv-proxy")

DB_PATH = "iptv_proxy.db"
CACHE_ENABLED = True
CACHE_DIR = "cache"
LOGO_CACHE_DIR = os.path.join(CACHE_DIR, "logos")
CACHE_TTL = 3600  # 缓存有效期1小时
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/91.0.4472.124 Safari/537.36"
MYTV_SUPER_TOKEN = os.environ.get('MYTV_SUPER_TOKEN', '')

# 创建Flask应用
app = Flask(__name__)

# 确保缓存目录存在
os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs(LOGO_CACHE_DIR, exist_ok=True)

def init_database():
    """初始化SQLite数据库"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 创建频道源表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS channel_sources (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        channel_id TEXT NOT NULL,
        url TEXT NOT NULL,
        priority INTEGER DEFAULT 1,
        is_active BOOLEAN DEFAULT 1,
        last_checked TIMESTAMP,
        success_count INTEGER DEFAULT 0,
        fail_count INTEGER DEFAULT 0,
        avg_response_time FLOAT DEFAULT 0,
        UNIQUE(channel_id, url)
    )
    ''')
    
    # 创建频道信息表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS channel_info (
        channel_id TEXT PRIMARY KEY,
        display_name TEXT,
        logo_url TEXT,
        epg_id TEXT,
        group_title TEXT,
        description TEXT,
        country TEXT,
        language TEXT,
        categories TEXT,
        added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # 创建台标缓存表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS logo_cache (
        logo_url TEXT PRIMARY KEY,
        local_path TEXT,
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # 创建访问日志
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS access_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        channel_id TEXT NOT NULL,
        source_url TEXT NOT NULL,
        access_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        status TEXT,
        bytes_sent INTEGER DEFAULT 0
    )
    ''')
    
    # 创建MyTV Super token表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS mytv_tokens (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        token TEXT UNIQUE NOT NULL,
        expiry TIMESTAMP,
        added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    conn.commit()
    conn.close()
    
    # 如果环境变量中有MyTV Super token，添加到数据库
    if MYTV_SUPER_TOKEN:
        add_mytv_token(MYTV_SUPER_TOKEN)

def add_mytv_token(token, expiry=None):
    """添加MyTV Super token到数据库"""
    if not token:
        return
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        if not expiry:
            # 默认30天有效期
            expiry = time.strftime('%Y-%m-%d %H:%M:%S', 
                                time.localtime(time.time() + 30*24*60*60))
        
        cursor.execute(
            "INSERT OR REPLACE INTO mytv_tokens (token, expiry) VALUES (?, ?)",
            (token, expiry)
        )
        conn.commit()
    except Exception as e:
        logger.error(f"添加MyTV Super token失败: {str(e)}")
    finally:
        conn.close()

def get_mytv_token():
    """获取有效的MyTV Super token"""
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
    return MYTV_SUPER_TOKEN

def get_best_source(channel_id):
    """获取指定频道的最佳源URL"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 按优先级获取活跃源
    cursor.execute("""
    SELECT url FROM channel_sources 
    WHERE channel_id = ? AND is_active = 1 
    ORDER BY priority, avg_response_time 
    LIMIT 1
    """, (channel_id,))
    
    result = cursor.fetchone()
    
    if not result:
        # 尝试获取任何源
        cursor.execute("""
        SELECT url FROM channel_sources 
        WHERE channel_id = ? 
        ORDER BY priority LIMIT 1
        """, (channel_id,))
        result = cursor.fetchone()
    
    conn.close()
    return result[0] if result else None

def update_source_status(channel_id, url, success, response_time=0):
    """更新源状态信息"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    if success:
        cursor.execute("""
        UPDATE channel_sources 
        SET is_active = 1, 
            last_checked = CURRENT_TIMESTAMP, 
            success_count = success_count + 1,
            avg_response_time = (avg_response_time * success_count + ?) / (success_count + 1)
        WHERE channel_id = ? AND url = ?
        """, (response_time, channel_id, url))
    else:
        cursor.execute("""
        UPDATE channel_sources 
        SET is_active = 0, 
            last_checked = CURRENT_TIMESTAMP, 
            fail_count = fail_count + 1
        WHERE channel_id = ? AND url = ?
        """, (channel_id, url))
    
    conn.commit()
    conn.close()

def get_channel_info(channel_id):
    """获取频道信息"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM channel_info WHERE channel_id = ?", (channel_id,))
    result = cursor.fetchone()
    
    conn.close()
    return dict(result) if result else None

def get_cache_path(channel_id, segment_id=None):
    """获取缓存文件路径"""
    if segment_id:
        return os.path.join(CACHE_DIR, f"{channel_id}_{segment_id}.ts")
    return os.path.join(CACHE_DIR, f"{channel_id}.stream")

def is_cache_valid(cache_path):
    """检查缓存是否有效"""
    if not os.path.exists(cache_path):
        return False
    
    file_age = time.time() - os.path.getmtime(cache_path)
    if file_age > CACHE_TTL:
        return False
    
    file_size = os.path.getsize(cache_path)
    if file_size < 1024:  # 小于1KB可能是损坏的文件
        return False
    
    return True

def fetch_with_retry(url, stream=False, max_retries=3, timeout=10, headers=None):
    """带重试的HTTP请求"""
    default_headers = {
        'User-Agent': USER_AGENT,
        'Referer': urlparse(url).scheme + '://' + urlparse(url).netloc + '/',
    }
    
    # 对CCTV流媒体URL使用特殊处理
    if ('cctv' in url.lower() or 
        'volcfcdn.com' in url.lower() or 
        'myqcloud.com' in url.lower() or 
        'myalicdn.com' in url.lower()):
        default_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
            'Accept': '*/*',
            'Accept-Language': 'en,zh-CN;q=0.9,zh;q=0.8',
            'Origin': 'https://tv.cctv.com',
            'Referer': 'https://tv.cctv.com/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'cross-site'
        }
    
    if headers:
        default_headers.update(headers)
    
    for attempt in range(max_retries):
        try:
            response = requests.get(
                url, 
                headers=default_headers, 
                stream=stream, 
                timeout=timeout,
                allow_redirects=True
            )
            
            if response.status_code == 200:
                return response
            
            logger.warning(f"请求失败 (尝试 {attempt+1}/{max_retries}): {url}, 状态码: {response.status_code}")
            time.sleep(1)
        
        except Exception as e:
            logger.warning(f"请求异常 (尝试 {attempt+1}/{max_retries}): {url}, 错误: {str(e)}")
            time.sleep(1)
    
    return None

def proxy_hls_manifest(channel_id, source_url):
    """处理HLS播放列表"""
    response = fetch_with_retry(source_url)
    if not response:
        return Response("无法获取HLS清单", status=404)
    
    content = response.text
    base_url = source_url.rsplit('/', 1)[0] + '/'
    
    # 处理内容
    lines = content.splitlines()
    processed_lines = []
    
    for line in lines:
        # 保留注释和标签行
        if line.startswith('#'):
            processed_lines.append(line)
        # 处理URL行 (非注释行)
        elif line.strip():  # 确保不是空行
            segment_url = line.strip()
            
            # 构建完整URL (相对URL转为绝对URL)
            if not segment_url.startswith('http'):
                if segment_url.startswith('/'):
                    # 根路径URL
                    parsed_url = urlparse(source_url)
                    full_url = f"{parsed_url.scheme}://{parsed_url.netloc}{segment_url}"
                else:
                    full_url = f"{base_url}{segment_url}"
            else:
                full_url = segment_url
            
            # 构建代理URL
            escaped_url = quote(full_url)
            proxy_url = f"/proxy/segment/{channel_id}?url={escaped_url}"
            processed_lines.append(proxy_url)
    
    processed_content = '\n'.join(processed_lines)
    
    return Response(
        processed_content,
        content_type='application/vnd.apple.mpegurl',
        headers={'Access-Control-Allow-Origin': '*'}
    )

def proxy_mpd_manifest(channel_id, source_url):
    """处理DASH MPD清单"""
    # 对于MyTV Super，添加token
    if 'mytvsuper' in source_url.lower() or 'mytv' in source_url.lower():
        token = get_mytv_token()
        if token:
            source_url = f"{source_url}{'&' if '?' in source_url else '?'}token={token}"
    
    response = fetch_with_retry(source_url)
    if not response:
        return Response("无法获取MPD清单", status=404)
    
    content = response.text
    base_url = source_url.rsplit('/', 1)[0] + '/'
    
    try:
        # 解析XML
        root = ET.fromstring(content)
        
        # 查找所有需要修改的URL
        for element in root.findall(".//*[@sourceURL]"):
            original_url = element.get("sourceURL")
            
            # 构建完整URL
            if not original_url.startswith('http'):
                if original_url.startswith('/'):
                    parsed_url = urlparse(source_url)
                    full_url = f"{parsed_url.scheme}://{parsed_url.netloc}{original_url}"
                else:
                    full_url = f"{base_url}{original_url}"
            else:
                full_url = original_url
            
            # 添加代理前缀
            escaped_url = quote(full_url)
            proxy_url = f"/proxy/segment/{channel_id}?url={escaped_url}"
            element.set("sourceURL", proxy_url)
        
        # 转换回XML文本
        processed_content = ET.tostring(root, encoding='unicode')
    except Exception as e:
        # 解析失败，使用正则替换
        logger.error(f"MPD解析失败，使用正则替换: {str(e)}")
        
        def rewrite_url(match):
            segment_url = match.group(1).strip()
            
            # 构建完整URL
            if not segment_url.startswith('http'):
                if segment_url.startswith('/'):
                    parsed_url = urlparse(source_url)
                    full_url = f"{parsed_url.scheme}://{parsed_url.netloc}{segment_url}"
                else:
                    full_url = f"{base_url}{segment_url}"
            else:
                full_url = segment_url
            
            # 添加代理前缀
            escaped_url = quote(full_url)
            proxy_url = f"/proxy/segment/{channel_id}?url={escaped_url}"
            return match.group(0).replace(segment_url, proxy_url)
        
        # 替换sourceURL属性
        processed_content = re.sub(r'sourceURL="([^"]+)"', rewrite_url, content)
    
    return Response(
        processed_content,
        content_type='application/dash+xml',
        headers={'Access-Control-Allow-Origin': '*'}
    )

def proxy_stream(channel_id, source_url=None):
    """代理流媒体内容"""
    if not source_url:
        source_url = get_best_source(channel_id)
        if not source_url:
            return Response("未找到可用源", status=404)
    
    # 对于MyTV Super链接，添加token
    if ('mytvsuper' in source_url.lower() or 'mytv' in source_url.lower()) and '.mpd' in source_url.lower():
        token = get_mytv_token()
        if token:
            source_url = f"{source_url}{'&' if '?' in source_url else '?'}token={token}"
    
    # 判断源类型
    if source_url.endswith('.m3u8'):
        return proxy_hls_manifest(channel_id, source_url)
    elif source_url.endswith('.mpd'):
        return proxy_mpd_manifest(channel_id, source_url)
    
    # 先检查缓存
    cache_path = get_cache_path(channel_id)
    if CACHE_ENABLED and is_cache_valid(cache_path):
        logger.info(f"使用缓存提供流: {channel_id}")
        
        def generate_from_cache():
            with open(cache_path, 'rb') as f:
                bytes_sent = 0
                while True:
                    chunk = f.read(8192)
                    if not chunk:
                        break
                    bytes_sent += len(chunk)
                    yield chunk
        
        return Response(
            stream_with_context(generate_from_cache()),
            content_type='video/MP2T',
            headers={'Access-Control-Allow-Origin': '*'}
        )
    
    # 定制头信息
    custom_headers = {'User-Agent': USER_AGENT}
    
    # 对于MyTV Super，添加token到header
    if 'mytvsuper' in source_url.lower() or 'mytv' in source_url.lower():
        token = get_mytv_token()
        if token:
            custom_headers['Authorization'] = f"Bearer {token}"
    
    # 尝试从源获取
    start_time = time.time()
    response = fetch_with_retry(source_url, stream=True, timeout=15, headers=custom_headers)
    response_time = time.time() - start_time
    
    if not response:
        # 源失败，记录并返回错误
        update_source_status(channel_id, source_url, False)
        return Response("无法获取流", status=503)
    
    # 成功获取，更新源状态
    update_source_status(channel_id, source_url, True, response_time)
    
    # 设置合适的内容类型
    content_type = 'video/MP2T'  # 默认MPEG-TS
    if source_url.endswith('.m3u8'):
        content_type = 'application/vnd.apple.mpegurl'
    elif source_url.endswith('.mpd'):
        content_type = 'application/dash+xml'
    elif source_url.endswith('.flv'):
        content_type = 'video/x-flv'
    elif source_url.endswith('.mp4'):
        content_type = 'video/mp4'
    elif 'content-type' in response.headers:
        content_type = response.headers['content-type']
        
    # 缓存并流式传输
    def generate():
        bytes_sent = 0
        cache_file = None
        
        if CACHE_ENABLED:
            try:
                cache_file = open(cache_path, 'wb')
            except Exception as e:
                logger.error(f"无法打开缓存文件: {str(e)}")
        
        try:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    bytes_sent += len(chunk)
                    
                    # 写入缓存
                    if cache_file:
                        try:
                            cache_file.write(chunk)
                        except Exception as e:
                            logger.error(f"缓存写入错误: {str(e)}")
                            if cache_file:
                                cache_file.close()
                                cache_file = None
                    
                    yield chunk
            
            # 更新源状态为成功
            update_source_status(channel_id, source_url, True, response_time)
            
        except Exception as e:
            logger.error(f"流传输错误: {str(e)}")
            # 更新源状态为失败
            update_source_status(channel_id, source_url, False)
        finally:
            if cache_file:
                cache_file.close()
    
    return Response(
        stream_with_context(generate()),
        content_type=content_type,
        headers={'Access-Control-Allow-Origin': '*'}
    )

def proxy_segment(channel_id, segment_url=None):
    """代理HLS/DASH分段"""
    if segment_url is None:
        segment_url = request.args.get('url')
        if not segment_url:
            return Response("缺少URL参数", status=400)
        segment_url = unquote(segment_url)
    
    # 对CCTV流媒体使用特殊处理
    custom_headers = None
    if ('cctv' in segment_url.lower() or 
        'volcfcdn.com' in segment_url.lower() or 
        'myqcloud.com' in segment_url.lower() or
        'liveplay.myqcloud.com' in segment_url.lower()):
        custom_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
            'Accept': '*/*',
            'Accept-Language': 'en,zh-CN;q=0.9,zh;q=0.8',
            'Origin': 'https://tv.cctv.com',
            'Referer': 'https://tv.cctv.com/',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'cross-site'
        }
    
    # 生成段ID用于缓存 - 更新以处理新的TS命名格式
    segment_id = urlparse(segment_url).path.split('/')[-1]
    # 处理数字后缀的TS文件，例如 cdrmldcctv2_1_td-435896241.ts
    if '-' in segment_id and segment_id.endswith('.ts'):
        segment_id = segment_id.replace('.ts', '') + '.ts'  # 保留完整标识符用于缓存
    
    cache_path = get_cache_path(channel_id, segment_id)
    
    # 检查缓存
    if CACHE_ENABLED and is_cache_valid(cache_path):
        logger.debug(f"使用缓存提供分段: {segment_id}")
        
        def generate_from_cache():
            with open(cache_path, 'rb') as f:
                data = f.read()
                yield data
        
        return Response(
            stream_with_context(generate_from_cache()),
            content_type='video/MP2T',
            headers={'Access-Control-Allow-Origin': '*'}
        )
    
    # 从源获取
    response = fetch_with_retry(segment_url, stream=True, headers=custom_headers)
    
    if not response:
        return Response("无法获取分段", status=503)
    
    # 设置正确的内容类型
    content_type = 'video/MP2T'  # 默认MPEG-TS
    if segment_url.endswith('.m4s'):
        content_type = 'video/iso.segment'
    elif segment_url.endswith('.mp4'):
        content_type = 'video/mp4'
    elif 'content-type' in response.headers:
        content_type = response.headers['content-type']
    
    # 缓存并流式传输
    def generate():
        cache_data = bytearray()
        
        try:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    if CACHE_ENABLED:
                        cache_data.extend(chunk)
                    yield chunk
            
            # 保存到缓存
            if CACHE_ENABLED and cache_data:
                try:
                    with open(cache_path, 'wb') as f:
                        f.write(cache_data)
                except Exception as e:
                    logger.error(f"缓存分段错误: {str(e)}")
        except Exception as e:
            logger.error(f"分段传输错误: {str(e)}")
    
    return Response(
        stream_with_context(generate()),
        content_type=content_type,
        headers={'Access-Control-Allow-Origin': '*'}
    )

def add_channel_source(channel_id, url, priority=100):
    """添加频道源"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
        INSERT INTO channel_sources (channel_id, url, priority)
        VALUES (?, ?, ?)
        ON CONFLICT(channel_id, url) DO UPDATE SET
        priority = ?, is_active = 1
        """, (channel_id, url, priority, priority))
        
        conn.commit()
        status = "添加成功"
    except Exception as e:
        status = f"添加失败: {str(e)}"
    
    conn.close()
    return status

def add_channel_info(channel_id, display_name=None, logo_url=None, epg_id=None, group_title=None):
    """添加或更新频道信息"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 检查频道是否已存在
    cursor.execute("SELECT channel_id FROM channel_info WHERE channel_id = ?", (channel_id,))
    exists = cursor.fetchone()
    
    if exists:
        # 更新现有频道
        update_fields = []
        params = []
        
        if display_name is not None:
            update_fields.append("display_name = ?")
            params.append(display_name)
        
        if logo_url is not None:
            update_fields.append("logo_url = ?")
            params.append(logo_url)
        
        if epg_id is not None:
            update_fields.append("epg_id = ?")
            params.append(epg_id)
        
        if group_title is not None:
            update_fields.append("group_title = ?")
            params.append(group_title)
        
        if update_fields:
            params.append(channel_id)
            query = f"UPDATE channel_info SET {', '.join(update_fields)} WHERE channel_id = ?"
            cursor.execute(query, params)
    else:
        # 插入新频道
        cursor.execute("""
        INSERT INTO channel_info 
        (channel_id, display_name, logo_url, epg_id, group_title)
        VALUES (?, ?, ?, ?, ?)
        """, (channel_id, display_name, logo_url, epg_id, group_title))
    
    conn.commit()
    conn.close()
    return True

# Flask路由定义
@app.route('/')
def index():
    """首页，显示基本状态"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # 获取频道总数
    cursor.execute("SELECT COUNT(DISTINCT channel_id) as count FROM channel_sources")
    channel_count = cursor.fetchone()['count']
    
    # 获取频道分组
    cursor.execute("""
    SELECT group_title, COUNT(*) as count 
    FROM channel_info 
    GROUP BY group_title 
    ORDER BY count DESC
    """)
    groups = [dict(row) for row in cursor.fetchall()]
    
    conn.close()
    
    # 简化的HTML页面
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>IPTV代理服务器</title>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            h1, h2 {{ color: #2c3e50; }}
            .card {{ background: #f9f9f9; border-radius: 5px; padding: 15px; margin-bottom: 15px; }}
        </style>
    </head>
    <body>
        <h1>IPTV代理服务器</h1>
        <div class="card">
            <p>总频道数: <strong>{channel_count}</strong></p>
            <p>频道分组数: <strong>{len(groups)}</strong></p>
        </div>
        
        <h2>播放列表链接</h2>
        <div class="card">
            <p><a href="/playlist.m3u">所有频道播放列表</a></p>
            {''.join([f'<p><a href="/playlist.m3u?group={g.get("group_title", "")}">{g.get("group_title", "未分组")}播放列表</a></p>' for g in groups])}
        </div>
    </body>
    </html>
    """
    
    return html

@app.route('/playlist.m3u')
def get_playlist():
    """生成M3U播放列表"""
    group = request.args.get('group')
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # 构建查询条件
    query = """
    SELECT cs.channel_id, ci.display_name, ci.logo_url, ci.group_title
    FROM channel_sources cs
    LEFT JOIN channel_info ci ON cs.channel_id = ci.channel_id
    """
    params = []
    
    if group:
        query += " WHERE ci.group_title = ?"
        params.append(group)
    
    query += " GROUP BY cs.channel_id ORDER BY ci.group_title, ci.display_name"
    
    cursor.execute(query, params)
    channels = cursor.fetchall()
    conn.close()
    
    # 生成M3U内容
    m3u_content = "#EXTM3U\n"
    server_url = request.url_root.rstrip('/')
    
    for channel in channels:
        channel_id = channel['channel_id']
        name = channel['display_name'] or channel_id
        group_title = channel['group_title'] or '其他'
        logo = f' tvg-logo="{channel["logo_url"]}"' if channel.get("logo_url") else ""
        
        m3u_content += f'#EXTINF:-1 tvg-id="{channel_id}"{logo} group-title="{group_title}", {name}\n'
        m3u_content += f'{server_url}/proxy/channel/{channel_id}\n'
    
    return Response(m3u_content, mimetype='audio/x-mpegurl')

@app.route('/proxy/channel/<channel_id>')
def route_proxy_channel(channel_id):
    """代理频道流"""
    return proxy_stream(channel_id)

@app.route('/proxy/segment/<channel_id>')
def route_proxy_segment(channel_id):
    """代理分段"""
    return proxy_segment(channel_id)

@app.route('/admin/add_channel_info', methods=['POST'])
def route_add_channel_info():
    """添加或更新频道信息"""
    # 检查授权
    auth_key = request.headers.get('X-Auth-Key')
    if auth_key != os.environ.get('ADMIN_KEY', 'changeme'):
        return jsonify({"error": "未授权"}), 401
    
    data = request.json
    if not data or 'channel_id' not in data:
        return jsonify({"error": "缺少频道ID"}), 400
    
    result = add_channel_info(
        data['channel_id'],
        display_name=data.get('display_name'),
        logo_url=data.get('logo_url'),
        epg_id=data.get('epg_id'),
        group_title=data.get('group_title')
    )
    
    return jsonify({"success": result})

@app.route('/admin/add_source', methods=['POST'])
def route_add_source():
    """添加频道源"""
    # 检查授权
    auth_key = request.headers.get('X-Auth-Key')
    if auth_key != os.environ.get('ADMIN_KEY', 'changeme'):
        return jsonify({"error": "未授权"}), 401
    
    data = request.json
    if not data or 'channel_id' not in data or 'url' not in data:
        return jsonify({"error": "缺少必要参数"}), 400
    
    status = add_channel_source(
        data['channel_id'],
        data['url'],
        priority=data.get('priority', 100)
    )
    
    return jsonify({"status": status})

@app.route('/admin/add_mytv_token', methods=['POST'])
def route_add_mytv_token():
    """添加MyTV Super token"""
    # 检查授权
    auth_key = request.headers.get('X-Auth-Key')
    if auth_key != os.environ.get('ADMIN_KEY', 'changeme'):
        return jsonify({"error": "未授权"}), 401
    
    data = request.json
    if not data or 'token' not in data:
        return jsonify({"error": "缺少token参数"}), 400
    
    add_mytv_token(data['token'], data.get('expiry'))
    return jsonify({"success": True})

def clean_cache():
    """清理过期缓存"""
    try:
        now = time.time()
        cache_cleaned = 0
        
        for filename in os.listdir(CACHE_DIR):
            if filename.endswith('.ts') or filename.endswith('.stream'):
                file_path = os.path.join(CACHE_DIR, filename)
                file_age = now - os.path.getmtime(file_path)
                
                if file_age > CACHE_TTL:
                    try:
                        os.remove(file_path)
                        cache_cleaned += 1
                    except:
                        pass
        
        logger.info(f"缓存清理完成，删除了 {cache_cleaned} 个过期文件")
    except Exception as e:
        logger.error(f"缓存清理错误: {str(e)}")

def update_cctv_sources():
    """定期更新CCTV源"""
    from cctv_discovery import discover_and_update
    
    while True:
        try:
            logger.info("开始更新CCTV源...")
            discover_and_update(DB_PATH)
            logger.info("CCTV源更新完成")
        except Exception as e:
            logger.error(f"更新CCTV源出错: {str(e)}")
        
        # 每6小时更新一次
        time.sleep(6 * 60 * 60)

def main():
    """启动代理服务器"""
    # 初始化数据库
    init_database()
    
    # 启动定期清理缓存的线程
    def clean_cache_task():
        while True:
            clean_cache()
            time.sleep(3600)  # 每小时清理一次
    
    # 启动清理线程
    threading.Thread(target=clean_cache_task, daemon=True).start()
    
    # 启动CCTV源更新线程
    threading.Thread(target=update_cctv_sources, daemon=True).start()
    
    # 启动Flask应用
    logger.info("IPTV代理服务器启动中...")
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))

if __name__ == "__main__":
    main()