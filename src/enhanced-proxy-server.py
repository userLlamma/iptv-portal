"""
增强型IPTV代理服务器
- 支持多种流媒体格式(HLS/MPEG-TS/FLV/MPD)
- 带缓存机制减少源服务器负担
- 多路径自动切换保证稳定性
- 支持频道台标和EPG关联
- 特别支持MyTV Super的MPD格式
"""

import os
import re
import time
import json
import logging
import sqlite3
import requests
import threading
import traceback
import xml.etree.ElementTree as ET
from urllib.parse import urlparse, urlencode, parse_qs, quote, unquote
from flask import Flask, request, Response, stream_with_context, jsonify, render_template_string, send_file

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("iptv_proxy.log"), logging.StreamHandler()]
)
logger = logging.getLogger("iptv-proxy")

# 数据库路径
DB_PATH = "iptv_proxy.db"

# 全局缓存设置
CACHE_ENABLED = True
CACHE_DIR = "cache"
LOGO_CACHE_DIR = os.path.join(CACHE_DIR, "logos")
CACHE_TTL = 3600  # 缓存有效期1小时
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

# MyTV Super配置
MYTV_SUPER_TOKEN = os.environ.get('MYTV_SUPER_TOKEN', '')

# 确保缓存目录存在
if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)
if not os.path.exists(LOGO_CACHE_DIR):
    os.makedirs(LOGO_CACHE_DIR)

def init_database():
    """初始化SQLite数据库"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 创建频道源表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS channel_sources (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        channel_id TEXT NOT NULL,          -- 频道标识
        url TEXT NOT NULL,                 -- 源URL
        priority INTEGER DEFAULT 1,        -- 优先级，数字越小优先级越高
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
        channel_id TEXT PRIMARY KEY,       -- 频道标识，唯一
        display_name TEXT,                 -- 显示名称
        logo_url TEXT,                     -- 台标URL
        epg_id TEXT,                       -- EPG ID
        group_title TEXT,                  -- 分组
        description TEXT,                  -- 描述
        country TEXT,                      -- 国家/地区
        language TEXT,                     -- 语言
        categories TEXT,                   -- 分类，JSON格式的数组
        added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # 创建台标缓存表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS logo_cache (
        logo_url TEXT PRIMARY KEY,        -- 原始台标URL
        local_path TEXT,                  -- 本地存储路径
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # 创建频道访问日志
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS access_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        channel_id TEXT NOT NULL,
        source_url TEXT NOT NULL,
        access_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        user_ip TEXT,
        user_agent TEXT,
        status TEXT,
        bytes_sent INTEGER DEFAULT 0
    )
    ''')
    
    # 创建EPG来源表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS epg_sources (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,               -- 来源名称
        url TEXT UNIQUE NOT NULL,         -- EPG XML URL
        is_active BOOLEAN DEFAULT 1,
        last_updated TIMESTAMP
    )
    ''')
    
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
    logger.info("数据库初始化完成")
    
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
        if expiry:
            cursor.execute(
                "INSERT OR REPLACE INTO mytv_tokens (token, expiry) VALUES (?, ?)",
                (token, expiry)
            )
        else:
            # 如果没有提供过期时间，默认30天
            expiry_date = time.strftime('%Y-%m-%d %H:%M:%S', 
                                       time.localtime(time.time() + 30*24*60*60))
            cursor.execute(
                "INSERT OR REPLACE INTO mytv_tokens (token, expiry) VALUES (?, ?)",
                (token, expiry_date)
            )
        
        conn.commit()
        logger.info("MyTV Super token添加成功")
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
    else:
        # 如果数据库中没有有效token，使用环境变量中的token
        return MYTV_SUPER_TOKEN

def get_best_source(channel_id):
    """获取指定频道的最佳源URL"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 按优先级获取活跃源
    cursor.execute("""
    SELECT url, avg_response_time 
    FROM channel_sources 
    WHERE channel_id = ? AND is_active = 1 
    ORDER BY priority, avg_response_time 
    LIMIT 1
    """, (channel_id,))
    
    result = cursor.fetchone()
    conn.close()
    
    if result:
        return result[0]
    else:
        # 如果没有活跃源，尝试获取任何源（即使不活跃）
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT url FROM channel_sources WHERE channel_id = ? ORDER BY priority LIMIT 1", (channel_id,))
        result = cursor.fetchone()
        conn.close()
        
        if result:
            return result[0]
    
    return None

def update_source_status(channel_id, url, success, response_time=0):
    """更新源状态信息"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    if success:
        # 成功时更新响应时间和成功计数
        cursor.execute("""
        UPDATE channel_sources 
        SET is_active = 1, 
            last_checked = CURRENT_TIMESTAMP, 
            success_count = success_count + 1,
            avg_response_time = (avg_response_time * success_count + ?) / (success_count + 1)
        WHERE channel_id = ? AND url = ?
        """, (response_time, channel_id, url))
    else:
        # 失败时更新失败计数和状态
        cursor.execute("""
        UPDATE channel_sources 
        SET is_active = 0, 
            last_checked = CURRENT_TIMESTAMP, 
            fail_count = fail_count + 1
        WHERE channel_id = ? AND url = ?
        """, (channel_id, url))
    
    conn.commit()
    conn.close()

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

def add_channel_info(channel_id, display_name=None, logo_url=None, epg_id=None, group_title=None, 
                    description=None, country=None, language=None, categories=None):
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
            # 下载logo到本地缓存
            if logo_url:
                download_logo(logo_url)
        
        if epg_id is not None:
            update_fields.append("epg_id = ?")
            params.append(epg_id)
        
        if group_title is not None:
            update_fields.append("group_title = ?")
            params.append(group_title)
        
        if description is not None:
            update_fields.append("description = ?")
            params.append(description)
        
        if country is not None:
            update_fields.append("country = ?")
            params.append(country)
        
        if language is not None:
            update_fields.append("language = ?")
            params.append(language)
        
        if categories is not None:
            if isinstance(categories, list):
                categories = json.dumps(categories)
            update_fields.append("categories = ?")
            params.append(categories)
        
        if update_fields:
            params.append(channel_id)
            query = f"UPDATE channel_info SET {', '.join(update_fields)} WHERE channel_id = ?"
            cursor.execute(query, params)
    else:
        # 插入新频道
        # 处理categories为JSON格式
        if categories and isinstance(categories, list):
            categories = json.dumps(categories)
            
        cursor.execute("""
        INSERT INTO channel_info 
        (channel_id, display_name, logo_url, epg_id, group_title, description, country, language, categories)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (channel_id, display_name, logo_url, epg_id, group_title, description, country, language, categories))
        
        # 下载logo到本地缓存
        if logo_url:
            download_logo(logo_url)
    
    conn.commit()
    conn.close()
    return True

def get_channel_info(channel_id):
    """获取频道信息"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM channel_info WHERE channel_id = ?", (channel_id,))
    result = cursor.fetchone()
    
    conn.close()
    
    if result:
        return dict(result)
    else:
        return None

def download_logo(logo_url):
    """下载台标并缓存到本地"""
    if not logo_url:
        return None
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 检查是否已缓存
    cursor.execute("SELECT local_path FROM logo_cache WHERE logo_url = ?", (logo_url,))
    cached = cursor.fetchone()
    
    if cached and os.path.exists(cached[0]):
        conn.close()
        return cached[0]
    
    # 生成本地文件路径
    parsed_url = urlparse(logo_url)
    file_name = os.path.basename(parsed_url.path)
    
    # 确保文件名有效且唯一
    if not file_name or len(file_name) < 5:
        file_name = f"logo_{hash(logo_url) % 10000000}.png"
    
    local_path = os.path.join(LOGO_CACHE_DIR, file_name)
    
    # 下载logo
    try:
        response = requests.get(logo_url, timeout=10)
        if response.status_code == 200:
            with open(local_path, 'wb') as f:
                f.write(response.content)
            
            # 更新缓存数据库
            cursor.execute("""
            INSERT OR REPLACE INTO logo_cache (logo_url, local_path, last_updated)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            """, (logo_url, local_path))
            
            conn.commit()
            conn.close()
            return local_path
    except Exception as e:
        logger.error(f"下载台标失败 {logo_url}: {str(e)}")
    
    conn.close()
    return None

def get_logo_path(logo_url):
    """获取台标的本地路径，如果不存在则下载"""
    if not logo_url:
        return None
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 检查是否已缓存
    cursor.execute("SELECT local_path FROM logo_cache WHERE logo_url = ?", (logo_url,))
    cached = cursor.fetchone()
    
    conn.close()
    
    if cached and os.path.exists(cached[0]):
        return cached[0]
    else:
        return download_logo(logo_url)

def log_access(channel_id, source_url, user_ip, user_agent, status, bytes_sent=0):
    """记录访问日志"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
    INSERT INTO access_logs (channel_id, source_url, user_ip, user_agent, status, bytes_sent)
    VALUES (?, ?, ?, ?, ?, ?)
    """, (channel_id, source_url, user_ip, user_agent, status, bytes_sent))
    
    conn.commit()
    conn.close()

def get_cache_path(channel_id, segment_id=None):
    """获取缓存文件路径"""
    if segment_id:
        # 针对HLS分段的缓存路径
        return os.path.join(CACHE_DIR, f"{channel_id}_{segment_id}.ts")
    else:
        # 针对直播流的缓存路径
        return os.path.join(CACHE_DIR, f"{channel_id}.stream")

def is_cache_valid(cache_path):
    """检查缓存是否有效"""
    if not os.path.exists(cache_path):
        return False
    
    # 检查文件是否过期
    file_age = time.time() - os.path.getmtime(cache_path)
    if file_age > CACHE_TTL:
        return False
    
    # 检查文件大小是否合理
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
        
        except (requests.exceptions.RequestException, Exception) as e:
            logger.warning(f"请求异常 (尝试 {attempt+1}/{max_retries}): {url}, 错误: {str(e)}")
            time.sleep(1)
    
    return None

def save_to_cache(response, cache_path):
    """保存响应内容到缓存"""
    try:
        if response.status_code == 200:
            with open(cache_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            return True
    except Exception as e:
        logger.error(f"缓存保存错误: {str(e)}")
    
    return False

def proxy_hls_manifest(channel_id, source_url):
    """处理HLS播放列表"""
    response = fetch_with_retry(source_url)
    if not response:
        return Response("无法获取HLS清单", status=404)
    
    content = response.text
    base_url = source_url.rsplit('/', 1)[0] + '/'
    
    # 将绝对路径转换为相对路径
    def rewrite_url(match):
        segment_url = match.group(1).strip()
        
        # 如果是完整URL则保留，否则添加基础路径
        if segment_url.startswith('http'):
            pass
        elif segment_url.startswith('/'):
            # 根路径URL
            parsed_url = urlparse(source_url)
            segment_url = f"{parsed_url.scheme}://{parsed_url.netloc}{segment_url}"
        else:
            segment_url = f"{base_url}{segment_url}"
        
        # 添加代理前缀，转义URL
        escaped_url = quote(segment_url)
        proxy_url = f"/proxy/segment/{channel_id}?url={escaped_url}"
        return match.group(0).replace(match.group(1), proxy_url)
    
    # 替换所有TS分段URL
    processed_content = re.sub(r'(?<=\n)([^#][^\n]+)', rewrite_url, content)
    
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
        if token and '?' not in source_url:
            source_url = f"{source_url}?token={token}"
        elif token:
            source_url = f"{source_url}&token={token}"
    
    response = fetch_with_retry(source_url)
    if not response:
        return Response("无法获取MPD清单", status=404)
    
    content = response.text
    try:
        # 解析XML
        root = ET.fromstring(content)
        
        # 查找所有需要修改的URL
        for element in root.findall(".//*[@sourceURL]"):
            original_url = element.get("sourceURL")
            
            # 构建完整URL
            if not original_url.startswith('http'):
                base_url = source_url.rsplit('/', 1)[0] + '/'
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
        
        base_url = source_url.rsplit('/', 1)[0] + '/'
        
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
        if token and '?' not in source_url:
            source_url = f"{source_url}?token={token}"
        elif token:
            source_url = f"{source_url}&token={token}"
    
    user_ip = request.remote_addr
    user_agent = request.headers.get('User-Agent', '')
    
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
                log_access(channel_id, source_url, user_ip, user_agent, "cache_hit", bytes_sent)
        
        return Response(
            stream_with_context(generate_from_cache()),
            content_type='video/MP2T',
            headers={'Access-Control-Allow-Origin': '*'}
        )
    
    # 定制头信息
    custom_headers = {
        'User-Agent': USER_AGENT,
    }
    
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
        log_access(channel_id, source_url, user_ip, user_agent, "source_error")
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
            
            log_access(channel_id, source_url, user_ip, user_agent, "success", bytes_sent)
        except Exception as e:
            logger.error(f"流传输错误: {str(e)}")
            log_access(channel_id, source_url, user_ip, user_agent, f"error: {str(e)}", bytes_sent)
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
    
    user_ip = request.remote_addr
    user_agent = request.headers.get('User-Agent', '')
    
    # 对于MyTV Super链接，添加token
    custom_headers = None
    if 'mytvsuper' in segment_url.lower() or 'mytv' in segment_url.lower():
        token = get_mytv_token()
        if token and '?' not in segment_url:
            segment_url = f"{segment_url}?token={token}"
        elif token and '?' in segment_url:
            segment_url = f"{segment_url}&token={token}"
        
        # 同时添加到header
        custom_headers = {'Authorization': f"Bearer {token}"}
    
    # 生成段ID用于缓存
    segment_id = urlparse(segment_url).path.split('/')[-1]
    cache_path = get_cache_path(channel_id, segment_id)
    
    # 检查缓存
    if CACHE_ENABLED and is_cache_valid(cache_path):
        logger.debug(f"使用缓存提供分段: {segment_id}")
        
        def generate_from_cache():
            with open(cache_path, 'rb') as f:
                data = f.read()
                yield data
            log_access(channel_id, segment_url, user_ip, user_agent, "segment_cache_hit", len(data))
        
        return Response(
            stream_with_context(generate_from_cache()),
            content_type='video/MP2T',
            headers={'Access-Control-Allow-Origin': '*'}
        )
    
    # 从源获取
    response = fetch_with_retry(segment_url, stream=True, headers=custom_headers)
    
    if not response:
        log_access(channel_id, segment_url, user_ip, user_agent, "segment_error")
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
        bytes_sent = 0
        
        try:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    bytes_sent += len(chunk)
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
            
            log_access(channel_id, segment_url, user_ip, user_agent, "segment_success", bytes_sent)
        except Exception as e:
            logger.error(f"分段传输错误: {str(e)}")
            log_access(channel_id, segment_url, user_ip, user_agent, f"segment_error: {str(e)}", bytes_sent)
    
    return Response(
        stream_with_context(generate()),
        content_type=content_type,
        headers={'Access-Control-Allow-Origin': '*'}
    )