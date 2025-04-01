#!/usr/bin/env python3
"""
IPTV流媒体验证模块
提供各种类型流媒体的验证功能
"""

import os
import time
import logging
import requests
import xml.etree.ElementTree as ET

# 配置日志
logger = logging.getLogger("stream-verifier")

def verify_hls_stream(url, timeout=10):
    """验证HLS (m3u8)流的有效性"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        start_time = time.time()
        
        # 获取m3u8清单
        response = requests.get(url, headers=headers, timeout=timeout)
        
        if response.status_code != 200:
            return False, 0, f"HTTP错误: {response.status_code}"
        
        # 检查内容是否为有效的m3u8格式
        content = response.text
        if not content or not content.strip():
            return False, 0, "空响应内容"
        
        # 检查是否包含基本的m3u8标记
        if not ("#EXTM3U" in content):
            return False, 0, "无效的M3U8格式"
        
        # 计算响应时间
        response_time = time.time() - start_time
        
        # 对于主播放列表，尝试获取第一个分段以进一步验证
        if "#EXTINF" in content and len(content) < 50000:  # 避免过大的清单
            # 查找第一个分段URL
            segment_url = None
            lines = content.splitlines()
            for i, line in enumerate(lines):
                if line.startswith("#EXTINF"):
                    if i + 1 < len(lines) and not lines[i+1].startswith('#'):
                        segment_url = lines[i+1].strip()
                        break
            
            # 如果找到分段URL，尝试获取第一个分段
            if segment_url:
                # 处理相对路径
                if not segment_url.startswith('http'):
                    base_url = url.rsplit('/', 1)[0]
                    segment_url = f"{base_url}/{segment_url}"
                
                try:
                    # 只获取前几个字节来验证可访问性
                    seg_response = requests.get(
                        segment_url, 
                        headers=headers, 
                        timeout=timeout, 
                        stream=True
                    )
                    
                    # 读取第一个块
                    next(seg_response.iter_content(chunk_size=1024), None)
                    
                    if seg_response.status_code != 200:
                        return False, response_time, f"分段HTTP错误: {seg_response.status_code}"
                    
                except Exception as e:
                    return False, response_time, f"分段访问错误: {str(e)}"
        
        return True, response_time, ""
        
    except requests.exceptions.Timeout:
        return False, 0, "连接超时"
    except requests.exceptions.ConnectionError:
        return False, 0, "连接错误"
    except Exception as e:
        return False, 0, f"未知错误: {str(e)}"


def verify_mpd_stream(url, timeout=10, db_path=None):
    """验证DASH (mpd)流的有效性"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        # 检查是否是MyTV Super的MPD
        if 'mytvsuper' in url.lower() or 'mytv' in url.lower():
            # 尝试从环境变量或配置获取token
            token = os.environ.get('MYTV_SUPER_TOKEN', '')
            
            # 从数据库获取token（如果提供了数据库路径）
            if db_path:
                try:
                    import sqlite3
                    conn = sqlite3.connect(db_path)
                    cursor = conn.cursor()
                    cursor.execute("SELECT token FROM mytv_tokens WHERE expiry > datetime('now') ORDER BY expiry DESC LIMIT 1")
                    result = cursor.fetchone()
                    conn.close()
                    
                    if result:
                        token = result[0]
                except Exception as e:
                    logger.warning(f"获取MyTV Super token失败: {str(e)}")
            
            # 添加token到URL
            if token:
                if '?' not in url:
                    url = f"{url}?token={token}"
                else:
                    url = f"{url}&token={token}"
                
                # 也添加到header
                headers['Authorization'] = f'Bearer {token}'
        
        start_time = time.time()
        
        # 获取MPD清单
        response = requests.get(url, headers=headers, timeout=timeout)
        
        if response.status_code != 200:
            return False, 0, f"HTTP错误: {response.status_code}"
        
        # 检查内容是否为有效的XML
        content = response.text
        if not content or not content.strip():
            return False, 0, "空响应内容"
        
        # 尝试解析XML
        try:
            root = ET.fromstring(content)
            
            # 检查是否是MPD格式
            if root.tag.endswith('MPD') or 'MPD' in root.tag:
                # 计算响应时间
                response_time = time.time() - start_time
                return True, response_time, ""
            else:
                return False, 0, "无效的MPD格式"
                
        except ET.ParseError:
            return False, 0, "XML解析错误"
        
    except requests.exceptions.Timeout:
        return False, 0, "连接超时"
    except requests.exceptions.ConnectionError:
        return False, 0, "连接错误"
    except Exception as e:
        return False, 0, f"未知错误: {str(e)}"


def verify_generic_stream(url, timeout=10):
    """验证其他类型流的有效性（如直接流、mp4等）"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        start_time = time.time()
        
        # 首先尝试HEAD请求，这样更高效
        head_response = requests.head(url, headers=headers, timeout=timeout)
        
        # 检查状态码
        if head_response.status_code == 200:
            response_time = time.time() - start_time
            
            # 检查内容类型，确保是媒体内容
            content_type = head_response.headers.get('content-type', '')
            if content_type and ('video' in content_type or 'audio' in content_type or 'application' in content_type):
                return True, response_time, ""
            else:
                # 内容类型可能不准确，尝试GET请求并读取一部分内容
                try:
                    get_response = requests.get(url, headers=headers, timeout=timeout, stream=True)
                    # 读取前8KB
                    content_sample = next(get_response.iter_content(chunk_size=8192), None)
                    
                    if content_sample and len(content_sample) > 0 and get_response.status_code == 200:
                        return True, time.time() - start_time, ""
                    else:
                        return False, 0, "内容验证失败"
                except Exception as e:
                    return False, response_time, f"GET请求错误: {str(e)}"
        else:
            # 某些服务器可能不支持HEAD，尝试GET
            get_response = requests.get(url, headers=headers, timeout=timeout, stream=True)
            
            # 只获取开始部分
            content = next(get_response.iter_content(chunk_size=8192), None)
            
            if content and len(content) > 0 and get_response.status_code == 200:
                response_time = time.time() - start_time
                return True, response_time, ""
            else:
                return False, 0, f"HTTP错误: {get_response.status_code}"
        
    except requests.exceptions.Timeout:
        return False, 0, "连接超时"
    except requests.exceptions.ConnectionError:
        return False, 0, "连接错误"
    except Exception as e:
        return False, 0, f"未知错误: {str(e)}"


def verify_stream(url, timeout=10, db_path=None):
    """
    主验证函数，根据URL类型选择合适的验证方法
    
    参数:
        url (str): 要验证的流URL
        timeout (int): 请求超时时间（秒）
        db_path (str): 数据库路径，用于MyTV Super token
        
    返回:
        tuple: (is_valid, response_time, error_message)
    """
    # 根据URL后缀选择验证方法
    if url.lower().endswith('.m3u8'):
        return verify_hls_stream(url, timeout)
    elif url.lower().endswith('.mpd'):
        return verify_mpd_stream(url, timeout, db_path)
    else:
        return verify_generic_stream(url, timeout)