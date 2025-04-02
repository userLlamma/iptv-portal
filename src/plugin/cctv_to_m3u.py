#!/usr/bin/env python3
"""
CCTV直播流转M3U完整代码
从CCTV官网获取直播流并转换为可用的M3U播放列表

特点:
- 自动获取CCTV频道的真实直播流URL
- 添加必要的HTTP头以绕过防盗链
- 可与IPTV代理服务器集成
- 支持VLC、KODI等主流播放器

用法:
    python cctv_to_m3u.py [--output cctv.m3u] [--proxy http://localhost:5000]
"""

import os
import re
import sys
import json
import time
import logging
import argparse
import requests
from urllib.parse import urlparse, quote
from concurrent.futures import ThreadPoolExecutor

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("cctv-to-m3u")

# CCTV频道信息
CCTV_CHANNELS = [
    {"name": "CCTV-1 综合", "id": "cctv1", "url": "https://tv.cctv.com/live/cctv1/"},
    {"name": "CCTV-2 财经", "id": "cctv2", "url": "https://tv.cctv.com/live/cctv2/"},
    {"name": "CCTV-3 综艺", "id": "cctv3", "url": "https://tv.cctv.com/live/cctv3/"},
    {"name": "CCTV-4 中文国际", "id": "cctv4", "url": "https://tv.cctv.com/live/cctv4/"},
    {"name": "CCTV-5 体育", "id": "cctv5", "url": "https://tv.cctv.com/live/cctv5/"},
    {"name": "CCTV-5+ 体育赛事", "id": "cctv5plus", "url": "https://tv.cctv.com/live/cctv5plus/"},
    {"name": "CCTV-6 电影", "id": "cctv6", "url": "https://tv.cctv.com/live/cctv6/"},
    {"name": "CCTV-7 国防军事", "id": "cctv7", "url": "https://tv.cctv.com/live/cctv7/"},
    {"name": "CCTV-8 电视剧", "id": "cctv8", "url": "https://tv.cctv.com/live/cctv8/"},
    {"name": "CCTV-9 纪录", "id": "cctv9", "url": "https://tv.cctv.com/live/cctvjilu/"},
    {"name": "CCTV-10 科教", "id": "cctv10", "url": "https://tv.cctv.com/live/cctv10/"},
    {"name": "CCTV-11 戏曲", "id": "cctv11", "url": "https://tv.cctv.com/live/cctv11/"},
    {"name": "CCTV-12 社会与法", "id": "cctv12", "url": "https://tv.cctv.com/live/cctv12/"},
    {"name": "CCTV-13 新闻", "id": "cctv13", "url": "https://tv.cctv.com/live/cctv13/"},
    {"name": "CCTV-14 少儿", "id": "cctv14", "url": "https://tv.cctv.com/live/cctvchild/"},
    {"name": "CCTV-15 音乐", "id": "cctv15", "url": "https://tv.cctv.com/live/cctv15/"},
    {"name": "CCTV-16 奥林匹克", "id": "cctv16", "url": "https://tv.cctv.com/live/cctv16/"},
    {"name": "CCTV-17 农业农村", "id": "cctv17", "url": "https://tv.cctv.com/live/cctv17/"},
    {"name": "CCTV 欧洲", "id": "cctveurope", "url": "https://tv.cctv.com/live/cctveurope/"},
    {"name": "CCTV 美洲", "id": "cctvamerica", "url": "https://tv.cctv.com/live/cctvamerica/"}
]

# CCTV标准HTTP头
CCTV_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
    'Accept': '*/*',
    'Accept-Language': 'en,zh-CN;q=0.9,zh;q=0.8,zh-TW;q=0.7',
    'Origin': 'https://tv.cctv.com',
    'Referer': 'https://tv.cctv.com/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'cross-site'
}

def get_channel_logo(channel_id):
    """获取频道LOGO URL"""
    channel_num = channel_id.replace('cctv', '')
    
    # 特殊频道处理
    if 'plus' in channel_num:
        channel_num = '5plus'
    elif channel_id == 'cctv9' or channel_id == 'cctvjilu':
        channel_num = '9'
    elif channel_id == 'cctv14' or channel_id == 'cctvchild':
        channel_num = '14'
    
    # CCTV官方台标URL
    logo_url = f"https://p1.img.cctvpic.com/photoAlbum/templet/common/DEPA1532314258547503/cctv-{channel_num}_180629.png"
    return logo_url

def get_stream_url_from_api(channel_id):
    """尝试从API获取流URL"""
    api_endpoints = [
        f"https://vdn.live.cntv.cn/api2/liveHtml5.do?channel={channel_id}&client=html5",
        f"https://api.cntv.cn/livechannelplayer/getchannel?channel={channel_id}&jsonp=callback",
        f"https://vdn.apps.cntv.cn/api/getLiveUrlCNTV.do?id={channel_id}&p=web"
    ]
    
    for api_url in api_endpoints:
        try:
            logger.debug(f"尝试API: {api_url}")
            response = requests.get(api_url, headers=CCTV_HEADERS, timeout=5)
            
            # 解析返回的数据
            if response.status_code == 200:
                if 'callback(' in response.text:
                    # JSONP格式
                    json_text = re.search(r'callback\((.*)\)', response.text)
                    if json_text:
                        data = json.loads(json_text.group(1))
                else:
                    # 纯JSON格式
                    data = response.json()
                
                # 提取流URL
                if 'hls_url' in data and data['hls_url']:
                    stream_url = data['hls_url'].replace('\\', '')
                    
                    # 验证URL是否有效
                    if validate_stream_url(stream_url):
                        logger.info(f"API成功获取流URL ({channel_id}): {stream_url}")
                        return stream_url
        except Exception as e:
            logger.debug(f"API请求失败 ({api_url}): {str(e)}")
    
    return None

def extract_stream_from_html(channel_url):
    """从页面HTML中提取流URL"""
    try:
        logger.debug(f"从HTML页面提取: {channel_url}")
        response = requests.get(channel_url, headers=CCTV_HEADERS, timeout=10)
        
        if response.status_code == 200:
            html_content = response.text
            
            # 查找播放器配置
            config_match = re.search(r'var\s+(?:html5VideoData|config)\s*=\s*(\{.+?\});', html_content, re.DOTALL)
            if config_match:
                try:
                    # 清理JavaScript对象为合法JSON
                    config_text = config_match.group(1)
                    config_text = re.sub(r'(\w+):', r'"\1":', config_text)
                    config_text = re.sub(r',\s*}', '}', config_text)
                    config_text = re.sub(r',\s*]', ']', config_text)
                    
                    config = json.loads(config_text)
                    
                    # 查找视频URL
                    if 'video' in config and 'chapters' in config['video']:
                        chapters = config['video']['chapters']
                        if chapters and len(chapters) > 0 and 'url' in chapters[0]:
                            stream_url = chapters[0]['url']
                            if validate_stream_url(stream_url):
                                logger.info(f"从HTML提取流URL: {stream_url}")
                                return stream_url
                except Exception as e:
                    logger.debug(f"解析配置失败: {str(e)}")
            
            # 查找GUID
            guid_match = re.search(r'var\s+guid\s*=\s*["\']([\w]+)["\']', html_content)
            if guid_match:
                guid = guid_match.group(1)
                api_url = f"https://vdn.apps.cntv.cn/api/getLiveUrlCNTV.do?id={guid}&p=web"
                
                try:
                    api_response = requests.get(api_url, headers=CCTV_HEADERS, timeout=5)
                    if api_response.status_code == 200:
                        data = api_response.json()
                        if 'hls_url' in data and data['hls_url']:
                            stream_url = data['hls_url']
                            if validate_stream_url(stream_url):
                                logger.info(f"从GUID API提取流URL: {stream_url}")
                                return stream_url
                except Exception as e:
                    logger.debug(f"GUID API失败: {str(e)}")
            
            # 直接搜索m3u8链接
            m3u8_matches = re.findall(r'(https?://[^"\']+\.m3u8[^"\']*)', html_content)
            for url in m3u8_matches:
                if validate_stream_url(url):
                    logger.info(f"从HTML直接提取流URL: {url}")
                    return url
        
    except Exception as e:
        logger.debug(f"HTML提取失败: {str(e)}")
    
    return None

def validate_stream_url(url):
    """验证流媒体URL是否可访问"""
    try:
        logger.debug(f"验证URL: {url}")
        response = requests.get(url, headers=CCTV_HEADERS, timeout=5, stream=True)
        
        # 读取一小部分数据验证
        content = next(response.iter_content(chunk_size=1024), None)
        
        if response.status_code == 200 and content:
            return True
        return False
    except Exception as e:
        logger.debug(f"验证失败: {str(e)}")
        return False

def get_stream_for_channel(channel):
    """获取频道的流媒体URL"""
    channel_id = channel['id']
    logger.info(f"处理频道: {channel['name']} ({channel_id})")
    
    # 方法1: 从API获取
    stream_url = get_stream_url_from_api(channel_id)
    if stream_url:
        return stream_url
    
    # 方法2: 从HTML页面提取
    stream_url = extract_stream_from_html(channel['url'])
    if stream_url:
        return stream_url
    
    # 方法3: 尝试常见的URL模式
    common_patterns = [
        f"https://ldncctvwbcdbyte.volcfcdn.com/ldncctvwbcd/cdrmld{channel_id}_1/index.m3u8",
        f"https://cctvalih5ca.v.myalicdn.com/live/{channel_id}_2/index.m3u8",
        f"https://cctvwbndks.v.kcdnvip.com/cctvwbnd/{channel_id}_2/index.m3u8",
        f"https://cctvwbndtxy.liveplay.myqcloud.com/cctvwbnd/{channel_id}_2/index.m3u8"
    ]
    
    for url in common_patterns:
        if validate_stream_url(url):
            logger.info(f"从常见模式匹配到流URL: {url}")
            return url
    
    logger.warning(f"未找到 {channel['name']} 的有效流URL")
    return None

def process_all_channels():
    """处理所有频道并获取流媒体URL"""
    channels_with_stream = []
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        # 创建频道ID到频道的映射
        channel_map = {channel['id']: channel for channel in CCTV_CHANNELS}
        
        # 创建任务
        future_to_channel = {executor.submit(get_stream_for_channel, channel): channel['id'] for channel in CCTV_CHANNELS}
        
        # 处理结果
        for future in future_to_channel:
            channel_id = future_to_channel[future]
            channel = channel_map[channel_id]
            
            try:
                stream_url = future.result()
                if stream_url:
                    # 添加流URL和LOGO
                    channel_info = channel.copy()
                    channel_info['stream_url'] = stream_url
                    channel_info['logo'] = get_channel_logo(channel_id)
                    channels_with_stream.append(channel_info)
            except Exception as e:
                logger.error(f"处理频道 {channel['name']} 时出错: {str(e)}")
    
    return channels_with_stream

def generate_m3u(channels, proxy_url=None):
    """生成M3U格式内容"""
    m3u_content = "#EXTM3U\n"
    
    for channel in channels:
        # 添加台标
        logo_attr = f' tvg-logo="{channel["logo"]}"' if 'logo' in channel else ''
        
        # 添加频道信息行
        m3u_content += f'#EXTINF:-1 tvg-id="{channel["id"]}"{logo_attr} group-title="CCTV", {channel["name"]}\n'
        
        # 添加流URL（如果使用代理，替换URL）
        if proxy_url:
            # 通过代理访问
            m3u_content += f'{proxy_url}/proxy/channel/{channel["id"]}\n'
            
            # 向代理添加频道信息和源
            try:
                requests.post(
                    f"{proxy_url}/admin/add_channel_info",
                    json={
                        "channel_id": channel["id"],
                        "display_name": channel["name"],
                        "logo_url": channel.get("logo", ""),
                        "group_title": "CCTV"
                    },
                    headers={"X-Auth-Key": os.environ.get("ADMIN_KEY", "changeme")},
                    timeout=1
                )
                
                requests.post(
                    f"{proxy_url}/admin/add_source",
                    json={
                        "channel_id": channel["id"],
                        "url": channel["stream_url"],
                        "priority": 10
                    },
                    headers={"X-Auth-Key": os.environ.get("ADMIN_KEY", "changeme")},
                    timeout=1
                )
                logger.info(f"已将频道 {channel['name']} 添加到代理服务器")
            except Exception as e:
                logger.error(f"添加到代理失败: {str(e)}")
        else:
            # 直接使用URL，添加必要的VLC选项
            m3u_content += f'#EXTVLCOPT:http-referrer=https://tv.cctv.com/\n'
            m3u_content += f'#EXTVLCOPT:http-user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36\n'
            m3u_content += f'{channel["stream_url"]}\n'
    
    return m3u_content

def main():
    parser = argparse.ArgumentParser(description="CCTV直播流转M3U工具")
    parser.add_argument("--output", default="cctv_channels.m3u", help="输出的M3U文件路径")
    parser.add_argument("--proxy", help="代理服务器URL (例如 http://localhost:5000)")
    parser.add_argument("--admin-key", help="代理服务器的管理密钥")
    parser.add_argument("--debug", action="store_true", help="启用调试日志")
    
    args = parser.parse_args()
    
    # 设置日志级别
    if args.debug:
        logger.setLevel(logging.DEBUG)
    
    # 设置管理密钥
    if args.admin_key:
        os.environ["ADMIN_KEY"] = args.admin_key
    
    # 获取所有可用的直播流
    logger.info("开始获取CCTV频道直播流...")
    channels = process_all_channels()
    
    if not channels:
        logger.error("未能获取任何可用的直播流")
        return 1
    
    # 生成M3U内容
    m3u_content = generate_m3u(channels, args.proxy)
    
    # 写入文件
    with open(args.output, "w", encoding="utf-8") as f:
        f.write(m3u_content)
    
    logger.info(f"已生成M3U文件: {args.output} (包含 {len(channels)} 个频道)")
    return 0

if __name__ == "__main__":
    sys.exit(main())