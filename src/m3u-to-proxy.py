#!/usr/bin/env python3
"""
M3U文件处理器
将现有M3U文件转换为使用本地代理的格式，并保留台标信息

用法:
    python m3u_processor.py input.m3u [--output output.m3u] [--proxy http://localhost:5000] [--logos]
"""

import os
import re
import sys
import time
import json
import logging
import argparse
import requests
from urllib.parse import urlparse, quote

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("m3u-processor")

def transform_m3u(input_path, output_path, proxy_url, keep_logos=True, download_logos=False, logo_dir=None):
    """
    转换M3U文件中的URL为本地代理URL，并处理图片
    
    参数:
        input_path: 输入的M3U文件路径
        output_path: 输出的M3U文件路径
        proxy_url: 本地代理服务器URL
        keep_logos: 是否保留原始台标URL
        download_logos: 是否下载台标到本地
        logo_dir: 台标保存目录
    """
    # 确保proxy_url不以/结尾
    if proxy_url.endswith('/'):
        proxy_url = proxy_url[:-1]
    
    # 确保logo_dir存在
    if download_logos and logo_dir:
        os.makedirs(logo_dir, exist_ok=True)
    
    # 读取输入文件
    with open(input_path, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read()
    
    lines = content.splitlines()
    output_lines = []
    processed_channels = 0
    
    # 定义正则表达式提取频道信息
    extinf_pattern = re.compile(r'^#EXTINF:.*?(?:tvg-id="([^"]*)")?.*?(?:tvg-name="([^"]*)")?.*?(?:tvg-logo="([^"]*)")?.*?(?:group-title="([^"]*)")?.*?,\s*(.*?))
    
    # 保留M3U头部（包括x-tvg-url等）
    if lines and lines[0].startswith('#EXTM3U'):
        output_lines.append(lines[0])
        lines = lines[1:]
    else:
        output_lines.append('#EXTM3U')
    
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        
        if line.startswith('#EXTINF:'):
            # 提取频道信息
            match = extinf_pattern.match(line)
            
            if match:
                tvg_id, tvg_name, logo_url, group_title, name = match.groups()
                
                # 确定channel_id
                channel_id = tvg_id or ''
                if not channel_id and name:
                    # 从名称生成标识符
                    channel_id = re.sub(r'[^a-zA-Z0-9_-]', '', name.lower().replace(' ', '_'))
                
                # 处理台标URL
                new_logo_url = None
                if logo_url and keep_logos:
                    if download_logos and logo_dir:
                        try:
                            # 下载台标
                            logo_filename = os.path.basename(urlparse(logo_url).path)
                            if not logo_filename or len(logo_filename) < 5:
                                logo_filename = f"{channel_id}.png"
                            
                            logo_path = os.path.join(logo_dir, logo_filename)
                            if not os.path.exists(logo_path):
                                response = requests.get(logo_url, timeout=5)
                                if response.status_code == 200:
                                    with open(logo_path, 'wb') as f:
                                        f.write(response.content)
                                    logger.info(f"已下载台标: {logo_filename}")
                            
                            # 使用本地路径
                            new_logo_url = f"{proxy_url}/logo/{channel_id}"
                        except Exception as e:
                            logger.warning(f"下载台标失败 {logo_url}: {str(e)}")
                            new_logo_url = logo_url
                    else:
                        new_logo_url = logo_url
                
                # 构建新的EXTINF行
                new_line = "#EXTINF:-1"
                if tvg_id:
                    new_line += f" tvg-id=\"{tvg_id}\""
                elif channel_id:
                    new_line += f" tvg-id=\"{channel_id}\""
                
                if tvg_name:
                    new_line += f" tvg-name=\"{tvg_name}\""
                
                if new_logo_url:
                    new_line += f" tvg-logo=\"{new_logo_url}\""
                
                if group_title:
                    new_line += f" group-title=\"{group_title}\""
                
                new_line += f", {name}"
                
                output_lines.append(new_line)
                
                # 读取下一行URL并替换
                i += 1
                if i < len(lines) and not lines[i].startswith('#'):
                    original_url = lines[i].strip()
                    
                    # 添加频道信息和源到代理服务器
                    try:
                        # 添加频道信息
                        info_data = {
                            "channel_id": channel_id,
                            "display_name": tvg_name or name,
                            "logo_url": logo_url,
                            "epg_id": tvg_id,
                            "group_title": group_title
                        }
                        requests.post(
                            f"{proxy_url}/admin/add_channel_info",
                            json=info_data,
                            headers={"X-Auth-Key": os.environ.get('ADMIN_KEY', 'changeme')},
                            timeout=1
                        )
                        
                        # 添加源
                        source_data = {
                            "channel_id": channel_id,
                            "url": original_url,
                            "priority": 10
                        }
                        requests.post(
                            f"{proxy_url}/admin/add_source",
                            json=source_data,
                            headers={"X-Auth-Key": os.environ.get('ADMIN_KEY', 'changeme')},
                            timeout=1
                        )
                    except:
                        # 忽略错误，继续处理
                        pass
                    
                    # 替换URL为代理URL
                    new_url = f"{proxy_url}/proxy/channel/{channel_id}"
                    output_lines.append(new_url)
                    
                    processed_channels += 1
            else:
                # 无法解析的EXTINF行，保持原样
                output_lines.append(line)
                i += 1
                if i < len(lines) and not lines[i].startswith('#'):
                    output_lines.append(lines[i])
        else:
            # 其他行保持原样
            output_lines.append(line)
        
        i += 1
    
    # 写入输出文件
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(output_lines))
    
    return processed_channels

def download_logo_batch(channels, logo_dir):
    """批量下载频道台标"""
    if not os.path.exists(logo_dir):
        os.makedirs(logo_dir)
    
    success_count = 0
    fail_count = 0
    
    for channel in channels:
        if 'logo_url' in channel and channel['logo_url']:
            try:
                channel_id = channel.get('channel_id', '')
                logo_url = channel['logo_url']
                
                # 生成文件名
                parsed_url = urlparse(logo_url)
                file_name = os.path.basename(parsed_url.path)
                
                if not file_name or len(file_name) < 5:
                    file_name = f"{channel_id}.png"
                
                local_path = os.path.join(logo_dir, file_name)
                
                # 下载图片
                response = requests.get(logo_url, timeout=10)
                if response.status_code == 200:
                    with open(local_path, 'wb') as f:
                        f.write(response.content)
                    success_count += 1
                else:
                    logger.warning(f"下载失败: {logo_url}, 状态码: {response.status_code}")
                    fail_count += 1
            except Exception as e:
                logger.error(f"下载出错: {logo_url}: {str(e)}")
                fail_count += 1
    
    return success_count, fail_count

def parse_m3u(file_path):
    """解析M3U文件，提取频道信息"""
    channels = []
    
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read()
    
    lines = content.splitlines()
    extinf_pattern = re.compile(r'^#EXTINF:.*?(?:tvg-id="([^"]*)")?.*?(?:tvg-name="([^"]*)")?.*?(?:tvg-logo="([^"]*)")?.*?(?:group-title="([^"]*)")?.*?,\s*(.*?))
    
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        
        if line.startswith('#EXTINF:'):
            match = extinf_pattern.match(line)
            
            if match:
                tvg_id, tvg_name, logo_url, group_title, name = match.groups()
                
                # 确定channel_id
                channel_id = tvg_id or ''
                if not channel_id and name:
                    channel_id = re.sub(r'[^a-zA-Z0-9_-]', '', name.lower().replace(' ', '_'))
                
                # 读取URL
                i += 1
                url = ''
                if i < len(lines) and not lines[i].startswith('#'):
                    url = lines[i].strip()
                
                # 添加频道信息
                channel = {
                    'channel_id': channel_id,
                    'name': name,
                    'tvg_name': tvg_name,
                    'tvg_id': tvg_id,
                    'logo_url': logo_url,
                    'group_title': group_title,
                    'url': url
                }
                
                channels.append(channel)
        
        i += 1
    
    return channels

def main():
    parser = argparse.ArgumentParser(description='M3U文件处理器 - 转换M3U格式并处理台标')
    parser.add_argument('input', help='输入的M3U文件路径')
    parser.add_argument('--output', help='输出的M3U文件路径')
    parser.add_argument('--proxy', default='http://localhost:5000', help='代理服务器URL')
    parser.add_argument('--logos', action='store_true', help='保留原始台标URL')
    parser.add_argument('--download-logos', action='store_true', help='下载台标到本地')
    parser.add_argument('--logo-dir', default='./logos', help='台标保存目录')
    parser.add_argument('--download-only', action='store_true', help='仅下载台标，不处理M3U')
    parser.add_argument('--admin-key', help='代理服务器的管理密钥')
    
    args = parser.parse_args()
    
    # 设置管理密钥
    if args.admin_key:
        os.environ['ADMIN_KEY'] = args.admin_key
    
    # 仅下载台标模式
    if args.download_only:
        channels = parse_m3u(args.input)
        success, fail = download_logo_batch(channels, args.logo_dir)
        print(f"台标下载完成：成功 {success}，失败 {fail}")
        return 0
    
    # 如果未指定输出文件，则在输入文件名基础上添加后缀
    if not args.output:
        base_name, ext = os.path.splitext(args.input)
        args.output = f"{base_name}_proxy{ext}"
    
    # 执行转换
    try:
        processed = transform_m3u(
            args.input, 
            args.output, 
            args.proxy,
            keep_logos=args.logos,
            download_logos=args.download_logos,
            logo_dir=args.logo_dir
        )
        
        print(f"处理完成! 已转换 {processed} 个频道到 {args.output}")
    except Exception as e:
        print(f"错误: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())