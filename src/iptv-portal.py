"""
IPTV多源聚合与自动更新系统
- 自动从多个公开源收集IPTV频道
- 定期验证频道有效性
- 提供API和M3U生成服务
"""

import sys
import os
import re
import time
import json
import logging
import requests
import threading
import concurrent.futures
import sqlite3
from urllib.parse import urlparse
from flask import Flask, request, Response, jsonify, render_template

# 添加源目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
# 确保当前目录在Python路径中
if current_dir not in sys.path:
    sys.path.append(current_dir)

# 导入verify-streams模块中的函数
# 注意：Python导入时会将破折号替换为下划线
from verify_streams import verify_stream

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("iptv_system.log"), logging.StreamHandler()]
)
logger = logging.getLogger("iptv-system")

# 定义IPTV源 - 这些URL需要根据实际情况更新
IPTV_SOURCES = [
    "https://iptv-org.github.io/iptv/index.m3u",
    "https://raw.githubusercontent.com/benmoose39/YouTube_to_m3u/refs/heads/main/youtube.m3u",
    "https://github.com/Free-TV/IPTV/blob/master/playlist.m3u8",
    # 更多来源...可以添加其他GitHub上的开源IPTV资源
]

# 频道分组
CHANNEL_GROUPS = {
    "央视": ["CCTV", "央视", "CGTN"],
    "卫视": ["卫视", "东方", "北京", "湖南", "江苏"],
    "NewTV": ["NewTV", "NEWTV", "新电视"],
    "体育": ["体育", "足球", "NBA", "SPORT"],
    "电影": ["电影", "影院", "MOVIE", "剧场"],
    "纪录": ["纪录", "探索", "发现", "DISCOVERY", "历史"],
    "4K": ["4K", "8K", "UHD", "2160P", "ULTRA"],
    "其他": []  # 默认分组
}

# 数据库初始化
DB_PATH = "iptv_channels.db"

def init_database():
    """初始化SQLite数据库"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 创建频道表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS channels (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        url TEXT UNIQUE NOT NULL,
        group_name TEXT NOT NULL,
        logo TEXT,
        last_checked TIMESTAMP,
        is_active BOOLEAN DEFAULT 1,
        success_count INTEGER DEFAULT 0,
        fail_count INTEGER DEFAULT 0,
        source TEXT,
        added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # 创建检查历史表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS check_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        channel_id INTEGER,
        check_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        status BOOLEAN,
        response_time FLOAT,
        error_message TEXT,
        FOREIGN KEY (channel_id) REFERENCES channels (id)
    )
    ''')
    
    conn.commit()
    conn.close()
    logger.info("数据库初始化完成")

def parse_m3u(m3u_content, source_url=""):
    """解析M3U文件内容并提取频道信息"""
    channels = []
    lines = m3u_content.split('\n')
    
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        
        # 查找EXTINF行
        if line.startswith('#EXTINF:'):
            # 提取频道名称和属性
            channel_info = {}
            
            # 从EXTINF行提取元数据
            tvg_name = re.search(r'tvg-name="([^"]*)"', line)
            tvg_logo = re.search(r'tvg-logo="([^"]*)"', line)
            group_title = re.search(r'group-title="([^"]*)"', line)
            channel_name = re.search(r',(.*?)$', line)
            
            if tvg_name:
                channel_info['name'] = tvg_name.group(1)
            elif channel_name:
                channel_info['name'] = channel_name.group(1).strip()
            else:
                channel_info['name'] = f"未命名频道 {len(channels)}"
                
            channel_info['logo'] = tvg_logo.group(1) if tvg_logo else ""
            channel_info['group'] = group_title.group(1) if group_title else "其他"
            
            # 读取下一行获取URL
            i += 1
            if i < len(lines) and not lines[i].startswith('#'):
                channel_url = lines[i].strip()
                if channel_url:
                    channel_info['url'] = channel_url
                    channel_info['source'] = source_url
                    channels.append(channel_info)
        i += 1
        
    return channels

def determine_group(channel_name):
    """根据频道名称确定分组"""
    channel_name = channel_name.upper()
    
    for group, keywords in CHANNEL_GROUPS.items():
        for keyword in keywords:
            if keyword.upper() in channel_name:
                return group
    
    return "其他"



def fetch_and_process_source(source_url):
    """获取并处理单个IPTV源"""
    try:
        logger.info(f"开始处理源: {source_url}")
        response = requests.get(source_url, timeout=30)
        
        if response.status_code == 200:
            channels = parse_m3u(response.text, source_url)
            logger.info(f"从 {source_url} 解析出 {len(channels)} 个频道")
            
            # 保存到数据库
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            
            for channel in channels:
                # 确定分组
                if 'group' not in channel or not channel['group']:
                    channel['group'] = determine_group(channel['name'])
                
                # 检查是否已存在
                cursor.execute("SELECT id FROM channels WHERE url = ?", (channel['url'],))
                existing = cursor.fetchone()
                
                if not existing:
                    cursor.execute(
                        "INSERT INTO channels (name, url, group_name, logo, source) VALUES (?, ?, ?, ?, ?)",
                        (channel['name'], channel['url'], channel['group'], channel.get('logo', ''), source_url)
                    )
            
            conn.commit()
            conn.close()
            return len(channels)
        else:
            logger.error(f"无法获取源 {source_url}: HTTP {response.status_code}")
            return 0
    
    except Exception as e:
        logger.error(f"处理源 {source_url} 时出错: {str(e)}")
        return 0

def collect_from_all_sources():
    """从所有源收集频道"""
    total_channels = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_url = {executor.submit(fetch_and_process_source, url): url for url in IPTV_SOURCES}
        
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                count = future.result()
                total_channels += count
            except Exception as e:
                logger.error(f"处理源 {url} 时出现未处理的异常: {str(e)}")
    
    logger.info(f"收集完成，总共获取 {total_channels} 个频道")
    return total_channels

# 修改verify_channels函数，确保创建新的连接
def verify_channels(max_channels=None, only_check_inactive=False):
    """验证频道的有效性"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # 获取待验证的频道
    if only_check_inactive:
        cursor.execute("SELECT id, name, url FROM channels WHERE is_active = 0 ORDER BY last_checked ASC LIMIT ?", 
                      (max_channels or 100,))
    else:
        cursor.execute("SELECT id, name, url FROM channels ORDER BY last_checked ASC LIMIT ?", 
                      (max_channels or 100,))
    
    channels = [dict(row) for row in cursor.fetchall()]
    conn.close()
    
    logger.info(f"准备验证 {len(channels)} 个频道")
    
    def check_single_channel(channel):
        channel_id = channel['id']
        channel_name = channel['name']
        channel_url = channel['url']
        
        logger.debug(f"验证频道: {channel_name}")
        
        # 使用导入的verify_stream函数，传递数据库路径
        is_valid, response_time, error = verify_stream(channel_url, timeout=10, db_path=DB_PATH)
        
        # 创建新的数据库连接（解决SQLite多线程问题）
        local_conn = sqlite3.connect(DB_PATH)
        local_cursor = local_conn.cursor()
        
        try:
            # 更新频道状态
            if is_valid:
                local_cursor.execute(
                    "UPDATE channels SET last_checked = CURRENT_TIMESTAMP, is_active = 1, success_count = success_count + 1 WHERE id = ?",
                    (channel_id,)
                )
            else:
                local_cursor.execute(
                    "UPDATE channels SET last_checked = CURRENT_TIMESTAMP, is_active = 0, fail_count = fail_count + 1 WHERE id = ?",
                    (channel_id,)
                )
            
            # 记录检查历史
            local_cursor.execute(
                "INSERT INTO check_history (channel_id, status, response_time, error_message) VALUES (?, ?, ?, ?)",
                (channel_id, 1 if is_valid else 0, response_time, error)
            )
            
            local_conn.commit()
        except Exception as e:
            logger.error(f"更新频道状态时出错: {str(e)}")
        finally:
            local_conn.close()
        
        return is_valid, channel_name
    
    # 多线程验证
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(check_single_channel, channels))
    
    # 统计结果
    valid_count = sum(1 for valid, _ in results if valid)
    logger.info(f"验证完成: {valid_count}/{len(channels)} 个频道有效")
    
    return valid_count, len(channels)

def generate_m3u_content(group_filter=None):
    """生成M3U格式的播放列表"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # 构建查询条件
    query = "SELECT * FROM channels WHERE is_active = 1"
    params = []
    
    if group_filter:
        query += " AND group_name = ?"
        params.append(group_filter)
    
    query += " ORDER BY group_name, name"
    cursor.execute(query, params)
    
    channels = cursor.fetchall()
    conn.close()
    
    # 生成M3U内容
    m3u_content = "#EXTM3U\n"
    
    for channel in channels:
        logo = f' tvg-logo="{channel["logo"]}"' if channel["logo"] else ""
        m3u_content += f'#EXTINF:-1 tvg-id="{channel["id"]}"{logo} group-title="{channel["group_name"]}", {channel["name"]}\n'
        m3u_content += f'{channel["url"]}\n'
    
    return m3u_content

# Flask应用
app = Flask(__name__)

@app.route('/')
def index():
    """主页，显示频道统计信息"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # 获取总体统计
    cursor.execute("SELECT COUNT(*) as total FROM channels")
    total = cursor.fetchone()['total']
    
    cursor.execute("SELECT COUNT(*) as active FROM channels WHERE is_active = 1")
    active = cursor.fetchone()['active']
    
    # 获取分组统计
    cursor.execute("SELECT group_name, COUNT(*) as count FROM channels WHERE is_active = 1 GROUP BY group_name ORDER BY count DESC")
    groups = cursor.fetchall()
    
    # 获取最近添加的频道
    cursor.execute("SELECT * FROM channels ORDER BY added_date DESC LIMIT 10")
    recent = cursor.fetchall()
    
    conn.close()
    
    return render_template('index.html', total=total, active=active, groups=groups, recent=recent)

@app.route('/api/channels')
def api_channels():
    """API: 获取频道列表"""
    group = request.args.get('group')
    active_only = request.args.get('active', 'true').lower() == 'true'
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    query = "SELECT * FROM channels"
    params = []
    
    conditions = []
    if active_only:
        conditions.append("is_active = 1")
    
    if group:
        conditions.append("group_name = ?")
        params.append(group)
    
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    
    query += " ORDER BY group_name, name"
    cursor.execute(query, params)
    
    channels = [dict(channel) for channel in cursor.fetchall()]
    conn.close()
    
    return jsonify(channels)

@app.route('/api/groups')
def api_groups():
    """API: 获取所有分组"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
    SELECT group_name, COUNT(*) as count 
    FROM channels 
    WHERE is_active = 1 
    GROUP BY group_name 
    ORDER BY count DESC
    """)
    
    groups = [{"name": row[0], "count": row[1]} for row in cursor.fetchall()]
    conn.close()
    
    return jsonify(groups)

@app.route('/playlist.m3u')
def playlist():
    """生成并返回M3U播放列表"""
    group = request.args.get('group')
    m3u_content = generate_m3u_content(group)
    
    return Response(
        m3u_content,
        mimetype='audio/x-mpegurl',
        headers={'Content-Disposition': 'attachment; filename=playlist.m3u'}
    )

@app.route('/admin/collect', methods=['POST'])
def admin_collect():
    """管理接口：触发收集频道"""
    auth_key = request.headers.get('X-Auth-Key')
    if auth_key != os.environ.get('ADMIN_KEY', 'changeme'):
        return jsonify({"error": "未授权"}), 401
    
    threading.Thread(target=collect_from_all_sources).start()
    return jsonify({"message": "频道收集已在后台启动"})

@app.route('/admin/verify', methods=['POST'])
def admin_verify():
    """管理接口：触发验证频道"""
    auth_key = request.headers.get('X-Auth-Key')
    if auth_key != os.environ.get('ADMIN_KEY', 'changeme'):
        return jsonify({"error": "未授权"}), 401
    
    max_channels = request.json.get('max_channels')
    only_inactive = request.json.get('only_inactive', False)
    
    threading.Thread(target=verify_channels, args=(max_channels, only_inactive)).start()
    return jsonify({"message": "频道验证已在后台启动"})

# 定时任务
def run_scheduled_tasks():
    """运行定时任务"""
    while True:
        try:
            # 每6小时收集一次新频道
            collect_from_all_sources()
            
            # 每小时验证一批频道
            verify_channels(max_channels=100)
            
            # 休眠一小时
            time.sleep(3600)
        except Exception as e:
            logger.error(f"定时任务异常: {str(e)}")
            time.sleep(300)  # 出错后等待5分钟再试

def main():
    """主函数"""
    # 初始化数据库
    init_database()
    
    # 启动定时任务
    threading.Thread(target=run_scheduled_tasks, daemon=True).start()
    
    # 运行Web服务
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))

if __name__ == "__main__":
    main()