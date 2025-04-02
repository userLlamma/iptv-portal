# IPTV Proxy Project

## 项目简介
这是一个 IPTV 相关的 Python 项目，包含 proxy server、m3u 处理和 MPD 支持模块。

## 使用方式

### 安装依赖
```bash
python3 -m venv iptvenv
source iptvenv/bin/activate
pip install -r requirements.txt
```

### proxy-server.py 单独实用
使用方法

#### 运行代理服务器:
```bash
python3 -m venv iptvenv

source iptvenv/bin/activate

pip install -r requirements.txt

nohup python src/proxy-server.py > proxy-server.log 2>&1 &
```

#### 配合M3U转换工具使用:
```bash
python src/m3u-to-proxy.py 你的M3U文件 --proxy http://你的服务器IP:5000
```

#### API访问点:

/playlist.m3u: 获取所有频道的播放列表
/playlist.m3u?group=央视: 获取特定分组的播放列表
/proxy/channel/<channel_id>: 代理特定频道的直播流
/admin/add_channel_info: 添加/更新频道信息
/admin/add_source: 添加频道源