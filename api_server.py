# api_server.py
# 新一代 LMArena Bridge 后端服务

import asyncio
import json
import logging
import mimetypes
import os
import random
import re
import subprocess
import threading
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Optional, Tuple

import aiohttp  # 新增：用于异步HTTP请求
import requests
import sys
import time
import urllib3
import uvicorn
from asyncio import Semaphore
from collections import deque
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse, Response, HTMLResponse
# --- 内部模块导入 ---
from modules.file_uploader import upload_to_file_bed
from modules.monitoring import monitoring_service, MonitorConfig
from packaging.version import parse as parse_version

# 图像自动增强功能已移除（已剥离为独立项目）
# 全局禁用SSL警告（可选，但推荐）
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- 基础配置 ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- 日志过滤器 ---
class EndpointFilter(logging.Filter):
    """过滤掉监控相关的API请求日志"""
    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        # 过滤掉所有 /api/monitor/ 和 /monitor 的GET请求
        is_monitor_request = "GET /api/monitor/" in message or "GET /monitor " in message
        return not is_monitor_request

# 将过滤器添加到uvicorn的访问日志记录器
logging.getLogger("uvicorn.access").addFilter(EndpointFilter())

# --- 全局状态与配置 ---
CONFIG = {} # 存储从 config.jsonc 加载的配置
# 新增：支持多标签页并发连接
# browser_connections 用于存储多个油猴脚本的 WebSocket 连接
# 键是标签页ID（tab_id），值是 WebSocket 对象
browser_connections: dict[str, WebSocket] = {}
browser_connections_lock = asyncio.Lock()  # 保护并发访问
# 新增：跟踪标签页连接时间
tab_connection_times: dict[str, float] = {}
# 兼容性：保留browser_ws用于向后兼容（指向第一个连接）
browser_ws: WebSocket | None = None
# response_channels 用于存储每个 API 请求的响应队列。
# 键是 request_id，值是 asyncio.Queue。
response_channels: dict[str, asyncio.Queue] = {}
# 新增：请求元数据存储（用于WebSocket重连后恢复请求）
request_metadata: dict[str, dict] = {}
# 新增：跟踪每个标签页的活跃请求数
tab_request_counts: dict[str, int] = {}
tab_request_counts_lock = asyncio.Lock()
last_activity_time = None # 记录最后一次活动的时间
idle_monitor_thread = None # 空闲监控线程
main_event_loop = None # 主事件循环
# 新增：用于跟踪是否因人机验证而刷新
IS_REFRESHING_FOR_VERIFICATION = False
# 新增：用于自动重试的请求暂存队列
pending_requests_queue = asyncio.Queue()
# 新增：WebSocket连接锁，保护并发访问
ws_lock = asyncio.Lock()
# 新增：全局aiohttp会话
aiohttp_session = None
# --- 图片自动下载配置 ---
IMAGE_SAVE_DIR = Path("./downloaded_images")
IMAGE_SAVE_DIR.mkdir(exist_ok=True)
# 使用deque限制大小，避免内存泄漏
downloaded_image_urls = deque(maxlen=5000)  # 最多记录5000个URL
downloaded_urls_set = set()  # 用于快速查重
# 新增：用于在运行时临时禁用失败的图床端点
DISABLED_ENDPOINTS = {}  # 改为字典，记录禁用时间
# 新增：用于轮询策略的全局索引
ROUND_ROBIN_INDEX = 0
# 新增：图床恢复时间（秒）
FILEBED_RECOVERY_TIME = 300  # 5分钟后自动恢复

# 新增：用于模型ID映射轮询的索引字典（需要线程安全保护）
MODEL_ROUND_ROBIN_INDEX = {}  # {model_name: current_index}
MODEL_ROUND_ROBIN_LOCK = Lock()  # 保护轮询索引的线程锁

# 新增：图片Base64缓存（避免重复下载和转换）
IMAGE_BASE64_CACHE = {}  # {url: (base64_data, timestamp)}
IMAGE_CACHE_MAX_SIZE = 1000  # 最多缓存100张图片
IMAGE_CACHE_TTL = 3600  # 缓存有效期1小时（秒）

# 新增：图床URL缓存（避免相同图片重复上传）
FILEBED_URL_CACHE = {}  # {image_hash: (uploaded_url, timestamp)}
FILEBED_URL_CACHE_TTL = 300  # 图床链接缓存5分钟（秒）
FILEBED_URL_CACHE_MAX_SIZE = 500  # 最多缓存500个图床链接

# 新增：并发下载控制
DOWNLOAD_SEMAPHORE: Optional[Semaphore] = None
MAX_CONCURRENT_DOWNLOADS = 50  # 默认最大并发下载数



# --- 模型映射 ---
# MODEL_NAME_TO_ID_MAP 现在将存储更丰富的对象： { "model_name": {"id": "...", "type": "..."} }
MODEL_NAME_TO_ID_MAP = {}
MODEL_ENDPOINT_MAP = {} # 新增：用于存储模型到 session/message ID 的映射
DEFAULT_MODEL_ID = None # 默认模型id: None

def load_model_endpoint_map():
    """从 model_endpoint_map.json 加载模型到端点的映射。"""
    global MODEL_ENDPOINT_MAP
    try:
        with open('model_endpoint_map.json', 'r', encoding='utf-8') as f:
            content = f.read()
            # 允许空文件
            if not content.strip():
                MODEL_ENDPOINT_MAP = {}
            else:
                MODEL_ENDPOINT_MAP = json.loads(content)
        logger.info(f"成功从 'model_endpoint_map.json' 加载了 {len(MODEL_ENDPOINT_MAP)} 个模型端点映射。")
    except FileNotFoundError:
        logger.warning("'model_endpoint_map.json' 文件未找到。将使用空映射。")
        MODEL_ENDPOINT_MAP = {}
    except json.JSONDecodeError as e:
        logger.error(f"加载或解析 'model_endpoint_map.json' 失败: {e}。将使用空映射。")
        MODEL_ENDPOINT_MAP = {}

def _parse_jsonc(jsonc_string: str) -> dict:
    """
    稳健地解析 JSONC 字符串，移除注释。
    改进版：正确处理字符串内的 // 和 /* */
    """
    lines = jsonc_string.splitlines()
    no_comments_lines = []
    in_block_comment = False
    
    for line in lines:
        if in_block_comment:
            # 在块注释中，查找结束标记
            if '*/' in line:
                in_block_comment = False
                # 保留块注释结束后的内容
                line = line.split('*/', 1)[1]
            else:
                continue
        
        # 处理可能的块注释开始
        if '/*' in line:
            # 需要更智能地处理，避免删除字符串中的 /*
            before_comment, _, after_comment = line.partition('/*')
            if '*/' in after_comment:
                # 单行块注释
                _, _, after_block = after_comment.partition('*/')
                line = before_comment + after_block
            else:
                # 多行块注释开始
                line = before_comment
                in_block_comment = True
        
        # 处理单行注释 //，但要避免删除字符串中的 //
        # 使用更智能的方法：查找不在引号内的 //
        processed_line = ""
        in_string = False
        escape_next = False
        i = 0
        
        while i < len(line):
            char = line[i]
            
            if escape_next:
                processed_line += char
                escape_next = False
                i += 1
                continue
            
            if char == '\\':
                processed_line += char
                escape_next = True
                i += 1
                continue
            
            if char == '"' and not in_string:
                in_string = True
                processed_line += char
            elif char == '"' and in_string:
                in_string = False
                processed_line += char
            elif char == '/' and i + 1 < len(line) and line[i + 1] == '/' and not in_string:
                # 找到了真正的注释，停止处理这一行
                break
            else:
                processed_line += char
            
            i += 1
        
        # 只有非空行才添加
        if processed_line.strip():
            no_comments_lines.append(processed_line)

    return json.loads("\n".join(no_comments_lines))

def load_config():
    """从 config.jsonc 加载配置，并处理 JSONC 注释。"""
    global CONFIG
    try:
        with open('config.jsonc', 'r', encoding='utf-8') as f:
            content = f.read()
        CONFIG = _parse_jsonc(content)
        logger.info("成功从 'config.jsonc' 加载配置。")
        # 打印关键配置状态
        logger.info(f"  - 酒馆模式 (Tavern Mode): {'✅ 启用' if CONFIG.get('tavern_mode_enabled') else '❌ 禁用'}")
        logger.info(f"  - 绕过模式 (Bypass Mode): {'✅ 启用' if CONFIG.get('bypass_enabled') else '❌ 禁用'}")
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.error(f"加载或解析 'config.jsonc' 失败: {e}。将使用默认配置。")
        CONFIG = {}

def load_model_map():
    """从 models.json 加载模型映射，支持 'id:type' 格式。"""
    global MODEL_NAME_TO_ID_MAP
    try:
        with open('models.json', 'r', encoding='utf-8') as f:
            raw_map = json.load(f)
            
        processed_map = {}
        for name, value in raw_map.items():
            if isinstance(value, str) and ':' in value:
                parts = value.split(':', 1)
                model_id = parts[0] if parts[0].lower() != 'null' else None
                model_type = parts[1]
                processed_map[name] = {"id": model_id, "type": model_type}
            else:
                # 默认或旧格式处理
                processed_map[name] = {"id": value, "type": "text"}

        MODEL_NAME_TO_ID_MAP = processed_map
        logger.info(f"成功从 'models.json' 加载并解析了 {len(MODEL_NAME_TO_ID_MAP)} 个模型。")

    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.error(f"加载 'models.json' 失败: {e}。将使用空模型列表。")
        MODEL_NAME_TO_ID_MAP = {}

# --- 公告处理 ---
def check_and_display_announcement():
    """检查并显示一次性公告。"""
    announcement_file = "announcement-lmarena.json"
    if os.path.exists(announcement_file):
        try:
            logger.info("="*60)
            logger.info("📢 检测到更新公告，内容如下:")
            with open(announcement_file, 'r', encoding='utf-8') as f:
                announcement = json.load(f)
                title = announcement.get("title", "公告")
                content = announcement.get("content", [])
                
                logger.info(f"   --- {title} ---")
                for line in content:
                    logger.info(f"   {line}")
                logger.info("="*60)

        except json.JSONDecodeError:
            logger.error(f"无法解析公告文件 '{announcement_file}'。文件内容可能不是有效的JSON。")
        except Exception as e:
            logger.error(f"读取公告文件时发生错误: {e}")
        finally:
            try:
                os.remove(announcement_file)
                logger.info(f"公告文件 '{announcement_file}' 已被移除。")
            except OSError as e:
                logger.error(f"删除公告文件 '{announcement_file}' 失败: {e}")

# --- 更新检查 ---
GITHUB_REPO = "zhongruichen/LMArenaBridge-mogai"

def download_and_extract_update(version):
    """下载并解压最新版本到临时文件夹。"""
    update_dir = "update_temp"
    if not os.path.exists(update_dir):
        os.makedirs(update_dir)

    try:
        zip_url = f"https://github.com/{GITHUB_REPO}/archive/refs/heads/mogai-version.zip"
        logger.info(f"正在从 {zip_url} 下载新版本...")
        response = requests.get(zip_url, timeout=60)
        response.raise_for_status()

        # 需要导入 zipfile 和 io
        import zipfile
        import io
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            z.extractall(update_dir)
        
        logger.info(f"新版本已成功下载并解压到 '{update_dir}' 文件夹。")
        return True
    except requests.RequestException as e:
        logger.error(f"下载更新失败: {e}")
    except zipfile.BadZipFile:
        logger.error("下载的文件不是一个有效的zip压缩包。")
    except Exception as e:
        logger.error(f"解压更新时发生未知错误: {e}")
    
    return False

def check_for_updates():
    """从 GitHub 检查新版本。"""
    if not CONFIG.get("enable_auto_update", True):
        logger.info("自动更新已禁用，跳过检查。")
        return

    current_version = CONFIG.get("version", "0.0.0")
    logger.info(f"当前版本: {current_version}。正在从 GitHub 检查更新...")

    try:
        config_url = f"https://raw.githubusercontent.com/{GITHUB_REPO}/mogai-version/config.jsonc"
        response = requests.get(config_url, timeout=10)
        response.raise_for_status()

        jsonc_content = response.text
        remote_config = _parse_jsonc(jsonc_content)
        
        remote_version_str = remote_config.get("version")
        if not remote_version_str:
            logger.warning("远程配置文件中未找到版本号，跳过更新检查。")
            return

        if parse_version(remote_version_str) > parse_version(current_version):
            logger.info("="*60)
            logger.info(f"🎉 发现新版本! 🎉")
            logger.info(f"  - 当前版本: {current_version}")
            logger.info(f"  - 最新版本: {remote_version_str}")
            if download_and_extract_update(remote_version_str):
                logger.info("准备应用更新。服务器将在5秒后关闭并启动更新脚本。")
                time.sleep(5)
                update_script_path = os.path.join("modules", "update_script.py")
                # 使用 Popen 启动独立进程
                subprocess.Popen([sys.executable, update_script_path])
                # 优雅地退出当前服务器进程
                os._exit(0)
            else:
                logger.error(f"自动更新失败。请访问 https://github.com/{GITHUB_REPO}/releases/latest 手动下载。")
            logger.info("="*60)
        else:
            logger.info("您的程序已是最新版本。")

    except requests.RequestException as e:
        logger.error(f"检查更新失败: {e}")
    except json.JSONDecodeError:
        logger.error("解析远程配置文件失败。")
    except Exception as e:
        logger.error(f"检查更新时发生未知错误: {e}")

# --- 模型更新 ---
def extract_models_from_html(html_content):
    """
    从 HTML 内容中提取完整的模型JSON对象，使用括号匹配确保完整性。
    """
    models = []
    model_names = set()
    
    # 查找所有可能的模型JSON对象的起始位置
    for start_match in re.finditer(r'\{\\"id\\":\\"[a-f0-9-]+\\"', html_content):
        start_index = start_match.start()
        
        # 从起始位置开始，进行花括号匹配
        open_braces = 0
        end_index = -1
        
        # 优化：设置一个合理的搜索上限，避免无限循环
        search_limit = start_index + 10000 # 假设一个模型定义不会超过10000个字符
        
        for i in range(start_index, min(len(html_content), search_limit)):
            if html_content[i] == '{':
                open_braces += 1
            elif html_content[i] == '}':
                open_braces -= 1
                if open_braces == 0:
                    end_index = i + 1
                    break
        
        if end_index != -1:
            # 提取完整的、转义的JSON字符串
            json_string_escaped = html_content[start_index:end_index]
            
            # 反转义
            json_string = json_string_escaped.replace('\\"', '"').replace('\\\\', '\\')
            
            try:
                model_data = json.loads(json_string)
                model_name = model_data.get('publicName')
                
                # 使用publicName去重
                if model_name and model_name not in model_names:
                    models.append(model_data)
                    model_names.add(model_name)
            except json.JSONDecodeError as e:
                logger.warning(f"解析提取的JSON对象时出错: {e} - 内容: {json_string[:150]}...")
                continue

    if models:
        logger.info(f"成功提取并解析了 {len(models)} 个独立模型。")
        return models
    else:
        logger.error("错误：在HTML响应中找不到任何匹配的完整模型JSON对象。")
        return None

def save_available_models(new_models_list, models_path="available_models.json"):
    """
    将提取到的完整模型对象列表保存到指定的JSON文件中。
    """
    logger.info(f"检测到 {len(new_models_list)} 个模型，正在更新 '{models_path}'...")
    
    try:
        with open(models_path, 'w', encoding='utf-8') as f:
            # 直接将完整的模型对象列表写入文件
            json.dump(new_models_list, f, indent=4, ensure_ascii=False)
        logger.info(f"✅ '{models_path}' 已成功更新，包含 {len(new_models_list)} 个模型。")
    except IOError as e:
        logger.error(f"❌ 写入 '{models_path}' 文件时出错: {e}")

# --- 自动重启逻辑 ---
def restart_server():
    """优雅地通知客户端刷新，然后重启服务器。"""
    logger.warning("="*60)
    logger.warning("检测到服务器空闲超时，准备自动重启...")
    logger.warning("="*60)
    
    # 1. (异步) 通知浏览器刷新
    async def notify_browser_refresh():
        if browser_ws:
            try:
                # 优先发送 'reconnect' 指令，让前端知道这是一个计划内的重启
                await browser_ws.send_text(json.dumps({"command": "reconnect"}, ensure_ascii=False))
                logger.info("已向浏览器发送 'reconnect' 指令。")
            except Exception as e:
                logger.error(f"发送 'reconnect' 指令失败: {e}")
    
    # 在主事件循环中运行异步通知函数
    # 使用`asyncio.run_coroutine_threadsafe`确保线程安全
    if browser_ws and browser_ws.client_state.name == 'CONNECTED' and main_event_loop:
        asyncio.run_coroutine_threadsafe(notify_browser_refresh(), main_event_loop)
    
    # 2. 延迟几秒以确保消息发送
    time.sleep(3)
    
    # 3. 执行重启
    logger.info("正在重启服务器...")
    os.execv(sys.executable, ['python'] + sys.argv)

def idle_monitor():
    """在后台线程中运行，监控服务器是否空闲。"""
    global last_activity_time
    
    # 等待，直到 last_activity_time 被首次设置
    while last_activity_time is None:
        time.sleep(1)
        
    logger.info("空闲监控线程已启动。")
    
    while True:
        if CONFIG.get("enable_idle_restart", False):
            timeout = CONFIG.get("idle_restart_timeout_seconds", 300)
            
            # 如果超时设置为-1，则禁用重启检查
            if timeout == -1:
                time.sleep(10) # 仍然需要休眠以避免繁忙循环
                continue

            idle_time = (datetime.now() - last_activity_time).total_seconds()
            
            if idle_time > timeout:
                logger.info(f"服务器空闲时间 ({idle_time:.0f}s) 已超过阈值 ({timeout}s)。")
                restart_server()
                break # 退出循环，因为进程即将被替换
                
        # 每 10 秒检查一次
        time.sleep(10)

# --- FastAPI 生命周期事件 ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    """在服务器启动时运行的生命周期函数。"""
    global idle_monitor_thread, last_activity_time, main_event_loop, aiohttp_session, DOWNLOAD_SEMAPHORE, MAX_CONCURRENT_DOWNLOADS
    main_event_loop = asyncio.get_running_loop() # 获取主事件循环
    load_config() # 首先加载配置
    
    # 从配置中读取并发和连接池设置
    MAX_CONCURRENT_DOWNLOADS = CONFIG.get("max_concurrent_downloads", 50)
    pool_config = CONFIG.get("connection_pool", {})
    
    # 🔧 创建自定义SSL上下文（修复CloudFlare R2的SSL连接问题）
    import ssl
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False  # 禁用主机名检查
    ssl_context.verify_mode = ssl.CERT_NONE  # 禁用证书验证
    logger.info("🔒 已创建自定义SSL上下文（禁用证书验证以提高连接稳定性）")
    
    # 创建优化的全局aiohttp会话
    connector = aiohttp.TCPConnector(
        ssl=ssl_context,  # 🔧 使用自定义SSL上下文而不是False
        limit=pool_config.get("total_limit", 200),                  # 增加总连接数
        limit_per_host=pool_config.get("per_host_limit", 50),      # 每个主机的连接限制
        ttl_dns_cache=pool_config.get("dns_cache_ttl", 300),       # DNS缓存时间
        force_close=False,                                          # 保持连接
        enable_cleanup_closed=True,                                 # 自动清理关闭的连接
        keepalive_timeout=pool_config.get("keepalive_timeout", 30)  # 保活超时
    )
    
    # 创建优化的超时配置
    timeout_config = CONFIG.get("download_timeout", {})
    timeout = aiohttp.ClientTimeout(
        total=timeout_config.get("total", 30),      # 总超时时间
        connect=timeout_config.get("connect", 5),   # 连接超时
        sock_read=timeout_config.get("sock_read", 10)  # 读取超时
    )
    
    aiohttp_session = aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        trust_env=True
    )
    
    # 初始化下载信号量
    DOWNLOAD_SEMAPHORE = Semaphore(MAX_CONCURRENT_DOWNLOADS)
    
    logger.info(f"全局aiohttp会话已创建（优化配置）")
    logger.info(f"  - 最大连接数: {pool_config.get('total_limit', 200)}")
    logger.info(f"  - 每主机连接数: {pool_config.get('per_host_limit', 50)}")
    logger.info(f"  - 最大并发下载: {MAX_CONCURRENT_DOWNLOADS}")
    
    # 图像自动增强功能已移除（已剥离为独立项目image_enhancer）
    
    # --- 打印当前的操作模式 ---
    mode = CONFIG.get("id_updater_last_mode", "direct_chat")
    target = CONFIG.get("id_updater_battle_target", "A")
    logger.info("="*60)
    logger.info(f"  当前操作模式: {mode.upper()}")
    if mode == 'battle':
        logger.info(f"  - Battle 模式目标: Assistant {target}")
    logger.info("  (可通过运行 id_updater.py 修改模式)")
    logger.info("="*60)
    
    # 添加监控面板信息
    logger.info(f"📊 监控面板: http://127.0.0.1:5102/monitor")
    logger.info("="*60)

    check_for_updates() # 检查程序更新
    load_model_map() # 重新启用模型加载
    load_model_endpoint_map() # 加载模型端点映射
    logger.info("服务器启动完成。等待油猴脚本连接...")

    # 检查并显示公告，放在启动信息的最后，使其更显眼
    check_and_display_announcement()

    # 在模型更新后，标记活动时间的起点
    last_activity_time = datetime.now()
    
    # 启动空闲监控线程
    if CONFIG.get("enable_idle_restart", False):
        idle_monitor_thread = threading.Thread(target=idle_monitor, daemon=True)
        idle_monitor_thread.start()
        

    # 启动内存监控任务
    asyncio.create_task(memory_monitor())
    
    yield
    
    # 清理资源
    if aiohttp_session:
        await aiohttp_session.close()
        logger.info("全局aiohttp会话已关闭")
    
    logger.info("服务器正在关闭。")

app = FastAPI(lifespan=lifespan)

# --- CORS 中间件配置 ---
# 允许所有来源、所有方法、所有请求头，这对于本地开发工具是安全的。
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- 辅助函数 ---
def save_config():
    """将当前的 CONFIG 对象写回 config.jsonc 文件，保留注释。"""
    try:
        # 读取原始文件以保留注释等
        with open('config.jsonc', 'r', encoding='utf-8') as f:
            lines = f.readlines()

        # 使用正则表达式安全地替换值
        def replacer(key, value, content):
            # 这个正则表达式会找到 key，然后匹配它的 value 部分，直到逗号或右花括号
            pattern = re.compile(rf'("{key}"\s*:\s*").*?("?)(,?\s*)$', re.MULTILINE)
            replacement = rf'\g<1>{value}\g<2>\g<3>'
            if not pattern.search(content): # 如果 key 不存在，就添加到文件末尾（简化处理）
                 content = re.sub(r'}\s*$', f'  ,"{key}": "{value}"\n}}', content)
            else:
                 content = pattern.sub(replacement, content)
            return content

        content_str = "".join(lines)
        content_str = replacer("session_id", CONFIG["session_id"], content_str)
        content_str = replacer("message_id", CONFIG["message_id"], content_str)
        
        with open('config.jsonc', 'w', encoding='utf-8') as f:
            f.write(content_str)
        logger.info("✅ 成功将会话信息更新到 config.jsonc。")
    except Exception as e:
        logger.error(f"❌ 写入 config.jsonc 时发生错误: {e}", exc_info=True)

# --- 负载均衡函数 ---
async def select_best_tab_for_request() -> Tuple[str, WebSocket]:
    """
    选择负载最低的标签页来处理新请求。
    返回 (tab_id, websocket)
    """
    global browser_connections, tab_request_counts
    
    async with browser_connections_lock:
        if not browser_connections:
            raise HTTPException(status_code=503, detail="没有可用的浏览器连接")
        
        # 🔧 关键修复：清理已断开连接的标签页计数
        stale_tabs = [tab_id for tab_id in tab_request_counts.keys() if tab_id not in browser_connections]
        for tab_id in stale_tabs:
            del tab_request_counts[tab_id]
            logger.debug(f"[LOAD_BALANCE] 清理已断开标签页 '{tab_id}' 的计数")
        
        # 确保所有活跃标签页都有计数
        for tab_id in browser_connections.keys():
            if tab_id not in tab_request_counts:
                tab_request_counts[tab_id] = 0
        
        # 🔧 关键修复：只从活跃连接中选择（而不是从tab_request_counts中选择）
        # 计算每个活跃标签页的当前负载
        active_tab_loads = {tab_id: tab_request_counts.get(tab_id, 0) for tab_id in browser_connections.keys()}
        
        # 选择负载最低的标签页
        best_tab_id = min(active_tab_loads, key=active_tab_loads.get)
        best_ws = browser_connections[best_tab_id]
        
        # 增加该标签页的请求计数
        tab_request_counts[best_tab_id] += 1
        
        logger.info(f"[LOAD_BALANCE] 选择标签页 '{best_tab_id}' (当前负载: {tab_request_counts[best_tab_id]}/6)")
        logger.info(f"[LOAD_BALANCE] 所有标签页负载: {tab_request_counts}")
        
        return best_tab_id, best_ws

async def release_tab_request(tab_id: str):
    """释放标签页的请求计数"""
    global tab_request_counts
    
    async with tab_request_counts_lock:
        if tab_id in tab_request_counts and tab_request_counts[tab_id] > 0:
            tab_request_counts[tab_id] -= 1
            logger.debug(f"[LOAD_BALANCE] 释放标签页 '{tab_id}' 的请求 (剩余负载: {tab_request_counts[tab_id]}/6)")

async def _process_openai_message(message: dict) -> dict:
    """
    处理OpenAI消息，分离文本和附件。
    - 将多模态内容列表分解为纯文本和附件列表。
    - 文件床逻辑已移至 chat_completions 预处理，此处仅处理常规附件构建。
    - 确保 user 角色的空内容被替换为空格，以避免 LMArena 出错。
    - 特殊处理assistant角色的图片：检测Markdown图片并转换为experimental_attachments
    """
    content = message.get("content")
    role = message.get("role")
    attachments = []
    experimental_attachments = []
    text_content = ""

    # 添加诊断日志
    logger.debug(f"[MSG_PROCESS] 处理消息 - 角色: {role}, 内容类型: {type(content).__name__}")
    
    # 特殊处理assistant角色的字符串内容中的Markdown图片
    if role == "assistant" and isinstance(content, str):
        import re
        # 匹配 ![...](url) 格式的Markdown图片
        markdown_pattern = r'!\[([^\]]*)\]\(([^)]+)\)'
        matches = re.findall(markdown_pattern, content)
        
        if matches:
            logger.info(f"[MSG_PROCESS] 在assistant消息中检测到 {len(matches)} 个Markdown图片")
            
            # 移除Markdown图片，只保留文本
            text_content = re.sub(markdown_pattern, '', content).strip()
            
            # 将图片转换为experimental_attachments格式
            for alt_text, url in matches:
                # 确定内容类型
                if url.startswith("data:"):
                    # base64格式
                    content_type = url.split(';')[0].split(':')[1] if ':' in url else 'image/png'
                elif url.startswith("http"):
                    # HTTP URL
                    content_type = mimetypes.guess_type(url)[0] or 'image/jpeg'
                else:
                    content_type = 'image/jpeg'
                
                # 生成文件名
                if '/' in url and not url.startswith("data:"):
                    # 从URL提取文件名
                    filename = url.split('/')[-1].split('?')[0]
                    if '.' not in filename:
                        filename = f"image_{uuid.uuid4()}.{content_type.split('/')[-1]}"
                else:
                    filename = f"image_{uuid.uuid4()}.{content_type.split('/')[-1]}"
                
                experimental_attachment = {
                    "name": filename,
                    "contentType": content_type,
                    "url": url
                }
                experimental_attachments.append(experimental_attachment)
                logger.debug(f"[MSG_PROCESS] 添加experimental_attachment: {filename}")
        else:
            text_content = content
    elif isinstance(content, list):
        text_parts = []
        for part in content:
            if part.get("type") == "text":
                text_parts.append(part.get("text", ""))
            elif part.get("type") == "image_url":
                # 此处的 URL 可能是 base64 或 http URL (已被预处理器替换)
                image_url_data = part.get("image_url", {})
                url = image_url_data.get("url")
                original_filename = image_url_data.get("detail")

                try:
                    # 对于 base64，我们需要提取 content_type
                    if url.startswith("data:"):
                        content_type = url.split(';')[0].split(':')[1]
                    else:
                        # 对于 http URL，我们尝试猜测 content_type
                        content_type = mimetypes.guess_type(url)[0] or 'application/octet-stream'

                    file_name = original_filename or f"image_{uuid.uuid4()}.{mimetypes.guess_extension(content_type).lstrip('.') or 'png'}"
                    
                    attachment = {
                        "name": file_name,
                        "contentType": content_type,
                        "url": url
                    }
                    
                    # Assistant角色使用experimental_attachments
                    if role == "assistant":
                        experimental_attachments.append(attachment)
                        logger.debug(f"[MSG_PROCESS] Assistant图片添加到experimental_attachments")
                    else:
                        attachments.append(attachment)
                        logger.debug(f"[MSG_PROCESS] {role}图片添加到attachments")

                except (AttributeError, IndexError, ValueError) as e:
                    logger.warning(f"处理附件URL时出错: {url[:100]}... 错误: {e}")

        text_content = "\n\n".join(text_parts)
    elif isinstance(content, str):
        text_content = content

    if role == "user" and not text_content.strip():
        text_content = " "

    # 构建返回结果
    result = {
        "role": role,
        "content": text_content,
        "attachments": attachments
    }
    
    # Assistant角色添加experimental_attachments
    if role == "assistant" and experimental_attachments:
        result["experimental_attachments"] = experimental_attachments
        logger.info(f"[MSG_PROCESS] Assistant消息包含 {len(experimental_attachments)} 个experimental_attachments")
    
    return result

async def convert_openai_to_lmarena_payload(openai_data: dict, session_id: str, message_id: str, mode_override: str = None, battle_target_override: str = None) -> dict:
    """
    将 OpenAI 请求体转换为油猴脚本所需的简化载荷，并应用酒馆模式、绕过模式以及对战模式。
    新增了模式覆盖参数，以支持模型特定的会话模式。
    """
    # 0. 预处理：从历史消息中剥离思维链（如果配置启用）
    messages = openai_data.get("messages", [])
    if CONFIG.get("strip_reasoning_from_history", True) and CONFIG.get("enable_lmarena_reasoning", False):
        reasoning_mode = CONFIG.get("reasoning_output_mode", "openai")
        
        # 仅对think_tag模式有效（OpenAI模式的reasoning_content不在content中）
        if reasoning_mode == "think_tag":
            import re
            think_pattern = re.compile(r'<think>.*?</think>\s*', re.DOTALL)
            
            for msg in messages:
                if msg.get("role") == "assistant" and isinstance(msg.get("content"), str):
                    original_content = msg["content"]
                    # 移除<think>标签及其内容
                    cleaned_content = think_pattern.sub('', original_content).strip()
                    if cleaned_content != original_content:
                        msg["content"] = cleaned_content
                        logger.debug(f"[REASONING_STRIP] 从历史消息中剥离了思维链内容")
    
    # 1. 规范化角色并处理消息
    #    - 将非标准的 'developer' 角色转换为 'system' 以提高兼容性。
    #    - 分离文本和附件。
    for msg in messages:
        if msg.get("role") == "developer":
            msg["role"] = "system"
            logger.info("消息角色规范化：将 'developer' 转换为 'system'。")
    
    processed_messages = []
    for msg in messages:
        processed_msg = await _process_openai_message(msg.copy())
        processed_messages.append(processed_msg)

    # 2. 应用酒馆模式 (Tavern Mode)
    if CONFIG.get("tavern_mode_enabled"):
        system_prompts = [msg['content'] for msg in processed_messages if msg['role'] == 'system']
        other_messages = [msg for msg in processed_messages if msg['role'] != 'system']
        
        merged_system_prompt = "\n\n".join(system_prompts)
        final_messages = []
        
        if merged_system_prompt:
            # 系统消息不应有附件
            final_messages.append({"role": "system", "content": merged_system_prompt, "attachments": []})
        
        final_messages.extend(other_messages)
        processed_messages = final_messages

    # 3. 确定目标模型 ID 和类型
    model_name = openai_data.get("model", "claude-3-5-sonnet-20241022")
    
    # 优先从 MODEL_ENDPOINT_MAP 获取模型类型（如果定义了）
    model_type = "text"  # 默认类型
    endpoint_info = MODEL_ENDPOINT_MAP.get(model_name, {})
    
    # 诊断日志：记录模型类型判断过程
    logger.info(f"[BYPASS_DEBUG] 开始判断模型 '{model_name}' 的类型...")
    logger.info(f"[BYPASS_DEBUG] endpoint_info 类型: {type(endpoint_info).__name__}, 内容: {endpoint_info}")
    
    if isinstance(endpoint_info, dict) and "type" in endpoint_info:
        model_type = endpoint_info.get("type", "text")
        logger.info(f"[BYPASS_DEBUG] 从 model_endpoint_map.json (dict) 获取模型类型: {model_type}")
    elif isinstance(endpoint_info, list) and endpoint_info:
        # 如果是列表格式，取第一个元素的类型
        first_endpoint = endpoint_info[0] if isinstance(endpoint_info[0], dict) else {}
        if "type" in first_endpoint:
            model_type = first_endpoint.get("type", "text")
            logger.info(f"[BYPASS_DEBUG] 从 model_endpoint_map.json (list) 获取模型类型: {model_type}")
    
    # 回退到 models.json 中的定义
    model_info = MODEL_NAME_TO_ID_MAP.get(model_name, {}) # 关键修复：确保 model_info 总是一个字典
    if not endpoint_info.get("type") and model_info:
        old_type = model_type
        model_type = model_info.get("type", "text")
        logger.info(f"[BYPASS_DEBUG] 从 models.json 获取模型类型: {old_type} -> {model_type}")
    
    logger.info(f"[BYPASS_DEBUG] 最终确定的模型类型: {model_type}")
    
    target_model_id = None
    if model_info:
        target_model_id = model_info.get("id")
    else:
        logger.warning(f"模型 '{model_name}' 在 'models.json' 中未找到。请求将不带特定模型ID发送。")

    if not target_model_id:
        logger.warning(f"模型 '{model_name}' 在 'models.json' 中未找到对应的ID。请求将不带特定模型ID发送。")

    # 4. 构建消息模板
    message_templates = []
    for msg in processed_messages:
        msg_template = {
            "role": msg["role"],
            "content": msg.get("content", ""),
            "attachments": msg.get("attachments", [])
        }
        
        # 对于user角色，附件需要放在experimental_attachments中
        if msg["role"] == "user" and msg.get("attachments"):
            msg_template["experimental_attachments"] = msg.get("attachments", [])
            logger.info(f"[LMARENA_CONVERT] 将user的 {len(msg['attachments'])} 个附件添加到experimental_attachments")
        
        # 保留assistant的experimental_attachments字段（图片生成模型需要）
        if msg["role"] == "assistant" and "experimental_attachments" in msg:
            msg_template["experimental_attachments"] = msg["experimental_attachments"]
            logger.info(f"[LMARENA_CONVERT] 保留assistant的 {len(msg['experimental_attachments'])} 个experimental_attachments")
        
        message_templates.append(msg_template)

    # 4.5 应用图片附件审查绕过 (Image Attachment Bypass) - 专用于image模型
    # 当使用image模型且最新的用户请求包含图片附件时，将文本内容分离到新请求中
    # 注：text模型有自己的绕过机制，search模型不需要（空内容会报错）
    if CONFIG.get("image_attachment_bypass_enabled", False) and model_type == "image":
        # 查找最后一条用户消息
        last_user_msg_idx = None
        for i in range(len(message_templates) - 1, -1, -1):
            if message_templates[i]["role"] == "user":
                last_user_msg_idx = i
                break
        
        if last_user_msg_idx is not None:
            last_user_msg = message_templates[last_user_msg_idx]
            
            # 检查是否包含图片附件
            has_image_attachment = False
            if last_user_msg.get("attachments"):
                for attachment in last_user_msg["attachments"]:
                    if attachment.get("contentType", "").startswith("image/"):
                        has_image_attachment = True
                        break
            
            # 如果包含图片附件且有文本内容，执行分离
            if has_image_attachment and last_user_msg.get("content", "").strip():
                original_content = last_user_msg["content"]
                original_attachments = last_user_msg["attachments"]
                
                # 创建两条消息：
                # 第一条：只包含图片附件（成为历史记录）
                image_only_msg = {
                    "role": "user",
                    "content": " ",  # 空内容或空格
                    "experimental_attachments": original_attachments,
                    "attachments": original_attachments
                }
                
                # 第二条：只包含文本内容（作为最新请求）
                text_only_msg = {
                    "role": "user",
                    "content": original_content,
                    "attachments": []
                }
                
                # 替换原消息为两条分离的消息
                message_templates[last_user_msg_idx] = image_only_msg
                message_templates.insert(last_user_msg_idx + 1, text_only_msg)
                
                logger.info(f"图片模型审查绕过已启用：将包含 {len(original_attachments)} 个附件的请求分离为两条消息")

    # 5. 应用绕过模式 (Bypass Mode) - 根据模型类型和配置决定是否启用
    # 获取细粒度的绕过设置
    bypass_settings = CONFIG.get("bypass_settings", {})
    global_bypass_enabled = CONFIG.get("bypass_enabled", False)
    
    # 诊断日志：详细记录绕过决策过程
    logger.info(f"[BYPASS_DEBUG] ===== 绕过决策开始 =====")
    logger.info(f"[BYPASS_DEBUG] 全局 bypass_enabled: {global_bypass_enabled}")
    logger.info(f"[BYPASS_DEBUG] bypass_settings: {bypass_settings}")
    logger.info(f"[BYPASS_DEBUG] 当前模型类型: {model_type}")
    
    # 根据模型类型确定是否启用绕过
    bypass_enabled_for_type = False
    
    # 修复：全局bypass_enabled为False时，无论bypass_settings如何设置都应该禁用
    if not global_bypass_enabled:
        bypass_enabled_for_type = False
        logger.info(f"[BYPASS_DEBUG] ⛔ 全局 bypass_enabled=False，强制禁用所有绕过功能")
    elif bypass_settings:
        # 如果有细粒度配置，检查是否明确定义了该类型
        if model_type in bypass_settings:
            # 如果明确定义了，使用定义的值（但仍受全局开关控制）
            bypass_enabled_for_type = bypass_settings.get(model_type, False)
            logger.info(f"[BYPASS_DEBUG] 使用 bypass_settings 中明确定义的值: bypass_settings['{model_type}'] = {bypass_enabled_for_type}")
        else:
            # 如果未明确定义，默认为False（更安全的默认值）
            bypass_enabled_for_type = False
            logger.info(f"[BYPASS_DEBUG] model_type '{model_type}' 未在 bypass_settings 中定义，默认禁用")
    else:
        # 如果没有细粒度配置，使用全局设置（保持向后兼容）
        # 但对于 image 和 search 类型，默认为 False（保持原有行为）
        if model_type in ["image", "search"]:
            bypass_enabled_for_type = False
            logger.info(f"[BYPASS_DEBUG] 无 bypass_settings，模型类型 '{model_type}' 属于 ['image', 'search']，强制设为 False")
        else:
            bypass_enabled_for_type = global_bypass_enabled
            logger.info(f"[BYPASS_DEBUG] 无 bypass_settings，使用全局 bypass_enabled: {bypass_enabled_for_type}")
    
    logger.info(f"[BYPASS_DEBUG] 最终决策：bypass_enabled_for_type = {bypass_enabled_for_type}")
    
    if bypass_enabled_for_type:
        # 从配置中读取绕过注入内容
        bypass_injection = CONFIG.get("bypass_injection", {})
        
        # 支持预设模式
        bypass_presets = bypass_injection.get("presets", {})
        active_preset_name = bypass_injection.get("active_preset", "default")
        
        # 尝试获取激活的预设
        injection_config = bypass_presets.get(active_preset_name)
        
        # 如果预设不存在，回退到自定义配置或默认值
        if not injection_config:
            logger.warning(f"[BYPASS_DEBUG] 预设 '{active_preset_name}' 不存在，使用自定义配置")
            injection_config = bypass_injection.get("custom", {
                "role": "user",
                "content": " ",
                "participantPosition": "a"
            })
        
        # 获取注入参数（带默认值）
        inject_role = injection_config.get("role", "user")
        inject_content = injection_config.get("content", " ")
        inject_position = injection_config.get("participantPosition", "a")
        
        logger.info(f"[BYPASS_DEBUG] ⚠️ 模型类型 '{model_type}' 的绕过模式已启用")
        logger.info(f"[BYPASS_DEBUG]   - 使用预设: {active_preset_name}")
        logger.info(f"[BYPASS_DEBUG]   - 注入角色: {inject_role}")
        logger.info(f"[BYPASS_DEBUG]   - 注入位置: {inject_position}")
        logger.info(f"[BYPASS_DEBUG]   - 注入内容: {inject_content[:50]}{'...' if len(inject_content) > 50 else ''}")
        
        message_templates.append({
            "role": inject_role,
            "content": inject_content,
            "participantPosition": inject_position,
            "attachments": []
        })
    else:
        if global_bypass_enabled or any(bypass_settings.values()) if bypass_settings else False:
            # 如果有任何绕过设置启用，但当前类型未启用，记录日志
            logger.info(f"[BYPASS_DEBUG] ✅ 模型类型 '{model_type}' 的绕过模式已禁用。")
    
    logger.info(f"[BYPASS_DEBUG] ===== 绕过决策结束 =====")

    # 6. 应用参与者位置 (Participant Position)
    # 优先使用覆盖的模式，否则回退到全局配置
    mode = mode_override or CONFIG.get("id_updater_last_mode", "direct_chat")
    target_participant = battle_target_override or CONFIG.get("id_updater_battle_target", "A")
    target_participant = target_participant.lower() # 确保是小写

    logger.info(f"正在根据模式 '{mode}' (目标: {target_participant if mode == 'battle' else 'N/A'}) 设置 Participant Positions...")

    for msg in message_templates:
        if msg['role'] == 'system':
            if mode == 'battle':
                # Battle 模式: system 与用户选择的助手在同一边 (A则a, B则b)
                msg['participantPosition'] = target_participant
            else:
                # DirectChat 模式: system 固定为 'b'
                msg['participantPosition'] = 'b'
        elif mode == 'battle':
            # Battle 模式下，非 system 消息使用用户选择的目标 participant
            msg['participantPosition'] = target_participant
        else: # DirectChat 模式
            # DirectChat 模式下，非 system 消息使用默认的 'a'
            msg['participantPosition'] = 'a'

    return {
        "message_templates": message_templates,
        "target_model_id": target_model_id,
        "session_id": session_id,
        "message_id": message_id
    }

# --- OpenAI 格式化辅助函数 (确保JSON序列化稳健) ---
def format_openai_chunk(content: str, model: str, request_id: str) -> str:
    """格式化为 OpenAI 流式块。"""
    chunk = {
        "id": request_id, "object": "chat.completion.chunk",
        "created": int(time.time()), "model": model,
        "choices": [{"index": 0, "delta": {"content": content}, "finish_reason": None}]
    }
    return f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n"

def format_openai_finish_chunk(model: str, request_id: str, reason: str = 'stop') -> str:
    """格式化为 OpenAI 结束块。"""
    chunk = {
        "id": request_id, "object": "chat.completion.chunk",
        "created": int(time.time()), "model": model,
        "choices": [{"index": 0, "delta": {}, "finish_reason": reason}]
    }
    return f"data: {json.dumps(chunk, ensure_ascii=False)}\n\ndata: [DONE]\n\n"

def format_openai_error_chunk(error_message: str, model: str, request_id: str) -> str:
    """格式化为 OpenAI 错误块。"""
    content = f"\n\n[LMArena Bridge Error]: {error_message}"
    return format_openai_chunk(content, model, request_id)

def format_openai_non_stream_response(content: str, model: str, request_id: str, reason: str = 'stop') -> dict:
    """构建符合 OpenAI 规范的非流式响应体。"""
    return {
        "id": request_id,
        "object": "chat.completion",
        "created": int(time.time()),
        "model": model,
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": content},
            "finish_reason": reason,
        }],
        "usage": {
            "prompt_tokens": 0,
            "completion_tokens": len(content) // 4,
            "total_tokens": len(content) // 4,
        },
    }


async def save_downloaded_image_async(image_data, url, request_id):
    """保存已下载的图片数据到本地（避免重复下载）"""
    global downloaded_image_urls, downloaded_urls_set
    
    # 避免重复保存
    if url in downloaded_urls_set:
        show_full_urls = CONFIG.get("debug_show_full_urls", False)
        url_display = url if show_full_urls else url[:CONFIG.get("url_display_length", 200)]
        logger.info(f"🎨 图片已存在记录，跳过保存: {url_display}{'...' if not show_full_urls and len(url) > CONFIG.get('url_display_length', 200) else ''}")
        return
    
    try:
        # 直接使用已下载的数据保存，避免重复下载
        await save_image_data(image_data, url, request_id)
        
        # 更新已下载记录（限制大小）
        if url not in downloaded_urls_set:
            downloaded_image_urls.append(url)
            downloaded_urls_set.add(url)
            # 当deque满了，自动删除最老的记录
            if len(downloaded_urls_set) > 5000:
                # 清理set中不在deque中的元素
                downloaded_urls_set = set(downloaded_image_urls)
                
    except Exception as e:
        logger.error(f"❌ 保存图片失败: {type(e).__name__}: {e}")

async def download_image_truly_async(url, request_id):
    """[已废弃] 真正的异步下载图片到本地 - 现在改为使用save_downloaded_image_async避免重复下载"""
    # 这个函数保留是为了兼容性，但不应该被调用
    logger.warning(f"⚠️ download_image_truly_async被调用了，这是不应该的，因为会导致重复下载")
    return

async def save_image_data(image_data, url, request_id):
    """保存图片数据到文件（异步）"""
    try:
        original_size_kb = len(image_data) / 1024
        
        # 创建日期文件夹
        date_folder = datetime.now().strftime("%Y%m%d")
        date_path = IMAGE_SAVE_DIR / date_folder
        date_path.mkdir(exist_ok=True)
        logger.info(f"📁 使用日期文件夹: {date_folder}")
        
        # 检查是否需要格式转换（本地保存）
        local_format_config = CONFIG.get("local_save_format", {})
        target_ext = 'png'  # 默认扩展名
        
        if local_format_config.get("enabled", False):
            target_format = local_format_config.get("format", "original").lower()
            
            if target_format != "original":
                try:
                    from io import BytesIO
                    from PIL import Image
                    
                    # 打开图片
                    img = Image.open(BytesIO(image_data))
                    
                    # 如果是RGBA模式且要转换为JPEG，需要先转换为RGB
                    if target_format in ['jpeg', 'jpg'] and img.mode in ('RGBA', 'LA', 'P'):
                        # 创建白色背景
                        background = Image.new('RGB', img.size, (255, 255, 255))
                        if img.mode == 'P':
                            img = img.convert('RGBA')
                        background.paste(img, mask=img.split()[-1] if img.mode == 'RGBA' else None)
                        img = background
                    
                    # 保存到BytesIO
                    output = BytesIO()
                    
                    # 根据目标格式保存
                    if target_format == 'png':
                        img.save(output, format='PNG', optimize=True)
                        target_ext = 'png'
                    elif target_format in ['jpeg', 'jpg']:
                        # 本地保存使用高质量
                        jpeg_quality = local_format_config.get("jpeg_quality", 100)
                        img.save(output, format='JPEG', quality=jpeg_quality, optimize=True)
                        target_ext = 'jpg'
                    elif target_format == 'webp':
                        img.save(output, format='WEBP', quality=95, optimize=True)
                        target_ext = 'webp'
                    else:
                        # 不支持的格式，使用原始数据
                        output = BytesIO(image_data)
                        # 从URL推断扩展名
                        if '.jpeg' in url.lower():
                            target_ext = 'jpeg'
                        elif '.jpg' in url.lower():
                            target_ext = 'jpg'
                        elif '.png' in url.lower():
                            target_ext = 'png'
                        elif '.webp' in url.lower():
                            target_ext = 'webp'
                    
                    # 获取转换后的数据
                    image_data = output.getvalue()
                    
                    converted_size_kb = len(image_data) / 1024
                    logger.info(f"🔄 本地保存已转换为 {target_format.upper()} 格式（{original_size_kb:.1f}KB → {converted_size_kb:.1f}KB）")
                    
                except Exception as e:
                    logger.warning(f"⚠️ 本地保存格式转换失败: {e}，使用原始格式")
                    # 从URL推断扩展名
                    if '.jpeg' in url.lower():
                        target_ext = 'jpeg'
                    elif '.jpg' in url.lower():
                        target_ext = 'jpg'
                    elif '.png' in url.lower():
                        target_ext = 'png'
                    elif '.webp' in url.lower():
                        target_ext = 'webp'
            else:
                # 保持原格式，从URL推断扩展名
                if '.jpeg' in url.lower():
                    target_ext = 'jpeg'
                elif '.jpg' in url.lower():
                    target_ext = 'jpg'
                elif '.png' in url.lower():
                    target_ext = 'png'
                elif '.webp' in url.lower():
                    target_ext = 'webp'
        else:
            # 未启用格式转换，从URL推断扩展名
            if '.jpeg' in url.lower():
                target_ext = 'jpeg'
            elif '.jpg' in url.lower():
                target_ext = 'jpg'
            elif '.png' in url.lower():
                target_ext = 'png'
            elif '.webp' in url.lower():
                target_ext = 'webp'
            elif '.' in url:
                possible_ext = url.split('.')[-1].split('?')[0].lower()
                if possible_ext in ['jpg', 'jpeg', 'png', 'gif', 'webp']:
                    target_ext = possible_ext
        
        # 生成文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]  # 添加毫秒
        
        # 使用时间戳和请求ID作为文件名
        filename = f"{timestamp}_{request_id[:8]}.{target_ext}"
        filepath = date_path / filename  # 使用日期文件夹路径
        
        # 异步保存文件
        await asyncio.get_event_loop().run_in_executor(None, filepath.write_bytes, image_data)
        
        # 计算文件大小
        size_kb = len(image_data) / 1024
        size_mb = size_kb / 1024
        
        if size_mb > 1:
            logger.info(f"✅ 图片已保存: {filename} ({size_mb:.2f}MB)")
        else:
            logger.info(f"✅ 图片已保存: {filename} ({size_kb:.1f}KB)")
        
        # 显示完整路径
        logger.info(f"   📁 保存位置: {filepath.absolute()}")
        
        # 图像自动增强功能已移除（如需增强请使用独立的image_enhancer项目）
            
    except Exception as e:
        logger.error(f"❌ 保存图片失败: {e}")

def download_image_async(url, request_id):
    """兼容旧版本的同步包装器（将被异步版本替代）"""
    global downloaded_image_urls, downloaded_urls_set

    # 避免重复下载
    if url in downloaded_urls_set:
        show_full_urls = CONFIG.get("debug_show_full_urls", False)
        url_display = url if show_full_urls else url[:CONFIG.get("url_display_length", 200)]
        logger.info(f"🎨 图片已存在，跳过下载: {url_display}{'...' if not show_full_urls and len(url) > CONFIG.get('url_display_length', 200) else ''}")
        return

    try:
        import time
        import urllib3

        # 禁用SSL警告
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        time.sleep(2)  # 稍微延迟，确保图片已经完全生成

        # 下载图片（关键：verify=False 跳过SSL验证）
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Referer': 'https://lmarena.ai/'
        }

        # ⚠️ 关键修改：添加 verify=False
        response = requests.get(url, timeout=30, headers=headers, verify=False)

        if response.status_code == 200:
            image_data = response.content
            original_size_kb = len(image_data) / 1024
            
            # 检查是否需要格式转换（本地保存）
            local_format_config = CONFIG.get("local_save_format", {})
            target_ext = 'png'  # 默认扩展名
            
            if local_format_config.get("enabled", False):
                target_format = local_format_config.get("format", "original").lower()
                
                if target_format != "original":
                    try:
                        from io import BytesIO
                        from PIL import Image
                        
                        # 打开图片
                        img = Image.open(BytesIO(image_data))
                        
                        # 如果是RGBA模式且要转换为JPEG，需要先转换为RGB
                        if target_format in ['jpeg', 'jpg'] and img.mode in ('RGBA', 'LA', 'P'):
                            # 创建白色背景
                            background = Image.new('RGB', img.size, (255, 255, 255))
                            if img.mode == 'P':
                                img = img.convert('RGBA')
                            background.paste(img, mask=img.split()[-1] if img.mode == 'RGBA' else None)
                            img = background
                        
                        # 保存到BytesIO
                        output = BytesIO()
                        
                        # 根据目标格式保存
                        if target_format == 'png':
                            img.save(output, format='PNG', optimize=True)
                            target_ext = 'png'
                        elif target_format in ['jpeg', 'jpg']:
                            # 本地保存使用高质量
                            jpeg_quality = local_format_config.get("jpeg_quality", 100)
                            img.save(output, format='JPEG', quality=jpeg_quality, optimize=True)
                            target_ext = 'jpg'
                        elif target_format == 'webp':
                            img.save(output, format='WEBP', quality=95, optimize=True)
                            target_ext = 'webp'
                        else:
                            # 不支持的格式，使用原始数据
                            output = BytesIO(image_data)
                            # 从URL推断扩展名
                            if '.jpeg' in url.lower():
                                target_ext = 'jpeg'
                            elif '.jpg' in url.lower():
                                target_ext = 'jpg'
                            elif '.png' in url.lower():
                                target_ext = 'png'
                            elif '.webp' in url.lower():
                                target_ext = 'webp'
                        
                        # 获取转换后的数据
                        image_data = output.getvalue()
                        
                        converted_size_kb = len(image_data) / 1024
                        logger.info(f"🔄 本地保存已转换为 {target_format.upper()} 格式（{original_size_kb:.1f}KB → {converted_size_kb:.1f}KB）")
                        
                    except Exception as e:
                        logger.warning(f"⚠️ 本地保存格式转换失败: {e}，使用原始格式")
                        # 转换失败，使用原始数据和扩展名
                        image_data = response.content
                        # 从URL推断扩展名
                        if '.jpeg' in url.lower():
                            target_ext = 'jpeg'
                        elif '.jpg' in url.lower():
                            target_ext = 'jpg'
                        elif '.png' in url.lower():
                            target_ext = 'png'
                        elif '.webp' in url.lower():
                            target_ext = 'webp'
                else:
                    # 保持原格式
                    # 从URL推断扩展名
                    if '.jpeg' in url.lower():
                        target_ext = 'jpeg'
                    elif '.jpg' in url.lower():
                        target_ext = 'jpg'
                    elif '.png' in url.lower():
                        target_ext = 'png'
                    elif '.webp' in url.lower():
                        target_ext = 'webp'
            else:
                # 未启用格式转换，从URL推断扩展名
                if '.jpeg' in url.lower():
                    target_ext = 'jpeg'
                elif '.jpg' in url.lower():
                    target_ext = 'jpg'
                elif '.png' in url.lower():
                    target_ext = 'png'
                elif '.webp' in url.lower():
                    target_ext = 'webp'
                elif '.' in url:
                    possible_ext = url.split('.')[-1].split('?')[0].lower()
                    if possible_ext in ['jpg', 'jpeg', 'png', 'gif', 'webp']:
                        target_ext = possible_ext

            # 创建日期文件夹
            date_folder = datetime.now().strftime("%Y%m%d")
            date_path = IMAGE_SAVE_DIR / date_folder
            date_path.mkdir(exist_ok=True)
            logger.info(f"📁 使用日期文件夹: {date_folder}")
            
            # 生成文件名
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]  # 添加毫秒
            
            # 使用时间戳和请求ID作为文件名
            filename = f"{timestamp}_{request_id[:8]}.{target_ext}"
            filepath = date_path / filename  # 使用日期文件夹路径

            # 保存文件
            filepath.write_bytes(image_data)

            # 更新已下载记录（限制大小）
            if url not in downloaded_urls_set:
                downloaded_image_urls.append(url)
                downloaded_urls_set.add(url)
                # 当deque满了，自动删除最老的记录
                if len(downloaded_urls_set) > 5000:
                    # 清理set中不在deque中的元素
                    downloaded_urls_set = set(downloaded_image_urls)

            # 计算文件大小
            size_kb = len(image_data) / 1024
            size_mb = size_kb / 1024

            if size_mb > 1:
                logger.info(f"✅ 图片已保存: {filename} ({size_mb:.2f}MB)")
            else:
                logger.info(f"✅ 图片已保存: {filename} ({size_kb:.1f}KB)")

            # 显示完整路径
            logger.info(f"   📁 保存位置: {filepath.absolute()}")
            
            # 图像自动增强功能已移除（如需增强请使用独立的image_enhancer项目）

        else:
            logger.error(f"❌ 图片下载失败，HTTP状态码: {response.status_code}")

    except requests.exceptions.Timeout:
        show_full_urls = CONFIG.get("debug_show_full_urls", False)
        url_display = url if show_full_urls else url[:CONFIG.get("url_display_length", 200)]
        logger.error(f"❌ 图片下载超时: {url_display}{'...' if not show_full_urls and len(url) > CONFIG.get('url_display_length', 200) else ''}")
    except requests.exceptions.SSLError as e:
        logger.error(f"❌ SSL错误（尝试使用verify=False）: {e}")
    except Exception as e:
        logger.error(f"❌ 图片下载失败: {type(e).__name__}: {e}")


async def _process_lmarena_stream(request_id: str):
    """
    核心内部生成器：处理来自浏览器的原始数据流，并产生结构化事件。
    事件类型: ('content', str), ('finish', str), ('error', str), ('retry_info', dict)
    """
    global IS_REFRESHING_FOR_VERIFICATION, aiohttp_session
    
    # 确保使用最新的配置
    load_config()
    
    queue = response_channels.get(request_id)
    if not queue:
        logger.error(f"PROCESSOR [ID: {request_id[:8]}]: 无法找到响应通道。")
        yield 'error', 'Internal server error: response channel not found.'
        return

    buffer = ""
    timeout = CONFIG.get("stream_response_timeout_seconds",360)
    text_pattern = re.compile(r'[ab]0:"((?:\\.|[^"\\])*)"')
    # 新增：用于匹配思维链内容的正则表达式
    reasoning_pattern = re.compile(r'ag:"((?:\\.|[^"\\])*)"')
    # 新增：用于匹配和提取图片URL的正则表达式
    image_pattern = re.compile(r'[ab]2:(\[.*?\])')
    finish_pattern = re.compile(r'[ab]d:(\{.*?"finishReason".*?\})')
    error_pattern = re.compile(r'(\{\s*"error".*?\})', re.DOTALL)
    cloudflare_patterns = [r'<title>Just a moment...</title>', r'Enable JavaScript and cookies to continue']
    
    has_yielded_content = False # 标记是否已产出过有效内容
    
    # 思维链相关变量
    # 注意：思维链数据应该总是被收集（用于监控和日志），但是否输出给客户端由配置决定
    enable_reasoning_output = CONFIG.get("enable_lmarena_reasoning", False)  # 是否输出给客户端
    reasoning_buffer = []  # 缓冲所有思维链片段
    has_reasoning = False  # 标记是否有思维链内容
    reasoning_ended = False  # 标记reasoning是否已结束
    
    # 诊断：添加流式性能追踪
    import time as time_module
    last_yield_time = time_module.time()
    chunk_count = 0
    total_chars = 0

    try:
        while True:
            # 关键修复：每次循环开始时重置reasoning_found标志
            reasoning_found_in_this_chunk = False
            
            try:
                # 诊断：记录接收数据的时间
                receive_start = time_module.time()
                raw_data = await asyncio.wait_for(queue.get(), timeout=timeout)
                receive_time = time_module.time() - receive_start
                
                if CONFIG.get("debug_stream_timing", False):
                    logger.debug(f"[STREAM_TIMING] 从队列获取数据耗时: {receive_time:.3f}秒")
                    # 诊断：显示原始数据的前200个字符
                    raw_data_str = str(raw_data)[:200] if raw_data else "None"
                    logger.debug(f"[STREAM_RAW] 原始数据: {raw_data_str}...")
                    
            except asyncio.TimeoutError:
                logger.warning(f"PROCESSOR [ID: {request_id[:8]}]: 等待浏览器数据超时（{timeout}秒）。")
                yield 'error', f'Response timed out after {timeout} seconds.'
                return

            # --- Cloudflare 人机验证处理 ---
            def handle_cloudflare_verification():
                global IS_REFRESHING_FOR_VERIFICATION
                if not IS_REFRESHING_FOR_VERIFICATION:
                    logger.warning(f"PROCESSOR [ID: {request_id[:8]}]: 首次检测到人机验证，将发送刷新指令。")
                    IS_REFRESHING_FOR_VERIFICATION = True
                    if browser_ws:
                        asyncio.create_task(browser_ws.send_text(json.dumps({"command": "refresh"}, ensure_ascii=False)))
                    return "检测到人机验证，已发送刷新指令，请稍后重试。"
                else:
                    logger.info(f"PROCESSOR [ID: {request_id[:8]}]: 检测到人机验证，但已在刷新中，将等待。")
                    return "正在等待人机验证完成..."

            # 1. 检查来自 WebSocket 端的直接错误或重试信息
            if isinstance(raw_data, dict):
                # 处理重试信息
                if 'retry_info' in raw_data:
                    retry_info = raw_data.get('retry_info', {})
                    logger.info(f"PROCESSOR [ID: {request_id[:8]}]: 收到重试信息 - 尝试 {retry_info.get('attempt')}/{retry_info.get('max_attempts')}")
                    # 可以选择将重试信息传递给客户端
                    yield 'retry_info', retry_info
                    continue
                
                # 处理错误
                if 'error' in raw_data:
                    error_msg = raw_data.get('error', 'Unknown browser error')
                if isinstance(error_msg, str):
                    if '413' in error_msg or 'too large' in error_msg.lower():
                        friendly_error_msg = "上传失败：附件大小超过了 LMArena 服务器的限制 (通常是 5MB左右)。请尝试压缩文件或上传更小的文件。"
                        logger.warning(f"PROCESSOR [ID: {request_id[:8]}]: 检测到附件过大错误 (413)。")
                        yield 'error', friendly_error_msg
                        return
                    if any(re.search(p, error_msg, re.IGNORECASE) for p in cloudflare_patterns):
                        yield 'error', handle_cloudflare_verification()
                        return
                yield 'error', error_msg
                return

            # 2. 检查 [DONE] 信号
            if raw_data == "[DONE]":
                # 状态重置逻辑已移至 websocket_endpoint，以确保连接恢复时状态一定被重置
                if has_yielded_content and IS_REFRESHING_FOR_VERIFICATION:
                     logger.info(f"PROCESSOR [ID: {request_id[:8]}]: 请求成功，人机验证状态将在下次连接时重置。")
                break

            # 3. 累加缓冲区并检查内容
            buffer += "".join(str(item) for item in raw_data) if isinstance(raw_data, list) else raw_data
            
            # 诊断：显示缓冲区大小
            if CONFIG.get("debug_stream_timing", False):
                logger.debug(f"[STREAM_BUFFER] 缓冲区大小: {len(buffer)} 字符")

            if any(re.search(p, buffer, re.IGNORECASE) for p in cloudflare_patterns):
                yield 'error', handle_cloudflare_verification()
                return
            
            if (error_match := error_pattern.search(buffer)):
                try:
                    error_json = json.loads(error_match.group(1))
                    yield 'error', error_json.get("error", "来自 LMArena 的未知错误")
                    return
                except json.JSONDecodeError: pass

            # 优先处理思维链内容（ag前缀）
            # 注意：思维链始终被解析和收集（用于监控），但只在配置启用时才输出给客户端
            reasoning_found_in_this_chunk = False
            while (match := reasoning_pattern.search(buffer)):
                try:
                    reasoning_content = json.loads(f'"{match.group(1)}"')
                    if reasoning_content:
                        # 警告：检测到reasoning在content之后出现（异常情况）
                        if reasoning_ended:
                            logger.warning(f"[REASONING_WARN] 检测到reasoning在content之后继续出现，这可能导致think_tag模式下内容丢失！")
                        
                        # 总是收集思维链（用于监控和日志）
                        has_reasoning = True
                        reasoning_buffer.append(reasoning_content)
                        reasoning_found_in_this_chunk = True
                        
                        # 只在配置启用时才输出给客户端
                        if enable_reasoning_output and CONFIG.get("preserve_streaming", True):
                            # 流式输出思维链
                            yield 'reasoning', reasoning_content
                        
                except (ValueError, json.JSONDecodeError) as e:
                    if CONFIG.get("debug_stream_timing", False):
                        logger.debug(f"[REASONING_ERROR] 解析错误: {e}")
                    pass
                buffer = buffer[match.end():]
            
            # 处理文本内容（a0前缀）- 添加诊断
            process_start = time_module.time()
            chunks_in_buffer = 0
            
            # 诊断：检查是否有匹配
            if CONFIG.get("debug_stream_timing", False):
                matches_found = text_pattern.findall(buffer)
                if matches_found:
                    logger.debug(f"[STREAM_MATCH] 找到 {len(matches_found)} 个文本匹配")
                    for idx, match in enumerate(matches_found[:3]):  # 只显示前3个
                        logger.debug(f"  匹配#{idx+1}: {match[:50]}...")
            
            while (match := text_pattern.search(buffer)):
                try:
                    text_content = json.loads(f'"{match.group(1)}"')
                    if text_content:
                        # 关键修复：在第一个content到来时，如果有reasoning且未结束，则标记结束
                        if has_reasoning and not reasoning_ended and not reasoning_found_in_this_chunk:
                            reasoning_ended = True
                            logger.info(f"[REASONING_END] 检测到reasoning结束（共{len(reasoning_buffer)}个片段）")
                            # 只在启用输出时才发送结束事件
                            if enable_reasoning_output:
                                yield 'reasoning_end', None
                        
                        has_yielded_content = True
                        chunk_count += 1
                        total_chars += len(text_content)
                        chunks_in_buffer += 1
                        
                        # 诊断：记录yield间隔
                        current_time = time_module.time()
                        yield_interval = current_time - last_yield_time
                        last_yield_time = current_time
                        
                        if CONFIG.get("debug_stream_timing", False):
                            logger.debug(f"[STREAM_TIMING] Yield间隔: {yield_interval:.3f}秒, "
                                       f"块#{chunk_count}, 字符数: {len(text_content)}, "
                                       f"累计字符: {total_chars}")
                        
                        yield 'content', text_content
                        
                        # 立即处理，不要等待
                        await asyncio.sleep(0)
                        
                except (ValueError, json.JSONDecodeError) as e:
                    if CONFIG.get("debug_stream_timing", False):
                        logger.debug(f"[STREAM_ERROR] 解析错误: {e}")
                    pass
                buffer = buffer[match.end():]
            
            # 诊断：记录处理时间
            if chunks_in_buffer > 0 and CONFIG.get("debug_stream_timing", False):
                process_time = time_module.time() - process_start
                logger.debug(f"[STREAM_TIMING] 处理{chunks_in_buffer}个文本块耗时: {process_time:.3f}秒")

            # 新增：处理图片内容
            while (match := image_pattern.search(buffer)):
                try:
                    image_data_list = json.loads(match.group(1))
                    if isinstance(image_data_list, list) and image_data_list:
                        image_info = image_data_list[0]
                        if image_info.get("type") == "image" and "image" in image_info:
                            image_url = image_info['image']
                            
                            # 将LMArena返回的图片URL转换为base64返回给客户端
                            # 检查是否需要显示完整URL（从配置中读取）
                            show_full_urls = CONFIG.get("debug_show_full_urls", False)
                            if show_full_urls:
                                logger.info(f"📥 LMArena返回图片URL（完整）: {image_url}")
                            else:
                                # 显示更多字符，默认200个（通常足够看到完整的URL）
                                display_length = CONFIG.get("url_display_length", 200)
                                if len(image_url) <= display_length:
                                    logger.info(f"📥 LMArena返回图片URL: {image_url}")
                                else:
                                    logger.info(f"📥 LMArena返回图片URL: {image_url[:display_length]}...")
                                    logger.debug(f"   完整URL: {image_url}")  # 在DEBUG级别记录完整URL
                            
                            # 记录开始时间
                            import time as time_module
                            import base64
                            process_start_time = time_module.time()
                            
                            # 获取返回模式配置
                            return_format_config = CONFIG.get("image_return_format", {})
                            return_mode = return_format_config.get("mode", "base64")
                            save_locally = CONFIG.get("save_images_locally", True)
                            
                            # 诊断日志：记录处理开始
                            logger.info(f"[IMG_PROCESS] 开始处理图片")
                            logger.info(f"  - 返回模式: {return_mode}")
                            logger.info(f"  - 本地保存: {save_locally}")
                            
                            # URL模式：立即返回，不阻塞
                            if return_mode == "url":
                                logger.info(f"[IMG_PROCESS] URL模式 - 立即返回URL给客户端")
                                # 立即返回URL给客户端，不等待任何下载
                                yield 'content', f"![Image]({image_url})"
                                
                                # 如果需要保存到本地，创建后台任务（不阻塞响应）
                                if save_locally:
                                    logger.info(f"[IMG_PROCESS] 启动后台任务异步下载并保存图片")
                                    
                                    async def async_download_and_save():
                                        try:
                                            download_start = time_module.time()
                                            img_data, err = await _download_image_data_with_retry(image_url)
                                            download_time = time_module.time() - download_start
                                            
                                            if img_data:
                                                logger.info(f"[IMG_PROCESS] 后台下载成功，耗时: {download_time:.2f}秒")
                                                await save_downloaded_image_async(img_data, image_url, request_id)
                                                logger.info(f"[IMG_PROCESS] 图片已保存到本地")
                                            else:
                                                logger.error(f"[IMG_PROCESS] 后台下载失败: {err}")
                                        except Exception as e:
                                            logger.error(f"[IMG_PROCESS] 后台任务异常: {e}")
                                    
                                    # 创建后台任务，不等待完成
                                    asyncio.create_task(async_download_and_save())
                                else:
                                    logger.info(f"[IMG_PROCESS] save_images_locally=false，跳过下载")
                                
                                # URL模式处理完成，继续处理下一个消息
                                continue
                            
                            # Base64模式：必须先下载才能转换
                            logger.info(f"[IMG_PROCESS] Base64模式 - 需要下载图片进行转换")
                            
                            # 下载图片数据
                            download_start_time = time_module.time()
                            image_data, download_error = await _download_image_data_with_retry(image_url)
                            download_time = time_module.time() - download_start_time
                            logger.info(f"[IMG_PROCESS] 图片下载完成，耗时: {download_time:.2f}秒")
                            
                            # 如果需要保存到本地
                            if save_locally and image_data:
                                logger.info(f"[IMG_PROCESS] 异步保存图片到本地")
                                asyncio.create_task(save_downloaded_image_async(image_data, image_url, request_id))
                            elif not save_locally:
                                logger.info(f"[IMG_PROCESS] save_images_locally=false，跳过本地保存")
                            
                            # Base64转换
                            if True:  # 这里确定是base64模式
                                if image_data:
                                    # --- Base64 转换和缓存逻辑 ---
                                    cache_key = image_url
                                    current_time = time_module.time()
                                    
                                    # 清理过期缓存
                                    if len(IMAGE_BASE64_CACHE) > IMAGE_CACHE_MAX_SIZE:
                                        sorted_items = sorted(IMAGE_BASE64_CACHE.items(), key=lambda x: x[1][1])
                                        for url, _ in sorted_items[:IMAGE_CACHE_MAX_SIZE // 2]:
                                            del IMAGE_BASE64_CACHE[url]
                                        logger.info(f"  🧹 清理了 {IMAGE_CACHE_MAX_SIZE // 2} 个旧缓存")

                                    # 检查缓存
                                    if cache_key in IMAGE_BASE64_CACHE:
                                        cached_data, cache_time = IMAGE_BASE64_CACHE[cache_key]
                                        if current_time - cache_time < IMAGE_CACHE_TTL:
                                            logger.info(f"  ⚡ 从缓存获取图片Base64")
                                            yield 'content', cached_data
                                            continue
                                    
                                    # 执行转换
                                    content_type = mimetypes.guess_type(image_url)[0] or 'image/png'
                                    image_base64 = base64.b64encode(image_data).decode('ascii')
                                    data_url = f"data:{content_type};base64,{image_base64}"
                                    markdown_image = f"![Image]({data_url})"
                                    
                                    # 存入缓存
                                    IMAGE_BASE64_CACHE[cache_key] = (markdown_image, current_time)
                                    
                                    # 计算总耗时
                                    total_time = time_module.time() - process_start_time
                                    logger.info(f"[IMG_PROCESS] Base64转换完成，总耗时: {total_time:.2f}秒")
                                    
                                    yield 'content', markdown_image
                                else:
                                    # 下载失败，降级返回URL
                                    logger.error(f"[IMG_PROCESS] ❌ 图片下载失败 ({download_error})，降级返回原始URL")
                                    total_time = time_module.time() - process_start_time
                                    logger.info(f"[IMG_PROCESS] 处理完成（失败降级），总耗时: {total_time:.2f}秒")
                                    yield 'content', f"![Image]({image_url})"

                except (json.JSONDecodeError, IndexError) as e:
                    logger.warning(f"解析图片URL时出错: {e}, buffer: {buffer[:150]}")
                buffer = buffer[match.end():]

            if (finish_match := finish_pattern.search(buffer)):
                try:
                    finish_data = json.loads(finish_match.group(1))
                    yield 'finish', finish_data.get("finishReason", "stop")
                except (json.JSONDecodeError, IndexError): pass
                buffer = buffer[finish_match.end():]

    except asyncio.CancelledError:
        logger.info(f"PROCESSOR [ID: {request_id[:8]}]: 任务被取消。")
    finally:
        # 在清理前，如果有思维链内容且未流式输出，则一次性输出
        if enable_reasoning_output and has_reasoning and not CONFIG.get("preserve_streaming", True):
            # 非流式模式：在最后一次性输出完整思维链
            full_reasoning = "".join(reasoning_buffer)
            yield 'reasoning_complete', full_reasoning
        
        # 诊断：输出流式性能统计
        if chunk_count > 0 and CONFIG.get("debug_stream_timing", False):
            total_time = time_module.time() - (last_yield_time - yield_interval if 'yield_interval' in locals() else last_yield_time)
            logger.info(f"[STREAM_STATS] 请求ID: {request_id[:8]}")
            logger.info(f"  - 总块数: {chunk_count}")
            logger.info(f"  - 总字符数: {total_chars}")
            logger.info(f"  - 平均块大小: {total_chars/chunk_count:.1f}字符")
            logger.info(f"  - 平均yield间隔: {total_time/chunk_count:.3f}秒")
        
        # 🔧 关键修复：释放标签页请求计数
        if request_id in request_metadata:
            tab_id = request_metadata[request_id].get("tab_id")
            if tab_id:
                await release_tab_request(tab_id)
                logger.debug(f"PROCESSOR [ID: {request_id[:8]}]: 已释放标签页 '{tab_id}' 的请求计数")
            
        if request_id in response_channels:
            del response_channels[request_id]
            logger.info(f"PROCESSOR [ID: {request_id[:8]}]: 响应通道已清理。")
        
        # 清理请求元数据（修复内存泄漏）
        if request_id in request_metadata:
            del request_metadata[request_id]
            logger.debug(f"PROCESSOR [ID: {request_id[:8]}]: 请求元数据已清理。")

async def stream_generator(request_id: str, model: str):
    """将内部事件流格式化为 OpenAI SSE 响应。"""
    response_id = f"chatcmpl-{uuid.uuid4()}"
    logger.info(f"STREAMER [ID: {request_id[:8]}]: 流式生成器启动。")
    
    finish_reason_to_send = 'stop'  # 默认的结束原因
    collected_content = []  # 收集响应内容用于存储
    reasoning_content = []  # 收集思维链内容
    
    # 诊断：添加流式性能追踪
    import time as time_module
    stream_start_time = time_module.time()
    chunks_sent = 0
    
    # 思维链配置
    # 注意：思维链数据总是被收集，但只在启用时才输出给客户端
    enable_reasoning_output = CONFIG.get("enable_lmarena_reasoning", False)
    reasoning_mode = CONFIG.get("reasoning_output_mode", "openai")
    preserve_streaming = CONFIG.get("preserve_streaming", True)

    async for event_type, data in _process_lmarena_stream(request_id):
        if event_type == 'retry_info':
            # 处理重试信息，可以发送给客户端作为注释
            retry_msg = f"\n[重试信息] 尝试 {data.get('attempt')}/{data.get('max_attempts')}，原因: {data.get('reason')}，等待 {data.get('delay')/1000}秒...\n"
            logger.info(f"STREAMER [ID: {request_id[:8]}]: {retry_msg.strip()}")
            # 可选：将重试信息作为注释发送给客户端
            if CONFIG.get("show_retry_info_to_client", False):
                yield format_openai_chunk(retry_msg, model, response_id)
        elif event_type == 'reasoning':
            # 处理思维链片段
            # 总是收集思维链（用于监控），但只在启用时输出给客户端
            reasoning_content.append(data)
            
            if enable_reasoning_output:
                if reasoning_mode == "openai" and preserve_streaming:
                    # OpenAI模式且启用流式：发送reasoning delta
                    chunk = {
                        "id": response_id,
                        "object": "chat.completion.chunk",
                        "created": int(time.time()),
                        "model": model,
                        "choices": [{
                            "index": 0,
                            "delta": {"reasoning_content": data},
                            "finish_reason": None
                        }]
                    }
                    yield f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n"
                # think_tag模式：收集不立即输出
                
        elif event_type == 'reasoning_end':
            # 新增：reasoning结束事件（think_tag模式专用）
            if enable_reasoning_output and reasoning_mode == "think_tag" and reasoning_content:
                # 立即输出完整的reasoning
                full_reasoning = "".join(reasoning_content)
                wrapped_reasoning = f"<think>{full_reasoning}</think>\n\n"
                yield format_openai_chunk(wrapped_reasoning, model, response_id)
                logger.info(f"[THINK_TAG] 已输出完整reasoning（{len(reasoning_content)}个片段）")
                
        elif event_type == 'reasoning_complete':
            # 处理完整思维链（非流式模式）
            # 总是收集，但只在启用时输出
            full_reasoning = data
            reasoning_content.append(full_reasoning)
            
            if enable_reasoning_output and not preserve_streaming:
                if reasoning_mode == "openai":
                    # OpenAI模式：发送完整reasoning
                    chunk = {
                        "id": response_id,
                        "object": "chat.completion.chunk",
                        "created": int(time.time()),
                        "model": model,
                        "choices": [{
                            "index": 0,
                            "delta": {"reasoning_content": full_reasoning},
                            "finish_reason": None
                        }]
                    }
                    yield f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n"
                elif reasoning_mode == "think_tag":
                    # think_tag模式：包裹后作为content输出
                    wrapped_reasoning = f"<think>{full_reasoning}</think>\n\n"
                    yield format_openai_chunk(wrapped_reasoning, model, response_id)
                    
        elif event_type == 'content':
            
            collected_content.append(data)  # 收集内容
            chunks_sent += 1
            
            # 立即生成并发送数据块，不要累积
            chunk_data = format_openai_chunk(data, model, response_id)
            
            if CONFIG.get("debug_stream_timing", False):
                logger.debug(f"[STREAM_OUTPUT] 发送块#{chunks_sent}, 大小: {len(chunk_data)}字节")
            
            # 重要：立即yield，不要等待
            yield chunk_data
            
            # 如果启用强制刷新（针对某些客户端）
            if CONFIG.get("force_stream_flush", True):
                # 添加一个微小的异步暂停来强制刷新
                await asyncio.sleep(0)
        elif event_type == 'finish':
            # 记录结束原因，但不要立即返回，等待浏览器发送 [DONE]
            finish_reason_to_send = data
            if data == 'content-filter':
                warning_msg = "\n\n响应被终止，可能是上下文超限或者模型内部审查（大概率）的原因"
                collected_content.append(warning_msg)  # 也收集警告信息
                yield format_openai_chunk(warning_msg, model, response_id)
        elif event_type == 'error':
            logger.error(f"STREAMER [ID: {request_id[:8]}]: 流中发生错误: {data}")
            monitoring_service.request_end(
                request_id,
                success=False,
                error=str(data),
                response_content="".join(collected_content) if collected_content else None
            )
            await monitoring_service.broadcast_to_monitors({
                "type": "request_end",
                "request_id": request_id,
                "success": False
            })
            yield format_openai_error_chunk(str(data), model, response_id)
            yield format_openai_finish_chunk(model, response_id, reason='stop')
            return # 发生错误时，可以立即终止

    # 只有在 _process_lmarena_stream 自然结束后 (即收到 [DONE]) 才执行
    yield format_openai_finish_chunk(model, response_id, reason=finish_reason_to_send)
    
    # 诊断：输出流式输出统计
    if CONFIG.get("debug_stream_timing", False) and chunks_sent > 0:
        total_time = time_module.time() - stream_start_time
        logger.info(f"[STREAM_OUTPUT_STATS] 请求ID: {request_id[:8]}")
        logger.info(f"  - 发送块数: {chunks_sent}")
        logger.info(f"  - 总耗时: {total_time:.2f}秒")
        logger.info(f"  - 平均发送间隔: {total_time/chunks_sent:.3f}秒/块")
    
    logger.info(f"STREAMER [ID: {request_id[:8]}]: 流式生成器正常结束。")

    # 记录请求成功（包含响应内容）
    full_response = "".join(collected_content)
    full_reasoning = "".join(reasoning_content) if reasoning_content else None
    
    # 从response_channels获取请求的原始信息来计算输入token
    input_tokens = 0
    if hasattr(monitoring_service, 'active_requests') and request_id in monitoring_service.active_requests:
        request_info = monitoring_service.active_requests[request_id]
        # 计算输入token数（简单估算：所有消息内容长度除以4）
        if request_info.request_messages:
            for msg in request_info.request_messages:
                if isinstance(msg, dict) and 'content' in msg:
                    content = msg.get('content', '')
                    if isinstance(content, str):
                        input_tokens += len(content) // 4
                    elif isinstance(content, list):
                        # 处理多模态消息
                        for part in content:
                            if isinstance(part, dict) and part.get('type') == 'text':
                                input_tokens += len(part.get('text', '')) // 4
    
    monitoring_service.request_end(
        request_id,
        success=True,
        response_content=full_response,
        reasoning_content=full_reasoning,  # 添加思维链内容
        input_tokens=input_tokens,  # 添加输入token
        output_tokens=len(full_response) // 4  # 简单估算输出token数
    )
    await monitoring_service.broadcast_to_monitors({
        "type": "request_end",
        "request_id": request_id,
        "success": True
    })

async def non_stream_response(request_id: str, model: str):
    """聚合内部事件流并返回单个 OpenAI JSON 响应。"""
    response_id = f"chatcmpl-{uuid.uuid4()}"
    logger.info(f"NON-STREAM [ID: {request_id[:8]}]: 开始处理非流式响应。")
    
    full_content = []
    reasoning_content = []
    finish_reason = "stop"
    
    # 思维链配置
    # 注意：思维链数据总是被收集，但只在启用时才输出给客户端
    enable_reasoning_output = CONFIG.get("enable_lmarena_reasoning", False)
    reasoning_mode = CONFIG.get("reasoning_output_mode", "openai")
    
    async for event_type, data in _process_lmarena_stream(request_id):
        if event_type == 'retry_info':
            # 非流式响应中记录重试信息
            logger.info(f"NON-STREAM [ID: {request_id[:8]}]: 重试信息 - 尝试 {data.get('attempt')}/{data.get('max_attempts')}")
            # 非流式响应通常不会将重试信息返回给客户端
        elif event_type == 'reasoning' or event_type == 'reasoning_complete':
            # 收集思维链内容（总是收集，用于监控）
            reasoning_content.append(data)
        elif event_type == 'content':
            full_content.append(data)
        elif event_type == 'finish':
            finish_reason = data
            if data == 'content-filter':
                full_content.append("\n\n响应被终止，可能是上下文超限或者模型内部审查（大概率）的原因")
            # 不要在这里 break，继续等待来自浏览器的 [DONE] 信号，以避免竞态条件
        elif event_type == 'error':
            logger.error(f"NON-STREAM [ID: {request_id[:8]}]: 处理时发生错误: {data}")
            
            monitoring_service.request_end(
                request_id,
                success=False,
                error=str(data),
                response_content="".join(full_content) if full_content else None
            )
            await monitoring_service.broadcast_to_monitors({
                "type": "request_end",
                "request_id": request_id,
                "success": False
            })
            
            # 统一流式和非流式响应的错误状态码
            status_code = 413 if "附件大小超过了" in str(data) else 500

            error_response = {
                "error": {
                    "message": f"[LMArena Bridge Error]: {data}",
                    "type": "bridge_error",
                    "code": "attachment_too_large" if status_code == 413 else "processing_error"
                }
            }
            return Response(content=json.dumps(error_response, ensure_ascii=False), status_code=status_code, media_type="application/json")

    # 处理思维链内容
    # 思维链总是被收集（用于监控），但只在启用时才输出给客户端
    if enable_reasoning_output and reasoning_content:
        full_reasoning = "".join(reasoning_content)
        
        if reasoning_mode == "openai":
            # OpenAI模式：添加reasoning_content字段
            final_content_str = "".join(full_content)
            response_data = {
                "id": response_id,
                "object": "chat.completion",
                "created": int(time.time()),
                "model": model,
                "choices": [{
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": final_content_str,
                        "reasoning_content": full_reasoning  # 添加思维链
                    },
                    "finish_reason": finish_reason,
                }],
                "usage": {
                    "prompt_tokens": 0,
                    "completion_tokens": len(final_content_str) // 4,
                    "total_tokens": len(final_content_str) // 4,
                },
            }
        elif reasoning_mode == "think_tag":
            # think_tag模式：将思维链包裹后放在content前面
            wrapped_reasoning = f"<think>{full_reasoning}</think>\n\n"
            final_content_str = wrapped_reasoning + "".join(full_content)
            response_data = format_openai_non_stream_response(final_content_str, model, response_id, reason=finish_reason)
    else:
        # 没有启用思维链输出，或者没有思维链内容，使用原有逻辑
        final_content_str = "".join(full_content)
        response_data = format_openai_non_stream_response(final_content_str, model, response_id, reason=finish_reason)
    
    logger.info(f"NON-STREAM [ID: {request_id[:8]}]: 响应聚合完成。")
    
    # 记录请求成功（非流式响应）
    # 计算输入token
    input_tokens = 0
    if hasattr(monitoring_service, 'active_requests') and request_id in monitoring_service.active_requests:
        request_info = monitoring_service.active_requests[request_id]
        if request_info.request_messages:
            for msg in request_info.request_messages:
                if isinstance(msg, dict) and 'content' in msg:
                    content = msg.get('content', '')
                    if isinstance(content, str):
                        input_tokens += len(content) // 4
                    elif isinstance(content, list):
                        for part in content:
                            if isinstance(part, dict) and part.get('type') == 'text':
                                input_tokens += len(part.get('text', '')) // 4
    
    # 计算完整响应内容（包括思维链）
    full_response_for_monitoring = final_content_str if 'final_content_str' in locals() else "".join(full_content)
    full_reasoning_for_monitoring = "".join(reasoning_content) if reasoning_content else None
    
    monitoring_service.request_end(
        request_id,
        success=True,
        response_content=full_response_for_monitoring,
        reasoning_content=full_reasoning_for_monitoring,  # 添加思维链内容
        input_tokens=input_tokens,
        output_tokens=len(full_response_for_monitoring) // 4
    )
    
    # 🔧 关键修复：释放标签页请求计数
    if request_id in request_metadata:
        tab_id = request_metadata[request_id].get("tab_id")
        if tab_id:
            await release_tab_request(tab_id)
            logger.debug(f"NON-STREAM [ID: {request_id[:8]}]: 已释放标签页 '{tab_id}' 的请求计数")
    
    return Response(content=json.dumps(response_data, ensure_ascii=False), media_type="application/json")

# --- WebSocket 端点 ---
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """处理来自油猴脚本的 WebSocket 连接（支持多标签页）。"""
    global browser_ws, browser_connections, IS_REFRESHING_FOR_VERIFICATION
    await websocket.accept()
    
    # 等待第一条消息（可能包含标签页ID）
    tab_id = "default"  # 默认标签页ID（向后兼容）
    first_message_handled = False
    
    try:
        # 设置3秒超时等待可能的标签页ID
        init_message_str = await asyncio.wait_for(websocket.receive_text(), timeout=3.0)
        init_message = json.loads(init_message_str)
        
        # 检查是否包含tab_id
        if "tab_id" in init_message:
            tab_id = init_message["tab_id"]
            first_message_handled = True
            logger.info(f"[WS_CONN] 📋 收到标签页ID: {tab_id}")
        else:
            # 旧版本脚本，没有发送tab_id，这条消息需要在后面处理
            logger.warning(f"[WS_CONN] ⚠️ 未检测到tab_id，使用默认值（可能是旧版本脚本）")
            # 暂存这条消息，稍后处理
            first_real_message = init_message_str
    except asyncio.TimeoutError:
        logger.warning(f"[WS_CONN] ⚠️ 等待tab_id超时，使用默认值（可能是旧版本脚本）")
    except json.JSONDecodeError:
        logger.warning(f"[WS_CONN] ⚠️ 无法解析初始化消息，使用默认tab_id")
    
    # 使用锁保护WebSocket连接的修改
    async with browser_connections_lock:
        # 检查是否已有相同tab_id的连接
        if tab_id in browser_connections:
            logger.warning(f"[WS_CONN] 标签页 {tab_id} 已存在连接，将被新连接替换")
        
        browser_connections[tab_id] = websocket
        # 记录连接时间
        tab_connection_times[tab_id] = time.time()
        
        # 兼容性：将第一个连接设置为browser_ws
        if not browser_ws or tab_id == "default":
            browser_ws = websocket
        
        # 只要有新的连接建立，就意味着人机验证流程已结束（或从未开始）
        if IS_REFRESHING_FOR_VERIFICATION:
            logger.info("✅ 新的 WebSocket 连接已建立，人机验证状态已自动重置。")
            IS_REFRESHING_FOR_VERIFICATION = False
        
        # 计算并发能力
        concurrent_capacity = len(browser_connections) * 6
        logger.info("="*80)
        logger.info(f"✅ 标签页 '{tab_id}' 已成功连接 WebSocket")
        logger.info(f"📊 当前连接状态:")
        logger.info(f"  - 活跃标签页数: {len(browser_connections)}")
        logger.info(f"  - 理论最大并发: {concurrent_capacity} 个请求 (每标签页6个)")
        logger.info(f"  - 未处理请求数: {len(response_channels)}")
        
        # 并发限制提示
        if len(browser_connections) == 1:
            logger.warning(f"⚠️  注意：单标签页模式，浏览器HTTP/1.1限制并发为6个请求")
            logger.warning(f"💡 如需更高并发，请打开额外的LMArena标签页并运行油猴脚本")
            logger.warning(f"   - 2个标签页 = 12并发")
            logger.warning(f"   - 3个标签页 = 18并发")
        else:
            logger.info(f"✅ 多标签页模式已激活！当前支持 {concurrent_capacity} 个并发请求")
        
        logger.info("="*80)
    
    # 广播浏览器连接状态到监控面板
    await monitoring_service.broadcast_to_monitors({
        "type": "browser_status",
        "connected": True
    })
    
    # 广播标签页状态更新
    await monitoring_service.broadcast_to_monitors({
        "type": "tab_connection",
        "action": "connected",
        "tab_id": tab_id,
        "total_tabs": len(browser_connections),
        "total_capacity": len(browser_connections) * 6
    })
    
    # --- 增强：处理所有待恢复的请求（包括pending_requests_queue和response_channels）---
    if CONFIG.get("enable_auto_retry", False):
        # 1. 首先处理pending_requests_queue中的请求
        if not pending_requests_queue.empty():
            logger.info(f"检测到 {pending_requests_queue.qsize()} 个暂存的请求，将在后台自动重试...")
            asyncio.create_task(process_pending_requests())
        
        # 2. 然后处理response_channels中未完成的请求（关键修复）
        if len(response_channels) > 0:
            logger.info(f"[REQUEST_RECOVERY] 检测到 {len(response_channels)} 个未完成的请求，准备恢复...")
            
            # 获取所有未完成请求的ID
            pending_request_ids = list(response_channels.keys())
            
            for request_id in pending_request_ids:
                # 尝试从多个来源获取请求数据
                request_data = None
                
                # 来源1：request_metadata（新增的存储）
                if request_id in request_metadata:
                    request_data = request_metadata[request_id]["openai_request"]
                    logger.info(f"[REQUEST_RECOVERY] 从request_metadata恢复请求 {request_id[:8]}")
                
                # 来源2：monitoring_service.active_requests（备用）
                elif hasattr(monitoring_service, 'active_requests') and request_id in monitoring_service.active_requests:
                    active_req = monitoring_service.active_requests[request_id]
                    # 重建OpenAI请求格式
                    request_data = {
                        "model": active_req.model,
                        "messages": active_req.request_messages if hasattr(active_req, 'request_messages') else [],
                        "stream": active_req.params.get("streaming", False) if hasattr(active_req, 'params') else False,
                        "temperature": active_req.params.get("temperature") if hasattr(active_req, 'params') else None,
                        "top_p": active_req.params.get("top_p") if hasattr(active_req, 'params') else None,
                        "max_tokens": active_req.params.get("max_tokens") if hasattr(active_req, 'params') else None,
                    }
                    logger.info(f"[REQUEST_RECOVERY] 从monitoring_service恢复请求 {request_id[:8]}")
                else:
                    logger.warning(f"[REQUEST_RECOVERY] ⚠️ 无法恢复请求 {request_id[:8]}：找不到原始数据")
                    # 清理这个无法恢复的请求
                    if request_id in response_channels:
                        await response_channels[request_id].put({"error": "Request data lost during reconnection"})
                        await response_channels[request_id].put("[DONE]")
                    continue
                
                # 如果成功获取到请求数据，将其加入重试队列
                if request_data:
                    # 创建一个新的future来等待重试结果
                    future = asyncio.get_event_loop().create_future()
                    
                    # 将请求放入pending队列
                    await pending_requests_queue.put({
                        "future": future,
                        "request_data": request_data,
                        "original_request_id": request_id  # 保留原始请求ID用于追踪
                    })
                    
                    logger.info(f"[REQUEST_RECOVERY] ✅ 请求 {request_id[:8]} 已加入重试队列")
            
            # 启动恢复处理
            if not pending_requests_queue.empty():
                logger.info(f"[REQUEST_RECOVERY] 开始处理 {pending_requests_queue.qsize()} 个恢复的请求...")
                asyncio.create_task(process_pending_requests())
            else:
                logger.info(f"[REQUEST_RECOVERY] 没有可恢复的请求")

    try:
        # 如果第一条消息未被处理（旧版本脚本），需要先处理它
        if not first_message_handled and 'first_real_message' in locals():
            message_str = first_real_message
            message = json.loads(message_str)
            
            request_id = message.get("request_id")
            data = message.get("data")
            
            if request_id and data is not None:
                if request_id in response_channels:
                    await response_channels[request_id].put(data)
                else:
                    logger.warning(f"[WS_MSG] 收到未知或已关闭请求的响应: {request_id}")
        
        while True:
            # 等待并接收来自油猴脚本的消息
            message_str = await websocket.receive_text()
            message = json.loads(message_str)
            
            request_id = message.get("request_id")
            data = message.get("data")

            if not request_id or data is None:
                logger.warning(f"[WS_MSG] 收到来自浏览器的无效消息: {message}")
                continue

            # 诊断：记录WebSocket消息
            if CONFIG.get("debug_stream_timing", False):
                import time as time_module
                current_time = time_module.time()
                data_preview = str(data)[:200] if data else "None"
                logger.debug(f"[WS_MSG] 时间: {current_time:.3f}, 请求ID: {request_id[:8]}, 数据预览: {data_preview}...")
                
                # 如果是字符串数据，检查是否包含多个文本块
                if isinstance(data, str) and 'a0:"' in data:
                    import re
                    text_pattern = re.compile(r'[ab]0:"((?:\\.|[^"\\])*)"')
                    matches = text_pattern.findall(data)
                    logger.debug(f"[WS_MSG] 单个WebSocket消息中包含 {len(matches)} 个文本块（问题：数据被累积！）")
                    if len(matches) > 1:
                        logger.warning(f"⚠️ 检测到流式数据被累积！单个WebSocket消息包含了 {len(matches)} 个文本块")
                        logger.warning(f"   这说明油猴脚本端累积了多个响应块后才发送")

            # 将收到的数据放入对应的响应通道
            if request_id in response_channels:
                await response_channels[request_id].put(data)
            else:
                logger.warning(f"[WS_MSG] 收到未知或已关闭请求的响应: {request_id}")

    except WebSocketDisconnect:
        logger.warning(f"❌ 标签页 '{tab_id}' 已断开连接。")
    except Exception as e:
        logger.error(f"[WS_ERROR] 标签页 '{tab_id}' WebSocket处理时发生错误: {e}", exc_info=True)
    finally:
        async with browser_connections_lock:
            # 移除断开的标签页连接
            if tab_id in browser_connections:
                del browser_connections[tab_id]
                logger.info(f"[WS_CONN] 标签页 '{tab_id}' 已移除")
            
            # 移除连接时间记录
            if tab_id in tab_connection_times:
                del tab_connection_times[tab_id]
            
            # 更新browser_ws（向后兼容）
            if browser_connections:
                # 如果还有其他连接，使用第一个
                browser_ws = list(browser_connections.values())[0]
                logger.info(f"[WS_CONN] browser_ws已更新为剩余的{len(browser_connections)}个连接中的第一个")
            else:
                browser_ws = None
                logger.info(f"[WS_CONN] 所有标签页已断开")
            
            # 计算剩余并发能力
            remaining_capacity = len(browser_connections) * 6
            logger.info(f"[WS_CONN] 剩余活跃标签页: {len(browser_connections)}")
            logger.info(f"[WS_CONN] 剩余并发能力: {remaining_capacity} 个请求")
            logger.info(f"[WS_CONN] 未处理请求数: {len(response_channels)}")
            
        # 广播浏览器断开状态到监控面板
        await monitoring_service.broadcast_to_monitors({
            "type": "browser_status",
            "connected": len(browser_connections) > 0
        })
        
        # 广播标签页状态更新
        await monitoring_service.broadcast_to_monitors({
            "type": "tab_connection",
            "action": "disconnected",
            "tab_id": tab_id,
            "total_tabs": len(browser_connections),
            "total_capacity": len(browser_connections) * 6
        })
        
        # 如果禁用了自动重试，则像以前一样清理通道
        if not CONFIG.get("enable_auto_retry", False):
            # 清理所有等待的响应通道，以防请求被挂起
            for queue in response_channels.values():
                await queue.put({"error": "Browser disconnected during operation"})
            response_channels.clear()
            logger.info("WebSocket 连接已清理（自动重试已禁用）。")
        else:
            logger.info("WebSocket 连接已关闭（自动重试已启用，请求将等待重连）。")

# --- OpenAI 兼容 API 端点 ---
@app.get("/v1/models")
async def get_models():
    """提供兼容 OpenAI 的模型列表 - 返回 model_endpoint_map.json 中配置的模型。"""
    # 优先返回 MODEL_ENDPOINT_MAP 中的模型（已配置会话的模型）
    if MODEL_ENDPOINT_MAP:
        return {
            "object": "list",
            "data": [
                {
                    "id": model_name,
                    "object": "model",
                    "created": int(time.time()),
                    "owned_by": "LMArenaBridge"
                }
                for model_name in MODEL_ENDPOINT_MAP.keys()
            ],
        }
    # 如果 MODEL_ENDPOINT_MAP 为空，则返回 models.json 中的模型作为备用
    elif MODEL_NAME_TO_ID_MAP:
        return {
            "object": "list",
            "data": [
                {
                    "id": model_name,
                    "object": "model",
                    "created": int(time.time()),
                    "owned_by": "LMArenaBridge"
                }
                for model_name in MODEL_NAME_TO_ID_MAP.keys()
            ],
        }
    else:
        return JSONResponse(
            status_code=404,
            content={"error": "模型列表为空。请配置 'model_endpoint_map.json' 或 'models.json'。"}
        )

@app.post("/internal/request_model_update")
async def request_model_update():
    """
    接收来自 model_updater.py 的请求，并通过 WebSocket 指令
    让油猴脚本发送页面源码。
    """
    if not browser_ws:
        logger.warning("MODEL UPDATE: 收到更新请求，但没有浏览器连接。")
        raise HTTPException(status_code=503, detail="Browser client not connected.")
    
    try:
        logger.info("MODEL UPDATE: 收到更新请求，正在通过 WebSocket 发送指令...")
        await browser_ws.send_text(json.dumps({"command": "send_page_source"}))
        logger.info("MODEL UPDATE: 'send_page_source' 指令已成功发送。")
        return JSONResponse({"status": "success", "message": "Request to send page source sent."})
    except Exception as e:
        logger.error(f"MODEL UPDATE: 发送指令时出错: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to send command via WebSocket.")

@app.post("/internal/update_available_models")
async def update_available_models_endpoint(request: Request):
    """
    接收来自油猴脚本的页面 HTML，提取并更新 available_models.json。
    """
    html_content = await request.body()
    if not html_content:
        logger.warning("模型更新请求未收到任何 HTML 内容。")
        return JSONResponse(
            status_code=400,
            content={"status": "error", "message": "No HTML content received."}
        )
    
    logger.info("收到来自油猴脚本的页面内容，开始提取可用模型...")
    new_models_list = extract_models_from_html(html_content.decode('utf-8'))
    
    if new_models_list:
        save_available_models(new_models_list)
        return JSONResponse({"status": "success", "message": "Available models file updated."})
    else:
        logger.error("未能从油猴脚本提供的 HTML 中提取模型数据。")
        return JSONResponse(
            status_code=400,
            content={"status": "error", "message": "Could not extract model data from HTML."}
        )


@app.post("/v1/chat/completions")
async def chat_completions(request: Request):
    """
    处理聊天补全请求。
    接收 OpenAI 格式的请求，将其转换为 LMArena 格式，
    通过 WebSocket 发送给油猴脚本，然后流式返回结果。
    """
    global last_activity_time
    last_activity_time = datetime.now() # 更新活动时间
    logger.info(f"API请求已收到，活动时间已更新为: {last_activity_time.strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        openai_req = await request.json()
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="无效的 JSON 请求体")

    model_name = openai_req.get("model")
    
    # 优先从 MODEL_ENDPOINT_MAP 获取模型类型
    model_type = "text"  # 默认类型
    endpoint_mapping = MODEL_ENDPOINT_MAP.get(model_name)
    if endpoint_mapping:
        if isinstance(endpoint_mapping, dict) and "type" in endpoint_mapping:
            model_type = endpoint_mapping.get("type", "text")
        elif isinstance(endpoint_mapping, list) and endpoint_mapping:
            first_mapping = endpoint_mapping[0] if isinstance(endpoint_mapping[0], dict) else {}
            if "type" in first_mapping:
                model_type = first_mapping.get("type", "text")
    
    # 回退到 models.json
    model_info = MODEL_NAME_TO_ID_MAP.get(model_name, {})
    if not (endpoint_mapping and (isinstance(endpoint_mapping, dict) and "type" in endpoint_mapping or
            isinstance(endpoint_mapping, list) and endpoint_mapping and "type" in endpoint_mapping[0])):
        model_type = model_info.get("type", "text")

    # --- 新增：基于模型类型的判断逻辑 ---
    if model_type == 'image':
        logger.info(f"检测到模型 '{model_name}' 类型为 'image'，将通过主聊天接口处理。")
        # 对于图像模型，我们不再调用独立的处理器，而是复用主聊天逻辑，
        # 因为 _process_lmarena_stream 现在已经能处理图片数据。
        # 这意味着图像生成现在原生支持流式和非流式响应。
        pass # 继续执行下面的通用聊天逻辑
    # --- 文生图逻辑结束 ---

    # 如果不是图像模型，则执行正常的文本生成逻辑
    load_config()  # 实时加载最新配置，确保会话ID等信息是最新的
    # --- API Key 验证 ---
    api_key = CONFIG.get("api_key")
    if api_key:
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            raise HTTPException(
                status_code=401,
                detail="未提供 API Key。请在 Authorization 头部中以 'Bearer YOUR_KEY' 格式提供。"
            )
        
        provided_key = auth_header.split(' ')[1]
        if provided_key != api_key:
            raise HTTPException(
                status_code=401,
                detail="提供的 API Key 不正确。"
            )

    # --- 增强的连接检查与自动重试逻辑 ---
    if not browser_ws:
        if CONFIG.get("enable_auto_retry", False):
            logger.warning("油猴脚本未连接，但自动重试已启用。请求将被暂存。")
            
            # 创建一个 future 来等待响应
            future = asyncio.get_event_loop().create_future()
            
            # 将请求的关键信息放入暂存队列
            await pending_requests_queue.put({
                "future": future,
                "request_data": openai_req
            })
            
            logger.info(f"一个新请求已被放入暂存队列。当前队列大小: {pending_requests_queue.qsize()}")

            try:
                # 从配置中读取超时时间，默认为60秒
                timeout = CONFIG.get("retry_timeout_seconds", 120)
                
                # 等待 future 完成（即，重试成功后，响应被设置进来）
                return await asyncio.wait_for(future, timeout=timeout)
            except asyncio.TimeoutError:
                logger.warning(f"一个暂存的请求等待了 {timeout} 秒后超时。")
                raise HTTPException(
                    status_code=503,
                    detail=f"浏览器与服务器连接断开，并在 {timeout} 秒内未能恢复。请求失败。"
                )

        else: # 如果未启用重试，则立即失败
            raise HTTPException(
                status_code=503,
                detail="油猴脚本客户端未连接。请确保 LMArena 页面已打开并激活脚本。"
            )

    if IS_REFRESHING_FOR_VERIFICATION and not browser_ws:
        raise HTTPException(
            status_code=503,
            detail="正在等待浏览器刷新以完成人机验证，请在几秒钟后重试。"
        )

    # --- 模型与会话ID映射逻辑 ---
    session_id, message_id = None, None
    mode_override, battle_target_override = None, None

    if model_name and model_name in MODEL_ENDPOINT_MAP:
        mapping_entry = MODEL_ENDPOINT_MAP[model_name]
        selected_mapping = None

        if isinstance(mapping_entry, list) and mapping_entry:
            # 使用线程安全的轮询策略选择映射
            global MODEL_ROUND_ROBIN_INDEX, MODEL_ROUND_ROBIN_LOCK
            
            # 关键修复：使用锁确保原子操作，避免并发竞态
            with MODEL_ROUND_ROBIN_LOCK:
                if model_name not in MODEL_ROUND_ROBIN_INDEX:
                    MODEL_ROUND_ROBIN_INDEX[model_name] = 0
                
                current_index = MODEL_ROUND_ROBIN_INDEX[model_name]
                selected_mapping = mapping_entry[current_index]
                
                # 诊断日志：显示并发轮询状态
                logger.info(f"[CONCURRENT_ROUND_ROBIN] 模型 '{model_name}' 轮询状态:")
                logger.info(f"  - 总映射数: {len(mapping_entry)}")
                logger.info(f"  - 当前选择索引: {current_index}")
                logger.info(f"  - 选择的映射: #{current_index + 1}/{len(mapping_entry)}")
                logger.info(f"  - Session ID后6位: ...{mapping_entry[current_index].get('session_id', 'N/A')[-6:]}")
                logger.info(f"  - 活跃请求通道数: {len(response_channels)} (并发请求)")
                
                # 更新索引，循环到下一个（原子操作内完成）
                MODEL_ROUND_ROBIN_INDEX[model_name] = (current_index + 1) % len(mapping_entry)
                next_index = MODEL_ROUND_ROBIN_INDEX[model_name]
                logger.info(f"  - 下次请求将使用索引: {next_index}")
            
            logger.info(f"✅ 为模型 '{model_name}' 从ID列表中轮询选择了映射 #{current_index + 1}/{len(mapping_entry)}（线程安全）")
        elif isinstance(mapping_entry, dict):
            selected_mapping = mapping_entry
            logger.info(f"为模型 '{model_name}' 找到了单个端点映射（旧格式）。")
        
        if selected_mapping:
            session_id = selected_mapping.get("session_id")
            message_id = selected_mapping.get("message_id")
            # 关键：同时获取模式信息
            mode_override = selected_mapping.get("mode") # 可能为 None
            battle_target_override = selected_mapping.get("battle_target") # 可能为 None
            log_msg = f"将使用 Session ID: ...{session_id[-6:] if session_id else 'N/A'}"
            if mode_override:
                log_msg += f" (模式: {mode_override}"
                if mode_override == 'battle':
                    log_msg += f", 目标: {battle_target_override or 'A'}"
                log_msg += ")"
            logger.info(log_msg)

    # 如果经过以上处理，session_id 仍然是 None，则进入全局回退逻辑
    if not session_id:
        if CONFIG.get("use_default_ids_if_mapping_not_found", True):
            session_id = CONFIG.get("session_id")
            message_id = CONFIG.get("message_id")
            # 当使用全局ID时，不设置模式覆盖，让其使用全局配置
            mode_override, battle_target_override = None, None
            logger.info(f"模型 '{model_name}' 未找到有效映射，根据配置使用全局默认 Session ID: ...{session_id[-6:] if session_id else 'N/A'}")
        else:
            logger.error(f"模型 '{model_name}' 未在 'model_endpoint_map.json' 中找到有效映射，且已禁用回退到默认ID。")
            raise HTTPException(
                status_code=400,
                detail=f"模型 '{model_name}' 没有配置独立的会话ID。请在 'model_endpoint_map.json' 中添加有效映射或在 'config.jsonc' 中启用 'use_default_ids_if_mapping_not_found'。"
            )

    # --- 验证最终确定的会话信息 ---
    if not session_id or not message_id or "YOUR_" in session_id or "YOUR_" in message_id:
        raise HTTPException(
            status_code=400,
            detail="最终确定的会话ID或消息ID无效。请检查 'model_endpoint_map.json' 和 'config.jsonc' 中的配置，或运行 `id_updater.py` 来更新默认值。"
        )

    if not model_name or model_name not in MODEL_NAME_TO_ID_MAP:
        logger.warning(f"请求的模型 '{model_name}' 不在 models.json 中，将使用默认模型ID。")

    request_id = str(uuid.uuid4())
    response_channels[request_id] = asyncio.Queue()
    
    # 新增：保存请求元数据用于断线恢复
    request_metadata[request_id] = {
        "openai_request": openai_req.copy(),  # 保存完整的OpenAI请求
        "model_name": model_name,
        "session_id": session_id,
        "message_id": message_id,
        "mode_override": mode_override,
        "battle_target_override": battle_target_override,
        "created_at": datetime.now().isoformat()
    }
    
    logger.info(f"API CALL [ID: {request_id[:8]}]: 已创建响应通道。")
    logger.debug(f"API CALL [ID: {request_id[:8]}]: 请求元数据已保存到request_metadata")
    
    # 记录请求开始到监控系统（增加详细信息）
    # 计算输入的大概token数
    input_token_estimate = 0
    for msg in openai_req.get("messages", []):
        content = msg.get("content", "")
        if isinstance(content, str):
            input_token_estimate += len(content) // 4
        elif isinstance(content, list):
            for part in content:
                if isinstance(part, dict) and part.get("type") == "text":
                    input_token_estimate += len(part.get("text", "")) // 4
    
    monitoring_service.request_start(
        request_id=request_id,
        model=model_name or "unknown",
        messages_count=len(openai_req.get("messages", [])),
        session_id=session_id[-6:] if session_id else None,
        mode=mode_override or CONFIG.get("id_updater_last_mode", "direct_chat"),
        messages=openai_req.get("messages", []),  # 添加消息内容
        params={  # 添加请求参数
            "temperature": openai_req.get("temperature"),
            "top_p": openai_req.get("top_p"),
            "max_tokens": openai_req.get("max_tokens"),
            "streaming": openai_req.get("stream", False)
        }
    )
    
    # 广播请求开始事件到监控面板
    await monitoring_service.broadcast_to_monitors({
        "type": "request_start",
        "request_id": request_id,
        "model": model_name,
        "timestamp": time.time()
    })

    # --- 新增：图片hash计算辅助函数 ---
    def calculate_image_hash(base64_data: str) -> str:
        """计算图片内容的SHA256 hash（用于缓存键）"""
        import hashlib
        # 移除data URI前缀（如果存在）
        if ',' in base64_data:
            _, data_only = base64_data.split(',', 1)
        else:
            data_only = base64_data
        # 计算hash（使用base64字符串，避免解码开销）
        return hashlib.sha256(data_only.encode('utf-8')).hexdigest()
    
    try:
        # --- 附件预处理（包括文件床上传） ---
        # 在与浏览器通信前，先处理好所有附件。如果失败，则立即返回错误。
        # 处理请求中所有角色（user、assistant、system）的base64图片 -> 转换为图床链接发送到LMArena
        if CONFIG.get("file_bed_enabled"):
            # 1. 过滤出已启用且未被临时禁用的端点（检查恢复时间）
            all_endpoints = CONFIG.get("file_bed_endpoints", [])
            current_time = time.time()
            
            # 自动恢复超时的端点
            endpoints_to_recover = []
            for endpoint_name, disable_time in list(DISABLED_ENDPOINTS.items()):
                if current_time - disable_time > FILEBED_RECOVERY_TIME:
                    endpoints_to_recover.append(endpoint_name)
            
            for endpoint_name in endpoints_to_recover:
                del DISABLED_ENDPOINTS[endpoint_name]
                logger.info(f"[FILEBED] 图床端点 '{endpoint_name}' 已自动恢复")
            
            active_endpoints = [ep for ep in all_endpoints if ep.get("enabled") and ep.get("name") not in DISABLED_ENDPOINTS]

            if not active_endpoints:
                logger.warning("文件床已启用，但没有可用的活动端点。将跳过上传。")
            else:
                # --- 新增：图床选择策略 ---
                global ROUND_ROBIN_INDEX
                strategy = CONFIG.get("file_bed_selection_strategy", "random")
                
                messages_to_process = openai_req.get("messages", [])
                logger.info(f"📋 文件床已启用，找到 {len(active_endpoints)} 个活动端点 (策略: {strategy})")
                logger.info(f"📋 准备处理 {len(messages_to_process)} 条消息中的图片")
                
                # 统计各角色的图片数量
                role_image_count = {}

                for msg_index, message in enumerate(messages_to_process):
                    role = message.get("role", "unknown")
                    content = message.get("content")
                    
                    logger.debug(f"  检查消息 #{msg_index + 1} (角色: {role})")
                    
                    # 处理字符串内容中的Markdown图片（常见于assistant角色）
                    if isinstance(content, str):
                        # 使用正则表达式匹配Markdown格式的图片（包括base64和http URL）
                        import re
                        # 匹配 ![...](url) 格式，包括base64和http URL
                        markdown_image_pattern = r'!\[([^\]]*)\]\(([^)]+)\)'
                        markdown_matches = re.findall(markdown_image_pattern, content)
                        
                        # 过滤出需要上传的图片（只处理base64格式）
                        base64_matches = [(alt, url) for alt, url in markdown_matches if url.startswith('data:')]
                        
                        if base64_matches:
                            logger.info(f"  📷 在 {role} 角色的字符串内容中发现 {len(base64_matches)} 个需要上传的Markdown格式base64图片")
                            markdown_matches = base64_matches  # 只处理base64图片
                            
                            for match_index, (alt_text, base64_url) in enumerate(markdown_matches):
                                role_image_count[role] = role_image_count.get(role, 0) + 1
                                
                                # 新增：检查图床URL缓存
                                image_hash = calculate_image_hash(base64_url)
                                current_time = time.time()
                                
                                # 清理过期缓存
                                if image_hash in FILEBED_URL_CACHE:
                                    cached_url, cache_time = FILEBED_URL_CACHE[image_hash]
                                    if current_time - cache_time < FILEBED_URL_CACHE_TTL:
                                        # 缓存命中且未过期
                                        old_markdown = f"![{alt_text}]({base64_url})"
                                        new_markdown = f"![{alt_text}]({cached_url})"
                                        content = content.replace(old_markdown, new_markdown)
                                        message["content"] = content
                                        
                                        show_full_urls = CONFIG.get("debug_show_full_urls", False)
                                        url_display = cached_url if show_full_urls else cached_url[:CONFIG.get("url_display_length", 200)]
                                        logger.info(f"    ⚡ {role} 角色字符串图片使用缓存URL: {url_display}{'...' if not show_full_urls and len(cached_url) > CONFIG.get('url_display_length', 200) else ''} (剩余: {FILEBED_URL_CACHE_TTL - (current_time - cache_time):.0f}秒)")
                                        continue  # 跳过上传
                                    else:
                                        # 缓存过期，删除
                                        del FILEBED_URL_CACHE[image_hash]
                                        logger.debug(f"    🗑️ 清理过期缓存: {image_hash[:8]}...")
                                
                                # 2. 根据策略选择和排序端点
                                if strategy == "failover":
                                    # 故障转移：固定使用当前索引的图床，失败后才切换
                                    start_index = ROUND_ROBIN_INDEX % len(active_endpoints)
                                    endpoints_to_try = active_endpoints[start_index:] + active_endpoints[:start_index]
                                elif strategy == "round_robin":
                                    # 轮询：每次都切换到下一个图床
                                    start_index = ROUND_ROBIN_INDEX % len(active_endpoints)
                                    endpoints_to_try = active_endpoints[start_index:] + active_endpoints[:start_index]
                                    ROUND_ROBIN_INDEX += 1 # 只有轮询模式才立即增加索引
                                else: # 默认为 random
                                    endpoints_to_try = random.sample(active_endpoints, len(active_endpoints))

                                upload_successful = False
                                last_error = "没有可用的图床端点。"
                                
                                for i, endpoint in enumerate(endpoints_to_try):
                                    endpoint_name = endpoint.get("name", "Unknown")
                                    
                                    # 跳过已临时禁用的
                                    if endpoint_name in DISABLED_ENDPOINTS:
                                        continue
                                    
                                    logger.info(f"    📤 上传 {role} 角色字符串中的第 {match_index + 1} 个base64图片，尝试使用 '{endpoint_name}' 端点...")
                                    
                                    final_url, error_message = await upload_to_file_bed(
                                        file_name=f"{role}_string_{msg_index}_{match_index}_{uuid.uuid4()}.png",
                                        file_data=base64_url,
                                        endpoint=endpoint
                                    )
                                    
                                    if not error_message:
                                        # 替换原内容中的base64为上传后的URL
                                        old_markdown = f"![{alt_text}]({base64_url})"
                                        new_markdown = f"![{alt_text}]({final_url})"
                                        content = content.replace(old_markdown, new_markdown)
                                        message["content"] = content  # 更新消息内容
                                        
                                        # 新增：将上传结果存入缓存
                                        FILEBED_URL_CACHE[image_hash] = (final_url, time.time())
                                        # 限制缓存大小
                                        if len(FILEBED_URL_CACHE) > FILEBED_URL_CACHE_MAX_SIZE:
                                            # 删除最旧的缓存项
                                            sorted_items = sorted(FILEBED_URL_CACHE.items(), key=lambda x: x[1][1])
                                            for old_hash, _ in sorted_items[:FILEBED_URL_CACHE_MAX_SIZE // 4]:
                                                del FILEBED_URL_CACHE[old_hash]
                                            logger.debug(f"    🧹 缓存已满，清理了 {FILEBED_URL_CACHE_MAX_SIZE // 4} 个旧条目")
                                        
                                        # 改进URL显示
                                        show_full_urls = CONFIG.get("debug_show_full_urls", False)
                                        url_display = final_url if show_full_urls else final_url[:CONFIG.get("url_display_length", 200)]
                                        logger.info(f"    ✅ {role} 角色字符串中的图片成功上传到 '{endpoint_name}': {url_display}{'...' if not show_full_urls and len(final_url) > CONFIG.get('url_display_length', 200) else ''} (已缓存)")
                                        upload_successful = True
                                        break
                                    else:
                                        logger.warning(f"    ⚠️ 端点 '{endpoint_name}' 上传失败: {error_message}。临时禁用并尝试下一个...")
                                        DISABLED_ENDPOINTS[endpoint_name] = time.time()  # 记录禁用时间
                                        logger.info(f"[FILEBED] {endpoint_name} 已禁用，当前禁用数: {len(DISABLED_ENDPOINTS)}")
                                        last_error = error_message
                                        # 如果是故障转移模式，且当前固定的图床失败了，则更新索引
                                        if strategy == "failover" and i == 0:
                                            ROUND_ROBIN_INDEX += 1
                                            logger.info(f"    🔄 [Failover] 默认图床 '{endpoint_name}' 失败，切换到下一个。")
                                
                                if not upload_successful:
                                    logger.error(f"    ❌ {role} 角色字符串中的图片上传失败")
                                    raise IOError(f"所有活动图床端点均上传失败。最后错误: {last_error}")
                    
                    # 处理列表内容（原有逻辑）
                    elif isinstance(content, list):
                        image_count_in_msg = 0
                        for part_index, part in enumerate(content):
                            if part.get("type") == "image_url":
                                url_content = part.get("image_url", {}).get("url")
                                
                                if url_content and url_content.startswith("data:"):
                                    image_count_in_msg += 1
                                    role_image_count[role] = role_image_count.get(role, 0) + 1
                                    
                                    # 新增：检查图床URL缓存
                                    image_hash = calculate_image_hash(url_content)
                                    current_time = time.time()
                                    
                                    # 清理过期缓存
                                    if image_hash in FILEBED_URL_CACHE:
                                        cached_url, cache_time = FILEBED_URL_CACHE[image_hash]
                                        if current_time - cache_time < FILEBED_URL_CACHE_TTL:
                                            # 缓存命中且未过期
                                            part["image_url"]["url"] = cached_url
                                            
                                            show_full_urls = CONFIG.get("debug_show_full_urls", False)
                                            url_display = cached_url if show_full_urls else cached_url[:CONFIG.get("url_display_length", 200)]
                                            logger.info(f"    ⚡ {role} 角色列表图片使用缓存URL: {url_display}{'...' if not show_full_urls and len(cached_url) > CONFIG.get('url_display_length', 200) else ''} (剩余: {FILEBED_URL_CACHE_TTL - (current_time - cache_time):.0f}秒)")
                                            continue  # 跳过上传
                                        else:
                                            # 缓存过期，删除
                                            del FILEBED_URL_CACHE[image_hash]
                                            logger.debug(f"    🗑️ 清理过期缓存: {image_hash[:8]}...")
                                    
                                    # 2. 根据策略选择和排序端点
                                    if strategy == "failover":
                                        start_index = ROUND_ROBIN_INDEX % len(active_endpoints)
                                        endpoints_to_try = active_endpoints[start_index:] + active_endpoints[:start_index]
                                    elif strategy == "round_robin":
                                        start_index = ROUND_ROBIN_INDEX % len(active_endpoints)
                                        endpoints_to_try = active_endpoints[start_index:] + active_endpoints[:start_index]
                                        ROUND_ROBIN_INDEX += 1
                                    else:
                                        endpoints_to_try = random.sample(active_endpoints, len(active_endpoints))

                                    upload_successful = False
                                    last_error = "没有可用的图床端点。"

                                    for i, endpoint in enumerate(endpoints_to_try):
                                        endpoint_name = endpoint.get("name", "Unknown")
                                        
                                        # 跳过已临时禁用的
                                        if endpoint_name in DISABLED_ENDPOINTS:
                                            continue

                                        logger.info(f"    📤 上传 {role} 角色列表中的第 {image_count_in_msg} 个base64图片，尝试使用 '{endpoint_name}' 端点...")
                                        
                                        final_url, error_message = await upload_to_file_bed(
                                            file_name=f"{role}_list_{msg_index}_{part_index}_{uuid.uuid4()}.png",
                                            file_data=url_content,
                                            endpoint=endpoint
                                        )

                                        if not error_message:
                                            part["image_url"]["url"] = final_url
                                            
                                            # 新增：将上传结果存入缓存
                                            FILEBED_URL_CACHE[image_hash] = (final_url, time.time())
                                            # 限制缓存大小
                                            if len(FILEBED_URL_CACHE) > FILEBED_URL_CACHE_MAX_SIZE:
                                                # 删除最旧的缓存项
                                                sorted_items = sorted(FILEBED_URL_CACHE.items(), key=lambda x: x[1][1])
                                                for old_hash, _ in sorted_items[:FILEBED_URL_CACHE_MAX_SIZE // 4]:
                                                    del FILEBED_URL_CACHE[old_hash]
                                                logger.debug(f"    🧹 缓存已满，清理了 {FILEBED_URL_CACHE_MAX_SIZE // 4} 个旧条目")
                                            
                                            # 改进URL显示
                                            show_full_urls = CONFIG.get("debug_show_full_urls", False)
                                            url_display = final_url if show_full_urls else final_url[:CONFIG.get("url_display_length", 200)]
                                            logger.info(f"    ✅ {role} 角色列表中的图片成功上传到 '{endpoint_name}': {url_display}{'...' if not show_full_urls and len(final_url) > CONFIG.get('url_display_length', 200) else ''} (已缓存)")
                                            upload_successful = True
                                            break
                                        else:
                                            logger.warning(f"    ⚠️ 端点 '{endpoint_name}' 上传失败: {error_message}。临时禁用并尝试下一个...")
                                            DISABLED_ENDPOINTS[endpoint_name] = time.time()  # 记录禁用时间
                                            logger.info(f"[FILEBED] {endpoint_name} 已禁用，当前禁用数: {len(DISABLED_ENDPOINTS)}")
                                            last_error = error_message
                                            if strategy == "failover" and i == 0:
                                                ROUND_ROBIN_INDEX += 1
                                                logger.info(f"    🔄 [Failover] 默认图床 '{endpoint_name}' 失败，切换到下一个。")
                                    
                                    if not upload_successful:
                                        logger.error(f"    ❌ {role} 角色列表中的图片上传失败")
                                        raise IOError(f"所有活动图床端点均上传失败。最后错误: {last_error}")

                # 输出统计信息
                if role_image_count:
                    logger.info(f"✅ 图片预处理完成。各角色图片统计：{role_image_count}")
                else:
                    logger.info("✅ 图片预处理完成（未发现需要上传的base64图片）")

        # 1. 转换请求 (此时已不包含需要上传的附件)
        lmarena_payload = await convert_openai_to_lmarena_payload(
            openai_req,
            session_id,
            message_id,
            mode_override=mode_override,
            battle_target_override=battle_target_override
        )
        
        # 关键补充：如果模型是图片类型，则向油猴脚本明确指出
        if model_type == 'image':
            lmarena_payload['is_image_request'] = True
        
        # 2. 包装成发送给浏览器的消息
        message_to_browser = {
            "request_id": request_id,
            "payload": lmarena_payload
        }
        
        
        # 3. 选择最佳标签页并通过 WebSocket 发送（负载均衡）
        selected_tab_id, selected_ws = await select_best_tab_for_request()
        
        # 保存标签页ID到请求元数据
        request_metadata[request_id]["tab_id"] = selected_tab_id
        
        logger.info(f"API CALL [ID: {request_id[:8]}]: 通过标签页 '{selected_tab_id}' 发送请求")
        await selected_ws.send_text(json.dumps(message_to_browser))

        # 4. 根据 stream 参数决定返回类型
        is_stream = openai_req.get("stream", False)

        if is_stream:
            # 返回流式响应（优化缓冲设置）
            response = StreamingResponse(
                stream_generator(request_id, model_name or "default_model"),
                media_type="text/event-stream",
                headers={
                    'Cache-Control': 'no-cache',
                    'Connection': 'keep-alive',
                    'X-Accel-Buffering': 'no',  # 禁用nginx缓冲
                    'Transfer-Encoding': 'chunked'  # 明确使用分块传输
                }
            )
            # 禁用FastAPI的响应缓冲
            response.headers['X-Content-Type-Options'] = 'nosniff'
            return response
        else:
            # 返回非流式响应
            result = await non_stream_response(request_id, model_name or "default_model")
            # 记录请求成功（已在non_stream_response内部处理）
            await monitoring_service.broadcast_to_monitors({
                "type": "request_end",
                "request_id": request_id,
                "success": True
            })
            return result
    except (ValueError, IOError) as e:
        # 捕获附件处理错误
        logger.error(f"API CALL [ID: {request_id[:8]}]: 附件预处理失败: {e}")
        # 记录请求失败
        monitoring_service.request_end(request_id, success=False, error=str(e))
        await monitoring_service.broadcast_to_monitors({
            "type": "request_end",
            "request_id": request_id,
            "success": False
        })
        
        # 🔧 关键修复：释放标签页请求计数
        if request_id in request_metadata:
            tab_id = request_metadata[request_id].get("tab_id")
            if tab_id:
                await release_tab_request(tab_id)
                logger.debug(f"API CALL [ID: {request_id[:8]}]: 错误处理中已释放标签页 '{tab_id}' 的请求计数")
        
        if request_id in response_channels:
            del response_channels[request_id]
        # 清理元数据
        if request_id in request_metadata:
            del request_metadata[request_id]
            logger.debug(f"API CALL [ID: {request_id[:8]}]: 已清理请求元数据")
        # 返回一个格式正确的JSON错误响应
        return JSONResponse(
            status_code=500,
            content={"error": {"message": f"[LMArena Bridge Error] 附件处理失败: {e}", "type": "attachment_error"}}
        )
    except Exception as e:
        # 捕获所有其他错误
        # 记录请求失败
        monitoring_service.request_end(request_id, success=False, error=str(e))
        await monitoring_service.broadcast_to_monitors({
            "type": "request_end",
            "request_id": request_id,
            "success": False
        })
        
        # 🔧 关键修复：释放标签页请求计数
        if request_id in request_metadata:
            tab_id = request_metadata[request_id].get("tab_id")
            if tab_id:
                await release_tab_request(tab_id)
                logger.debug(f"API CALL [ID: {request_id[:8]}]: 错误处理中已释放标签页 '{tab_id}' 的请求计数")
        
        if request_id in response_channels:
            del response_channels[request_id]
        # 清理元数据
        if request_id in request_metadata:
            del request_metadata[request_id]
            logger.debug(f"API CALL [ID: {request_id[:8]}]: 已清理请求元数据")
        logger.error(f"API CALL [ID: {request_id[:8]}]: 处理请求时发生致命错误: {e}", exc_info=True)
        # 确保也返回格式正确的JSON
        return JSONResponse(
            status_code=500,
            content={"error": {"message": str(e), "type": "internal_server_error"}}
        )

# --- 内部通信端点 ---
@app.post("/internal/start_id_capture")
async def start_id_capture():
    """
    接收来自 id_updater.py 的通知，并通过 WebSocket 指令
    激活油猴脚本的 ID 捕获模式。
    """
    if not browser_ws:
        logger.warning("ID CAPTURE: 收到激活请求，但没有浏览器连接。")
        raise HTTPException(status_code=503, detail="Browser client not connected.")
    
    try:
        logger.info("ID CAPTURE: 收到激活请求，正在通过 WebSocket 发送指令...")
        await browser_ws.send_text(json.dumps({"command": "activate_id_capture"}))
        logger.info("ID CAPTURE: 激活指令已成功发送。")
        return JSONResponse({"status": "success", "message": "Activation command sent."})
    except Exception as e:
        logger.error(f"ID CAPTURE: 发送激活指令时出错: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to send command via WebSocket.")
# --- 监控相关API端点 ---
@app.websocket("/ws/monitor")
async def monitor_websocket(websocket: WebSocket):
    """监控面板的WebSocket连接"""
    await websocket.accept()
    monitoring_service.add_monitor_client(websocket)
    
    try:
        # 发送初始数据
        summary = monitoring_service.get_summary()
        await websocket.send_json({
            "type": "initial_data",
            "stats": summary['stats'],
            "model_stats": summary['model_stats'],
            "active_requests": summary['active_requests_list'],
            "browser_connected": browser_ws is not None,
            "mode": {
                "mode": CONFIG.get("id_updater_last_mode", "direct_chat"),
                "target": CONFIG.get("id_updater_battle_target", "A")
            }
        })
        
        while True:
            # 保持连接
            await websocket.receive_text()
            
    except WebSocketDisconnect:
        monitoring_service.remove_monitor_client(websocket)

@app.get("/monitor", response_class=HTMLResponse)
async def monitor_dashboard():
    """返回监控面板HTML页面"""
    try:
        with open('monitor.html', 'r', encoding='utf-8') as f:
            html_content = f.read()
        return HTMLResponse(content=html_content)
    except FileNotFoundError:
        return HTMLResponse(
            content="<h1>监控面板文件未找到</h1><p>请确保 monitor.html 文件在正确的位置。</p>",
            status_code=404
        )

@app.get("/api/monitor/stats")
async def get_monitor_stats():
    """获取监控统计数据"""
    summary = monitoring_service.get_summary()
    return {
        "stats": summary['stats'],
        "model_stats": summary['model_stats'],
        "browser_connected": browser_ws is not None,
        "mode": {
            "mode": CONFIG.get("id_updater_last_mode", "direct_chat"),
            "target": CONFIG.get("id_updater_battle_target", "A")
        }
    }

@app.get("/api/monitor/active")
async def get_active_requests():
    """获取活跃请求列表"""
    return monitoring_service.get_active_requests()

@app.get("/api/monitor/logs/requests")
async def get_request_logs(limit: int = 50):
    """获取请求日志"""
    return monitoring_service.log_manager.read_recent_logs("requests", limit)

@app.get("/api/monitor/logs/errors")
async def get_error_logs(limit: int = 30):
    """获取错误日志"""  
    return monitoring_service.log_manager.read_recent_logs("errors", limit)

@app.get("/api/monitor/recent")
async def get_recent_data():
    """获取最近的请求和错误"""
    return {
        "recent_requests": monitoring_service.get_recent_requests(50),
        "recent_errors": monitoring_service.get_recent_errors(30)
    }

@app.get("/api/monitor/performance")
async def get_performance_metrics():
    """获取性能指标"""
    metrics = {
        "download_semaphore": {
            "max_concurrent": MAX_CONCURRENT_DOWNLOADS,
            "current_active": MAX_CONCURRENT_DOWNLOADS - DOWNLOAD_SEMAPHORE._value if DOWNLOAD_SEMAPHORE else 0,
            "available": DOWNLOAD_SEMAPHORE._value if DOWNLOAD_SEMAPHORE else MAX_CONCURRENT_DOWNLOADS
        },
        "aiohttp_session": {
            "connector_limit": aiohttp_session.connector.limit if aiohttp_session else 0,
            "connector_limit_per_host": aiohttp_session.connector.limit_per_host if aiohttp_session else 0,
            "connector_active": len(aiohttp_session.connector._conns) if aiohttp_session and hasattr(aiohttp_session.connector, '_conns') else 0
        },
        "cache_stats": {
            "image_cache_size": len(IMAGE_BASE64_CACHE),
            "image_cache_max": IMAGE_CACHE_MAX_SIZE,
            "downloaded_urls": len(downloaded_urls_set),
            "response_channels": len(response_channels),
            "disabled_endpoints": len(DISABLED_ENDPOINTS)
        },
        "config": {
            "max_concurrent_downloads": CONFIG.get("max_concurrent_downloads", 50),
            "download_timeout": CONFIG.get("download_timeout", {}),
            "connection_pool": CONFIG.get("connection_pool", {}),
            "memory_management": CONFIG.get("memory_management", {})
        }
    }
    return metrics

@app.get("/api/monitor/tabs")
async def get_tab_connections():
    """获取标签页连接状态"""
    async with browser_connections_lock:
        tabs_info = []
        current_time = time.time()
        
        for tab_id, ws in browser_connections.items():
            # 计算该标签页的连接时长
            connection_start = tab_connection_times.get(tab_id, current_time)
            connected_duration = current_time - connection_start
            
            # 获取该标签页的请求负载
            load = tab_request_counts.get(tab_id, 0)
            
            tabs_info.append({
                "tab_id": tab_id,
                "connected": ws.client_state.name == 'CONNECTED' if ws else False,
                "active_requests": load,
                "max_concurrent": 6,  # 浏览器HTTP/1.1限制
                "load_percentage": (load / 6) * 100 if load < 6 else 100,
                "status": "busy" if load >= 6 else "available",
                "connected_duration": connected_duration,
                "connected_at": connection_start
            })
        
        return {
            "total_tabs": len(browser_connections),
            "total_capacity": len(browser_connections) * 6,
            "total_active_requests": sum(tab_request_counts.values()),
            "tabs": tabs_info
        }



# 新增：获取请求详情的API端点
@app.get("/api/request/{request_id}")
async def get_request_details(request_id: str):
    """获取特定请求的详细信息"""
    details = monitoring_service.get_request_details(request_id)
    if details:
        return details
    else:
        raise HTTPException(status_code=404, detail="请求详情未找到")

# 新增：下载日志文件的端点
@app.get("/api/logs/download")
async def download_logs(log_type: str = "requests"):
    """下载日志文件"""
    from fastapi.responses import FileResponse

    if log_type == "requests":
        log_path = MonitorConfig.LOG_DIR / MonitorConfig.REQUEST_LOG_FILE
    elif log_type == "errors":
        log_path = MonitorConfig.LOG_DIR / MonitorConfig.ERROR_LOG_FILE
    else:
        raise HTTPException(status_code=400, detail="无效的日志类型")
    
    if not log_path.exists():
        raise HTTPException(status_code=404, detail="日志文件不存在")
    
    return FileResponse(
        path=str(log_path),
        filename=f"{log_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl",
        media_type="application/json"
    )


# 新增：图片库API端点
@app.get("/api/images/list")
async def get_image_list():
    """获取downloaded_images目录中的图片列表（包括子文件夹）"""

    images = []
    image_dir = IMAGE_SAVE_DIR
    
    if image_dir.exists():
        # 遍历主目录和所有子目录
        for item in image_dir.iterdir():
            # 处理主目录中的文件（兼容旧文件）
            if item.is_file() and item.suffix.lower() in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp']:
                stat = item.stat()
                images.append({
                    "filename": item.name,
                    "size": stat.st_size,
                    "modified": stat.st_mtime,
                    "url": f"/api/images/{item.name}",
                    "folder": None  # 主目录文件
                })
            # 处理日期文件夹
            elif item.is_dir() and item.name.isdigit() and len(item.name) == 8:  # YYYYMMDD格式
                for file_path in item.iterdir():
                    if file_path.is_file() and file_path.suffix.lower() in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp']:
                        stat = file_path.stat()
                        images.append({
                            "filename": file_path.name,
                            "size": stat.st_size,
                            "modified": stat.st_mtime,
                            "url": f"/api/images/{item.name}/{file_path.name}",
                            "folder": item.name  # 日期文件夹名
                        })
    
    # 按修改时间倒序排序（最新的在前）
    images.sort(key=lambda x: x['modified'], reverse=True)
    
    return {
        "total": len(images),
        "images": images
    }

@app.get("/api/images/{filename:path}")
async def get_image(filename: str):
    """获取单个图片文件（支持子文件夹）"""
    from fastapi.responses import FileResponse
    
    # 支持 "filename" 或 "folder/filename" 格式
    file_path = IMAGE_SAVE_DIR / filename
    
    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail="图片未找到")
    
    # 获取正确的媒体类型
    content_type = mimetypes.guess_type(str(file_path))[0] or 'application/octet-stream'
    
    return FileResponse(
        path=str(file_path),
        media_type=content_type,
        filename=filename
    )


# --- 新增：处理暂存请求的后台任务 ---
async def process_pending_requests():
    """在后台处理暂存队列中的所有请求。"""
    while not pending_requests_queue.empty():
        pending_item = await pending_requests_queue.get()
        future = pending_item["future"]
        request_data = pending_item["request_data"]
        original_request_id = pending_item.get("original_request_id")  # 可能为None
        
        if original_request_id:
            logger.info(f"正在恢复请求 {original_request_id[:8]}...")
        else:
            logger.info("正在重试一个暂存的请求...")

        try:
            # 重新调用 chat_completions 的核心逻辑
            response = await handle_single_completion(request_data)
            
            # 将成功的结果设置到 future 中，以唤醒等待的客户端
            future.set_result(response)
            
            if original_request_id:
                logger.info(f"✅ 请求 {original_request_id[:8]} 已成功恢复并返回响应。")
                # 清理原始请求的元数据和通道
                if original_request_id in response_channels:
                    del response_channels[original_request_id]
                if original_request_id in request_metadata:
                    del request_metadata[original_request_id]
            else:
                logger.info("✅ 一个暂存的请求已成功重试并返回响应。")

        except Exception as e:
            logger.error(f"重试暂存请求时发生错误: {e}", exc_info=True)
            # 将错误设置到 future 中，以便客户端知道请求失败了
            future.set_exception(e)
            
            # 清理失败请求的数据
            if original_request_id:
                if original_request_id in response_channels:
                    del response_channels[original_request_id]
                if original_request_id in request_metadata:
                    del request_metadata[original_request_id]
        
        # 添加短暂的延迟，避免同时发送过多请求
        await asyncio.sleep(1)

async def handle_single_completion(openai_req: dict):
    """
    处理单个聊天补全请求的核心逻辑，可被主端点和重试任务复用。
    """
    # 这个函数包含了 chat_completions 中的大部分逻辑，除了连接检查和暂存部分
    model_name = openai_req.get("model")
    
    # ... (从 chat_completions 复制并粘贴模型/会话ID的逻辑) ...
    # --- 模型与会话ID映射逻辑 ---
    session_id, message_id = None, None
    mode_override, battle_target_override = None, None

    if model_name and model_name in MODEL_ENDPOINT_MAP:
        mapping_entry = MODEL_ENDPOINT_MAP[model_name]
        selected_mapping = None

        if isinstance(mapping_entry, list) and mapping_entry:
            # 使用线程安全的轮询策略选择映射（与主函数保持一致）
            global MODEL_ROUND_ROBIN_INDEX, MODEL_ROUND_ROBIN_LOCK
            
            # 关键修复：使用锁确保原子操作
            with MODEL_ROUND_ROBIN_LOCK:
                if model_name not in MODEL_ROUND_ROBIN_INDEX:
                    MODEL_ROUND_ROBIN_INDEX[model_name] = 0
                
                current_index = MODEL_ROUND_ROBIN_INDEX[model_name]
                selected_mapping = mapping_entry[current_index]
                
                # 诊断日志：重试恢复时的轮询状态
                logger.info(f"[RETRY_ROUND_ROBIN] 模型 '{model_name}' 重试请求轮询:")
                logger.info(f"  - 选择索引: {current_index}/{len(mapping_entry)}")
                logger.info(f"  - Session ID后6位: ...{mapping_entry[current_index].get('session_id', 'N/A')[-6:]}")
                
                # 更新索引，循环到下一个（原子操作内完成）
                MODEL_ROUND_ROBIN_INDEX[model_name] = (current_index + 1) % len(mapping_entry)
        elif isinstance(mapping_entry, dict):
            selected_mapping = mapping_entry
        
        if selected_mapping:
            session_id = selected_mapping.get("session_id")
            message_id = selected_mapping.get("message_id")
            mode_override = selected_mapping.get("mode")
            battle_target_override = selected_mapping.get("battle_target")

    if not session_id:
        if CONFIG.get("use_default_ids_if_mapping_not_found", True):
            session_id = CONFIG.get("session_id")
            message_id = CONFIG.get("message_id")
            mode_override, battle_target_override = None, None
        else:
            raise ValueError(f"模型 '{model_name}' 未找到有效映射，且已禁用回退。")
    
    if not session_id or not message_id or "YOUR_" in session_id or "YOUR_" in message_id:
        raise ValueError("最终确定的会话ID或消息ID无效。")

    request_id = str(uuid.uuid4())
    response_channels[request_id] = asyncio.Queue()
    
    monitoring_service.request_start(
        request_id=request_id,
        model=model_name or "unknown",
        messages_count=len(openai_req.get("messages", [])),
        session_id=session_id[-6:] if session_id else None,
        mode=mode_override or CONFIG.get("id_updater_last_mode", "direct_chat"),
        messages=openai_req.get("messages", []),
        params={
            "temperature": openai_req.get("temperature"),
            "top_p": openai_req.get("top_p"),
            "max_tokens": openai_req.get("max_tokens"),
            "streaming": openai_req.get("stream", False)
        }
    )
    
    await monitoring_service.broadcast_to_monitors({
        "type": "request_start", "request_id": request_id, "model": model_name, "timestamp": time.time()
    })

    try:
        messages_to_process = openai_req.get("messages", [])
        for message in messages_to_process:
            content = message.get("content")
            if isinstance(content, list):
                for i, part in enumerate(content):
                    if part.get("type") == "image_url" and CONFIG.get("file_bed_enabled"):
                        # ... (文件床上传逻辑) ...
                        pass

        lmarena_payload = await convert_openai_to_lmarena_payload(
            openai_req, session_id, message_id,
            mode_override=mode_override, battle_target_override=battle_target_override
        )

        model_type = MODEL_NAME_TO_ID_MAP.get(model_name, {}).get("type", "text")
        if model_type == 'image':
            lmarena_payload['is_image_request'] = True

        message_to_browser = {"request_id": request_id, "payload": lmarena_payload}
        
        if not browser_ws:
            raise ConnectionError("Browser disconnected before sending the payload.")

        await browser_ws.send_text(json.dumps(message_to_browser))

        is_stream = openai_req.get("stream", False)
        if is_stream:
            # 优化的流式响应配置
            response = StreamingResponse(
                stream_generator(request_id, model_name or "default_model"),
                media_type="text/event-stream",
                headers={
                    'Cache-Control': 'no-cache',
                    'Connection': 'keep-alive',
                    'X-Accel-Buffering': 'no',  # 禁用nginx缓冲
                    'Transfer-Encoding': 'chunked'  # 明确使用分块传输
                }
            )
            response.headers['X-Content-Type-Options'] = 'nosniff'
            return response
        else:
            result = await non_stream_response(request_id, model_name or "default_model")
            await monitoring_service.broadcast_to_monitors({
                "type": "request_end", "request_id": request_id, "success": True
            })
            return result
            
    except Exception as e:
        monitoring_service.request_end(request_id, success=False, error=str(e))
        await monitoring_service.broadcast_to_monitors({
            "type": "request_end", "request_id": request_id, "success": False
        })
        
        # 🔧 关键修复：释放标签页请求计数
        if request_id in request_metadata:
            tab_id = request_metadata[request_id].get("tab_id")
            if tab_id:
                await release_tab_request(tab_id)
                logger.debug(f"重试函数错误处理中已释放标签页 '{tab_id}' 的请求计数")
        
        if request_id in response_channels:
            del response_channels[request_id]
        # 清理元数据
        if request_id in request_metadata:
            del request_metadata[request_id]
            logger.debug(f"请求 {request_id[:8]} 失败，已清理元数据")
        
        raise e

# --- 新增：图片下载辅助函数 ---
async def _download_image_data_with_retry(url: str) -> Tuple[Optional[bytes], Optional[str]]:
    """优化的异步图片下载器，带重试和并发控制"""
    global aiohttp_session, DOWNLOAD_SEMAPHORE
    
    if not DOWNLOAD_SEMAPHORE:
        DOWNLOAD_SEMAPHORE = Semaphore(MAX_CONCURRENT_DOWNLOADS)
    
    last_error = None
    max_retries = CONFIG.get("download_timeout", {}).get("max_retries", 2)
    retry_delays = [1, 2]  # 减少重试延迟
    
    # 🔍 诊断日志：并发控制状态
    semaphore_available = DOWNLOAD_SEMAPHORE._value if DOWNLOAD_SEMAPHORE else 0
    logger.info(f"[DOWNLOAD_DEBUG] 准备下载图片")
    logger.info(f"  - 可用下载槽: {semaphore_available}/{MAX_CONCURRENT_DOWNLOADS}")
    logger.info(f"  - 活跃下载: {MAX_CONCURRENT_DOWNLOADS - semaphore_available}")
    logger.info(f"  - 最大重试: {max_retries}")
    logger.info(f"  - URL前100字符: {url[:100]}...")
    
    # 🔧 下载延迟机制（避免TCP端口耗尽）
    delay_config = CONFIG.get("download_delay", {})
    if delay_config.get("enabled", False):
        delay_seconds = delay_config.get("delay_seconds", 0.5)
        logger.info(f"[DOWNLOAD_DEBUG] ⏱️ 延迟 {delay_seconds} 秒后开始（避免并发冲突）")
        await asyncio.sleep(delay_seconds)
    
    # 记录等待信号量的时间
    import time as time_module
    wait_start = time_module.time()
    
    # 使用信号量控制并发
    async with DOWNLOAD_SEMAPHORE:
        wait_time = time_module.time() - wait_start
        if wait_time > 1:
            logger.warning(f"[DOWNLOAD_DEBUG] ⚠️ 等待下载槽耗时: {wait_time:.2f}秒（并发阻塞！）")
        for retry_count in range(max_retries):
            try:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
                    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                    'Referer': 'https://lmarena.ai/'
                }
                
                if not aiohttp_session:
                    # 🔧 创建紧急会话（使用相同的SSL配置）
                    import ssl
                    ssl_context = ssl.create_default_context()
                    ssl_context.check_hostname = False
                    ssl_context.verify_mode = ssl.CERT_NONE
                    connector = aiohttp.TCPConnector(ssl=ssl_context, limit=100, limit_per_host=30)
                    aiohttp_session = aiohttp.ClientSession(connector=connector)
                    logger.warning("[DOWNLOAD_DEBUG] 创建了紧急aiohttp会话（使用自定义SSL上下文）")
                
                # 优化的超时设置（从配置读取）
                timeout_config = CONFIG.get("download_timeout", {})
                timeout = aiohttp.ClientTimeout(
                    total=timeout_config.get("total", 30),
                    connect=timeout_config.get("connect", 5),
                    sock_read=timeout_config.get("sock_read", 10)
                )
                
                # 🔍 诊断日志：超时配置
                logger.info(f"[DOWNLOAD_DEBUG] 重试 #{retry_count + 1}/{max_retries}")
                logger.info(f"  - 连接超时: {timeout_config.get('connect', 5)}秒")
                logger.info(f"  - 读取超时: {timeout_config.get('sock_read', 10)}秒")
                logger.info(f"  - 总超时: {timeout_config.get('total', 30)}秒")
                
                # 添加性能日志
                import time as time_module
                start_time = time_module.time()
                
                # 🔍 诊断日志：连接开始
                logger.info(f"[DOWNLOAD_DEBUG] 开始建立连接...")
                
                async with aiohttp_session.get(
                    url,
                    timeout=timeout,
                    headers=headers,
                    allow_redirects=True
                ) as response:
                    connect_time = time_module.time() - start_time
                    logger.info(f"[DOWNLOAD_DEBUG] 连接建立成功，耗时: {connect_time:.2f}秒")
                    
                    if response.status == 200:
                        logger.info(f"[DOWNLOAD_DEBUG] HTTP 200 OK，开始读取数据...")
                        read_start = time_module.time()
                        data = await response.read()
                        read_time = time_module.time() - read_start
                        download_time = time_module.time() - start_time
                        
                        # 🔍 详细性能分析
                        logger.info(f"[DOWNLOAD_DEBUG] 下载完成")
                        logger.info(f"  - 连接时间: {connect_time:.2f}秒")
                        logger.info(f"  - 读取时间: {read_time:.2f}秒")
                        logger.info(f"  - 总时间: {download_time:.2f}秒")
                        logger.info(f"  - 数据大小: {len(data) / 1024:.1f}KB")
                        logger.info(f"  - 下载速度: {(len(data) / 1024) / download_time:.1f}KB/s")
                        
                        # 记录慢速下载
                        slow_threshold = CONFIG.get("performance_monitoring", {}).get("slow_threshold_seconds", 10)
                        if download_time > slow_threshold:
                            logger.warning(f"[DOWNLOAD] ⚠️ 下载耗时较长: {download_time:.2f}秒 (阈值: {slow_threshold}秒)")
                        
                        return data, None
                    else:
                        last_error = f"HTTP {response.status}"
                        logger.error(f"[DOWNLOAD_DEBUG] ❌ HTTP错误: {response.status}")
                        
            except asyncio.TimeoutError as e:
                elapsed = time_module.time() - start_time
                last_error = f"超时（第{retry_count+1}次尝试）"
                logger.error(f"[DOWNLOAD_DEBUG] ❌ 超时")
                logger.error(f"  - 已等待: {elapsed:.2f}秒")
                logger.error(f"  - 配置总超时: {timeout_config.get('total', 30)}秒")
                logger.error(f"  - 可能原因: 网络慢、服务器响应慢、或数据量大")
            except aiohttp.ClientError as e:
                elapsed = time_module.time() - start_time
                last_error = f"网络错误: {str(e)}"
                logger.error(f"[DOWNLOAD_DEBUG] ❌ 网络错误: {e.__class__.__name__}")
                logger.error(f"  - 错误详情: {str(e)[:200]}")
                logger.error(f"  - 发生时间: {elapsed:.2f}秒后")
                # 🔍 诊断SSL错误
                if "SSL" in str(e) or "ssl" in str(e).lower():
                    logger.error(f"  - 💡 检测到SSL错误，可能是证书问题或防火墙拦截")
            except Exception as e:
                elapsed = time_module.time() - start_time
                last_error = str(e)
                logger.error(f"[DOWNLOAD_DEBUG] ❌ 未知错误: {e}")
                logger.error(f"  - 错误类型: {type(e).__name__}")
                logger.error(f"  - 发生时间: {elapsed:.2f}秒后")
            
            # 重试延迟
            if retry_count < max_retries - 1:
                delay = retry_delays[retry_count]
                logger.info(f"[DOWNLOAD_DEBUG] 等待{delay}秒后重试...")
                await asyncio.sleep(delay)
    
    return None, last_error

# --- 内存监控任务 ---
async def memory_monitor():
    """优化的内存监控任务"""
    import gc
    import psutil
    import os
    import time as time_module
    
    process = psutil.Process(os.getpid())
    last_gc_time = time_module.time()
    
    while True:
        try:
            # 每分钟检查一次（更频繁的监控）
            await asyncio.sleep(60)
            
            # 获取内存使用情况
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / (1024 * 1024)
            
            # 获取下载并发状态
            active_downloads = MAX_CONCURRENT_DOWNLOADS - DOWNLOAD_SEMAPHORE._value if DOWNLOAD_SEMAPHORE else 0
            
            # 记录内存状态（更详细的信息）
            logger.info(f"[MEM_MONITOR] 内存: {memory_mb:.2f}MB | "
                       f"活跃下载: {active_downloads}/{MAX_CONCURRENT_DOWNLOADS} | "
                       f"响应通道: {len(response_channels)} | "
                       f"请求元数据: {len(request_metadata)} | "
                       f"缓存图片: {len(IMAGE_BASE64_CACHE)} | "
                       f"图床URL缓存: {len(FILEBED_URL_CACHE)} | "
                       f"下载历史: {len(downloaded_urls_set)}")
            
            # 新增：清理过期的图床URL缓存
            if len(FILEBED_URL_CACHE) > 0:
                current_time = time_module.time()
                expired_hashes = []
                for img_hash, (url, cache_time) in FILEBED_URL_CACHE.items():
                    if current_time - cache_time > FILEBED_URL_CACHE_TTL:
                        expired_hashes.append(img_hash)
                
                if expired_hashes:
                    for img_hash in expired_hashes:
                        del FILEBED_URL_CACHE[img_hash]
                    logger.info(f"[MEM_MONITOR] 清理了 {len(expired_hashes)} 个过期的图床URL缓存")
            
            # 新增：监控和清理超时的请求元数据
            if len(request_metadata) > 10:  # 如果元数据过多，可能有内存泄漏
                logger.warning(f"[MEM_MONITOR] request_metadata数量较多: {len(request_metadata)}")
                logger.warning(f"[MEM_MONITOR] 开始清理超时的请求元数据...")
                
                # 实现超时清理逻辑
                current_time = datetime.now()
                timeout_threshold = CONFIG.get("metadata_timeout_minutes", 30)  # 默认30分钟超时
                stale_request_ids = []
                
                for req_id, metadata in request_metadata.items():
                    created_at_str = metadata.get("created_at")
                    if created_at_str:
                        try:
                            created_at = datetime.fromisoformat(created_at_str)
                            age_minutes = (current_time - created_at).total_seconds() / 60
                            
                            if age_minutes > timeout_threshold:
                                stale_request_ids.append(req_id)
                                logger.info(f"[MEM_MONITOR] 发现超时元数据: {req_id[:8]} (存活: {age_minutes:.1f}分钟)")
                        except (ValueError, TypeError) as e:
                            logger.warning(f"[MEM_MONITOR] 无法解析元数据时间: {req_id[:8]}, 错误: {e}")
                            stale_request_ids.append(req_id)  # 无效时间戳也清理
                
                # 清理超时的元数据
                for req_id in stale_request_ids:
                    del request_metadata[req_id]
                    # 同时清理对应的响应通道（如果还存在）
                    if req_id in response_channels:
                        del response_channels[req_id]
                        logger.debug(f"[MEM_MONITOR] 一并清理响应通道: {req_id[:8]}")
                
                if stale_request_ids:
                    logger.info(f"[MEM_MONITOR] 已清理 {len(stale_request_ids)} 个超时的请求元数据")
                else:
                    logger.info(f"[MEM_MONITOR] 未发现超时元数据，但数量仍然较多，可能是正常情况")
            else:
                logger.debug(f"[MEM_MONITOR] request_metadata: {len(request_metadata)}")
            
            # 从配置读取内存管理阈值
            mem_config = CONFIG.get("memory_management", {})
            gc_threshold = mem_config.get("gc_threshold_mb", 500)
            cache_config = mem_config.get("cache_config", {})
            
            # 根据内存使用情况动态调整
            if memory_mb > gc_threshold:
                current_time = time_module.time()
                # 防止过于频繁的GC
                if current_time - last_gc_time > 300:  # 5分钟最多GC一次
                    logger.warning(f"[MEM_MONITOR] 触发垃圾回收 (内存: {memory_mb:.2f}MB > {gc_threshold}MB)")
                    
                    # 清理图片缓存
                    cache_max = cache_config.get("image_cache_max_size", 500)
                    cache_keep = cache_config.get("image_cache_keep_size", 200)
                    if len(IMAGE_BASE64_CACHE) > cache_max:
                        # 保留最新的指定数量
                        sorted_items = sorted(IMAGE_BASE64_CACHE.items(),
                                            key=lambda x: x[1][1], reverse=True)
                        IMAGE_BASE64_CACHE.clear()
                        for url, data in sorted_items[:cache_keep]:
                            IMAGE_BASE64_CACHE[url] = data
                        logger.info(f"[MEM_MONITOR] 清理图片缓存: {len(sorted_items)} -> {cache_keep}")
                    
                    # 清理下载记录
                    url_history_max = cache_config.get("url_history_max", 2000)
                    url_history_keep = cache_config.get("url_history_keep", 1000)
                    if len(downloaded_urls_set) > url_history_max:
                        downloaded_urls_set.clear()
                        # 保留最近的记录
                        downloaded_urls_set.update(list(downloaded_image_urls)[-url_history_keep:])
                        logger.info(f"[MEM_MONITOR] 清理下载记录: {url_history_max} -> {url_history_keep}")
                    
                    # 执行垃圾回收
                    gc.collect()
                    last_gc_time = current_time
                    
                    # 再次检查内存
                    new_memory_mb = process.memory_info().rss / (1024 * 1024)
                    logger.info(f"[MEM_MONITOR] GC后内存: {memory_mb:.2f}MB -> {new_memory_mb:.2f}MB "
                               f"(释放: {memory_mb - new_memory_mb:.2f}MB)")
                    
        except Exception as e:
            logger.error(f"[MEM_MONITOR] 错误: {e}")

# --- 主程序入口 ---
if __name__ == "__main__":
    # 建议从 config.jsonc 中读取端口，此处为临时硬编码
    api_port = 5102
    logger.info(f"🚀 LMArena Bridge v2.0 API 服务器正在启动...")
    logger.info(f"   - 监听地址: http://127.0.0.1:{api_port}")
    logger.info(f"   - WebSocket 端点: ws://127.0.0.1:{api_port}/ws")
    
    uvicorn.run(app, host="0.0.0.0", port=api_port)
