# api_server.py
# Next-generation LMArena Bridge Backend Service

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

import aiohttp  # New: for asynchronous HTTP requests
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
# --- Internal module imports ---
from modules.file_uploader import upload_to_file_bed
from modules.monitoring import monitoring_service, MonitorConfig
from packaging.version import parse as parse_version

# Image auto-enhancement feature has been removed (stripped into a separate project)
# Globally disable SSL warnings (optional, but recommended)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- Basic Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Log Filter ---
class EndpointFilter(logging.Filter):
    """Filters out API request logs related to monitoring"""
    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        # Filter out all GET requests to /api/monitor/ and /monitor
        is_monitor_request = "GET /api/monitor/" in message or "GET /monitor " in message
        return not is_monitor_request

# Add the filter to uvicorn's access logger
logging.getLogger("uvicorn.access").addFilter(EndpointFilter())

# --- Global State and Configuration ---
CONFIG = {} # Stores configuration loaded from config.jsonc
# New: Supports concurrent connections for multiple tabs
# browser_connections is used to store WebSocket connections for multiple Tampermonkey scripts
# Key is tab ID (tab_id), value is WebSocket object
browser_connections: dict[str, WebSocket] = {}
browser_connections_lock = asyncio.Lock()  # Protects concurrent access
# New: Track tab connection times
tab_connection_times: dict[str, float] = {}
# Compatibility: Keep browser_ws for backward compatibility (points to the first connection)
browser_ws: WebSocket | None = None
# response_channels is used to store response queues for each API request.
# Key is request_id, value is asyncio.Queue.
response_channels: dict[str, asyncio.Queue] = {}
# New: Request metadata storage (for restoring requests after WebSocket reconnection)
request_metadata: dict[str, dict] = {}
# New: Track active request count for each tab
tab_request_counts: dict[str, int] = {}
tab_request_counts_lock = asyncio.Lock()
last_activity_time = None # Records the time of the last activity
idle_monitor_thread = None # Idle monitoring thread
main_event_loop = None # Main event loop
# New: Used to track if refreshing due to human verification
IS_REFRESHING_FOR_VERIFICATION = False
# New: Request staging queue for automatic retries
pending_requests_queue = asyncio.Queue()
# New: WebSocket connection lock, protects concurrent access
ws_lock = asyncio.Lock()
# New: Global aiohttp session
aiohttp_session = None
# --- Image Auto-download Configuration ---
IMAGE_SAVE_DIR = Path("./downloaded_images")
IMAGE_SAVE_DIR.mkdir(exist_ok=True)
# Use deque to limit size and avoid memory leaks
downloaded_image_urls = deque(maxlen=5000)  # Records up to 5000 URLs
downloaded_urls_set = set()  # Used for quick deduplication
# New: Used to temporarily disable failed image bed endpoints at runtime
DISABLED_ENDPOINTS = {}  # Changed to a dictionary, records disable time
# New: Global index for round-robin strategy
ROUND_ROBIN_INDEX = 0
# New: Image bed recovery time (seconds)
FILEBED_RECOVERY_TIME = 300  # Automatically recovers after 5 minutes

# New: Index dictionary for model ID mapping round-robin (requires thread-safe protection)
MODEL_ROUND_ROBIN_INDEX = {}  # {model_name: current_index}
MODEL_ROUND_ROBIN_LOCK = Lock()  # Thread lock to protect round-robin index

# New: Image Base64 cache (avoids duplicate downloads and conversions)
IMAGE_BASE64_CACHE = {}  # {url: (base64_data, timestamp)}
IMAGE_CACHE_MAX_SIZE = 1000  # Caches up to 100 images
IMAGE_CACHE_TTL = 3600  # Cache validity 1 hour (seconds)

# New: Image bed URL cache (avoids re-uploading the same image)
FILEBED_URL_CACHE = {}  # {image_hash: (uploaded_url, timestamp)}
FILEBED_URL_CACHE_TTL = 300  # Image bed link cache 5 minutes (seconds)
FILEBED_URL_CACHE_MAX_SIZE = 500  # Caches up to 500 image bed links

# New: Concurrent download control
DOWNLOAD_SEMAPHORE: Optional[Semaphore] = None
MAX_CONCURRENT_DOWNLOADS = 50  # Default maximum concurrent downloads

# --- Model Mapping ---
# MODEL_NAME_TO_ID_MAP now stores richer objects: { "model_name": {"id": "...", "type": "..."} }
MODEL_NAME_TO_ID_MAP = {}
MODEL_ENDPOINT_MAP = {} # New: Used to store model to session/message ID mapping
DEFAULT_MODEL_ID = None # Default model ID: None

def load_model_endpoint_map():
    """Loads model to endpoint mapping from model_endpoint_map.json."""
    global MODEL_ENDPOINT_MAP
    try:
        with open('model_endpoint_map.json', 'r', encoding='utf-8') as f:
            content = f.read()
            # Allow empty file
            if not content.strip():
                MODEL_ENDPOINT_MAP = {}
            else:
                MODEL_ENDPOINT_MAP = json.loads(content)
        logger.info(f"Successfully loaded {len(MODEL_ENDPOINT_MAP)} model endpoint mappings from 'model_endpoint_map.json'.")
    except FileNotFoundError:
        logger.warning("'model_endpoint_map.json' file not found. An empty mapping will be used.")
        MODEL_ENDPOINT_MAP = {}
    except json.JSONDecodeError as e:
        logger.error(f"Failed to load or parse 'model_endpoint_map.json': {e}. An empty mapping will be used.")
        MODEL_ENDPOINT_MAP = {}

def _parse_jsonc(jsonc_string: str) -> dict:
    """
    Robustly parses a JSONC string, removing comments.
    Improved version: correctly handles // and /* */ within strings
    """
    lines = jsonc_string.splitlines()
    no_comments_lines = []
    in_block_comment = False

    for line in lines:
        if in_block_comment:
            # In a block comment, look for the end marker
            if '*/' in line:
                in_block_comment = False
                # Keep content after the block comment ends
                line = line.split('*/', 1)[1]
            else:
                continue

        # Handle potential block comment start
        if '/*' in line:
            # Needs smarter handling to avoid deleting /* within strings
            before_comment, _, after_comment = line.partition('/*')
            if '*/' in after_comment:
                # Single-line block comment
                _, _, after_block = after_comment.partition('*/')
                line = before_comment + after_block
            else:
                # Multi-line block comment start
                line = before_comment
                in_block_comment = True

        # Handle single-line comment //, but avoid deleting // within strings
        # Use a smarter method: find // not within quotes
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
                # Found a real comment, stop processing this line
                break
            else:
                processed_line += char

            i += 1

        # Only add non-empty lines
        if processed_line.strip():
            no_comments_lines.append(processed_line)

    return json.loads("\n".join(no_comments_lines))

def load_config():
    """Loads configuration from config.jsonc and handles JSONC comments."""
    global CONFIG
    try:
        with open('config.jsonc', 'r', encoding='utf-8') as f:
            content = f.read()
        CONFIG = _parse_jsonc(content)
        logger.info("Successfully loaded configuration from 'config.jsonc'.")
        # Print key configuration status
        logger.info(f"  - Tavern Mode: {'✅ Enabled' if CONFIG.get('tavern_mode_enabled') else '❌ Disabled'}")
        logger.info(f"  - Bypass Mode: {'✅ Enabled' if CONFIG.get('bypass_enabled') else '❌ Disabled'}")
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.error(f"Failed to load or parse 'config.jsonc': {e}. Default configuration will be used.")
        CONFIG = {}

def load_model_map():
    """Loads model mapping from models.json, supporting 'id:type' format."""
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
                # Default or old format handling
                processed_map[name] = {"id": value, "type": "text"}

        MODEL_NAME_TO_ID_MAP = processed_map
        logger.info(f"Successfully loaded and parsed {len(MODEL_NAME_TO_ID_MAP)} models from 'models.json'.")

    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.error(f"Failed to load 'models.json': {e}. An empty model list will be used.")
        MODEL_NAME_TO_ID_MAP = {}

# --- Announcement Handling ---
def check_and_display_announcement():
    """Checks and displays a one-time announcement."""
    announcement_file = "announcement-lmarena.json"
    if os.path.exists(announcement_file):
        try:
            logger.info("="*60)
            logger.info("📢 Update announcement detected, content as follows:")
            with open(announcement_file, 'r', encoding='utf-8') as f:
                announcement = json.load(f)
                title = announcement.get("title", "Announcement")
                content = announcement.get("content", [])

                logger.info(f"   --- {title} ---")
                for line in content:
                    logger.info(f"   {line}")
                logger.info("="*60)

        except json.JSONDecodeError:
            logger.error(f"Failed to parse announcement file '{announcement_file}'. File content may not be valid JSON.")
        except Exception as e:
            logger.error(f"Error reading announcement file: {e}")
        finally:
            try:
                os.remove(announcement_file)
                logger.info(f"Announcement file '{announcement_file}' has been removed.")
            except OSError as e:
                logger.error(f"Failed to delete announcement file '{announcement_file}': {e}")

# --- Update Check ---
GITHUB_REPO = "zhongruichen/LMArenaBridge-mogai"

def download_and_extract_update(version):
    """Downloads and extracts the latest version to a temporary folder."""
    update_dir = "update_temp"
    if not os.path.exists(update_dir):
        os.makedirs(update_dir)

    try:
        zip_url = f"https://github.com/{GITHUB_REPO}/archive/refs/heads/mogai-version.zip"
        logger.info(f"Downloading new version from {zip_url}...")
        response = requests.get(zip_url, timeout=60)
        response.raise_for_status()

        # Need to import zipfile and io
        import zipfile
        import io
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            z.extractall(update_dir)

        logger.info(f"New version successfully downloaded and extracted to '{update_dir}' folder.")
        return True
    except requests.RequestException as e:
        logger.error(f"Failed to download update: {e}")
    except zipfile.BadZipFile:
        logger.error("Downloaded file is not a valid zip archive.")
    except Exception as e:
        logger.error(f"Unknown error occurred while extracting update: {e}")

    return False

def check_for_updates():
    """Checks for new versions from GitHub."""
    if not CONFIG.get("enable_auto_update", True):
        logger.info("Auto-update is disabled, skipping check.")
        return

    current_version = CONFIG.get("version", "0.0.0")
    logger.info(f"Current version: {current_version}. Checking for updates from GitHub...")

    try:
        config_url = f"https://raw.githubusercontent.com/{GITHUB_REPO}/mogai-version/config.jsonc"
        response = requests.get(config_url, timeout=10)
        response.raise_for_status()

        jsonc_content = response.text
        remote_config = _parse_jsonc(jsonc_content)

        remote_version_str = remote_config.get("version")
        if not remote_version_str:
            logger.warning("Version number not found in remote configuration file, skipping update check.")
            return

        if parse_version(remote_version_str) > parse_version(current_version):
            logger.info("="*60)
            logger.info(f"🎉 New version found! 🎉")
            logger.info(f"  - Current version: {current_version}")
            logger.info(f"  - Latest version: {remote_version_str}")
            if download_and_extract_update(remote_version_str):
                logger.info("Preparing to apply update. Server will shut down in 5 seconds and start the update script.")
                time.sleep(5)
                update_script_path = os.path.join("modules", "update_script.py")
                # Use Popen to start an independent process
                subprocess.Popen([sys.executable, update_script_path])
                # Gracefully exit the current server process
                os._exit(0)
            else:
                logger.error(f"Automatic update failed. Please visit https://github.com/{GITHUB_REPO}/releases/latest to download manually.")
            logger.info("="*60)
        else:
            logger.info("Your program is already the latest version.")

    except requests.RequestException as e:
        logger.error(f"Failed to check for updates: {e}")
    except json.JSONDecodeError:
        logger.error("Failed to parse remote configuration file.")
    except Exception as e:
        logger.error(f"Unknown error occurred while checking for updates: {e}")

# --- Model Update ---
def extract_models_from_html(html_content):
    """
    Extracts complete model JSON objects from HTML content, using bracket matching to ensure completeness.
    """
    models = []
    model_names = set()

    # Find the starting positions of all possible model JSON objects
    for start_match in re.finditer(r'\{\\"id\\":\\"[a-f0-9-]+\\"', html_content):
        start_index = start_match.start()

        # From the starting position, perform curly brace matching
        open_braces = 0
        end_index = -1

        # Optimization: set a reasonable search limit to avoid infinite loops
        search_limit = start_index + 10000 # Assume a model definition won't exceed 10000 characters

        for i in range(start_index, min(len(html_content), search_limit)):
            if html_content[i] == '{':
                open_braces += 1
            elif html_content[i] == '}':
                open_braces -= 1
                if open_braces == 0:
                    end_index = i + 1
                    break

        if end_index != -1:
            # Extract the complete, escaped JSON string
            json_string_escaped = html_content[start_index:end_index]

            # Unescape
            json_string = json_string_escaped.replace('\\"', '"').replace('\\\\', '\\')

            try:
                model_data = json.loads(json_string)
                model_name = model_data.get('publicName')

                # Deduplicate using publicName
                if model_name and model_name not in model_names:
                    models.append(model_data)
                    model_names.add(model_name)
            except json.JSONDecodeError as e:
                logger.warning(f"Error parsing extracted JSON object: {e} - Content: {json_string[:150]}...")
                continue

    if models:
        logger.info(f"Successfully extracted and parsed {len(models)} independent models.")
        return models
    else:
        logger.error("Error: No matching complete model JSON objects found in HTML response.")
        return None

def save_available_models(new_models_list, models_path="available_models.json"):
    """
    Saves the extracted list of complete model objects to the specified JSON file.
    """
    logger.info(f"Detected {len(new_models_list)} models, updating '{models_path}'...")

    try:
        with open(models_path, 'w', encoding='utf-8') as f:
            # Directly write the complete list of model objects to the file
            json.dump(new_models_list, f, indent=4, ensure_ascii=False)
        logger.info(f"✅ '{models_path}' successfully updated with {len(new_models_list)} models.")
    except IOError as e:
        logger.error(f"❌ Error writing to '{models_path}' file: {e}")

# --- Auto-restart Logic ---
def restart_server():
    """Gracefully notifies clients to refresh, then restarts the server."""
    logger.warning("="*60)
    logger.warning("Idle timeout detected, preparing to auto-restart...")
    logger.warning("="*60)

    # 1. (Asynchronously) Notify browser to refresh
    async def notify_browser_refresh():
        if browser_ws:
            try:
                # Prioritize sending 'reconnect' command so frontend knows it's a planned restart
                await browser_ws.send_text(json.dumps({"command": "reconnect"}, ensure_ascii=False))
                logger.info("Sent 'reconnect' command to browser.")
            except Exception as e:
                logger.error(f"Failed to send 'reconnect' command: {e}")

    # Run the asynchronous notification function in the main event loop
    # Use `asyncio.run_coroutine_threadsafe` to ensure thread safety
    if browser_ws and browser_ws.client_state.name == 'CONNECTED' and main_event_loop:
        asyncio.run_coroutine_threadsafe(notify_browser_refresh(), main_event_loop)

    # 2. Delay a few seconds to ensure message is sent
    time.sleep(3)

    # 3. Perform restart
    logger.info("Restarting server...")
    os.execv(sys.executable, ['python'] + sys.argv)

def idle_monitor():
    """Runs in a background thread, monitoring if the server is idle."""
    global last_activity_time

    # Wait until last_activity_time is first set
    while last_activity_time is None:
        time.sleep(1)

    logger.info("Idle monitoring thread started.")

    while True:
        if CONFIG.get("enable_idle_restart", False):
            timeout = CONFIG.get("idle_restart_timeout_seconds", 300)

            # If timeout is set to -1, disable restart check
            if timeout == -1:
                time.sleep(10) # Still need to sleep to avoid busy loop
                continue

            idle_time = (datetime.now() - last_activity_time).total_seconds()

            if idle_time > timeout:
                logger.info(f"Server idle time ({idle_time:.0f}s) has exceeded threshold ({timeout}s).")
                restart_server()
                break # Exit loop, as process is about to be replaced

        # Check every 10 seconds
        time.sleep(10)

# --- FastAPI Lifespan Events ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan function that runs when the server starts."""
    global idle_monitor_thread, last_activity_time, main_event_loop, aiohttp_session, DOWNLOAD_SEMAPHORE, MAX_CONCURRENT_DOWNLOADS
    main_event_loop = asyncio.get_running_loop() # Get the main event loop
    load_config() # First load configuration

    # Read concurrency and connection pool settings from configuration
    MAX_CONCURRENT_DOWNLOADS = CONFIG.get("max_concurrent_downloads", 50)
    pool_config = CONFIG.get("connection_pool", {})

    # 🔧 Create custom SSL context (fixes CloudFlare R2 SSL connection issues)
    import ssl
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False  # Disable hostname checking
    ssl_context.verify_mode = ssl.CERT_NONE  # Disable certificate verification
    logger.info("🔒 Custom SSL context created (certificate verification disabled for improved connection stability)")

    # Create optimized global aiohttp session
    connector = aiohttp.TCPConnector(
        ssl=ssl_context,  # 🔧 Use custom SSL context instead of False
        limit=pool_config.get("total_limit", 200),                  # Increase total connections
        limit_per_host=pool_config.get("per_host_limit", 50),      # Connection limit per host
        ttl_dns_cache=pool_config.get("dns_cache_ttl", 300),       # DNS cache time
        force_close=False,                                          # Keep connection alive
        enable_cleanup_closed=True,                                 # Automatically clean up closed connections
        keepalive_timeout=pool_config.get("keepalive_timeout", 30)  # Keep-alive timeout
    )

    # Create optimized timeout configuration
    timeout_config = CONFIG.get("download_timeout", {})
    timeout = aiohttp.ClientTimeout(
        total=timeout_config.get("total", 30),      # Total timeout
        connect=timeout_config.get("connect", 5),   # Connection timeout
        sock_read=timeout_config.get("sock_read", 10)  # Read timeout
    )

    aiohttp_session = aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        trust_env=True
    )

    # Initialize download semaphore
    DOWNLOAD_SEMAPHORE = Semaphore(MAX_CONCURRENT_DOWNLOADS)

    logger.info(f"Global aiohttp session created (optimized configuration)")
    logger.info(f"  - Max connections: {pool_config.get('total_limit', 200)}")
    logger.info(f"  - Connections per host: {pool_config.get('per_host_limit', 50)}")
    logger.info(f"  - Max concurrent downloads: {MAX_CONCURRENT_DOWNLOADS}")

    # Image auto-enhancement feature has been removed (stripped into a separate project image_enhancer)

    # --- Print current operating mode ---
    mode = CONFIG.get("id_updater_last_mode", "direct_chat")
    target = CONFIG.get("id_updater_battle_target", "A")
    logger.info("="*60)
    logger.info(f"  Current operating mode: {mode.upper()}")
    if mode == 'battle':
        logger.info(f"  - Battle mode target: Assistant {target}")
    logger.info("  (Can be changed by running id_updater.py)")
    logger.info("="*60)

    # Add monitoring panel information
    logger.info(f"📊 Monitoring Panel: http://127.0.0.1:5102/monitor")
    logger.info("="*60)

    check_for_updates() # Check for program updates
    load_model_map() # Re-enable model loading
    load_model_endpoint_map() # Load model endpoint mapping
    logger.info("Server startup complete. Waiting for Tampermonkey script connection...")

    # Check and display announcement, placed at the end of startup info to make it more prominent
    check_and_display_announcement()

    # After model update, mark the start of activity time
    last_activity_time = datetime.now()

    # Start idle monitoring thread
    if CONFIG.get("enable_idle_restart", False):
        idle_monitor_thread = threading.Thread(target=idle_monitor, daemon=True)
        idle_monitor_thread.start()


    # Start memory monitoring task
    asyncio.create_task(memory_monitor())

    yield

    # Clean up resources
    if aiohttp_session:
        await aiohttp_session.close()
        logger.info("Global aiohttp session closed")

    logger.info("Server is shutting down.")

app = FastAPI(lifespan=lifespan)

# --- CORS Middleware Configuration ---
# Allows all origins, all methods, all headers, which is safe for local development tools.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Helper Functions ---
def save_config():
    """Writes the current CONFIG object back to the config.jsonc file, preserving comments."""
    try:
        # Read original file to preserve comments, etc.
        with open('config.jsonc', 'r', encoding='utf-8') as f:
            lines = f.readlines()

        # Safely replace values using regex
        def replacer(key, value, content):
            # This regex finds the key, then matches its value part until a comma or closing curly brace
            pattern = re.compile(rf'("{key}"\s*:\s*").*?("?)(,?\s*)$', re.MULTILINE)
            replacement = rf'\g<1>{value}\g<2>\g<3>'
            if not pattern.search(content): # If key doesn't exist, add to end of file (simplified handling)
                 content = re.sub(r'}\s*$', f'  ,"{key}": "{value}"\n}}', content)
            else:
                 content = pattern.sub(replacement, content)
            return content

        content_str = "".join(lines)
        content_str = replacer("session_id", CONFIG["session_id"], content_str)
        content_str = replacer("message_id", CONFIG["message_id"], content_str)

        with open('config.jsonc', 'w', encoding='utf-8') as f:
            f.write(content_str)
        logger.info("✅ Successfully updated session information to config.jsonc.")
    except Exception as e:
        logger.error(f"❌ Error writing to config.jsonc: {e}", exc_info=True)

# --- Load Balancing Function ---
async def select_best_tab_for_request() -> Tuple[str, WebSocket]:
    """
    Selects the tab with the lowest load to handle new requests.
    Returns (tab_id, websocket)
    """
    global browser_connections, tab_request_counts

    async with browser_connections_lock:
        if not browser_connections:
            raise HTTPException(status_code=503, detail="No available browser connections")

        # 🔧 Critical fix: Clean up counts for disconnected tabs
        stale_tabs = [tab_id for tab_id in tab_request_counts.keys() if tab_id not in browser_connections]
        for tab_id in stale_tabs:
            del tab_request_counts[tab_id]
            logger.debug(f"[LOAD_BALANCE] Cleaned up count for disconnected tab '{tab_id}'")

        # Ensure all active tabs have a count
        for tab_id in browser_connections.keys():
            if tab_id not in tab_request_counts:
                tab_request_counts[tab_id] = 0

        # 🔧 Critical fix: Only select from active connections (not from tab_request_counts)
        # Calculate current load for each active tab
        active_tab_loads = {tab_id: tab_request_counts.get(tab_id, 0) for tab_id in browser_connections.keys()}

        # Select the tab with the lowest load
        best_tab_id = min(active_tab_loads, key=active_tab_loads.get)
        best_ws = browser_connections[best_tab_id]

        # Increment the request count for this tab
        tab_request_counts[best_tab_id] += 1

        logger.info(f"[LOAD_BALANCE] Selected tab '{best_tab_id}' (current load: {tab_request_counts[best_tab_id]}/6)")
        logger.info(f"[LOAD_BALANCE] All tab loads: {tab_request_counts}")

        return best_tab_id, best_ws

async def release_tab_request(tab_id: str):
    """Releases the request count for a tab"""
    global tab_request_counts

    async with tab_request_counts_lock:
        if tab_id in tab_request_counts and tab_request_counts[tab_id] > 0:
            tab_request_counts[tab_id] -= 1
            logger.debug(f"[LOAD_BALANCE] Released request for tab '{tab_id}' (remaining load: {tab_request_counts[tab_id]}/6)")

async def _process_openai_message(message: dict) -> dict:
    """
    Processes OpenAI messages, separating text and attachments.
    - Decomposes multimodal content lists into plain text and attachment lists.
    - File bed logic has been moved to chat_completions preprocessing, here only handles general attachment construction.
    - Ensures empty content for 'user' role is replaced with a space to avoid LMArena errors.
    - Special handling for assistant role images: detects Markdown images and converts them to experimental_attachments.
    """
    content = message.get("content")
    role = message.get("role")
    attachments = []
    experimental_attachments = []
    text_content = ""

    # Add diagnostic log
    logger.debug(f"[MSG_PROCESS] Processing message - Role: {role}, Content type: {type(content).__name__}")

    # Special handling for Markdown images in assistant role string content
    if role == "assistant" and isinstance(content, str):
        import re
        # Match ![...](url) format Markdown images
        markdown_pattern = r'!\[([^\]]*)\]\(([^)]+)\)'
        matches = re.findall(markdown_pattern, content)

        if matches:
            logger.info(f"[MSG_PROCESS] Detected {len(matches)} Markdown images in assistant message")

            # Remove Markdown images, keep only text
            text_content = re.sub(markdown_pattern, '', content).strip()

            # Convert images to experimental_attachments format
            for alt_text, url in matches:
                # Determine content type
                if url.startswith("data:"):
                    # base64 format
                    content_type = url.split(';')[0].split(':')[1] if ':' in url else 'image/png'
                elif url.startswith("http"):
                    # HTTP URL
                    content_type = mimetypes.guess_type(url)[0] or 'image/jpeg'
                else:
                    content_type = 'image/jpeg'

                # Generate filename
                if '/' in url and not url.startswith("data:"):
                    # Extract filename from URL
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
                logger.debug(f"[MSG_PROCESS] Added experimental_attachment: {filename}")
        else:
            text_content = content
    elif isinstance(content, list):
        text_parts = []
        for part in content:
            if part.get("type") == "text":
                text_parts.append(part.get("text", ""))
            elif part.get("type") == "image_url":
                # URL here can be base64 or http URL (already replaced by preprocessor)
                image_url_data = part.get("image_url", {})
                url = image_url_data.get("url")
                original_filename = image_url_data.get("detail")

                try:
                    # For base64, we need to extract content_type
                    if url.startswith("data:"):
                        content_type = url.split(';')[0].split(':')[1]
                    else:
                        # For http URL, we try to guess content_type
                        content_type = mimetypes.guess_type(url)[0] or 'application/octet-stream'

                    file_name = original_filename or f"image_{uuid.uuid4()}.{mimetypes.guess_extension(content_type).lstrip('.') or 'png'}"

                    attachment = {
                        "name": file_name,
                        "contentType": content_type,
                        "url": url
                    }

                    # Assistant role uses experimental_attachments
                    if role == "assistant":
                        experimental_attachments.append(attachment)
                        logger.debug(f"[MSG_PROCESS] Assistant image added to experimental_attachments")
                    else:
                        attachments.append(attachment)
                        logger.debug(f"[MSG_PROCESS] {role} image added to attachments")

                except (AttributeError, IndexError, ValueError) as e:
                    logger.warning(f"Error processing attachment URL: {url[:100]}... Error: {e}")

        text_content = "\n\n".join(text_parts)
    elif isinstance(content, str):
        text_content = content

    if role == "user" and not text_content.strip():
        text_content = " "

    # Construct return result
    result = {
        "role": role,
        "content": text_content,
        "attachments": attachments
    }

    # Assistant role adds experimental_attachments
    if role == "assistant" and experimental_attachments:
        result["experimental_attachments"] = experimental_attachments
        logger.info(f"[MSG_PROCESS] Assistant message contains {len(experimental_attachments)} experimental_attachments")

    return result

async def convert_openai_to_lmarena_payload(openai_data: dict, session_id: str, message_id: str, mode_override: str = None, battle_target_override: str = None) -> dict:
    """
    Converts an OpenAI request body to the simplified payload required by the Tampermonkey script,
    and applies Tavern Mode, Bypass Mode, and Battle Mode.
    Added mode override parameters to support model-specific session modes.
    """
    # 0. Preprocessing: Strip Chain-of-Thought from historical messages (if configured)
    messages = openai_data.get("messages", [])
    if CONFIG.get("strip_reasoning_from_history", True) and CONFIG.get("enable_lmarena_reasoning", False):
        reasoning_mode = CONFIG.get("reasoning_output_mode", "openai")

        # Only effective for think_tag mode (reasoning_content in OpenAI mode is not in content)
        if reasoning_mode == "think_tag":
            import re
            think_pattern = re.compile(r'<think>.*?</think>\s*', re.DOTALL)

            for msg in messages:
                if msg.get("role") == "assistant" and isinstance(msg.get("content"), str):
                    original_content = msg["content"]
                    # Remove <think> tags and their content
                    cleaned_content = think_pattern.sub('', original_content).strip()
                    if cleaned_content != original_content:
                        msg["content"] = cleaned_content
                        logger.debug(f"[REASONING_STRIP] Stripped Chain-of-Thought content from historical messages")

    # 1. Normalize roles and process messages
    #    - Converts non-standard 'developer' role to 'system' for better compatibility.
    #    - Separates text and attachments.
    for msg in messages:
        if msg.get("role") == "developer":
            msg["role"] = "system"
            logger.info("Message role normalized: 'developer' converted to 'system'.")

    processed_messages = []
    for msg in messages:
        processed_msg = await _process_openai_message(msg.copy())
        processed_messages.append(processed_msg)

    # 2. Apply Tavern Mode
    if CONFIG.get("tavern_mode_enabled"):
        system_prompts = [msg['content'] for msg in processed_messages if msg['role'] == 'system']
        other_messages = [msg for msg in processed_messages if msg['role'] != 'system']

        merged_system_prompt = "\n\n".join(system_prompts)
        final_messages = []

        if merged_system_prompt:
            # System messages should not have attachments
            final_messages.append({"role": "system", "content": merged_system_prompt, "attachments": []})

        final_messages.extend(other_messages)
        processed_messages = final_messages

    # 3. Determine target model ID and type
    model_name = openai_data.get("model", "claude-3-5-sonnet-20241022")

    # Prioritize getting model type from MODEL_ENDPOINT_MAP (if defined)
    model_type = "text"  # Default type
    endpoint_info = MODEL_ENDPOINT_MAP.get(model_name, {})

    # Diagnostic log: record model type determination process
    logger.info(f"[BYPASS_DEBUG] Starting to determine type for model '{model_name}'...")
    logger.info(f"[BYPASS_DEBUG] endpoint_info type: {type(endpoint_info).__name__}, content: {endpoint_info}")

    if isinstance(endpoint_info, dict) and "type" in endpoint_info:
        model_type = endpoint_info.get("type", "text")
        logger.info(f"[BYPASS_DEBUG] Retrieved model type from model_endpoint_map.json (dict): {model_type}")
    elif isinstance(endpoint_info, list) and endpoint_info:
        # If it's a list, take the type of the first element
        first_endpoint = endpoint_info[0] if isinstance(endpoint_info[0], dict) else {}
        if "type" in first_endpoint:
            model_type = first_endpoint.get("type", "text")
            logger.info(f"[BYPASS_DEBUG] Retrieved model type from model_endpoint_map.json (list): {model_type}")

    # Fallback to definition in models.json
    model_info = MODEL_NAME_TO_ID_MAP.get(model_name, {}) # Critical fix: ensure model_info is always a dictionary
    if not endpoint_info.get("type") and model_info:
        old_type = model_type
        model_type = model_info.get("type", "text")
        logger.info(f"[BYPASS_DEBUG] Retrieved model type from models.json: {old_type} -> {model_type}")

    logger.info(f"[BYPASS_DEBUG] Final determined model type: {model_type}")

    target_model_id = None
    if model_info:
        target_model_id = model_info.get("id")
    else:
        logger.warning(f"Model '{model_name}' not found in 'models.json'. Request will be sent without a specific model ID.")

    if not target_model_id:
        logger.warning(f"Model '{model_name}' not found with corresponding ID in 'models.json'. Request will be sent without a specific model ID.")

    # 4. Construct message templates
    message_templates = []
    for msg in processed_messages:
        msg_template = {
            "role": msg["role"],
            "content": msg.get("content", ""),
            "attachments": msg.get("attachments", [])
        }

        # For user role, attachments need to be in experimental_attachments
        if msg["role"] == "user" and msg.get("attachments"):
            msg_template["experimental_attachments"] = msg.get("attachments", [])
            logger.info(f"[LMARENA_CONVERT] Added {len(msg['attachments'])} attachments for user to experimental_attachments")

        # Retain assistant's experimental_attachments field (needed for image generation models)
        if msg["role"] == "assistant" and "experimental_attachments" in msg:
            msg_template["experimental_attachments"] = msg["experimental_attachments"]
            logger.info(f"[LMARENA_CONVERT] Retained {len(msg['experimental_attachments'])} experimental_attachments for assistant")

        message_templates.append(msg_template)

    # 4.5 Apply Image Attachment Bypass - specifically for image models
    # When using an image model and the latest user request contains image attachments,
    # separate text content into a new request.
    # Note: text models have their own bypass mechanism, search models don't need it (empty content would cause errors).
    if CONFIG.get("image_attachment_bypass_enabled", False) and model_type == "image":
        # Find the last user message
        last_user_msg_idx = None
        for i in range(len(message_templates) - 1, -1, -1):
            if message_templates[i]["role"] == "user":
                last_user_msg_idx = i
                break

        if last_user_msg_idx is not None:
            last_user_msg = message_templates[last_user_msg_idx]

            # Check if it contains image attachments
            has_image_attachment = False
            if last_user_msg.get("attachments"):
                for attachment in last_user_msg["attachments"]:
                    if attachment.get("contentType", "").startswith("image/"):
                        has_image_attachment = True
                        break

            # If it contains image attachments and has text content, perform separation
            if has_image_attachment and last_user_msg.get("content", "").strip():
                original_content = last_user_msg["content"]
                original_attachments = last_user_msg["attachments"]

                # Create two messages:
                # First: only image attachments (becomes historical record)
                image_only_msg = {
                    "role": "user",
                    "content": " ",  # Empty content or space
                    "experimental_attachments": original_attachments,
                    "attachments": original_attachments
                }

                # Second: only text content (as the latest request)
                text_only_msg = {
                    "role": "user",
                    "content": original_content,
                    "attachments": []
                }

                # Replace original message with two separated messages
                message_templates[last_user_msg_idx] = image_only_msg
                message_templates.insert(last_user_msg_idx + 1, text_only_msg)

                logger.info(f"Image model bypass enabled: request with {len(original_attachments)} attachments separated into two messages")

    # 5. Apply Bypass Mode - determined by model type and configuration
    # Get granular bypass settings
    bypass_settings = CONFIG.get("bypass_settings", {})
    global_bypass_enabled = CONFIG.get("bypass_enabled", False)

    # Diagnostic log: detailed bypass decision process
    logger.info(f"[BYPASS_DEBUG] ===== Bypass decision started =====")
    logger.info(f"[BYPASS_DEBUG] Global bypass_enabled: {global_bypass_enabled}")
    logger.info(f"[BYPASS_DEBUG] bypass_settings: {bypass_settings}")
    logger.info(f"[BYPASS_DEBUG] Current model type: {model_type}")

    # Determine if bypass is enabled for the model type
    bypass_enabled_for_type = False

    # Fix: If global_bypass_enabled is False, bypass should be disabled regardless of bypass_settings
    if not global_bypass_enabled:
        bypass_enabled_for_type = False
        logger.info(f"[BYPASS_DEBUG] ⛔ Global bypass_enabled=False, forcing all bypass features disabled")
    elif bypass_settings:
        # If granular configuration exists, check if the type is explicitly defined
        if model_type in bypass_settings:
            # If explicitly defined, use the defined value (but still controlled by global switch)
            bypass_enabled_for_type = bypass_settings.get(model_type, False)
            logger.info(f"[BYPASS_DEBUG] Using explicitly defined value from bypass_settings: bypass_settings['{model_type}'] = {bypass_enabled_for_type}")
        else:
            # If not explicitly defined, default to False (safer default)
            bypass_enabled_for_type = False
            logger.info(f"[BYPASS_DEBUG] model_type '{model_type}' not defined in bypass_settings, defaulting to disabled")
    else:
        # If no granular configuration, use global settings (for backward compatibility)
        # But for image and search types, default to False (maintaining original behavior)
        if model_type in ["image", "search"]:
            bypass_enabled_for_type = False
            logger.info(f"[BYPASS_DEBUG] No bypass_settings, model type '{model_type}' is in ['image', 'search'], forcing to False")
        else:
            bypass_enabled_for_type = global_bypass_enabled
            logger.info(f"[BYPASS_DEBUG] No bypass_settings, using global bypass_enabled: {bypass_enabled_for_type}")

    logger.info(f"[BYPASS_DEBUG] Final decision: bypass_enabled_for_type = {bypass_enabled_for_type}")

    if bypass_enabled_for_type:
        # Read bypass injection content from configuration
        bypass_injection = CONFIG.get("bypass_injection", {})

        # Support preset modes
        bypass_presets = bypass_injection.get("presets", {})
        active_preset_name = bypass_injection.get("active_preset", "default")

        # Try to get the active preset
        injection_config = bypass_presets.get(active_preset_name)

        # If preset doesn't exist, fallback to custom configuration or default values
        if not injection_config:
            logger.warning(f"[BYPASS_DEBUG] Preset '{active_preset_name}' does not exist, using custom configuration")
            injection_config = bypass_injection.get("custom", {
                "role": "user",
                "content": " ",
                "participantPosition": "a"
            })

        # Get injection parameters (with default values)
        inject_role = injection_config.get("role", "user")
        inject_content = injection_config.get("content", " ")
        inject_position = injection_config.get("participantPosition", "a")

        logger.info(f"[BYPASS_DEBUG] ⚠️ Bypass mode enabled for model type '{model_type}'")
        logger.info(f"[BYPASS_DEBUG]   - Using preset: {active_preset_name}")
        logger.info(f"[BYPASS_DEBUG]   - Injected role: {inject_role}")
        logger.info(f"[BYPASS_DEBUG]   - Injected position: {inject_position}")
        logger.info(f"[BYPASS_DEBUG]   - Injected content: {inject_content[:50]}{'...' if len(inject_content) > 50 else ''}")

        message_templates.append({
            "role": inject_role,
            "content": inject_content,
            "participantPosition": inject_position,
            "attachments": []
        })
    else:
        if global_bypass_enabled or any(bypass_settings.values()) if bypass_settings else False:
            # If any bypass setting is enabled, but not for the current type, log it
            logger.info(f"[BYPASS_DEBUG] ✅ Bypass mode disabled for model type '{model_type}'.")

    logger.info(f"[BYPASS_DEBUG] ===== Bypass decision ended =====")

    # 6. Apply Participant Position
    # Prioritize overridden mode, otherwise fallback to global configuration
    mode = mode_override or CONFIG.get("id_updater_last_mode", "direct_chat")
    target_participant = battle_target_override or CONFIG.get("id_updater_battle_target", "A")
    target_participant = target_participant.lower() # Ensure lowercase

    logger.info(f"Setting Participant Positions based on mode '{mode}' (target: {target_participant if mode == 'battle' else 'N/A'})...")

    for msg in message_templates:
        if msg['role'] == 'system':
            if mode == 'battle':
                # Battle mode: system is on the same side as the user-selected assistant (A for a, B for b)
                msg['participantPosition'] = target_participant
            else:
                # DirectChat mode: system is fixed to 'b'
                msg['participantPosition'] = 'b'
        elif mode == 'battle':
            # In Battle mode, non-system messages use the user-selected target participant
            msg['participantPosition'] = target_participant
        else: # DirectChat mode
            # In DirectChat mode, non-system messages use the default 'a'
            msg['participantPosition'] = 'a'

    return {
        "message_templates": message_templates,
        "target_model_id": target_model_id,
        "session_id": session_id,
        "message_id": message_id
    }

# --- OpenAI Formatting Helper Functions (ensuring robust JSON serialization) ---
def format_openai_chunk(content: str, model: str, request_id: str) -> str:
    """Formats as an OpenAI streaming chunk."""
    chunk = {
        "id": request_id, "object": "chat.completion.chunk",
        "created": int(time.time()), "model": model,
        "choices": [{"index": 0, "delta": {"content": content}, "finish_reason": None}]
    }
    return f"data: {json.dumps(chunk, ensure_ascii=False)}\n\n"

def format_openai_finish_chunk(model: str, request_id: str, reason: str = 'stop') -> str:
    """Formats as an OpenAI end chunk."""
    chunk = {
        "id": request_id, "object": "chat.completion.chunk",
        "created": int(time.time()), "model": model,
        "choices": [{"index": 0, "delta": {}, "finish_reason": reason}]
    }
    return f"data: {json.dumps(chunk, ensure_ascii=False)}\n\ndata: [DONE]\n\n"

def format_openai_error_chunk(error_message: str, model: str, request_id: str) -> str:
    """Formats as an OpenAI error chunk."""
    content = f"\n\n[LMArena Bridge Error]: {error_message}"
    return format_openai_chunk(content, model, request_id)

def format_openai_non_stream_response(content: str, model: str, request_id: str, reason: str = 'stop') -> dict:
    """Constructs a non-streaming response body compliant with OpenAI specifications."""
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
    """Saves downloaded image data locally (avoids duplicate downloads)"""
    global downloaded_image_urls, downloaded_urls_set

    # Avoid duplicate saving
    if url in downloaded_urls_set:
        show_full_urls = CONFIG.get("debug_show_full_urls", False)
        url_display = url if show_full_urls else url[:CONFIG.get("url_display_length", 200)]
        logger.info(f"🎨 Image already recorded, skipping save: {url_display}{'...' if not show_full_urls and len(url) > CONFIG.get('url_display_length', 200) else ''}")
        return

    try:
        # Directly use downloaded data for saving, avoiding duplicate downloads
        await save_image_data(image_data, url, request_id)

        # Update downloaded records (limited size)
        if url not in downloaded_urls_set:
            downloaded_image_urls.append(url)
            downloaded_urls_set.add(url)
            # When deque is full, automatically delete the oldest record
            if len(downloaded_urls_set) > 5000:
                # Clean up elements in set that are not in deque
                downloaded_urls_set = set(downloaded_image_urls)

    except Exception as e:
        logger.error(f"❌ Failed to save image: {type(e).__name__}: {e}")

async def download_image_truly_async(url, request_id):
    """[Deprecated] Truly asynchronous image download to local - now uses save_downloaded_image_async to avoid duplicate downloads"""
    # This function is kept for compatibility, but should not be called
    logger.warning(f"⚠️ download_image_truly_async was called, which should not happen as it leads to duplicate downloads")
    return

async def save_image_data(image_data, url, request_id):
    """Saves image data to a file (asynchronously)"""
    try:
        original_size_kb = len(image_data) / 1024

        # Create date folder
        date_folder = datetime.now().strftime("%Y%m%d")
        date_path = IMAGE_SAVE_DIR / date_folder
        date_path.mkdir(exist_ok=True)
        logger.info(f"📁 Using date folder: {date_folder}")

        # Check if format conversion is needed (local save)
        local_format_config = CONFIG.get("local_save_format", {})
        target_ext = 'png'  # Default extension

        if local_format_config.get("enabled", False):
            target_format = local_format_config.get("format", "original").lower()

            if target_format != "original":
                try:
                    from io import BytesIO
                    from PIL import Image

                    # Open image
                    img = Image.open(BytesIO(image_data))

                    # If RGBA mode and converting to JPEG, need to convert to RGB first
                    if target_format in ['jpeg', 'jpg'] and img.mode in ('RGBA', 'LA', 'P'):
                        # Create white background
                        background = Image.new('RGB', img.size, (255, 255, 255))
                        if img.mode == 'P':
                            img = img.convert('RGBA')
                        background.paste(img, mask=img.split()[-1] if img.mode == 'RGBA' else None)
                        img = background

                    # Save to BytesIO
                    output = BytesIO()

                    # Save according to target format
                    if target_format == 'png':
                        img.save(output, format='PNG', optimize=True)
                        target_ext = 'png'
                    elif target_format in ['jpeg', 'jpg']:
                        # Local save uses high quality
                        jpeg_quality = local_format_config.get("jpeg_quality", 100)
                        img.save(output, format='JPEG', quality=jpeg_quality, optimize=True)
                        target_ext = 'jpg'
                    elif target_format == 'webp':
                        img.save(output, format='WEBP', quality=95, optimize=True)
                        target_ext = 'webp'
                    else:
                        # Unsupported format, use original data
                        output = BytesIO(image_data)
                        # Infer extension from URL
                        if '.jpeg' in url.lower():
                            target_ext = 'jpeg'
                        elif '.jpg' in url.lower():
                            target_ext = 'jpg'
                        elif '.png' in url.lower():
                            target_ext = 'png'
                        elif '.webp' in url.lower():
                            target_ext = 'webp'

                    # Get converted data
                    image_data = output.getvalue()

                    converted_size_kb = len(image_data) / 1024
                    logger.info(f"🔄 Local save converted to {target_format.upper()} format ({original_size_kb:.1f}KB → {converted_size_kb:.1f}KB)")

                except Exception as e:
                    logger.warning(f"⚠️ Local save format conversion failed: {e}, using original format")
                    # Infer extension from URL
                    if '.jpeg' in url.lower():
                        target_ext = 'jpeg'
                    elif '.jpg' in url.lower():
                        target_ext = 'jpg'
                    elif '.png' in url.lower():
                        target_ext = 'png'
                    elif '.webp' in url.lower():
                        target_ext = 'webp'
            else:
                # Keep original format, infer extension from URL
                if '.jpeg' in url.lower():
                    target_ext = 'jpeg'
                elif '.jpg' in url.lower():
                    target_ext = 'jpg'
                elif '.png' in url.lower():
                    target_ext = 'png'
                elif '.webp' in url.lower():
                    target_ext = 'webp'
        else:
            # Format conversion not enabled, infer extension from URL
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

        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]  # Add milliseconds

        # Use timestamp and request ID as filename
        filename = f"{timestamp}_{request_id[:8]}.{target_ext}"
        filepath = date_path / filename  # Use date folder path

        # Asynchronously save file
        await asyncio.get_event_loop().run_in_executor(None, filepath.write_bytes, image_data)

        # Calculate file size
        size_kb = len(image_data) / 1024
        size_mb = size_kb / 1024

        if size_mb > 1:
            logger.info(f"✅ Image saved: {filename} ({size_mb:.2f}MB)")
        else:
            logger.info(f"✅ Image saved: {filename} ({size_kb:.1f}KB)")

        # Display full path
        logger.info(f"   📁 Save location: {filepath.absolute()}")

        # Image auto-enhancement feature has been removed (use independent image_enhancer project if enhancement is needed)

    except Exception as e:
        logger.error(f"❌ Failed to save image: {e}")

def download_image_async(url, request_id):
    """Compatibility wrapper for old versions (will be replaced by async version)"""
    global downloaded_image_urls, downloaded_urls_set

    # Avoid duplicate downloads
    if url in downloaded_urls_set:
        show_full_urls = CONFIG.get("debug_show_full_urls", False)
        url_display = url if show_full_urls else url[:CONFIG.get("url_display_length", 200)]
        logger.info(f"🎨 Image already exists, skipping download: {url_display}{'...' if not show_full_urls and len(url) > CONFIG.get('url_display_length', 200) else ''}")
        return

    try:
        import time
        import urllib3

        # Disable SSL warnings
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        time.sleep(2)  # Slight delay to ensure image is fully generated

        # Download image (key: verify=False to skip SSL verification)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Referer': 'https://lmarena.ai/'
        }

        # ⚠️ Critical modification: Add verify=False
        response = requests.get(url, timeout=30, headers=headers, verify=False)

        if response.status_code == 200:
            image_data = response.content
            original_size_kb = len(image_data) / 1024

            # Check if format conversion is needed (local save)
            local_format_config = CONFIG.get("local_save_format", {})
            target_ext = 'png'  # Default extension

            if local_format_config.get("enabled", False):
                target_format = local_format_config.get("format", "original").lower()

                if target_format != "original":
                    try:
                        from io import BytesIO
                        from PIL import Image

                        # Open image
                        img = Image.open(BytesIO(image_data))

                        # If RGBA mode and converting to JPEG, need to convert to RGB first
                        if target_format in ['jpeg', 'jpg'] and img.mode in ('RGBA', 'LA', 'P'):
                            # Create white background
                            background = Image.new('RGB', img.size, (255, 255, 255))
                            if img.mode == 'P':
                                img = img.convert('RGBA')
                            background.paste(img, mask=img.split()[-1] if img.mode == 'RGBA' else None)
                            img = background

                        # Save to BytesIO
                        output = BytesIO()

                        # Save according to target format
                        if target_format == 'png':
                            img.save(output, format='PNG', optimize=True)
                            target_ext = 'png'
                        elif target_format in ['jpeg', 'jpg']:
                            # Local save uses high quality
                            jpeg_quality = local_format_config.get("jpeg_quality", 100)
                            img.save(output, format='JPEG', quality=jpeg_quality, optimize=True)
                            target_ext = 'jpg'
                        elif target_format == 'webp':
                            img.save(output, format='WEBP', quality=95, optimize=True)
                            target_ext = 'webp'
                        else:
                            # Unsupported format, use original data
                            output = BytesIO(image_data)
                            # Infer extension from URL
                            if '.jpeg' in url.lower():
                                target_ext = 'jpeg'
                            elif '.jpg' in url.lower():
                                target_ext = 'jpg'
                            elif '.png' in url.lower():
                                target_ext = 'png'
                            elif '.webp' in url.lower():
                                target_ext = 'webp'

                        # Get converted data
                        image_data = output.getvalue()

                        converted_size_kb = len(image_data) / 1024
                        logger.info(f"🔄 Local save converted to {target_format.upper()} format ({original_size_kb:.1f}KB → {converted_size_kb:.1f}KB)")

                    except Exception as e:
                        logger.warning(f"⚠️ Local save format conversion failed: {e}, using original format")
                        # Conversion failed, use original data and extension
                        image_data = response.content
                        # Infer extension from URL
                        if '.jpeg' in url.lower():
                            target_ext = 'jpeg'
                        elif '.jpg' in url.lower():
                            target_ext = 'jpg'
                        elif '.png' in url.lower():
                            target_ext = 'png'
                        elif '.webp' in url.lower():
                            target_ext = 'webp'
                else:
                    # Keep original format
                    # Infer extension from URL
                    if '.jpeg' in url.lower():
                        target_ext = 'jpeg'
                    elif '.jpg' in url.lower():
                        target_ext = 'jpg'
                    elif '.png' in url.lower():
                        target_ext = 'png'
                    elif '.webp' in url.lower():
                        target_ext = 'webp'
            else:
                # Format conversion not enabled, infer extension from URL
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

            # Create date folder
            date_folder = datetime.now().strftime("%Y%m%d")
            date_path = IMAGE_SAVE_DIR / date_folder
            date_path.mkdir(exist_ok=True)
            logger.info(f"📁 Using date folder: {date_folder}")

            # Generate filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]  # Add milliseconds

            # Use timestamp and request ID as filename
            filename = f"{timestamp}_{request_id[:8]}.{target_ext}"
            filepath = date_path / filename  # Use date folder path

            # Save file
            filepath.write_bytes(image_data)

            # Update downloaded records (limited size)
            if url not in downloaded_urls_set:
                downloaded_image_urls.append(url)
                downloaded_urls_set.add(url)
                # When deque is full, automatically delete the oldest record
                if len(downloaded_urls_set) > 5000:
                    # Clean up elements in set that are not in deque
                    downloaded_urls_set = set(downloaded_image_urls)

            # Calculate file size
            size_kb = len(image_data) / 1024
            size_mb = size_kb / 1024

            if size_mb > 1:
                logger.info(f"✅ Image saved: {filename} ({size_mb:.2f}MB)")
            else:
                logger.info(f"✅ Image saved: {filename} ({size_kb:.1f}KB)")

            # Display full path
            logger.info(f"   📁 Save location: {filepath.absolute()}")

            # Image auto-enhancement feature has been removed (use independent image_enhancer project if enhancement is needed)

        else:
            logger.error(f"❌ Image download failed, HTTP status code: {response.status_code}")

    except requests.exceptions.Timeout:
        show_full_urls = CONFIG.get("debug_show_full_urls", False)
        url_display = url if show_full_urls else url[:CONFIG.get("url_display_length", 200)]
        logger.error(f"❌ Image download timed out: {url_display}{'...' if not show_full_urls and len(url) > CONFIG.get('url_display_length', 200) else ''}")
    except requests.exceptions.SSLError as e:
        logger.error(f"❌ SSL error (try using verify=False): {e}")
    except Exception as e:
        logger.error(f"❌ Image download failed: {type(e).__name__}: {e}")

async def _process_lmarena_stream(request_id: str):
    """
    Core internal generator: processes raw data stream from the browser and yields structured events.
    Event types: ('content', str), ('finish', str), ('error', str), ('retry_info', dict)
    """
    global IS_REFRESHING_FOR_VERIFICATION, aiohttp_session

    # Ensure using the latest configuration
    load_config()

    queue = response_channels.get(request_id)
    if not queue:
        logger.error(f"PROCESSOR [ID: {request_id[:8]}]: Response channel not found.")
        yield 'error', 'Internal server error: response channel not found.'
        return

    buffer = ""
    timeout = CONFIG.get("stream_response_timeout_seconds",360)
    text_pattern = re.compile(r'[ab]0:"((?:\\.|[^"\\])*)"')
    # New: Regex for matching Chain-of-Thought content
    reasoning_pattern = re.compile(r'ag:"((?:\\.|[^"\\])*)"')
    # New: Regex for matching and extracting image URLs
    image_pattern = re.compile(r'[ab]2:(\[.*?\])')
    finish_pattern = re.compile(r'[ab]d:(\{.*?"finishReason".*?\})')
    error_pattern = re.compile(r'(\{\s*"error".*?\})', re.DOTALL)
    cloudflare_patterns = [r'<title>Just a moment...</title>', r'Enable JavaScript and cookies to continue']

    has_yielded_content = False # Flag whether valid content has been yielded

    # Chain-of-Thought related variables
    # Note: Chain-of-Thought data should always be collected (for monitoring and logging), but whether it's output to the client depends on configuration
    enable_reasoning_output = CONFIG.get("enable_lmarena_reasoning", False)  # Whether to output to client
    reasoning_buffer = []  # Buffer all Chain-of-Thought fragments
    has_reasoning = False  # Flag whether there is Chain-of-Thought content
    reasoning_ended = False  # Flag whether reasoning has ended

    # Diagnostic: Add streaming performance tracking
    import time as time_module
    last_yield_time = time_module.time()
    chunk_count = 0
    total_chars = 0

    try:
        while True:
            # Critical fix: Reset reasoning_found flag at the beginning of each loop
            reasoning_found_in_this_chunk = False

            try:
                # Diagnostic: Record time to receive data
                receive_start = time_module.time()
                raw_data = await asyncio.wait_for(queue.get(), timeout=timeout)
                receive_time = time_module.time() - receive_start

                if CONFIG.get("debug_stream_timing", False):
                    logger.debug(f"[STREAM_TIMING] Time to get data from queue: {receive_time:.3f} seconds")
                    # Diagnostic: Display first 200 characters of raw data
                    raw_data_str = str(raw_data)[:200] if raw_data else "None"
                    logger.debug(f"[STREAM_RAW] Raw data: {raw_data_str}...")

            except asyncio.TimeoutError:
                logger.warning(f"PROCESSOR [ID: {request_id[:8]}]: Waiting for browser data timed out ({timeout} seconds).")
                yield 'error', f'Response timed out after {timeout} seconds.'
                return

            # --- Cloudflare Human Verification Handling ---
            def handle_cloudflare_verification():
                global IS_REFRESHING_FOR_VERIFICATION
                if not IS_REFRESHING_FOR_VERIFICATION:
                    logger.warning(f"PROCESSOR [ID: {request_id[:8]}]: First human verification detected, sending refresh command.")
                    IS_REFRESHING_FOR_VERIFICATION = True
                    if browser_ws:
                        asyncio.create_task(browser_ws.send_text(json.dumps({"command": "refresh"}, ensure_ascii=False)))
                    return "Human verification detected, refresh command sent, please try again later."
                else:
                    logger.info(f"PROCESSOR [ID: {request_id[:8]}]: Human verification detected, but already refreshing, will wait.")
                    return "Waiting for human verification to complete..."

            # 1. Check for direct errors or retry information from WebSocket
            if isinstance(raw_data, dict):
                # Handle retry information
                if 'retry_info' in raw_data:
                    retry_info = raw_data.get('retry_info', {})
                    logger.info(f"PROCESSOR [ID: {request_id[:8]}]: Received retry information - Attempt {retry_info.get('attempt')}/{retry_info.get('max_attempts')}")
                    # Can choose to pass retry information to the client
                    yield 'retry_info', retry_info
                    continue

                # Handle errors
                if 'error' in raw_data:
                    error_msg = raw_data.get('error', 'Unknown browser error')
                if isinstance(error_msg, str):
                    if '413' in error_msg or 'too large' in error_msg.lower():
                        friendly_error_msg = "Upload failed: attachment size exceeds LMArena server limits (usually around 5MB). Please try compressing the file or uploading a smaller file."
                        logger.warning(f"PROCESSOR [ID: {request_id[:8]}]: Detected attachment too large error (413).")
                        yield 'error', friendly_error_msg
                        return
                    if any(re.search(p, error_msg, re.IGNORECASE) for p in cloudflare_patterns):
                        yield 'error', handle_cloudflare_verification()
                        return
                yield 'error', error_msg
                return

            # 2. Check for [DONE] signal
            if raw_data == "[DONE]":
                # State reset logic has been moved to websocket_endpoint to ensure state is reset when connection is restored
                if has_yielded_content and IS_REFRESHING_FOR_VERIFICATION:
                     logger.info(f"PROCESSOR [ID: {request_id[:8]}]: Request successful, human verification status will be reset on next connection.")
                break

            # 3. Accumulate buffer and check content
            buffer += "".join(str(item) for item in raw_data) if isinstance(raw_data, list) else raw_data

            # Diagnostic: Display buffer size
            if CONFIG.get("debug_stream_timing", False):
                logger.debug(f"[STREAM_BUFFER] Buffer size: {len(buffer)} characters")

            if any(re.search(p, buffer, re.IGNORECASE) for p in cloudflare_patterns):
                yield 'error', handle_cloudflare_verification()
                return

            if (error_match := error_pattern.search(buffer)):
                try:
                    error_json = json.loads(error_match.group(1))
                    yield 'error', error_json.get("error", "Unknown error from LMArena")
                    return
                except json.JSONDecodeError: pass

            # Prioritize Chain-of-Thought content (ag prefix)
            # Note: Chain-of-Thought is always parsed and collected (for monitoring), but only output to client if configured
            reasoning_found_in_this_chunk = False
            while (match := reasoning_pattern.search(buffer)):
                try:
                    reasoning_content = json.loads(f'"{match.group(1)}"')
                    if reasoning_content:
                        # Warning: Detected reasoning appearing after content (abnormal situation)
                        if reasoning_ended:
                            logger.warning(f"[REASONING_WARN] Detected reasoning continuing after content, this may lead to content loss in think_tag mode!")

                        # Always collect Chain-of-Thought (for monitoring and logging)
                        has_reasoning = True
                        reasoning_buffer.append(reasoning_content)
                        reasoning_found_in_this_chunk = True

                        # Only output to client if configured
                        if enable_reasoning_output and CONFIG.get("preserve_streaming", True):
                            # Stream Chain-of-Thought
                            yield 'reasoning', reasoning_content

                except (ValueError, json.JSONDecodeError) as e:
                    if CONFIG.get("debug_stream_timing", False):
                        logger.debug(f"[REASONING_ERROR] Parsing error: {e}")
                    pass
                buffer = buffer[match.end():]

            # Process text content (a0 prefix) - add diagnostic
            process_start = time_module.time()
            chunks_in_buffer = 0

            # Diagnostic: Check for matches
            if CONFIG.get("debug_stream_timing", False):
                matches_found = text_pattern.findall(buffer)
                if matches_found:
                    logger.debug(f"[STREAM_MATCH] Found {len(matches_found)} text matches")
                    for idx, match in enumerate(matches_found[:3]):  # Only show first 3
                        logger.debug(f"  Match#{idx+1}: {match[:50]}...")

            while (match := text_pattern.search(buffer)):
                try:
                    text_content = json.loads(f'"{match.group(1)}"')
                    if text_content:
                        # Critical fix: When the first content arrives, if reasoning exists and hasn't ended, mark it as ended
                        if has_reasoning and not reasoning_ended and not reasoning_found_in_this_chunk:
                            reasoning_ended = True
                            logger.info(f"[REASONING_END] Reasoning ended (total {len(reasoning_buffer)} fragments)")
                            # Only send end event if output is enabled
                            if enable_reasoning_output:
                                yield 'reasoning_end', None

                        has_yielded_content = True
                        chunk_count += 1
                        total_chars += len(text_content)
                        chunks_in_buffer += 1

                        # Diagnostic: Record yield interval
                        current_time = time_module.time()
                        yield_interval = current_time - last_yield_time
                        last_yield_time = current_time

                        if CONFIG.get("debug_stream_timing", False):
                            logger.debug(f"[STREAM_TIMING] Yield interval: {yield_interval:.3f} seconds, "
                                       f"Chunk#{chunk_count}, Characters: {len(text_content)}, "
                                       f"Cumulative characters: {total_chars}")

                        yield 'content', text_content

                        # Process immediately, do not wait
                        await asyncio.sleep(0)

                except (ValueError, json.JSONDecodeError) as e:
                    if CONFIG.get("debug_stream_timing", False):
                        logger.debug(f"[STREAM_ERROR] Parsing error: {e}")
                    pass
                buffer = buffer[match.end():]

            # Diagnostic: Record processing time
            if chunks_in_buffer > 0 and CONFIG.get("debug_stream_timing", False):
                process_time = time_module.time() - process_start
                logger.debug(f"[STREAM_TIMING] Time to process {chunks_in_buffer} text chunks: {process_time:.3f} seconds")

            # New: Process image content
            while (match := image_pattern.search(buffer)):
                try:
                    image_data_list = json.loads(match.group(1))
                    if isinstance(image_data_list, list) and image_data_list:
                        image_info = image_data_list[0]
                        if image_info.get("type") == "image" and "image" in image_info:
                            image_url = image_info['image']

                            # Convert LMArena returned image URL to base64 for client
                            # Check if full URL needs to be displayed (read from config)
                            show_full_urls = CONFIG.get("debug_show_full_urls", False)
                            if show_full_urls:
                                logger.info(f"📥 LMArena returned image URL (full): {image_url}")
                            else:
                                # Display more characters, default 200 (usually enough to see full URL)
                                display_length = CONFIG.get("url_display_length", 200)
                                if len(image_url) <= display_length:
                                    logger.info(f"📥 LMArena returned image URL: {image_url}")
                                else:
                                    logger.info(f"📥 LMArena returned image URL: {image_url[:display_length]}...")
                                    logger.debug(f"   Full URL: {image_url}")  # Log full URL at DEBUG level

                            # Record start time
                            import time as time_module
                            import base64
                            process_start_time = time_module.time()

                            # Get return mode configuration
                            return_format_config = CONFIG.get("image_return_format", {})
                            return_mode = return_format_config.get("mode", "base64")
                            save_locally = CONFIG.get("save_images_locally", True)

                            # Diagnostic log: record processing start
                            logger.info(f"[IMG_PROCESS] Starting image processing")
                            logger.info(f"  - Return mode: {return_mode}")
                            logger.info(f"  - Local save: {save_locally}")

                            # URL mode: return immediately, non-blocking
                            if return_mode == "url":
                                logger.info(f"[IMG_PROCESS] URL mode - returning URL to client immediately")
                                # Return URL to client immediately, without waiting for any download
                                yield 'content', f"![Image]({image_url})"

                                # If local saving is needed, create a background task (non-blocking response)
                                if save_locally:
                                    logger.info(f"[IMG_PROCESS] Starting background task to asynchronously download and save image")

                                    async def async_download_and_save():
                                        try:
                                            download_start = time_module.time()
                                            img_data, err = await _download_image_data_with_retry(image_url)
                                            download_time = time_module.time() - download_start

                                            if img_data:
                                                logger.info(f"[IMG_PROCESS] Background download successful, time taken: {download_time:.2f} seconds")
                                                await save_downloaded_image_async(img_data, image_url, request_id)
                                                logger.info(f"[IMG_PROCESS] Image saved locally")
                                            else:
                                                logger.error(f"[IMG_PROCESS] Background download failed: {err}")
                                        except Exception as e:
                                            logger.error(f"[IMG_PROCESS] Background task exception: {e}")

                                    # Create background task, do not wait for completion
                                    asyncio.create_task(async_download_and_save())
                                else:
                                    logger.info(f"[IMG_PROCESS] save_images_locally=false, skipping download")

                                # URL mode processing complete, continue to next message
                                continue

                            # Base64 mode: must download first to convert
                            logger.info(f"[IMG_PROCESS] Base64 mode - requires image download for conversion")

                            # Download image data
                            download_start_time = time_module.time()
                            image_data, download_error = await _download_image_data_with_retry(image_url)
                            download_time = time_module.time() - download_start_time
                            logger.info(f"[IMG_PROCESS] Image download complete, time taken: {download_time:.2f} seconds")

                            # If local saving is needed
                            if save_locally and image_data:
                                logger.info(f"[IMG_PROCESS] Asynchronously saving image locally")
                                asyncio.create_task(save_downloaded_image_async(image_data, image_url, request_id))
                            elif not save_locally:
                                logger.info(f"[IMG_PROCESS] save_images_locally=false, skipping local save")

                            # Base64 conversion
                            if True:  # Here it's confirmed to be base64 mode
                                if image_data:
                                    # --- Base64 Conversion and Caching Logic ---
                                    cache_key = image_url
                                    current_time = time_module.time()

                                    # Clean up expired cache
                                    if len(IMAGE_BASE64_CACHE) > IMAGE_CACHE_MAX_SIZE:
                                        sorted_items = sorted(IMAGE_BASE64_CACHE.items(), key=lambda x: x[1][1])
                                        for url, _ in sorted_items[:IMAGE_CACHE_MAX_SIZE // 2]:
                                            del IMAGE_BASE64_CACHE[url]
                                        logger.info(f"  🧹 Cleaned {IMAGE_CACHE_MAX_SIZE // 2} old cache entries")

                                    # Check cache
                                    if cache_key in IMAGE_BASE64_CACHE:
                                        cached_data, cache_time = IMAGE_BASE64_CACHE[cache_key]
                                        if current_time - cache_time < IMAGE_CACHE_TTL:
                                            # Cache hit and not expired
                                            logger.info(f"  ⚡ Retrieved image Base64 from cache")
                                            yield 'content', cached_data
                                            continue

                                    # Perform conversion
                                    content_type = mimetypes.guess_type(image_url)[0] or 'image/png'
                                    image_base64 = base64.b64encode(image_data).decode('ascii')
                                    data_url = f"data:{content_type};base64,{image_base64}"
                                    markdown_image = f"![Image]({data_url})"

                                    # Store in cache
                                    IMAGE_BASE64_CACHE[cache_key] = (markdown_image, current_time)

                                    # Calculate total time taken
                                    total_time = time_module.time() - process_start_time
                                    logger.info(f"[IMG_PROCESS] Base64 conversion complete, total time: {total_time:.2f} seconds")

                                    yield 'content', markdown_image
                                else:
                                    # Download failed, fallback to returning URL
                                    logger.error(f"[IMG_PROCESS] ❌ Image download failed ({download_error}), falling back to original URL")
                                    total_time = time_module.time() - process_start_time
                                    logger.info(f"[IMG_PROCESS] Processing complete (failed fallback), total time: {total_time:.2f} seconds")
                                    yield 'content', f"![Image]({image_url})"

                except (json.JSONDecodeError, IndexError) as e:
                    logger.warning(f"Error parsing image URL: {e}, buffer: {buffer[:150]}")
                buffer = buffer[match.end():]

            if (finish_match := finish_pattern.search(buffer)):
                try:
                    finish_data = json.loads(finish_match.group(1))
                    yield 'finish', finish_data.get("finishReason", "stop")
                except (json.JSONDecodeError, IndexError): pass
                buffer = buffer[finish_match.end():]

    except asyncio.CancelledError:
        logger.info(f"PROCESSOR [ID: {request_id[:8]}]: Task cancelled.")
    finally:
        # Before cleanup, if Chain-of-Thought content exists and hasn't been streamed, output it all at once
        if enable_reasoning_output and has_reasoning and not CONFIG.get("preserve_streaming", True):
            # Non-streaming mode: output complete Chain-of-Thought at the end
            full_reasoning = "".join(reasoning_buffer)
            yield 'reasoning_complete', full_reasoning

        # Diagnostic: Output streaming performance statistics
        if chunk_count > 0 and CONFIG.get("debug_stream_timing", False):
            total_time = time_module.time() - (last_yield_time - yield_interval if 'yield_interval' in locals() else last_yield_time)
            logger.info(f"[STREAM_STATS] Request ID: {request_id[:8]}")
            logger.info(f"  - Total chunks: {chunk_count}")
            logger.info(f"  - Total characters: {total_chars}")
            logger.info(f"  - Average chunk size: {total_chars/chunk_count:.1f} characters")
            logger.info(f"  - Average yield interval: {total_time/chunk_count:.3f} seconds")

        # 🔧 Critical fix: Release tab request count
        if request_id in request_metadata:
            tab_id = request_metadata[request_id].get("tab_id")
            if tab_id:
                await release_tab_request(tab_id)
                logger.debug(f"PROCESSOR [ID: {request_id[:8]}]: Released request count for tab '{tab_id}'")

        if request_id in response_channels:
            del response_channels[request_id]
            logger.info(f"PROCESSOR [ID: {request_id[:8]}]: Response channel cleaned up.")

        # Clean up request metadata (fixes memory leak)
        if request_id in request_metadata:
            del request_metadata[request_id]
            logger.debug(f"PROCESSOR [ID: {request_id[:8]}]: Request metadata cleaned up.")

async def stream_generator(request_id: str, model: str):
    """Formats internal event stream into OpenAI SSE response."""
    response_id = f"chatcmpl-{uuid.uuid4()}"
    logger.info(f"STREAMER [ID: {request_id[:8]}]: Streaming generator started.")

    finish_reason_to_send = 'stop'  # Default finish reason
    collected_content = []  # Collect response content for storage
    reasoning_content = []  # Collect Chain-of-Thought content

    # Diagnostic: Add streaming performance tracking
    import time as time_module
    stream_start_time = time_module.time()
    chunks_sent = 0

    # Chain-of-Thought configuration
    # Note: Chain-of-Thought data is always collected, but only output to client if enabled
    enable_reasoning_output = CONFIG.get("enable_lmarena_reasoning", False)
    reasoning_mode = CONFIG.get("reasoning_output_mode", "openai")
    preserve_streaming = CONFIG.get("preserve_streaming", True)

    async for event_type, data in _process_lmarena_stream(request_id):
        if event_type == 'retry_info':
            # Handle retry information, can send to client as a comment
            retry_msg = f"\n[Retry Info] Attempt {data.get('attempt')}/{data.get('max_attempts')}, Reason: {data.get('reason')}, Waiting {data.get('delay')/1000} seconds...\n"
            logger.info(f"STREAMER [ID: {request_id[:8]}]: {retry_msg.strip()}")
            # Optional: send retry information as a comment to the client
            if CONFIG.get("show_retry_info_to_client", False):
                yield format_openai_chunk(retry_msg, model, response_id)
        elif event_type == 'reasoning':
            # Handle Chain-of-Thought fragments
            # Always collect Chain-of-Thought (for monitoring), but only output to client if enabled
            reasoning_content.append(data)

            if enable_reasoning_output:
                if reasoning_mode == "openai" and preserve_streaming:
                    # OpenAI mode and streaming enabled: send reasoning delta
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
                # think_tag mode: collect, do not output immediately

        elif event_type == 'reasoning_end':
            # New: reasoning end event (think_tag mode specific)
            if enable_reasoning_output and reasoning_mode == "think_tag" and reasoning_content:
                # Immediately output complete reasoning
                full_reasoning = "".join(reasoning_content)
                wrapped_reasoning = f"<think>{full_reasoning}</think>\n\n"
                yield format_openai_chunk(wrapped_reasoning, model, response_id)
                logger.info(f"[THINK_TAG] Outputted complete reasoning ({len(reasoning_content)} fragments)")

        elif event_type == 'reasoning_complete':
            # Handle complete Chain-of-Thought (non-streaming mode)
            # Always collect, but only output if enabled
            full_reasoning = data
            reasoning_content.append(full_reasoning)

            if enable_reasoning_output and not preserve_streaming:
                if reasoning_mode == "openai":
                    # OpenAI mode: send complete reasoning
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
                    # think_tag mode: wrap and output as content
                    wrapped_reasoning = f"<think>{full_reasoning}</think>\n\n"
                    yield format_openai_chunk(wrapped_reasoning, model, response_id)

        elif event_type == 'content':

            collected_content.append(data)  # Collect content
            chunks_sent += 1

            # Immediately generate and send data chunk, do not accumulate
            chunk_data = format_openai_chunk(data, model, response_id)

            if CONFIG.get("debug_stream_timing", False):
                logger.debug(f"[STREAM_OUTPUT] Sending chunk#{chunks_sent}, size: {len(chunk_data)} bytes")

            # Important: yield immediately, do not wait
            yield chunk_data

            # If forced flush is enabled (for some clients)
            if CONFIG.get("force_stream_flush", True):
                # Add a tiny asynchronous pause to force flush
                await asyncio.sleep(0)
        elif event_type == 'finish':
            # Record finish reason, but do not return immediately, wait for browser to send [DONE]
            finish_reason_to_send = data
            if data == 'content-filter':
                warning_msg = "\n\nResponse terminated, possibly due to context overflow or model internal censorship (most likely)"
                collected_content.append(warning_msg)  # Also collect warning message
                yield format_openai_chunk(warning_msg, model, response_id)
        elif event_type == 'error':
            logger.error(f"STREAMER [ID: {request_id[:8]}]: Error occurred in stream: {data}")
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
            return # On error, can terminate immediately

    # Only execute when _process_lmarena_stream naturally ends (i.e., receives [DONE])
    yield format_openai_finish_chunk(model, response_id, reason=finish_reason_to_send)

    # Diagnostic: Output streaming output statistics
    if CONFIG.get("debug_stream_timing", False) and chunks_sent > 0:
        total_time = time_module.time() - (last_yield_time - yield_interval if 'yield_interval' in locals() else last_yield_time)
        logger.info(f"[STREAM_OUTPUT_STATS] Request ID: {request_id[:8]}")
        logger.info(f"  - Chunks sent: {chunks_sent}")
        logger.info(f"  - Total time: {total_time:.2f} seconds")
        logger.info(f"  - Average send interval: {total_time/chunks_sent:.3f} seconds/chunk")

    logger.info(f"STREAMER [ID: {request_id[:8]}]: Streaming generator ended normally.")

    # Log successful request (including response content)
    full_response = "".join(collected_content)
    full_reasoning = "".join(reasoning_content) if reasoning_content else None

    # Get original request information from response_channels to calculate input tokens
    input_tokens = 0
    if hasattr(monitoring_service, 'active_requests') and request_id in monitoring_service.active_requests:
        request_info = monitoring_service.active_requests[request_id]
        # Calculate input token count (simple estimate: all message content length divided by 4)
        if request_info.request_messages:
            for msg in request_info.request_messages:
                if isinstance(msg, dict) and 'content' in msg:
                    content = msg.get('content', '')
                    if isinstance(content, str):
                        input_tokens += len(content) // 4
                    elif isinstance(content, list):
                        # Handle multimodal messages
                        for part in content:
                            if isinstance(part, dict) and part.get('type') == 'text':
                                input_tokens += len(part.get('text', '')) // 4

    monitoring_service.request_end(
        request_id,
        success=True,
        response_content=full_response,
        reasoning_content=full_reasoning,  # Add Chain-of-Thought content
        input_tokens=input_tokens,  # Add input tokens
        output_tokens=len(full_response) // 4  # Simple estimate of output tokens
    )
    await monitoring_service.broadcast_to_monitors({
        "type": "request_end",
        "request_id": request_id,
        "success": True
    })

async def non_stream_response(request_id: str, model: str):
    """Aggregates internal event stream and returns a single OpenAI JSON response."""
    response_id = f"chatcmpl-{uuid.uuid4()}"
    logger.info(f"NON-STREAM [ID: {request_id[:8]}]: Starting non-streaming response processing.")

    full_content = []
    reasoning_content = []
    finish_reason = "stop"

    # Chain-of-Thought configuration
    # Note: Chain-of-Thought data is always collected, but only output to client if enabled
    enable_reasoning_output = CONFIG.get("enable_lmarena_reasoning", False)
    reasoning_mode = CONFIG.get("reasoning_output_mode", "openai")

    async for event_type, data in _process_lmarena_stream(request_id):
        if event_type == 'retry_info':
            # Record retry information in non-streaming response
            logger.info(f"NON-STREAM [ID: {request_id[:8]}]: Retry information - Attempt {data.get('attempt')}/{data.get('max_attempts')}")
            # Non-streaming responses typically do not return retry information to the client
        elif event_type == 'reasoning' or event_type == 'reasoning_complete':
            # Collect Chain-of-Thought content (always collected for monitoring)
            reasoning_content.append(data)
        elif event_type == 'content':
            full_content.append(data)
        elif event_type == 'finish':
            finish_reason = data
            if data == 'content-filter':
                full_content.append("\n\nResponse terminated, possibly due to context overflow or model internal censorship (most likely)")
            # Do not break here, continue waiting for [DONE] signal from browser to avoid race conditions
        elif event_type == 'error':
            logger.error(f"NON-STREAM [ID: {request_id[:8]}]: Error occurred during processing: {data}")

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

            # Unify error status codes for streaming and non-streaming responses
            status_code = 413 if "attachment size exceeds" in str(data) else 500

            error_response = {
                "error": {
                    "message": f"[LMArena Bridge Error]: {data}",
                    "type": "bridge_error",
                    "code": "attachment_too_large" if status_code == 413 else "processing_error"
                }
            }
            return Response(content=json.dumps(error_response, ensure_ascii=False), status_code=status_code, media_type="application/json")

    # Process Chain-of-Thought content
    # Chain-of-Thought is always collected (for monitoring), but only output to client if enabled
    if enable_reasoning_output and reasoning_content:
        full_reasoning = "".join(reasoning_content)

        if reasoning_mode == "openai":
            # OpenAI mode: add reasoning_content field
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
                        "reasoning_content": full_reasoning  # Add Chain-of-Thought
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
            # think_tag mode: wrap Chain-of-Thought and place before content
            wrapped_reasoning = f"<think>{full_reasoning}</think>\n\n"
            final_content_str = wrapped_reasoning + "".join(full_content)
            response_data = format_openai_non_stream_response(final_content_str, model, response_id, reason=finish_reason)
    else:
        # Chain-of-Thought output not enabled, or no Chain-of-Thought content, use original logic
        final_content_str = "".join(full_content)
        response_data = format_openai_non_stream_response(final_content_str, model, response_id, reason=finish_reason)

    logger.info(f"NON-STREAM [ID: {request_id[:8]}]: Response aggregation complete.")

    # Log successful request (non-streaming response)
    # Calculate input tokens
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

    # Calculate complete response content (including Chain-of-Thought)
    full_response_for_monitoring = final_content_str if 'final_content_str' in locals() else "".join(full_content)
    full_reasoning_for_monitoring = "".join(reasoning_content) if reasoning_content else None

    monitoring_service.request_end(
        request_id,
        success=True,
        response_content=full_response_for_monitoring,
        reasoning_content=full_reasoning_for_monitoring,  # Add Chain-of-Thought content
        input_tokens=input_tokens,
        output_tokens=len(full_response_for_monitoring) // 4
    )

    # 🔧 Critical fix: Release tab request count
    if request_id in request_metadata:
        tab_id = request_metadata[request_id].get("tab_id")
        if tab_id:
            await release_tab_request(tab_id)
            logger.debug(f"NON-STREAM [ID: {request_id[:8]}]: Released request count for tab '{tab_id}'")

    return Response(content=json.dumps(response_data, ensure_ascii=False), media_type="application/json")

# --- WebSocket Endpoint ---
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """Handles WebSocket connections from Tampermonkey scripts (supports multiple tabs)."""
    global browser_ws, browser_connections, IS_REFRESHING_FOR_VERIFICATION
    await websocket.accept()

    # Wait for the first message (may contain tab ID)
    tab_id = "default"  # Default tab ID (backward compatible)
    first_message_handled = False

    try:
        # Set 3-second timeout to wait for potential tab ID
        init_message_str = await asyncio.wait_for(websocket.receive_text(), timeout=3.0)
        init_message = json.loads(init_message_str)

        # Check if it contains tab_id
        if "tab_id" in init_message:
            tab_id = init_message["tab_id"]
            first_message_handled = True
            logger.info(f"[WS_CONN] 📋 Received tab ID: {tab_id}")
        else:
            # Old version script, no tab_id sent, this message needs to be processed later
            logger.warning(f"[WS_CONN] ⚠️ No tab_id detected, using default value (possibly old version script)")
            # Stash this message, process later
            first_real_message = init_message_str
    except asyncio.TimeoutError:
        logger.warning(f"[WS_CONN] ⚠️ Waiting for tab_id timed out, using default value (possibly old version script)")
    except json.JSONDecodeError:
        logger.warning(f"[WS_CONN] ⚠️ Failed to parse initial message, using default tab_id")

    # Use lock to protect modifications to WebSocket connections
    async with browser_connections_lock:
        # Check if a connection with the same tab_id already exists
        if tab_id in browser_connections:
            logger.warning(f"[WS_CONN] Tab {tab_id} already has a connection, will be replaced by new connection")

        browser_connections[tab_id] = websocket
        # Record connection time
        tab_connection_times[tab_id] = time.time()

        # Compatibility: Set the first connection as browser_ws
        if not browser_ws or tab_id == "default":
            browser_ws = websocket

        # As long as a new connection is established, it means human verification process has ended (or never started)
        if IS_REFRESHING_FOR_VERIFICATION:
            logger.info("✅ New WebSocket connection established, human verification status automatically reset.")
            IS_REFRESHING_FOR_VERIFICATION = False

        # Calculate concurrency capacity
        concurrent_capacity = len(browser_connections) * 6
        logger.info("="*80)
        logger.info(f"✅ Tab '{tab_id}' successfully connected WebSocket")
        logger.info(f"📊 Current connection status:")
        logger.info(f"  - Active tabs: {len(browser_connections)}")
        logger.info(f"  - Theoretical maximum concurrency: {concurrent_capacity} requests (6 per tab)")
        logger.info(f"  - Unprocessed requests: {len(response_channels)}")

        # Concurrency limit hint
        if len(browser_connections) == 1:
            logger.warning(f"⚠️  Note: Single tab mode, browser HTTP/1.1 limits concurrency to 6 requests")
            logger.warning(f"💡 For higher concurrency, open additional LMArena tabs and run the Tampermonkey script")
            logger.warning(f"   - 2 tabs = 12 concurrency")
            logger.warning(f"   - 3 tabs = 18 concurrency")
        else:
            logger.info(f"✅ Multi-tab mode activated! Currently supports {concurrent_capacity} concurrent requests")

        logger.info("="*80)

    # Broadcast browser connection status to monitoring panel
    await monitoring_service.broadcast_to_monitors({
        "type": "browser_status",
        "connected": True
    })

    # Broadcast tab status update
    await monitoring_service.broadcast_to_monitors({
        "type": "tab_connection",
        "action": "connected",
        "tab_id": tab_id,
        "total_tabs": len(browser_connections),
        "total_capacity": len(browser_connections) * 6
    })

    # --- Enhancement: Process all pending requests (including pending_requests_queue and response_channels) ---
    if CONFIG.get("enable_auto_retry", False):
        # 1. First process requests in pending_requests_queue
        if not pending_requests_queue.empty():
            logger.info(f"Detected {pending_requests_queue.qsize()} staged requests, will automatically retry in background...")
            asyncio.create_task(process_pending_requests())

        # 2. Then process unfinished requests in response_channels (critical fix)
        if len(response_channels) > 0:
            logger.info(f"[REQUEST_RECOVERY] Detected {len(response_channels)} unfinished requests, preparing to recover...")

            # Get IDs of all unfinished requests
            pending_request_ids = list(response_channels.keys())

            for request_id in pending_request_ids:
                # Try to get request data from multiple sources
                request_data = None

                # Source 1: request_metadata (new storage)
                if request_id in request_metadata:
                    request_data = request_metadata[request_id]["openai_request"]
                    logger.info(f"[REQUEST_RECOVERY] Recovering request {request_id[:8]} from request_metadata")

                # Source 2: monitoring_service.active_requests (fallback)
                elif hasattr(monitoring_service, 'active_requests') and request_id in monitoring_service.active_requests:
                    active_req = monitoring_service.active_requests[request_id]
                    # Reconstruct OpenAI request format
                    request_data = {
                        "model": active_req.model,
                        "messages": active_req.request_messages if hasattr(active_req, 'request_messages') else [],
                        "stream": active_req.params.get("streaming", False) if hasattr(active_req, 'params') else False,
                        "temperature": active_req.params.get("temperature") if hasattr(active_req, 'params') else None,
                        "top_p": active_req.params.get("top_p") if hasattr(active_req, 'params') else None,
                        "max_tokens": active_req.params.get("max_tokens") if hasattr(active_req, 'params') else None,
                    }
                    logger.info(f"[REQUEST_RECOVERY] Recovering request {request_id[:8]} from monitoring_service")
                else:
                    logger.warning(f"[REQUEST_RECOVERY] ⚠️ Failed to recover request {request_id[:8]}: original data not found")
                    # Clean up this unrecoverable request
                    if request_id in response_channels:
                        await response_channels[request_id].put({"error": "Request data lost during reconnection"})
                        await response_channels[request_id].put("[DONE]")
                    continue

                # If request data was successfully obtained, add it to the retry queue
                if request_data:
                    # Create a new future to wait for retry result
                    future = asyncio.get_event_loop().create_future()

                    # Put the request into the pending queue
                    await pending_requests_queue.put({
                        "future": future,
                        "request_data": request_data,
                        "original_request_id": request_id  # Retain original request ID for tracking
                    })

                    logger.info(f"[REQUEST_RECOVERY] ✅ Request {request_id[:8]} added to retry queue")

            # Start recovery processing
            if not pending_requests_queue.empty():
                logger.info(f"[REQUEST_RECOVERY] Starting to process {pending_requests_queue.qsize()} recovered requests...")
                asyncio.create_task(process_pending_requests())
            else:
                logger.info(f"[REQUEST_RECOVERY] No recoverable requests")

    try:
        # If the first message was not handled (old version script), it needs to be processed first
        if not first_message_handled and 'first_real_message' in locals():
            message_str = first_real_message
            message = json.loads(message_str)

            request_id = message.get("request_id")
            data = message.get("data")

            if request_id and data is not None:
                if request_id in response_channels:
                    await response_channels[request_id].put(data)
                else:
                    logger.warning(f"[WS_MSG] Received response for unknown or closed request: {request_id}")

        while True:
            # Wait for and receive messages from Tampermonkey script
            message_str = await websocket.receive_text()
            message = json.loads(message_str)

            request_id = message.get("request_id")
            data = message.get("data")

            if not request_id or data is None:
                logger.warning(f"[WS_MSG] Received invalid message from browser: {message}")
                continue

            # Diagnostic: Log WebSocket messages
            if CONFIG.get("debug_stream_timing", False):
                import time as time_module
                current_time = time_module.time()
                data_preview = str(data)[:200] if data else "None"
                logger.debug(f"[WS_MSG] Time: {current_time:.3f}, Request ID: {request_id[:8]}, Data preview: {data_preview}...")

                # If string data, check if it contains multiple text blocks
                if isinstance(data, str) and 'a0:"' in data:
                    import re
                    text_pattern = re.compile(r'[ab]0:"((?:\\.|[^"\\])*)"')
                    matches = text_pattern.findall(data)
                    logger.debug(f"[WS_MSG] Single WebSocket message contains {len(matches)} text blocks (Problem: data is accumulated!)")
                    if len(matches) > 1:
                        logger.warning(f"⚠️ Detected streaming data accumulation! Single WebSocket message contains {len(matches)} text blocks")
                        logger.warning(f"   This indicates that the Tampermonkey script accumulated multiple response blocks before sending")

            # Put received data into the corresponding response channel
            if request_id in response_channels:
                await response_channels[request_id].put(data)
            else:
                logger.warning(f"[WS_MSG] Received response for unknown or closed request: {request_id}")

    except WebSocketDisconnect:
        logger.warning(f"❌ Tab '{tab_id}' disconnected.")
    except Exception as e:
        logger.error(f"[WS_ERROR] Error occurred during WebSocket processing for tab '{tab_id}': {e}", exc_info=True)
    finally:
        async with browser_connections_lock:
            # Remove disconnected tab connection
            if tab_id in browser_connections:
                del browser_connections[tab_id]
                logger.info(f"[WS_CONN] Tab '{tab_id}' removed")

            # Remove connection time record
            if tab_id in tab_connection_times:
                del tab_connection_times[tab_id]

            # Update browser_ws (backward compatible)
            if browser_connections:
                # If other connections remain, use the first one
                browser_ws = list(browser_connections.values())[0]
                logger.info(f"[WS_CONN] browser_ws updated to the first of the remaining {len(browser_connections)} connections")
            else:
                browser_ws = None
                logger.info(f"[WS_CONN] All tabs disconnected")

            # Calculate remaining concurrency capacity
            remaining_capacity = len(browser_connections) * 6
            logger.info(f"[WS_CONN] Remaining active tabs: {len(browser_connections)}")
            logger.info(f"[WS_CONN] Remaining concurrency capacity: {remaining_capacity} requests")
            logger.info(f"[WS_CONN] Unprocessed requests: {len(response_channels)}")

        # Broadcast browser disconnection status to monitoring panel
        await monitoring_service.broadcast_to_monitors({
            "type": "browser_status",
            "connected": len(browser_connections) > 0
        })

        # Broadcast tab status update
        await monitoring_service.broadcast_to_monitors({
            "type": "tab_connection",
            "action": "disconnected",
            "tab_id": tab_id,
            "total_tabs": len(browser_connections),
            "total_capacity": len(browser_connections) * 6
        })

        # If auto-retry is disabled, clean up channels as before
        if not CONFIG.get("enable_auto_retry", False):
            # Clean up all waiting response channels, in case requests are hung
            for queue in response_channels.values():
                await queue.put({"error": "Browser disconnected during operation"})
            response_channels.clear()
            logger.info("WebSocket connection cleaned up (auto-retry disabled).")
        else:
            logger.info("WebSocket connection closed (auto-retry enabled, requests will wait for reconnection).")

# --- OpenAI Compatible API Endpoint ---
@app.get("/v1/models")
async def get_models():
    """Provides an OpenAI compatible list of models - returns models configured in model_endpoint_map.json."""
    # Prioritize returning models from MODEL_ENDPOINT_MAP (models with configured sessions)
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
    # If MODEL_ENDPOINT_MAP is empty, fallback to models from models.json
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
            content={"error": "Model list is empty. Please configure 'model_endpoint_map.json' or 'models.json'."}
        )

@app.post("/internal/request_model_update")
async def request_model_update():
    """
    Receives requests from model_updater.py and sends WebSocket commands
    to the Tampermonkey script to send page source.
    """
    if not browser_ws:
        logger.warning("MODEL UPDATE: Received update request, but no browser connection.")
        raise HTTPException(status_code=503, detail="Browser client not connected.")

    try:
        logger.info("MODEL UPDATE: Received update request, sending command via WebSocket...")
        await browser_ws.send_text(json.dumps({"command": "send_page_source"}))
        logger.info("MODEL UPDATE: 'send_page_source' command successfully sent.")
        return JSONResponse({"status": "success", "message": "Request to send page source sent."})
    except Exception as e:
        logger.error(f"MODEL UPDATE: Error sending command: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to send command via WebSocket.")

@app.post("/internal/update_available_models")
async def update_available_models_endpoint(request: Request):
    """
    Receives page HTML from the Tampermonkey script, extracts and updates available_models.json.
    """
    html_content = await request.body()
    if not html_content:
        logger.warning("Model update request received no HTML content.")
        return JSONResponse(
            status_code=400,
            content={"status": "error", "message": "No HTML content received."}
        )

    logger.info("Received page content from Tampermonkey script, starting to extract available models...")
    new_models_list = extract_models_from_html(html_content.decode('utf-8'))

    if new_models_list:
        save_available_models(new_models_list)
        return JSONResponse({"status": "success", "message": "Available models file updated."})
    else:
        logger.error("Failed to extract model data from HTML provided by Tampermonkey script.")
        return JSONResponse(
            status_code=400,
            content={"status": "error", "message": "Could not extract model data from HTML."}
        )

@app.post("/v1/chat/completions")
async def chat_completions(request: Request):
    """
    Handles chat completion requests.
    Receives OpenAI formatted requests, converts them to LMArena format,
    sends them to the Tampermonkey script via WebSocket, then streams back the results.
    """
    global last_activity_time
    last_activity_time = datetime.now() # Update activity time
    logger.info(f"API request received, activity time updated to: {last_activity_time.strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        openai_req = await request.json()
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON request body")

    model_name = openai_req.get("model")

    # Prioritize getting model type from MODEL_ENDPOINT_MAP
    model_type = "text"  # Default type
    endpoint_mapping = MODEL_ENDPOINT_MAP.get(model_name)
    if endpoint_mapping:
        if isinstance(endpoint_mapping, dict) and "type" in endpoint_mapping:
            model_type = endpoint_mapping.get("type", "text")
        elif isinstance(endpoint_mapping, list) and endpoint_mapping:
            first_mapping = endpoint_mapping[0] if isinstance(endpoint_mapping[0], dict) else {}
            if "type" in first_mapping:
                model_type = first_mapping.get("type", "text")

    # Fallback to models.json
    model_info = MODEL_NAME_TO_ID_MAP.get(model_name, {})
    if not (endpoint_mapping and (isinstance(endpoint_mapping, dict) and "type" in endpoint_mapping or
            isinstance(endpoint_mapping, list) and endpoint_mapping and "type" in endpoint_mapping[0])):
        model_type = model_info.get("type", "text")

    # --- New: Logic based on model type ---
    if model_type == 'image':
        logger.info(f"Detected model '{model_name}' as type 'image', will be processed via main chat interface.")
        # For image models, we no longer call a separate processor, but reuse the main chat logic,
        # because _process_lmarena_stream can now handle image data.
        # This means image generation now natively supports streaming and non-streaming responses.
        pass # Continue to execute the general chat logic below
    # --- Image generation logic ends ---

    # If not an image model, execute normal text generation logic
    load_config()  # Load latest configuration in real-time, ensuring session ID and other info are up-to-date
    # --- API Key Verification ---
    api_key = CONFIG.get("api_key")
    if api_key:
        auth_header = request.headers.get('Authorization')
        if not auth_header or not auth_header.startswith('Bearer '):
            raise HTTPException(
                status_code=401,
                detail="API Key not provided. Please provide it in the Authorization header in 'Bearer YOUR_KEY' format."
            )

        provided_key = auth_header.split(' ')[1]
        if provided_key != api_key:
            raise HTTPException(
                status_code=401,
                detail="Provided API Key is incorrect."
            )

    # --- Enhanced Connection Check and Auto-retry Logic ---
    if not browser_ws:
        if CONFIG.get("enable_auto_retry", False):
            logger.warning("Tampermonkey script not connected, but auto-retry is enabled. Request will be staged.")

            # Create a future to wait for response
            future = asyncio.get_event_loop().create_future()

            # Put key request information into staging queue
            await pending_requests_queue.put({
                "future": future,
                "request_data": openai_req
            })

            logger.info(f"A new request has been placed in the staging queue. Current queue size: {pending_requests_queue.qsize()}")

            try:
                # Read timeout from configuration, default to 60 seconds
                timeout = CONFIG.get("retry_timeout_seconds", 120)

                # Wait for future to complete (i.e., after retry succeeds, response is set)
                return await asyncio.wait_for(future, timeout=timeout)
            except asyncio.TimeoutError:
                logger.warning(f"A staged request timed out after {timeout} seconds.")
                raise HTTPException(
                    status_code=503,
                    detail=f"Browser disconnected from server and failed to reconnect within {timeout} seconds. Request failed."
                )

        else: # If retry is not enabled, fail immediately
            raise HTTPException(
                status_code=503,
                detail="Tampermonkey client not connected. Please ensure LMArena page is open and script is active."
            )

    if IS_REFRESHING_FOR_VERIFICATION and not browser_ws:
        raise HTTPException(
            status_code=503,
            detail="Waiting for browser to refresh to complete human verification, please try again in a few seconds."
        )

    # --- Model and Session ID Mapping Logic ---
    session_id, message_id = None, None
    mode_override, battle_target_override = None, None

    if model_name and model_name in MODEL_ENDPOINT_MAP:
        mapping_entry = MODEL_ENDPOINT_MAP[model_name]
        selected_mapping = None

        if isinstance(mapping_entry, list) and mapping_entry:
            # Use thread-safe round-robin strategy to select mapping
            global MODEL_ROUND_ROBIN_INDEX, MODEL_ROUND_ROBIN_LOCK

            # Critical fix: Use lock to ensure atomic operations, avoid concurrent race conditions
            with MODEL_ROUND_ROBIN_LOCK:
                if model_name not in MODEL_ROUND_ROBIN_INDEX:
                    MODEL_ROUND_ROBIN_INDEX[model_name] = 0

                current_index = MODEL_ROUND_ROBIN_INDEX[model_name]
                selected_mapping = mapping_entry[current_index]

                # Diagnostic log: display concurrent round-robin status
                logger.info(f"[CONCURRENT_ROUND_ROBIN] Model '{model_name}' round-robin status:")
                logger.info(f"  - Total mappings: {len(mapping_entry)}")
                logger.info(f"  - Current selected index: {current_index}")
                logger.info(f"  - Selected mapping: #{current_index + 1}/{len(mapping_entry)}")
                logger.info(f"  - Session ID last 6 digits: ...{mapping_entry[current_index].get('session_id', 'N/A')[-6:]}")
                logger.info(f"  - Active request channels: {len(response_channels)} (concurrent requests)")

                # Update index, loop to next (completed within atomic operation)
                MODEL_ROUND_ROBIN_INDEX[model_name] = (current_index + 1) % len(mapping_entry)
                next_index = MODEL_ROUND_ROBIN_INDEX[model_name]
                logger.info(f"  - Next request will use index: {next_index}")

            logger.info(f"✅ For model '{model_name}', mapping #{current_index + 1}/{len(mapping_entry)} selected via round-robin (thread-safe)")
        elif isinstance(mapping_entry, dict):
            selected_mapping = mapping_entry
            logger.info(f"Found single endpoint mapping for model '{model_name}' (old format).")

        if selected_mapping:
            session_id = selected_mapping.get("session_id")
            message_id = selected_mapping.get("message_id")
            # Key: also get mode information
            mode_override = selected_mapping.get("mode") # May be None
            battle_target_override = selected_mapping.get("battle_target") # May be None
            log_msg = f"Will use Session ID: ...{session_id[-6:] if session_id else 'N/A'}"
            if mode_override:
                log_msg += f" (Mode: {mode_override}"
                if mode_override == 'battle':
                    log_msg += f", Target: {battle_target_override or 'A'}"
                log_msg += ")"
            logger.info(log_msg)

    # If session_id is still None after the above processing, enter global fallback logic
    if not session_id:
        if CONFIG.get("use_default_ids_if_mapping_not_found", True):
            session_id = CONFIG.get("session_id")
            message_id = CONFIG.get("message_id")
            # When using global IDs, do not set mode override, let it use global configuration
            mode_override, battle_target_override = None, None
            logger.info(f"Model '{model_name}' not found valid mapping, using global default Session ID according to configuration: ...{session_id[-6:] if session_id else 'N/A'}")
        else:
            logger.error(f"Model '{model_name}' not found valid mapping in 'model_endpoint_map.json', and fallback to default ID is disabled.")
            raise HTTPException(
                status_code=400,
                detail=f"Model '{model_name}' does not have a configured independent session ID. Please add a valid mapping in 'model_endpoint_map.json' or enable 'use_default_ids_if_mapping_not_found' in 'config.jsonc'."
            )

    # --- Verify final determined session information ---
    if not session_id or not message_id or "YOUR_" in session_id or "YOUR_" in message_id:
        raise HTTPException(
            status_code=400,
            detail="Final determined session ID or message ID is invalid. Please check configurations in 'model_endpoint_map.json' and 'config.jsonc', or run `id_updater.py` to update default values."
        )

    if not model_name or model_name not in MODEL_NAME_TO_ID_MAP:
        logger.warning(f"Requested model '{model_name}' is not in models.json, using default model ID.")

    request_id = str(uuid.uuid4())
    response_channels[request_id] = asyncio.Queue()

    # New: Save request metadata for disconnection recovery
    request_metadata[request_id] = {
        "openai_request": openai_req.copy(),  # Save complete OpenAI request
        "model_name": model_name,
        "session_id": session_id,
        "message_id": message_id,
        "mode_override": mode_override,
        "battle_target_override": battle_target_override,
        "created_at": datetime.now().isoformat()
    }

    logger.info(f"API CALL [ID: {request_id[:8]}]: Response channel created.")
    logger.debug(f"API CALL [ID: {request_id[:8]}]: Request metadata saved to request_metadata")

    # Log request start to monitoring system (add detailed information)
    # Estimate input token count
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
        messages=openai_req.get("messages", []),  # Add message content
        params={  # Add request parameters
            "temperature": openai_req.get("temperature"),
            "top_p": openai_req.get("top_p"),
            "max_tokens": openai_req.get("max_tokens"),
            "streaming": openai_req.get("stream", False)
        }
    )

    # Broadcast request start event to monitoring panel
    await monitoring_service.broadcast_to_monitors({
        "type": "request_start",
        "request_id": request_id,
        "model": model_name,
        "timestamp": time.time()
    })

    # --- New: Image hash calculation helper function ---
    def calculate_image_hash(base64_data: str) -> str:
        """Calculates SHA256 hash of image content (for cache key)"""
        import hashlib
        # Remove data URI prefix (if exists)
        if ',' in base64_data:
            _, data_only = base64_data.split(',', 1)
        else:
            data_only = base64_data
        # Calculate hash (using base64 string, avoids decoding overhead)
        return hashlib.sha256(data_only.encode('utf-8')).hexdigest()

    try:
        # --- Attachment Preprocessing (including file bed upload) ---
        # Before communicating with the browser, process all attachments. If it fails, return an error immediately.
        # Process base64 images for all roles (user, assistant, system) in the request -> convert to image bed links and send to LMArena
        if CONFIG.get("file_bed_enabled"):
            # 1. Filter out enabled and not temporarily disabled endpoints (check recovery time)
            all_endpoints = CONFIG.get("file_bed_endpoints", [])
            current_time = time.time()

            # Automatically recover timed-out endpoints
            endpoints_to_recover = []
            for endpoint_name, disable_time in list(DISABLED_ENDPOINTS.items()):
                if current_time - disable_time > FILEBED_RECOVERY_TIME:
                    endpoints_to_recover.append(endpoint_name)

            for endpoint_name in endpoints_to_recover:
                del DISABLED_ENDPOINTS[endpoint_name]
                logger.info(f"[FILEBED] Image bed endpoint '{endpoint_name}' automatically recovered")

            active_endpoints = [ep for ep in all_endpoints if ep.get("enabled") and ep.get("name") not in DISABLED_ENDPOINTS]

            if not active_endpoints:
                logger.warning("File bed is enabled, but no active endpoints are available. Upload will be skipped.")
            else:
                # --- New: Image bed selection strategy ---
                global ROUND_ROBIN_INDEX
                strategy = CONFIG.get("file_bed_selection_strategy", "random")

                messages_to_process = openai_req.get("messages", [])
                logger.info(f"📋 File bed enabled, found {len(active_endpoints)} active endpoints (strategy: {strategy})")
                logger.info(f"📋 Preparing to process images in {len(messages_to_process)} messages")

                # Count images per role
                role_image_count = {}

                for msg_index, message in enumerate(messages_to_process):
                    role = message.get("role", "unknown")
                    content = message.get("content")

                    logger.debug(f"  Checking message #{msg_index + 1} (Role: {role})")

                    # Process Markdown images in string content (common in assistant role)
                    if isinstance(content, str):
                        # Use regex to match Markdown formatted images (including base64 and http URLs)
                        import re
                        # Match ![...](url) format, including base64 and http URLs
                        markdown_image_pattern = r'!\[([^\]]*)\]\(([^)]+)\)'
                        markdown_matches = re.findall(markdown_image_pattern, content)

                        # Filter out images that need to be uploaded (only process base64 format)
                        base64_matches = [(alt, url) for alt, url in markdown_matches if url.startswith('data:')]

                        if base64_matches:
                            logger.info(f"  📷 Found {len(base64_matches)} Markdown formatted base64 images in {role} role string content that need uploading")
                            markdown_matches = base64_matches  # Only process base64 images

                            for match_index, (alt_text, base64_url) in enumerate(markdown_matches):
                                role_image_count[role] = role_image_count.get(role, 0) + 1

                                # New: Check image bed URL cache
                                image_hash = calculate_image_hash(base64_url)
                                current_time = time.time()

                                # Clean up expired cache
                                if image_hash in FILEBED_URL_CACHE:
                                    cached_url, cache_time = FILEBED_URL_CACHE[image_hash]
                                    if current_time - cache_time < FILEBED_URL_CACHE_TTL:
                                        # Cache hit and not expired
                                        old_markdown = f"![{alt_text}]({base64_url})"
                                        new_markdown = f"![{alt_text}]({cached_url})"
                                        content = content.replace(old_markdown, new_markdown)
                                        message["content"] = content

                                        show_full_urls = CONFIG.get("debug_show_full_urls", False)
                                        url_display = cached_url if show_full_urls else cached_url[:CONFIG.get("url_display_length", 200)]
                                        logger.info(f"    ⚡ {role} role string image using cached URL: {url_display}{'...' if not show_full_urls and len(cached_url) > CONFIG.get('url_display_length', 200) else ''} (remaining: {FILEBED_URL_CACHE_TTL - (current_time - cache_time):.0f} seconds)")
                                        continue  # Skip upload
                                    else:
                                        # Cache expired, delete
                                        del FILEBED_URL_CACHE[image_hash]
                                        logger.debug(f"    🗑️ Cleaning expired cache: {image_hash[:8]}...")

                                # 2. Select and sort endpoints based on strategy
                                if strategy == "failover":
                                    # Failover: fixed use of current index image bed, only switch after failure
                                    start_index = ROUND_ROBIN_INDEX % len(active_endpoints)
                                    endpoints_to_try = active_endpoints[start_index:] + active_endpoints[:start_index]
                                elif strategy == "round_robin":
                                    # Round-robin: switch to next image bed each time
                                    start_index = ROUND_ROBIN_INDEX % len(active_endpoints)
                                    endpoints_to_try = active_endpoints[start_index:] + active_endpoints[:start_index]
                                    ROUND_ROBIN_INDEX += 1 # Only increment index immediately in round-robin mode
                                else: # Default to random
                                    endpoints_to_try = random.sample(active_endpoints, len(active_endpoints))

                                upload_successful = False
                                last_error = "No available image bed endpoints."

                                for i, endpoint in enumerate(endpoints_to_try):
                                    endpoint_name = endpoint.get("name", "Unknown")

                                    # Skip if temporarily disabled
                                    if endpoint_name in DISABLED_ENDPOINTS:
                                        continue

                                    logger.info(f"    📤 Uploading {match_index + 1}th base64 image in {role} role string, attempting with '{endpoint_name}' endpoint...")

                                    final_url, error_message = await upload_to_file_bed(
                                        file_name=f"{role}_string_{msg_index}_{match_index}_{uuid.uuid4()}.png",
                                        file_data=base64_url,
                                        endpoint=endpoint
                                    )

                                    if not error_message:
                                        # Replace base64 in original content with uploaded URL
                                        old_markdown = f"![{alt_text}]({base64_url})"
                                        new_markdown = f"![{alt_text}]({final_url})"
                                        content = content.replace(old_markdown, new_markdown)
                                        message["content"] = content  # Update message content

                                        # New: Store upload result in cache
                                        FILEBED_URL_CACHE[image_hash] = (final_url, time.time())
                                        # Limit cache size
                                        if len(FILEBED_URL_CACHE) > FILEBED_URL_CACHE_MAX_SIZE:
                                            # Delete oldest cache entries
                                            sorted_items = sorted(FILEBED_URL_CACHE.items(), key=lambda x: x[1][1])
                                            for old_hash, _ in sorted_items[:FILEBED_URL_CACHE_MAX_SIZE // 4]:
                                                del FILEBED_URL_CACHE[old_hash]
                                            logger.debug(f"    🧹 Cache full, cleaned {FILEBED_URL_CACHE_MAX_SIZE // 4} old entries")

                                        # Improve URL display
                                        show_full_urls = CONFIG.get("debug_show_full_urls", False)
                                        url_display = final_url if show_full_urls else final_url[:CONFIG.get("url_display_length", 200)]
                                        logger.info(f"    ✅ Image in {role} role string successfully uploaded to '{endpoint_name}': {url_display}{'...' if not show_full_urls and len(final_url) > CONFIG.get('url_display_length', 200) else ''} (cached)")
                                        upload_successful = True
                                        break
                                    else:
                                        logger.warning(f"    ⚠️ Endpoint '{endpoint_name}' upload failed: {error_message}. Temporarily disabling and trying next...")
                                        DISABLED_ENDPOINTS[endpoint_name] = time.time()  # Record disable time
                                        logger.info(f"[FILEBED] {endpoint_name} disabled, current disabled count: {len(DISABLED_ENDPOINTS)}")
                                        last_error = error_message
                                        # If in failover mode, and current fixed image bed failed, update index
                                        if strategy == "failover" and i == 0:
                                            ROUND_ROBIN_INDEX += 1
                                            logger.info(f"    🔄 [Failover] Default image bed '{endpoint_name}' failed, switching to next.")

                                if not upload_successful:
                                    logger.error(f"    ❌ Image in {role} role string failed to upload")
                                    raise IOError(f"All active image bed endpoints failed to upload. Last error: {last_error}")

                    # Process list content (original logic)
                    elif isinstance(content, list):
                        image_count_in_msg = 0
                        for part_index, part in enumerate(content):
                            if part.get("type") == "image_url":
                                url_content = part.get("image_url", {}).get("url")

                                if url_content and url_content.startswith("data:"):
                                    image_count_in_msg += 1
                                    role_image_count[role] = role_image_count.get(role, 0) + 1

                                    # New: Check image bed URL cache
                                    image_hash = calculate_image_hash(url_content)
                                    current_time = time.time()

                                    # Clean up expired cache
                                    if image_hash in FILEBED_URL_CACHE:
                                        cached_url, cache_time = FILEBED_URL_CACHE[image_hash]
                                        if current_time - cache_time < FILEBED_URL_CACHE_TTL:
                                            # Cache hit and not expired
                                            part["image_url"]["url"] = cached_url

                                            show_full_urls = CONFIG.get("debug_show_full_urls", False)
                                            url_display = cached_url if show_full_urls else cached_url[:CONFIG.get("url_display_length", 200)]
                                            logger.info(f"    ⚡ {role} role list image using cached URL: {url_display}{'...' if not show_full_urls and len(cached_url) > CONFIG.get('url_display_length', 200) else ''} (remaining: {FILEBED_URL_CACHE_TTL - (current_time - cache_time):.0f} seconds)")
                                            continue  # Skip upload
                                        else:
                                            # Cache expired, delete
                                            del FILEBED_URL_CACHE[image_hash]
                                            logger.debug(f"    🗑️ Cleaning expired cache: {image_hash[:8]}...")

                                    # 2. Select and sort endpoints based on strategy
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
                                    last_error = "No available image bed endpoints."

                                    for i, endpoint in enumerate(endpoints_to_try):
                                        endpoint_name = endpoint.get("name", "Unknown")

                                        # Skip if temporarily disabled
                                        if endpoint_name in DISABLED_ENDPOINTS:
                                            continue

                                        logger.info(f"    📤 Uploading {image_count_in_msg}th base64 image in {role} role list, attempting with '{endpoint_name}' endpoint...")

                                        final_url, error_message = await upload_to_file_bed(
                                            file_name=f"{role}_list_{msg_index}_{part_index}_{uuid.uuid4()}.png",
                                            file_data=url_content,
                                            endpoint=endpoint
                                        )

                                        if not error_message:
                                            part["image_url"]["url"] = final_url

                                            # New: Store upload result in cache
                                            FILEBED_URL_CACHE[image_hash] = (final_url, time.time())
                                            # Limit cache size
                                            if len(FILEBED_URL_CACHE) > FILEBED_URL_CACHE_MAX_SIZE:
                                                # Delete oldest cache entries
                                                sorted_items = sorted(FILEBED_URL_CACHE.items(), key=lambda x: x[1][1])
                                                for old_hash, _ in sorted_items[:FILEBED_URL_CACHE_MAX_SIZE // 4]:
                                                    del FILEBED_URL_CACHE[old_hash]
                                                logger.debug(f"    🧹 Cache full, cleaned {FILEBED_URL_CACHE_MAX_SIZE // 4} old entries")

                                            # Improve URL display
                                            show_full_urls = CONFIG.get("debug_show_full_urls", False)
                                            url_display = final_url if show_full_urls else final_url[:CONFIG.get("url_display_length", 200)]
                                            logger.info(f"    ✅ Image in {role} role list successfully uploaded to '{endpoint_name}': {url_display}{'...' if not show_full_urls and len(final_url) > CONFIG.get('url_display_length', 200) else ''} (cached)")
                                            upload_successful = True
                                            break
                                        else:
                                            logger.warning(f"    ⚠️ Endpoint '{endpoint_name}' upload failed: {error_message}. Temporarily disabling and trying next...")
                                            DISABLED_ENDPOINTS[endpoint_name] = time.time()  # Record disable time
                                            logger.info(f"[FILEBED] {endpoint_name} disabled, current disabled count: {len(DISABLED_ENDPOINTS)}")
                                            last_error = error_message
                                            if strategy == "failover" and i == 0:
                                                ROUND_ROBIN_INDEX += 1
                                                logger.info(f"    🔄 [Failover] Default image bed '{endpoint_name}' failed, switching to next.")

                                    if not upload_successful:
                                        logger.error(f"    ❌ Image in {role} role list failed to upload")
                                        raise IOError(f"All active image bed endpoints failed to upload. Last error: {last_error}")

                # Output statistics
                if role_image_count:
                    logger.info(f"✅ Image preprocessing complete. Image statistics per role: {role_image_count}")
                else:
                    logger.info("✅ Image preprocessing complete (no base64 images found that needed uploading)")

        # 1. Convert request (now without attachments that needed uploading)
        lmarena_payload = await convert_openai_to_lmarena_payload(
            openai_req,
            session_id,
            message_id,
            mode_override=mode_override,
            battle_target_override=battle_target_override
        )

        # Key addition: if model is image type, explicitly indicate to Tampermonkey script
        if model_type == 'image':
            lmarena_payload['is_image_request'] = True

        # 2. Wrap into message to send to browser
        message_to_browser = {
            "request_id": request_id,
            "payload": lmarena_payload
        }


        # 3. Select best tab and send via WebSocket (load balancing)
        selected_tab_id, selected_ws = await select_best_tab_for_request()

        # Save tab ID to request metadata
        request_metadata[request_id]["tab_id"] = selected_tab_id

        logger.info(f"API CALL [ID: {request_id[:8]}]: Sending request via tab '{selected_tab_id}'")
        await selected_ws.send_text(json.dumps(message_to_browser))

        # 4. Determine return type based on stream parameter
        is_stream = openai_req.get("stream", False)

        if is_stream:
            # Return streaming response (optimized buffer settings)
            response = StreamingResponse(
                stream_generator(request_id, model_name or "default_model"),
                media_type="text/event-stream",
                headers={
                    'Cache-Control': 'no-cache',
                    'Connection': 'keep-alive',
                    'X-Accel-Buffering': 'no',  # Disable nginx buffering
                    'Transfer-Encoding': 'chunked'  # Explicitly use chunked transfer
                }
            )
            # Disable FastAPI's response buffering
            response.headers['X-Content-Type-Options'] = 'nosniff'
            return response
        else:
            # Return non-streaming response
            result = await non_stream_response(request_id, model_name or "default_model")
            # Log successful request (already handled inside non_stream_response)
            await monitoring_service.broadcast_to_monitors({
                "type": "request_end",
                "request_id": request_id,
                "success": True
            })
            return result
    except (ValueError, IOError) as e:
        # Catch attachment processing errors
        logger.error(f"API CALL [ID: {request_id[:8]}]: Attachment preprocessing failed: {e}")
        # Log failed request
        monitoring_service.request_end(request_id, success=False, error=str(e))
        await monitoring_service.broadcast_to_monitors({
            "type": "request_end",
            "request_id": request_id,
            "success": False
        })

        # 🔧 Critical fix: Release tab request count
        if request_id in request_metadata:
            tab_id = request_metadata[request_id].get("tab_id")
            if tab_id:
                await release_tab_request(tab_id)
                logger.debug(f"API CALL [ID: {request_id[:8]}]: Released request count for tab '{tab_id}' during error handling")

        if request_id in response_channels:
            del response_channels[request_id]
        # Clean up metadata
        if request_id in request_metadata:
            del request_metadata[request_id]
            logger.debug(f"API CALL [ID: {request_id[:8]}]: Request metadata cleaned up")
        # Return a correctly formatted JSON error response
        return JSONResponse(
            status_code=500,
            content={"error": {"message": f"[LMArena Bridge Error] Attachment processing failed: {e}", "type": "attachment_error"}}
        )
    except Exception as e:
        # Catch all other errors
        # Log failed request
        monitoring_service.request_end(request_id, success=False, error=str(e))
        await monitoring_service.broadcast_to_monitors({
            "type": "request_end",
            "request_id": request_id,
            "success": False
        })

        # 🔧 Critical fix: Release tab request count
        if request_id in request_metadata:
            tab_id = request_metadata[request_id].get("tab_id")
            if tab_id:
                await release_tab_request(tab_id)
                logger.debug(f"API CALL [ID: {request_id[:8]}]: Released request count for tab '{tab_id}' during error handling")

        if request_id in response_channels:
            del response_channels[request_id]
        # Clean up metadata
        if request_id in request_metadata:
            del request_metadata[request_id]
            logger.debug(f"API CALL [ID: {request_id[:8]}]: Request metadata cleaned up")
        logger.error(f"API CALL [ID: {request_id[:8]}]: Fatal error occurred while processing request: {e}", exc_info=True)
        # Ensure a correctly formatted JSON is also returned
        return JSONResponse(
            status_code=500,
            content={"error": {"message": str(e), "type": "internal_server_error"}}
        )

# --- Internal Communication Endpoints ---
@app.post("/internal/start_id_capture")
async def start_id_capture():
    """
    Receives notifications from id_updater.py and sends WebSocket commands
    to activate ID capture mode in the Tampermonkey script.
    """
    if not browser_ws:
        logger.warning("ID CAPTURE: Received activation request, but no browser connection.")
        raise HTTPException(status_code=503, detail="Browser client not connected.")

    try:
        logger.info("ID CAPTURE: Received activation request, sending command via WebSocket...")
        await browser_ws.send_text(json.dumps({"command": "activate_id_capture"}))
        logger.info("ID CAPTURE: Activation command successfully sent.")
        return JSONResponse({"status": "success", "message": "Activation command sent."})
    except Exception as e:
        logger.error(f"ID CAPTURE: Error sending activation command: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to send command via WebSocket.")
# --- Monitoring Related API Endpoints ---
@app.websocket("/ws/monitor")
async def monitor_websocket(websocket: WebSocket):
    """WebSocket connection for monitoring panel"""
    await websocket.accept()
    monitoring_service.add_monitor_client(websocket)

    try:
        # Send initial data
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
            # Keep connection alive
            await websocket.receive_text()

    except WebSocketDisconnect:
        monitoring_service.remove_monitor_client(websocket)

@app.get("/monitor", response_class=HTMLResponse)
async def monitor_dashboard():
    """Returns the monitoring panel HTML page"""
    try:
        with open('monitor.html', 'r', encoding='utf-8') as f:
            html_content = f.read()
        return HTMLResponse(content=html_content)
    except FileNotFoundError:
        return HTMLResponse(
            content="<h1>Monitoring panel file not found</h1><p>Please ensure monitor.html file is in the correct location.</p>",
            status_code=404
        )

@app.get("/api/monitor/stats")
async def get_monitor_stats():
    """Gets monitoring statistics"""
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
    """Gets list of active requests"""
    return monitoring_service.get_active_requests()

@app.get("/api/monitor/logs/requests")
async def get_request_logs(limit: int = 50):
    """Gets request logs"""
    return monitoring_service.log_manager.read_recent_logs("requests", limit)

@app.get("/api/monitor/logs/errors")
async def get_error_logs(limit: int = 30):
    """Gets error logs"""
    return monitoring_service.log_manager.read_recent_logs("errors", limit)

@app.get("/api/monitor/recent")
async def get_recent_data():
    """Gets recent requests and errors"""
    return {
        "recent_requests": monitoring_service.get_recent_requests(50),
        "recent_errors": monitoring_service.get_recent_errors(30)
    }

@app.get("/api/monitor/performance")
async def get_performance_metrics():
    """Gets performance metrics"""
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
    """Gets tab connection status"""
    async with browser_connections_lock:
        tabs_info = []
        current_time = time.time()

        for tab_id, ws in browser_connections.items():
            # Calculate connection duration for this tab
            connection_start = tab_connection_times.get(tab_id, current_time)
            connected_duration = current_time - connection_start

            # Get request load for this tab
            load = tab_request_counts.get(tab_id, 0)

            tabs_info.append({
                "tab_id": tab_id,
                "connected": ws.client_state.name == 'CONNECTED' if ws else False,
                "active_requests": load,
                "max_concurrent": 6,  # Browser HTTP/1.1 limit
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

# New: API endpoint to get request details
@app.get("/api/request/{request_id}")
async def get_request_details(request_id: str):
    """Gets detailed information for a specific request"""
    details = monitoring_service.get_request_details(request_id)
    if details:
        return details
    else:
        raise HTTPException(status_code=404, detail="Request details not found")

# New: Endpoint to download log files
@app.get("/api/logs/download")
async def download_logs(log_type: str = "requests"):
    """Downloads log files"""
    from fastapi.responses import FileResponse

    if log_type == "requests":
        log_path = MonitorConfig.LOG_DIR / MonitorConfig.REQUEST_LOG_FILE
    elif log_type == "errors":
        log_path = MonitorConfig.LOG_DIR / MonitorConfig.ERROR_LOG_FILE
    else:
        raise HTTPException(status_code=400, detail="Invalid log type")

    if not log_path.exists():
        raise HTTPException(status_code=404, detail="Log file does not exist")

    return FileResponse(
        path=str(log_path),
        filename=f"{log_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl",
        media_type="application/json"
    )

# New: Image library API endpoint
@app.get("/api/images/list")
async def get_image_list():
    """Gets a list of images in the downloaded_images directory (including subfolders)"""

    images = []
    image_dir = IMAGE_SAVE_DIR

    if image_dir.exists():
        # Iterate through main directory and all subdirectories
        for item in image_dir.iterdir():
            # Process files in main directory (for compatibility with old files)
            if item.is_file() and item.suffix.lower() in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp']:
                stat = item.stat()
                images.append({
                    "filename": item.name,
                    "size": stat.st_size,
                    "modified": stat.st_mtime,
                    "url": f"/api/images/{item.name}",
                    "folder": None  # Main directory file
                })
            # Process date folders
            elif item.is_dir() and item.name.isdigit() and len(item.name) == 8:  # YYYYMMDD format
                for file_path in item.iterdir():
                    if file_path.is_file() and file_path.suffix.lower() in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp']:
                        stat = file_path.stat()
                        images.append({
                            "filename": file_path.name,
                            "size": stat.st_size,
                            "modified": stat.st_mtime,
                            "url": f"/api/images/{item.name}/{file_path.name}",
                            "folder": item.name  # Date folder name
                        })

    # Sort by modification time in descending order (newest first)
    images.sort(key=lambda x: x['modified'], reverse=True)

    return {
        "total": len(images),
        "images": images
    }

@app.get("/api/images/{filename:path}")
async def get_image(filename: str):
    """Gets a single image file (supports subfolders)"""
    from fastapi.responses import FileResponse

    # Supports "filename" or "folder/filename" format
    file_path = IMAGE_SAVE_DIR / filename

    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail="Image not found")

    # Get correct media type
    content_type = mimetypes.guess_type(str(file_path))[0] or 'application/octet-stream'

    return FileResponse(
        path=str(file_path),
        media_type=content_type,
        filename=filename
    )

# --- New: Background task to process staged requests ---
async def process_pending_requests():
    """Processes all requests in the staging queue in the background."""
    while not pending_requests_queue.empty():
        pending_item = await pending_requests_queue.get()
        future = pending_item["future"]
        request_data = pending_item["request_data"]
        original_request_id = pending_item.get("original_request_id")  # May be None

        if original_request_id:
            logger.info(f"Recovering request {original_request_id[:8]}...")
        else:
            logger.info("Retrying a staged request...")

        try:
            # Re-call the core logic of chat_completions
            response = await handle_single_completion(request_data)

            # Set the successful result to the future, to wake up the waiting client
            future.set_result(response)

            if original_request_id:
                logger.info(f"✅ Request {original_request_id[:8]} successfully recovered and returned response.")
                # Clean up original request's metadata and channel
                if original_request_id in response_channels:
                    del response_channels[original_request_id]
                if original_request_id in request_metadata:
                    del request_metadata[original_request_id]
            else:
                logger.info("✅ A staged request successfully retried and returned response.")

        except Exception as e:
            logger.error(f"Error retrying staged request: {e}", exc_info=True)
            # Set the error to the future, so the client knows the request failed
            future.set_exception(e)

            # Clean up data for failed request
            if original_request_id:
                if original_request_id in response_channels:
                    del response_channels[original_request_id]
                if original_request_id in request_metadata:
                    del request_metadata[original_request_id]

        # Add a short delay to avoid sending too many requests simultaneously
        await asyncio.sleep(1)

async def handle_single_completion(openai_req: dict):
    """
    Core logic for processing a single chat completion request, reusable by main endpoint and retry tasks.
    """
    # This function contains most of the logic from chat_completions, except for connection checks and staging parts
    model_name = openai_req.get("model")

    # ... (Copy and paste model/session ID logic from chat_completions) ...
    # --- Model and Session ID Mapping Logic ---
    session_id, message_id = None, None
    mode_override, battle_target_override = None, None

    if model_name and model_name in MODEL_ENDPOINT_MAP:
        mapping_entry = MODEL_ENDPOINT_MAP[model_name]
        selected_mapping = None

        if isinstance(mapping_entry, list) and mapping_entry:
            # Use thread-safe round-robin strategy to select mapping (consistent with main function)
            global MODEL_ROUND_ROBIN_INDEX, MODEL_ROUND_ROBIN_LOCK

            # Critical fix: Use lock to ensure atomic operations
            with MODEL_ROUND_ROBIN_LOCK:
                if model_name not in MODEL_ROUND_ROBIN_INDEX:
                    MODEL_ROUND_ROBIN_INDEX[model_name] = 0

                current_index = MODEL_ROUND_ROBIN_INDEX[model_name]
                selected_mapping = mapping_entry[current_index]

                # Diagnostic log: round-robin status during retry recovery
                logger.info(f"[RETRY_ROUND_ROBIN] Model '{model_name}' retry request round-robin:")
                logger.info(f"  - Selected index: {current_index}/{len(mapping_entry)}")
                logger.info(f"  - Session ID last 6 digits: ...{mapping_entry[current_index].get('session_id', 'N/A')[-6:]}")

                # Update index, loop to next (completed within atomic operation)
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
            raise ValueError(f"Model '{model_name}' not found valid mapping, and fallback is disabled.")

    if not session_id or not message_id or "YOUR_" in session_id or "YOUR_" in message_id:
        raise ValueError("Final determined session ID or message ID is invalid.")

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
                        # ... (file bed upload logic) ...
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
            # Optimized streaming response configuration
            response = StreamingResponse(
                stream_generator(request_id, model_name or "default_model"),
                media_type="text/event-stream",
                headers={
                    'Cache-Control': 'no-cache',
                    'Connection': 'keep-alive',
                    'X-Accel-Buffering': 'no',  # Disable nginx buffering
                    'Transfer-Encoding': 'chunked'  # Explicitly use chunked transfer
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

        # 🔧 Critical fix: Release tab request count
        if request_id in request_metadata:
            tab_id = request_metadata[request_id].get("tab_id")
            if tab_id:
                await release_tab_request(tab_id)
                logger.debug(f"Tab '{tab_id}' request count released during retry function error handling")

        if request_id in response_channels:
            del response_channels[request_id]
        # Clean up metadata
        if request_id in request_metadata:
            del request_metadata[request_id]
            logger.debug(f"Request {request_id[:8]} failed, metadata cleaned up")

        raise e

# --- New: Image download helper function ---
async def _download_image_data_with_retry(url: str) -> Tuple[Optional[bytes], Optional[str]]:
    """Optimized asynchronous image downloader with retry and concurrency control"""
    global aiohttp_session, DOWNLOAD_SEMAPHORE

    if not DOWNLOAD_SEMAPHORE:
        DOWNLOAD_SEMAPHORE = Semaphore(MAX_CONCURRENT_DOWNLOADS)

    last_error = None
    max_retries = CONFIG.get("download_timeout", {}).get("max_retries", 2)
    retry_delays = [1, 2]  # Reduce retry delay

    # 🔍 Diagnostic log: concurrency control status
    semaphore_available = DOWNLOAD_SEMAPHORE._value if DOWNLOAD_SEMAPHORE else 0
    logger.info(f"[DOWNLOAD_DEBUG] Preparing to download image")
    logger.info(f"  - Available download slots: {semaphore_available}/{MAX_CONCURRENT_DOWNLOADS}")
    logger.info(f"  - Active downloads: {MAX_CONCURRENT_DOWNLOADS - semaphore_available}")
    logger.info(f"  - Max retries: {max_retries}")
    logger.info(f"  - URL first 100 characters: {url[:100]}...")

    # 🔧 Download delay mechanism (avoids TCP port exhaustion)
    delay_config = CONFIG.get("download_delay", {})
    if delay_config.get("enabled", False):
        delay_seconds = delay_config.get("delay_seconds", 0.5)
        logger.info(f"[DOWNLOAD_DEBUG] ⏱️ Delaying {delay_seconds} seconds before starting (to avoid concurrency conflicts)")
        await asyncio.sleep(delay_seconds)

    # Record time waiting for semaphore
    import time as time_module
    wait_start = time_module.time()

    # Use semaphore to control concurrency
    async with DOWNLOAD_SEMAPHORE:
        wait_time = time_module.time() - wait_start
        if wait_time > 1:
            logger.warning(f"[DOWNLOAD_DEBUG] ⚠️ Waited {wait_time:.2f} seconds for download slot (concurrency blocked!)")
        for retry_count in range(max_retries):
            try:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
                    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
                    'Referer': 'https://lmarena.ai/'
                }

                if not aiohttp_session:
                    # 🔧 Create emergency session (using same SSL configuration)
                    import ssl
                    ssl_context = ssl.create_default_context()
                    ssl_context.check_hostname = False
                    ssl_context.verify_mode = ssl.CERT_NONE
                    connector = aiohttp.TCPConnector(ssl=ssl_context, limit=100, limit_per_host=30)
                    aiohttp_session = aiohttp.ClientSession(connector=connector)
                    logger.warning("[DOWNLOAD_DEBUG] Created emergency aiohttp session (using custom SSL context)")

                # Optimized timeout settings (read from configuration)
                timeout_config = CONFIG.get("download_timeout", {})
                timeout = aiohttp.ClientTimeout(
                    total=timeout_config.get("total", 30),
                    connect=timeout_config.get("connect", 5),
                    sock_read=timeout_config.get("sock_read", 10)
                )

                # 🔍 Diagnostic log: timeout configuration
                logger.info(f"[DOWNLOAD_DEBUG] Retry #{retry_count + 1}/{max_retries}")
                logger.info(f"  - Connect timeout: {timeout_config.get('connect', 5)} seconds")
                logger.info(f"  - Read timeout: {timeout_config.get('sock_read', 10)} seconds")
                logger.info(f"  - Total timeout: {timeout_config.get('total', 30)} seconds")

                # Add performance log
                import time as time_module
                start_time = time_module.time()

                # 🔍 Diagnostic log: connection start
                logger.info(f"[DOWNLOAD_DEBUG] Starting to establish connection...")

                async with aiohttp_session.get(
                    url,
                    timeout=timeout,
                    headers=headers,
                    allow_redirects=True
                ) as response:
                    connect_time = time_module.time() - start_time
                    logger.info(f"[DOWNLOAD_DEBUG] Connection established successfully, time taken: {connect_time:.2f} seconds")

                    if response.status == 200:
                        logger.info(f"[DOWNLOAD_DEBUG] HTTP 200 OK, starting to read data...")
                        read_start = time_module.time()
                        data = await response.read()
                        read_time = time_module.time() - read_start
                        download_time = time_module.time() - start_time

                        # 🔍 Detailed performance analysis
                        logger.info(f"[DOWNLOAD_DEBUG] Download complete")
                        logger.info(f"  - Connection time: {connect_time:.2f} seconds")
                        logger.info(f"  - Read time: {read_time:.2f} seconds")
                        logger.info(f"  - Total time: {download_time:.2f} seconds")
                        logger.info(f"  - Data size: {len(data) / 1024:.1f}KB")
                        logger.info(f"  - Download speed: {(len(data) / 1024) / download_time:.1f}KB/s")

                        # Log slow downloads
                        slow_threshold = CONFIG.get("performance_monitoring", {}).get("slow_threshold_seconds", 10)
                        if download_time > slow_threshold:
                            logger.warning(f"[DOWNLOAD] ⚠️ Download took a long time: {download_time:.2f} seconds (threshold: {slow_threshold} seconds)")

                        return data, None
                    else:
                        last_error = f"HTTP {response.status}"
                        logger.error(f"[DOWNLOAD_DEBUG] ❌ HTTP error: {response.status}")

            except asyncio.TimeoutError as e:
                elapsed = time_module.time() - start_time
                last_error = f"Timeout (attempt {retry_count+1})"
                logger.error(f"[DOWNLOAD_DEBUG] ❌ Timeout")
                logger.error(f"  - Waited: {elapsed:.2f} seconds")
                logger.error(f"  - Configured total timeout: {timeout_config.get('total', 30)} seconds")
                logger.error(f"  - Possible causes: slow network, slow server response, or large data volume")
            except aiohttp.ClientError as e:
                elapsed = time_module.time() - start_time
                last_error = f"Network error: {str(e)}"
                logger.error(f"[DOWNLOAD_DEBUG] ❌ Network error: {e.__class__.__name__}")
                logger.error(f"  - Error details: {str(e)[:200]}")
                logger.error(f"  - Occurred after: {elapsed:.2f} seconds")
                # 🔍 Diagnose SSL errors
                if "SSL" in str(e) or "ssl" in str(e).lower():
                    logger.error(f"  - 💡 Detected SSL error, possibly certificate issue or firewall blocking")
            except Exception as e:
                elapsed = time_module.time() - start_time
                last_error = str(e)
                logger.error(f"[DOWNLOAD_DEBUG] ❌ Unknown error: {e}")
                logger.error(f"  - Error type: {type(e).__name__}")
                logger.error(f"  - Occurred after: {elapsed:.2f} seconds")

            # Retry delay
            if retry_count < max_retries - 1:
                delay = retry_delays[retry_count]
                logger.info(f"[DOWNLOAD_DEBUG] Waiting {delay} seconds before retrying...")
                await asyncio.sleep(delay)

    return None, last_error

# --- Memory Monitoring Task ---
async def memory_monitor():
    """Optimized memory monitoring task"""
    import gc
    import psutil
    import os
    import time as time_module

    process = psutil.Process(os.getpid())
    last_gc_time = time_module.time()

    while True:
        try:
            # Check once per minute (more frequent monitoring)
            await asyncio.sleep(60)

            # Get memory usage
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / (1024 * 1024)

            # Get download concurrency status
            active_downloads = MAX_CONCURRENT_DOWNLOADS - DOWNLOAD_SEMAPHORE._value if DOWNLOAD_SEMAPHORE else 0

            # Log memory status (more detailed information)
            logger.info(f"[MEM_MONITOR] Memory: {memory_mb:.2f}MB | "
                       f"Active downloads: {active_downloads}/{MAX_CONCURRENT_DOWNLOADS} | "
                       f"Response channels: {len(response_channels)} | "
                       f"Request metadata: {len(request_metadata)} | "
                       f"Cached images: {len(IMAGE_BASE64_CACHE)} | "
                       f"Image bed URL cache: {len(FILEBED_URL_CACHE)} | "
                       f"Download history: {len(downloaded_urls_set)}")

            # New: Clean up expired image bed URL cache
            if len(FILEBED_URL_CACHE) > 0:
                current_time = time_module.time()
                expired_hashes = []
                for img_hash, (url, cache_time) in FILEBED_URL_CACHE.items():
                    if current_time - cache_time > FILEBED_URL_CACHE_TTL:
                        expired_hashes.append(img_hash)

                if expired_hashes:
                    for img_hash in expired_hashes:
                        del FILEBED_URL_CACHE[img_hash]
                    logger.info(f"[MEM_MONITOR] Cleaned {len(expired_hashes)} expired image bed URL cache entries")

            # New: Monitor and clean up timed-out request metadata
            if len(request_metadata) > 10:  # If metadata is too much, there might be a memory leak
                logger.warning(f"[MEM_MONITOR] request_metadata count is high: {len(request_metadata)}")
                logger.warning(f"[MEM_MONITOR] Starting to clean up timed-out request metadata...")

                # Implement timeout cleanup logic
                current_time = datetime.now()
                timeout_threshold = CONFIG.get("metadata_timeout_minutes", 30)  # Default 30 minutes timeout
                stale_request_ids = []

                for req_id, metadata in request_metadata.items():
                    created_at_str = metadata.get("created_at")
                    if created_at_str:
                        try:
                            created_at = datetime.fromisoformat(created_at_str)
                            age_minutes = (current_time - created_at).total_seconds() / 60

                            if age_minutes > timeout_threshold:
                                stale_request_ids.append(req_id)
                                logger.info(f"[MEM_MONITOR] Found timed-out metadata: {req_id[:8]} (age: {age_minutes:.1f} minutes)")
                        except (ValueError, TypeError) as e:
                            logger.warning(f"[MEM_MONITOR] Failed to parse metadata time: {req_id[:8]}, error: {e}")
                            stale_request_ids.append(req_id)  # Clean up invalid timestamps too

                # Clean up timed-out metadata
                for req_id in stale_request_ids:
                    del request_metadata[req_id]
                    # Also clean up corresponding response channel (if still exists)
                    if req_id in response_channels:
                        del response_channels[req_id]
                        logger.debug(f"[MEM_MONITOR] Also cleaned up response channel: {req_id[:8]}")

                if stale_request_ids:
                    logger.info(f"[MEM_MONITOR] Cleaned {len(stale_request_ids)} timed-out request metadata entries")
                else:
                    logger.info(f"[MEM_MONITOR] No timed-out metadata found, but count is still high, which might be normal")
            else:
                logger.debug(f"[MEM_MONITOR] request_metadata: {len(request_metadata)}")

            # Read memory management thresholds from configuration
            mem_config = CONFIG.get("memory_management", {})
            gc_threshold = mem_config.get("gc_threshold_mb", 500)
            cache_config = mem_config.get("cache_config", {})

            # Dynamically adjust based on memory usage
            if memory_mb > gc_threshold:
                current_time = time_module.time()
                # Prevent overly frequent GC
                if current_time - last_gc_time > 300:  # Max one GC every 5 minutes
                    logger.warning(f"[MEM_MONITOR] Triggering garbage collection (Memory: {memory_mb:.2f}MB > {gc_threshold}MB)")

                    # Clean up image cache
                    cache_max = cache_config.get("image_cache_max_size", 500)
                    cache_keep = cache_config.get("image_cache_keep_size", 200)
                    if len(IMAGE_BASE64_CACHE) > cache_max:
                        # Keep the latest specified number
                        sorted_items = sorted(IMAGE_BASE64_CACHE.items(),
                                            key=lambda x: x[1][1], reverse=True)
                        IMAGE_BASE64_CACHE.clear()
                        for url, data in sorted_items[:cache_keep]:
                            IMAGE_BASE64_CACHE[url] = data
                        logger.info(f"[MEM_MONITOR] Cleaned image cache: {len(sorted_items)} -> {cache_keep}")

                    # Clean up download records
                    url_history_max = cache_config.get("url_history_max", 2000)
                    url_history_keep = cache_config.get("url_history_keep", 1000)
                    if len(downloaded_urls_set) > url_history_max:
                        downloaded_urls_set.clear()
                        # Keep recent records
                        downloaded_urls_set.update(list(downloaded_image_urls)[-url_history_keep:])
                        logger.info(f"[MEM_MONITOR] Cleaned download records: {url_history_max} -> {url_history_keep}")

                    # Perform garbage collection
                    gc.collect()
                    last_gc_time = current_time

                    # Check memory again
                    new_memory_mb = process.memory_info().rss / (1024 * 1024)
                    logger.info(f"[MEM_MONITOR] Memory after GC: {memory_mb:.2f}MB -> {new_memory_mb:.2f}MB "
                               f"(Released: {memory_mb - new_memory_mb:.2f}MB)")

        except Exception as e:
            logger.error(f"[MEM_MONITOR] Error: {e}")

# --- Main Program Entry Point ---
if __name__ == "__main__":
    # Recommended to read port from config.jsonc, hardcoded here temporarily
    api_port = 5102
    logger.info(f"🚀 LMArena Bridge v2.0 API Server is starting...")
    logger.info(f"   - Listening address: http://127.0.0.1:{api_port}")
    logger.info(f"   - WebSocket endpoint: ws://127.0.0.1:{api_port}/ws")

    uvicorn.run(app, host="0.0.0.0", port=api_port)
