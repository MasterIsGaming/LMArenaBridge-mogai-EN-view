"""
Monitoring Module - Used for collecting and managing request statistics
"""

import json
import time
import threading
from datetime import datetime, timedelta
from collections import defaultdict, deque, OrderedDict
from dataclasses import dataclass, asdict
from typing import Dict, Optional, List
import logging
from pathlib import Path
import sys

logger = logging.getLogger(__name__)

# Configuration
class MonitorConfig:
    """Monitoring Configuration"""
    LOG_DIR = Path("logs")
    REQUEST_LOG_FILE = "requests.jsonl"
    ERROR_LOG_FILE = "errors.jsonl"
    STATS_FILE = "stats.json"  # New: Statistics data file
    MAX_LOG_SIZE = 400 * 1024 * 1024  # 10MB
    MAX_LOG_FILES = 10
    MAX_RECENT_REQUESTS = 10000
    MAX_RECENT_ERRORS = 50
    STATS_UPDATE_INTERVAL = 5  # Seconds

# Ensure the log directory exists
MonitorConfig.LOG_DIR.mkdir(exist_ok=True)

@dataclass
class RequestInfo:
    """Request Information"""
    request_id: str
    timestamp: float
    model: str
    status: str  # 'active', 'success', 'failed'
    duration: Optional[float] = None
    error: Optional[str] = None
    messages_count: int = 0
    session_id: Optional[str] = None
    mode: Optional[str] = None
    # New detailed information fields
    request_messages: Optional[List[dict]] = None
    request_params: Optional[dict] = None
    response_content: Optional[str] = None
    reasoning_content: Optional[str] = None  # New: Chain of thought content
    input_tokens: int = 0
    output_tokens: int = 0

@dataclass
class Stats:
    """Statistics Data"""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    active_requests: int = 0
    avg_duration: float = 0.0
    total_messages: int = 0
    uptime: float = 0.0

class LogManager:
    """Log Manager"""

    def __init__(self):
        self.request_log_path = MonitorConfig.LOG_DIR / MonitorConfig.REQUEST_LOG_FILE
        self.error_log_path = MonitorConfig.LOG_DIR / MonitorConfig.ERROR_LOG_FILE
        self._lock = threading.Lock()

    def write_request_log(self, log_entry: dict):
        """Writes a request log entry"""
        with self._lock:
            try:
                with open(self.request_log_path, 'a', encoding='utf-8') as f:
                    f.write(json.dumps(log_entry, ensure_ascii=False) + '\n')
            except Exception as e:
                logger.error(f"Failed to write request log: {e}")

    def write_error_log(self, log_entry: dict):
        """Writes an error log entry"""
        with self._lock:
            try:
                with open(self.error_log_path, 'a', encoding='utf-8') as f:
                    f.write(json.dumps(log_entry, ensure_ascii=False) + '\n')
            except Exception as e:
                logger.error(f"Failed to write error log: {e}")

    def read_recent_logs(self, log_type: str = "requests", limit: int = 50) -> List[dict]:
        """Reads recent logs (returns only completed requests)"""
        log_path = self.request_log_path if log_type == "requests" else self.error_log_path
        logs = []

        if not log_path.exists():
            return logs

        try:
            with open(log_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                # Read from back to front, collecting recent 'request_end' type logs
                for line in reversed(lines):
                    if len(logs) >= limit:
                        break
                    try:
                        log_entry = json.loads(line.strip())
                        # Only return 'request_end' type logs (containing complete information)
                        if log_type == "requests" and log_entry.get('type') == 'request_end':
                            logs.append(log_entry)
                        elif log_type == "errors":
                            # Error logs do not need filtering
                            logs.append(log_entry)
                    except json.JSONDecodeError:
                        continue
        except Exception as e:
            logger.error(f"Failed to read logs: {e}")

        return logs  # Already in reverse order (newest first)

class MonitoringService:
    """Monitoring Service"""

    def __init__(self):
        self.startup_time = time.time()
        self.log_manager = LogManager()
        self.active_requests: Dict[str, RequestInfo] = {}
        self.recent_requests = deque(maxlen=MonitorConfig.MAX_RECENT_REQUESTS)
        self.recent_errors = deque(maxlen=MonitorConfig.MAX_RECENT_ERRORS)
        self.model_stats = defaultdict(lambda: {
            'total': 0, 'success': 0, 'failed': 0,
            'total_duration': 0, 'count_with_duration': 0
        })
        self._lock = threading.Lock()

        # New: Store complete request details (for detailed viewing)
        # Use OrderedDict for better memory management
        self.request_details_cache = OrderedDict()  # Use OrderedDict to manage the cache
        self.MAX_DETAILS_CACHE = 10000  # Keep the original cache size
        self.cache_size_limit_mb = 500  # Increase cache size limit to 500MB to ensure data integrity

        # WebSocket client management
        self.monitor_clients = set()

        # Load persisted statistics
        self._load_persisted_stats()

        logger.info("Monitoring service initialized")

    def request_start(self, request_id: str, model: str, messages_count: int = 0,
                     session_id: str = None, mode: str = None,
                     messages: List[dict] = None, params: dict = None):
        """Records the start of a request (with detailed information)"""
        with self._lock:
            # Estimate input tokens
            estimated_input_tokens = 0
            if messages:
                for msg in messages:
                    if isinstance(msg, dict) and 'content' in msg:
                        content = msg.get('content', '')
                        if isinstance(content, str):
                            estimated_input_tokens += len(content) // 4
                        elif isinstance(content, list):
                            # Handle multimodal messages
                            for part in content:
                                if isinstance(part, dict) and part.get('type') == 'text':
                                    estimated_input_tokens += len(part.get('text', '')) // 4

            request_info = RequestInfo(
                request_id=request_id,
                timestamp=time.time(),
                model=model,
                status='active',
                messages_count=messages_count,
                session_id=session_id,
                mode=mode,
                request_messages=messages,
                request_params=params,
                input_tokens=estimated_input_tokens  # Set estimated input tokens
            )
            self.active_requests[request_id] = request_info

            # Also store in details cache
            self._store_request_details(request_id, request_info)

            # Write to log
            log_entry = {
                'type': 'request_start',
                'timestamp': request_info.timestamp,
                'request_id': request_id,
                'model': model,
                'messages_count': messages_count,
                'session_id': session_id,
                'mode': mode
            }
            self.log_manager.write_request_log(log_entry)

            logger.info(f"Request started [ID: {request_id[:8]}] Model: {model}")

    def request_end(self, request_id: str, success: bool, error: str = None,
                    response_content: str = None, reasoning_content: str = None,
                    input_tokens: int = 0, output_tokens: int = 0):
        """Records the end of a request (with response content and chain of thought)"""
        with self._lock:
            if request_id not in self.active_requests:
                logger.warning(f"Request {request_id} not found")
                return

            request_info = self.active_requests[request_id]
            request_info.status = 'success' if success else 'failed'
            request_info.duration = time.time() - request_info.timestamp
            request_info.error = error
            request_info.response_content = response_content
            request_info.reasoning_content = reasoning_content
            request_info.input_tokens = input_tokens
            request_info.output_tokens = output_tokens

            # Update details cache
            self._store_request_details(request_id, request_info)

            # Update model statistics
            model = request_info.model
            self.model_stats[model]['total'] += 1
            if success:
                self.model_stats[model]['success'] += 1
            else:
                self.model_stats[model]['failed'] += 1

            if request_info.duration:
                self.model_stats[model]['total_duration'] += request_info.duration
                self.model_stats[model]['count_with_duration'] += 1

            # Persist statistics
            self._persist_stats()

            # Add to recent requests list
            self.recent_requests.append(asdict(request_info))

            # If failed, add to error list
            if not success:
                error_info = {
                    'timestamp': time.time(),
                    'request_id': request_id,
                    'model': model,
                    'error': error or 'Unknown error'
                }
                self.recent_errors.append(error_info)
                self.log_manager.write_error_log(error_info)

            # Write request log (including full details)
            log_entry = {
                'type': 'request_end',
                'timestamp': time.time(),
                'request_id': request_id,
                'model': model,
                'status': request_info.status,
                'duration': request_info.duration,
                'error': error,
                'mode': request_info.mode,
                'session_id': request_info.session_id,
                'messages_count': request_info.messages_count,
                'input_tokens': request_info.input_tokens,
                'output_tokens': request_info.output_tokens,
                # Include detailed information
                'request_messages': request_info.request_messages,
                'request_params': request_info.request_params,
                'response_content': request_info.response_content,
                'reasoning_content': request_info.reasoning_content
            }
            self.log_manager.write_request_log(log_entry)

            # Remove from active requests
            del self.active_requests[request_id]

            logger.info(f"Request ended [ID: {request_id[:8]}] Status: {request_info.status} Duration: {request_info.duration:.2f}s")

    def get_stats(self) -> Stats:
        """Retrieves statistics"""
        with self._lock:
            stats = Stats()
            stats.uptime = time.time() - self.startup_time
            stats.active_requests = len(self.active_requests)

            # Get all-time statistics (from persisted data)
            # Prioritize persisted totals to maintain accuracy even after server restarts
            total_all_time = sum(s['total'] for s in self.model_stats.values())
            success_all_time = sum(s['success'] for s in self.model_stats.values())
            failed_all_time = sum(s['failed'] for s in self.model_stats.values())

            # Use all-time totals
            stats.total_requests = total_all_time
            stats.successful_requests = success_all_time
            stats.failed_requests = failed_all_time

            # Calculate total messages (accumulated from recent requests)
            stats.total_messages = sum(req.get('messages_count', 0) for req in self.recent_requests)

            # Calculate average response time (using the last 100 requests)
            recent_durations = []
            for req in list(self.recent_requests)[-100:]:  # Last 100 requests
                if req.get('duration'):
                    recent_durations.append(req['duration'])

            if recent_durations:
                stats.avg_duration = sum(recent_durations) / len(recent_durations)

            return stats

    def get_model_stats(self) -> List[dict]:
        """Retrieves model statistics"""
        with self._lock:
            model_stats_list = []
            for model, stats in self.model_stats.items():
                avg_duration = 0
                if stats['count_with_duration'] > 0:
                    avg_duration = stats['total_duration'] / stats['count_with_duration']

                success_rate = 0
                if stats['total'] > 0:
                    success_rate = (stats['success'] / stats['total']) * 100

                model_stats_list.append({
                    'model': model,
                    'total_requests': stats['total'],
                    'successful_requests': stats['success'],
                    'failed_requests': stats['failed'],
                    'avg_duration': avg_duration,
                    'success_rate': success_rate
                })

            # Sort by total requests
            model_stats_list.sort(key=lambda x: x['total_requests'], reverse=True)
            return model_stats_list

    def get_active_requests(self) -> List[dict]:
        """Retrieves a list of active requests"""
        with self._lock:
            return [asdict(req) for req in self.active_requests.values()]

    def get_recent_requests(self, limit: int = 50) -> List[dict]:
        """Retrieves recent requests"""
        with self._lock:
            requests = list(self.recent_requests)
            return requests[-limit:][::-1]  # Newest first

    def get_recent_errors(self, limit: int = 30) -> List[dict]:
        """Retrieves recent errors"""
        with self._lock:
            errors = list(self.recent_errors)
            return errors[-limit:][::-1]  # Newest first

    def get_summary(self) -> dict:
        """Retrieves monitoring summary"""
        stats = self.get_stats()
        model_stats = self.get_model_stats()

        return {
            'stats': asdict(stats),
            'model_stats': model_stats,
            'active_requests_list': self.get_active_requests(),
            'recent_errors_count': len(self.recent_errors)
        }

    async def broadcast_to_monitors(self, data: dict):
        """Broadcasts data to all monitoring clients"""
        if not self.monitor_clients:
            return

        disconnected = []
        for client in self.monitor_clients:
            try:
                await client.send_json(data)
            except:
                disconnected.append(client)

        # Clean up disconnected connections
        for client in disconnected:
            self.monitor_clients.discard(client)

    def add_monitor_client(self, websocket):
        """Adds a monitoring client"""
        self.monitor_clients.add(websocket)
        logger.debug(f"Monitor client connected, current client count: {len(self.monitor_clients)}")

    def remove_monitor_client(self, websocket):
        """Removes a monitoring client"""
        self.monitor_clients.discard(websocket)
        logger.debug(f"Monitor client disconnected, current client count: {len(self.monitor_clients)}")

    def _store_request_details(self, request_id: str, request_info: RequestInfo):
        """Stores request details to cache (maintaining data integrity)"""

        # Create data to be stored - maintain integrity, do not truncate
        request_data = asdict(request_info)

        # Check cache size (rough estimate)
        cache_size_bytes = sys.getsizeof(self.request_details_cache)
        cache_size_mb = cache_size_bytes / (1024 * 1024)

        # If cache is too large (exceeds 500MB), delete the oldest 10% of items
        if cache_size_mb > self.cache_size_limit_mb and len(self.request_details_cache) > 0:
            # Delete the oldest 10% of items
            items_to_remove = max(1, len(self.request_details_cache) // 10)
            for _ in range(items_to_remove):
                self.request_details_cache.popitem(last=False)
            cache_size_bytes = sys.getsizeof(self.request_details_cache)
            cache_size_mb = cache_size_bytes / (1024 * 1024)
            logger.info(f"[CACHE] Cache exceeded limit, {items_to_remove} old items cleaned, current size: ~{cache_size_mb:.2f}MB")

        # Limit the number of cache items
        if len(self.request_details_cache) >= self.MAX_DETAILS_CACHE:
            # Delete the oldest cache item (FIFO)
            self.request_details_cache.popitem(last=False)

        # Store new item - maintain data integrity
        self.request_details_cache[request_id] = request_data

        # Periodically log cache status (every 500 requests)
        if len(self.request_details_cache) % 500 == 0:
            logger.debug(f"[CACHE] Details cache status - Items: {len(self.request_details_cache)}, Size: ~{cache_size_mb:.2f}MB")

    def get_request_details(self, request_id: str) -> Optional[dict]:
        """Retrieves request details"""
        with self._lock:
            # First, look in the cache
            if request_id in self.request_details_cache:
                return self.request_details_cache[request_id]

            # Look in active requests
            if request_id in self.active_requests:
                return asdict(self.active_requests[request_id])

            # Look in recent requests
            for req in self.recent_requests:
                if req.get('request_id') == request_id:
                    return req

            # If not found in memory, look in log files
            return self._find_request_in_logs(request_id)

    def _find_request_in_logs(self, request_id: str) -> Optional[dict]:
        """Finds request details in log files"""
        try:
            if not self.log_manager.request_log_path.exists():
                return None

            with open(self.log_manager.request_log_path, 'r', encoding='utf-8') as f:
                # Read from back to front to improve search efficiency
                lines = f.readlines()
                for line in reversed(lines):
                    try:
                        log_entry = json.loads(line.strip())
                        if (log_entry.get('request_id') == request_id and
                            log_entry.get('type') == 'request_end'):
                            # Found complete request record
                            return log_entry
                    except json.JSONDecodeError:
                        continue
        except Exception as e:
            logger.error(f"Failed to find request details in log file: {e}")

        return None

    def _persist_stats(self):
        """Persists statistics data to a file"""
        try:
            stats_path = MonitorConfig.LOG_DIR / MonitorConfig.STATS_FILE

            # Prepare data to be saved
            stats_data = {
                'last_update': time.time(),
                'startup_time': self.startup_time,
                'model_stats': dict(self.model_stats),
                # Save overall statistics
                'total_requests_all_time': sum(s['total'] for s in self.model_stats.values()),
                'total_success_all_time': sum(s['success'] for s in self.model_stats.values()),
                'total_failed_all_time': sum(s['failed'] for s in self.model_stats.values())
            }

            # Write to file
            with open(stats_path, 'w', encoding='utf-8') as f:
                json.dump(stats_data, f, ensure_ascii=False, indent=2)

        except Exception as e:
            logger.error(f"Failed to persist statistics data: {e}")

    def _load_persisted_stats(self):
        """Loads persisted statistics data from a file"""
        try:
            stats_path = MonitorConfig.LOG_DIR / MonitorConfig.STATS_FILE

            if not stats_path.exists():
                logger.info("No persisted statistics data found, starting from scratch")
                return

            with open(stats_path, 'r', encoding='utf-8') as f:
                stats_data = json.load(f)

            # Restore model statistics
            if 'model_stats' in stats_data:
                self.model_stats = defaultdict(
                    lambda: {'total': 0, 'success': 0, 'failed': 0,
                            'total_duration': 0, 'count_with_duration': 0},
                    stats_data['model_stats']
                )

            # Restore recent requests and errors
            if 'recent_requests' in stats_data:
                for req in stats_data['recent_requests']:
                    self.recent_requests.append(req)

            if 'recent_errors' in stats_data:
                for err in stats_data['recent_errors']:
                    self.recent_errors.append(err)

            # If it's the same running session, keep the original startup time
            # Otherwise, reset the startup time
            if 'startup_time' in stats_data:
                time_since_last_update = time.time() - stats_data.get('last_update', 0)
                # If more than 1 hour has passed since the last update, consider it a new session
                if time_since_last_update > 3600:
                    self.startup_time = time.time()
                else:
                    self.startup_time = stats_data['startup_time']

            logger.info(f"Persisted statistics data loaded: {len(self.model_stats)} model statistics")

        except Exception as e:
            logger.error(f"Failed to load persisted statistics data: {e}")

    def get_all_time_stats(self) -> dict:
        """Retrieves all-time statistics (calculated from log files)"""
        try:
            if not self.log_manager.request_log_path.exists():
                return {
                    'total_requests': 0,
                    'total_success': 0,
                    'total_failed': 0,
                    'models': {}
                }

            model_counts = defaultdict(lambda: {'total': 0, 'success': 0, 'failed': 0})
            total_requests = 0
            total_success = 0
            total_failed = 0

            with open(self.log_manager.request_log_path, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        log_entry = json.loads(line.strip())
                        if log_entry.get('type') == 'request_end':
                            model = log_entry.get('model', 'unknown')
                            status = log_entry.get('status', 'failed')

                            total_requests += 1
                            model_counts[model]['total'] += 1

                            if status == 'success':
                                total_success += 1
                                model_counts[model]['success'] += 1
                            else:
                                total_failed += 1
                                model_counts[model]['failed'] += 1

                    except json.JSONDecodeError:
                        continue

            return {
                'total_requests': total_requests,
                'total_success': total_success,
                'total_failed': total_failed,
                'models': dict(model_counts)
            }

        except Exception as e:
            logger.error(f"Failed to calculate all-time statistics: {e}")
            return {
                'total_requests': 0,
                'total_success': 0,
                'total_failed': 0,
                'models': {}
            }

# Create a global monitoring service instance
monitoring_service = MonitoringService()
