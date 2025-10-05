#!/usr/bin/env python3
"""
并发限制诊断工具
用于验证浏览器HTTP/1.1的6连接限制问题
"""

import asyncio
import aiohttp
import time
from datetime import datetime
from typing import List

# 配置
API_BASE_URL = "http://127.0.0.1:5102"
CONCURRENT_REQUESTS = 10

class RequestTracker:
    """请求追踪器"""
    def __init__(self):
        self.requests = {}
        self.lock = asyncio.Lock()
        
    async def start(self, req_id: int):
        async with self.lock:
            self.requests[req_id] = {
                "id": req_id,
                "start_time": time.time(),
                "first_chunk_time": None,
                "end_time": None,
                "status": "等待响应"
            }
            self._print_status()
    
    async def first_chunk(self, req_id: int):
        async with self.lock:
            if req_id in self.requests:
                self.requests[req_id]["first_chunk_time"] = time.time()
                self.requests[req_id]["status"] = "正在接收"
                self._print_status()
    
    async def end(self, req_id: int):
        async with self.lock:
            if req_id in self.requests:
                self.requests[req_id]["end_time"] = time.time()
                self.requests[req_id]["status"] = "已完成"
                self._print_status()
    
    def _print_status(self):
        """打印当前并发状态"""
        current_time = time.time()
        print(f"\n{'='*80}")
        print(f"[{datetime.now().strftime('%H:%M:%S')}] 并发状态监控")
        print(f"{'='*80}")
        
        waiting = streaming = completed = 0
        
        for req_id, info in sorted(self.requests.items()):
            status = info["status"]
            elapsed = current_time - info["start_time"]
            
            if status == "等待响应":
                waiting += 1
                print(f"  ⏳ 请求#{req_id:02d}: {status} (已等待 {elapsed:.1f}秒)")
            elif status == "正在接收":
                streaming += 1
                wait_time = info["first_chunk_time"] - info["start_time"]
                stream_time = elapsed - wait_time
                print(f"  📥 请求#{req_id:02d}: {status} (等待{wait_time:.1f}秒 -> 接收{stream_time:.1f}秒)")
            elif status == "已完成":
                completed += 1
                total_time = info["end_time"] - info["start_time"]
                print(f"  ✅ 请求#{req_id:02d}: {status} (总耗时 {total_time:.1f}秒)")
        
        print(f"\n  📊 统计: 等待={waiting} | 接收={streaming} | 完成={completed} | 总计={len(self.requests)}")
        
        if waiting > 0 and streaming == 6:
            print(f"\n  ⚠️  检测到浏览器并发限制！{streaming}个请求在处理，{waiting}个在排队")
            print(f"  💡 这是Edge/Chrome的HTTP/1.1限制（每个域名最多6个并发连接）")

async def test_request(session: aiohttp.ClientSession, req_id: int, tracker: RequestTracker, model_name: str):
    """发送单个测试请求"""
    await tracker.start(req_id)
    
    payload = {
        "model": model_name,
        "messages": [
            {"role": "user", "content": f"测试请求#{req_id}，请用一句话回复"}
        ],
        "stream": True,
        "max_tokens": 50
    }
    
    try:
        async with session.post(
            f"{API_BASE_URL}/v1/chat/completions",
            json=payload,
            headers={"Content-Type": "application/json"}
        ) as response:
            first_chunk = True
            async for line in response.content:
                if first_chunk:
                    await tracker.first_chunk(req_id)
                    first_chunk = False
                # 解析并处理数据...
                
        await tracker.end(req_id)
        
    except Exception as e:
        print(f"  ❌ 请求#{req_id:02d} 失败: {e}")

async def main():
    """主测试函数"""
    print("="*80)
    print("🔍 浏览器并发限制诊断工具")
    print("="*80)
    print(f"\n将同时发送 {CONCURRENT_REQUESTS} 个请求到 LMArena Bridge...")
    print("如果观察到只有6个请求在处理，其余4个在等待，则证实是浏览器限制。\n")
    
    input("按Enter开始测试...")
    
    tracker = RequestTracker()
    
    # 使用TCPConnector避免客户端的连接池限制
    connector = aiohttp.TCPConnector(limit=20, limit_per_host=20)
    
    async with aiohttp.ClientSession(connector=connector) as session:
        # 准备任务（使用你配置的模型名）
        tasks = []
        for i in range(CONCURRENT_REQUESTS):
            # 替换为你的实际模型名
            model_name = f"your-model-name-{i}"  # ⚠️ 请修改为实际模型名
            tasks.append(test_request(session, i+1, tracker, model_name))
        
        # 同时启动所有请求
        print(f"\n🚀 同时发起 {CONCURRENT_REQUESTS} 个请求...\n")
        await asyncio.gather(*tasks)
    
    print("\n" + "="*80)
    print("✅ 测试完成")
    print("="*80)

if __name__ == "__main__":
    asyncio.run(main())