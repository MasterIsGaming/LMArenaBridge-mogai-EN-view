// ==UserScript==
// @name         LMArena API Bridge test.3
// @namespace    http://tampermonkey.net/
// @version      2.7
// @description  Bridges LMArena to a local API server via WebSocket. Supports multi-tab concurrent requests to bypass browser HTTP/1.1 limits.
// @author       Lianues
// @match        https://lmarena.ai/*
// @match        https://*.lmarena.ai/*
// @icon         https://www.google.com/s2/favicons?sz=64&domain=lmarena.ai
// @grant        none
// @run-at       document-end
// ==/UserScript==

(function () {
    'use strict';

    // --- 配置 ---
    const SERVER_URL = "ws://localhost:5102/ws"; // 与 api_server.py 中的端口匹配
    let socket;
    let isCaptureModeActive = false; // ID捕获模式的开关

    // --- 生成唯一的标签页ID ---
    const TAB_ID = `tab_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
    console.log(`[API Bridge] 本标签页ID: ${TAB_ID}`);

    // --- 页面可见性管理 ---
    const visibilityManager = {
        isHidden: document.hidden,
        bufferQueue: [],
        bufferTimer: null,

        init() {
            document.addEventListener('visibilitychange', () => {
                this.isHidden = document.hidden;
                console.log(`[API Bridge] 页面可见性变化: ${document.visibilityState} (hidden=${this.isHidden})`);

                // 当页面变为可见时，立即发送缓冲的数据
                if (!this.isHidden && this.bufferQueue.length > 0) {
                    this.flushBuffer();
                }
            });
        },

        flushBuffer() {
            if (this.bufferQueue.length === 0) return;

            const combinedData = this.bufferQueue.join('');
            this.bufferQueue = [];

            if (this.bufferTimer) {
                clearTimeout(this.bufferTimer);
                this.bufferTimer = null;
            }

            // 直接发送组合的数据
            return combinedData;
        },

        scheduleFlush(requestId, sendFn, delay = 100) {
            if (this.bufferTimer) {
                clearTimeout(this.bufferTimer);
            }

            this.bufferTimer = setTimeout(() => {
                const data = this.flushBuffer();
                if (data) {
                    sendFn(requestId, data);
                }
                this.bufferTimer = null;
            }, delay);
        }
    };

    // --- 初始化页面可见性管理 ---
    visibilityManager.init();

    // --- 核心逻辑 ---
    function connect() {
        console.log(`[API Bridge] 正在连接到本地服务器: ${SERVER_URL}...`);
        socket = new WebSocket(SERVER_URL);

        socket.onopen = () => {
            console.log("[API Bridge] ✅ 与本地服务器的 WebSocket 连接已建立。");
            console.log(`[API Bridge] 发送标签页ID: ${TAB_ID}`);

            // 立即发送标签页ID给服务器
            socket.send(JSON.stringify({ tab_id: TAB_ID }));

            document.title = "✅ " + document.title;
        };

        socket.onmessage = async (event) => {
            try {
                const message = JSON.parse(event.data);

                // 检查是否是指令，而不是标准的聊天请求
                if (message.command) {
                    console.log(`[API Bridge] ⬇️ 收到指令: ${message.command}`);
                    if (message.command === 'refresh' || message.command === 'reconnect') {
                        console.log(`[API Bridge] 收到 '${message.command}' 指令，正在执行页面刷新...`);
                        location.reload();
                    } else if (message.command === 'activate_id_capture') {
                        console.log("[API Bridge] ✅ ID 捕获模式已激活。请在页面上触发一次 'Retry' 操作。");
                        isCaptureModeActive = true;
                        // 可以选择性地给用户一个视觉提示
                        document.title = "🎯 " + document.title;
                    } else if (message.command === 'send_page_source') {
                       console.log("[API Bridge] 收到发送页面源码的指令，正在发送...");
                       sendPageSource();
                    }
                    return;
                }

                const { request_id, payload } = message;

                if (!request_id || !payload) {
                    console.error("[API Bridge] 收到来自服务器的无效消息:", message);
                    return;
                }

                console.log(`[API Bridge] ⬇️ 收到聊天请求 ${request_id.substring(0, 8)}。准备执行 fetch 操作。`);
                await executeFetchAndStreamBack(request_id, payload);

            } catch (error) {
                console.error("[API Bridge] 处理服务器消息时出错:", error);
            }
        };

        socket.onclose = () => {
            console.warn("[API Bridge] 🔌 与本地服务器的连接已断开。将在5秒后尝试重新连接...");
            if (document.title.startsWith("✅ ")) {
                document.title = document.title.substring(2);
            }
            setTimeout(connect, 5000);
        };

        socket.onerror = (error) => {
            console.error("[API Bridge] ❌ WebSocket 发生错误:", error);
            socket.close(); // 会触发 onclose 中的重连逻辑
        };
    }

    async function executeFetchAndStreamBack(requestId, payload, retryCount = 0) {
        console.log(`[API Bridge] 当前操作域名: ${window.location.hostname}`);
        const { is_image_request, message_templates, target_model_id, session_id, message_id } = payload;

        // 重试配置
        const MAX_RETRIES = 5;
        const BASE_DELAY = 1000; // 1秒基础延迟
        const MAX_DELAY = 30000; // 最大延迟30秒

        if (retryCount > 0) {
            console.log(`[API Bridge] 🔄 重试请求 ${requestId.substring(0, 8)}，重试次数: ${retryCount}/${MAX_RETRIES}`);
        }

        // 关键修复：为每个请求创建独立的buffer，避免并发时内容混串
        const requestBuffer = {
            queue: [],
            timer: null
        };

        // --- 使用从后端配置传递的会话信息 ---
        if (!session_id || !message_id) {
            const errorMsg = "从后端收到的会话信息 (session_id 或 message_id) 为空。请先运行 `id_updater.py` 脚本进行设置。";
            console.error(`[API Bridge] ${errorMsg}`);
            sendToServer(requestId, { error: errorMsg });
            sendToServer(requestId, "[DONE]");
            return;
        }

        // URL 对于聊天和文生图是相同的
        const apiUrl = `/nextjs-api/stream/retry-evaluation-session-message/${session_id}/messages/${message_id}`;
        const httpMethod = 'PUT';

        console.log(`[API Bridge] 使用 API 端点: ${apiUrl}`);

        const newMessages = [];
        let lastMsgIdInChain = null;

        if (!message_templates || message_templates.length === 0) {
            const errorMsg = "从后端收到的消息列表为空。";
            console.error(`[API Bridge] ${errorMsg}`);
            sendToServer(requestId, { error: errorMsg });
            sendToServer(requestId, "[DONE]");
            return;
        }

        // 这个循环逻辑对于聊天和文生图是通用的，因为后端已经准备好了正确的 message_templates
        for (let i = 0; i < message_templates.length; i++) {
            const template = message_templates[i];
            const currentMsgId = crypto.randomUUID();
            const parentIds = lastMsgIdInChain ? [lastMsgIdInChain] : [];

            // 如果是文生图请求，状态总是 'success'
            // 否则，只有最后一条消息是 'pending'
            const status = is_image_request ? 'success' : ((i === message_templates.length - 1) ? 'pending' : 'success');

            newMessages.push({
                role: template.role,
                content: template.content,
                id: currentMsgId,
                evaluationId: null,
                evaluationSessionId: session_id,
                parentMessageIds: parentIds,
                experimental_attachments: Array.isArray(template.experimental_attachments) ? template.experimental_attachments : [],
                failureReason: null,
                metadata: null,
                participantPosition: template.participantPosition || "a",
                createdAt: new Date().toISOString(),
                updatedAt: new Date().toISOString(),
                status: status,
            });
            lastMsgIdInChain = currentMsgId;
        }

        const body = {
            messages: newMessages,
            modelId: target_model_id,
        };

        console.log("[API Bridge] 准备发送到 LMArena API 的最终载荷:", JSON.stringify(body, null, 2));

        // 设置一个标志，让我们的 fetch 拦截器知道这个请求是脚本自己发起的
        window.isApiBridgeRequest = true;
        try {
            const response = await fetch(apiUrl, {
                method: httpMethod,
                headers: {
                    'Content-Type': 'text/plain;charset=UTF-8', // LMArena 使用 text/plain
                    'Accept': '*/*',
                },
                body: JSON.stringify(body),
                credentials: 'include' // 必须包含 cookie
            });

            if (!response.ok || !response.body) {
                const errorBody = await response.text();
                throw new Error(`网络响应不正常。状态: ${response.status}. 内容: ${errorBody}`);
            }

            const reader = response.body.getReader();
            const decoder = new TextDecoder();
            let buffer = '';
            let chunkCount = 0;
            let totalBytes = 0;
            let hasReceivedContent = false; // 标记是否收到实际内容
            let emptyResponseDetected = false; // 标记是否检测到空响应
            const startTime = Date.now();

            // 优化的流处理函数 - 使用请求级别的buffer
            const processAndSend = (requestId, data) => {
                if (visibilityManager.isHidden) {
                    // 页面在后台时，批量缓冲数据到请求专属buffer
                    requestBuffer.queue.push(data);

                    // 清除旧timer并设置新的
                    if (requestBuffer.timer) {
                        clearTimeout(requestBuffer.timer);
                    }
                    requestBuffer.timer = setTimeout(() => {
                        if (requestBuffer.queue.length > 0) {
                            const combinedData = requestBuffer.queue.join('');
                            requestBuffer.queue = [];
                            sendToServer(requestId, combinedData);
                        }
                        requestBuffer.timer = null;
                    }, 100);
                } else {
                    // 页面在前台时，立即发送
                    // 先发送请求buffer中的数据（如果有）
                    if (requestBuffer.queue.length > 0) {
                        const bufferedData = requestBuffer.queue.join('');
                        requestBuffer.queue = [];
                        if (requestBuffer.timer) {
                            clearTimeout(requestBuffer.timer);
                            requestBuffer.timer = null;
                        }
                        sendToServer(requestId, bufferedData);
                    }
                    // 然后发送当前数据
                    sendToServer(requestId, data);
                }
            };

            while (true) {
                const { value, done } = await reader.read();
                if (done) {
                    // 检测空响应
                    if (!hasReceivedContent || totalBytes === 0) {
                        emptyResponseDetected = true;
                        console.warn(`[API Bridge] ⚠️ 检测到空响应！请求 ${requestId.substring(0, 8)}，总字节数: ${totalBytes}`);

                        // 如果还有重试机会
                        if (retryCount < MAX_RETRIES) {
                            // 计算指数退避延迟
                            const delay = Math.min(BASE_DELAY * Math.pow(2, retryCount), MAX_DELAY);
                            console.log(`[API Bridge] ⏳ 等待 ${delay/1000} 秒后重试...`);

                            // 发送重试通知给服务器
                            sendToServer(requestId, {
                                retry_info: {
                                    attempt: retryCount + 1,
                                    max_attempts: MAX_RETRIES,
                                    delay: delay,
                                    reason: "Empty response detected"
                                }
                            });

                            // 等待指定时间
                            await new Promise(resolve => setTimeout(resolve, delay));

                            // 递归重试
                            await executeFetchAndStreamBack(requestId, payload, retryCount + 1);
                            return; // 重要：返回避免发送 [DONE]
                        } else {
                            // 超过最大重试次数
                            console.error(`[API Bridge] ❌ 超过最大重试次数 (${MAX_RETRIES})，请求失败: ${requestId.substring(0, 8)}`);
                            sendToServer(requestId, {
                                error: `Empty response after ${MAX_RETRIES} retries. Server may be overloaded.`,
                                final_attempt: true
                            });
                            sendToServer(requestId, "[DONE]");
                            return;
                        }
                    }

                    // 正常响应结束
                    console.log(`[API Bridge] ✅ 请求 ${requestId.substring(0, 8)} 的流已成功结束。`);
                    console.log(`[API Bridge Debug] 流统计: ${chunkCount} 块, ${totalBytes} 字节, 耗时 ${(Date.now() - startTime) / 1000} 秒`);

                    if (retryCount > 0) {
                        console.log(`[API Bridge] 🎉 重试成功！在第 ${retryCount + 1} 次尝试时获得有效响应。`);
                    }

                    // 发送请求buffer中的剩余数据
                    if (requestBuffer.queue.length > 0) {
                        const remainingData = requestBuffer.queue.join('');
                        requestBuffer.queue = [];
                        if (requestBuffer.timer) {
                            clearTimeout(requestBuffer.timer);
                            requestBuffer.timer = null;
                        }
                        sendToServer(requestId, remainingData);
                    }

                    // 如果还有未发送的buffer数据
                    if (buffer) {
                        sendToServer(requestId, buffer);
                    }

                    sendToServer(requestId, "[DONE]");
                    break;
                }

                chunkCount++;
                totalBytes += value.length;

                const chunk = decoder.decode(value, { stream: true });
                buffer += chunk;

                // 解析文本块和思维链块（移除延迟机制）
                // 匹配 [ab]0: (正文), ag: (思维链), [ab]2: (图片), [ab]d: (完成)
                const contentPattern = /(?:[ab]0|ag|[ab]2|[ab]d):"((?:\\.|[^"\\])*)"|(?:[ab]d:\{[^}]*\})/g;
                let match;
                let lastIndex = 0;

                while ((match = contentPattern.exec(buffer)) !== null) {
                    const matchedBlock = match[0];
                    const blockToSend = buffer.substring(lastIndex, match.index + matchedBlock.length);

                    // 标记已收到内容
                    hasReceivedContent = true;

                    // 使用优化的发送机制（无延迟）
                    processAndSend(requestId, blockToSend);

                    lastIndex = match.index + matchedBlock.length;
                }

                // 更新buffer
                if (lastIndex > 0) {
                    buffer = buffer.substring(lastIndex);
                }

                // 如果buffer太大，发送出去
                if (buffer.length > 10000) {
                    processAndSend(requestId, buffer);
                    buffer = '';
                }

                // 使用 requestAnimationFrame 替代 setTimeout（仅在前台时）
                if (!visibilityManager.isHidden) {
                    await new Promise(resolve => {
                        if ('requestAnimationFrame' in window) {
                            requestAnimationFrame(() => resolve());
                        } else {
                            resolve();
                        }
                    });
                }
            }

        } catch (error) {
            console.error(`[API Bridge] ❌ 在为请求 ${requestId.substring(0, 8)} 执行 fetch 时出错:`, error);

            // 判断是否应该重试
            const shouldRetry = (
                retryCount < MAX_RETRIES &&
                (error.message.includes('NetworkError') ||
                 error.message.includes('Failed to fetch') ||
                 error.message.includes('502') ||
                 error.message.includes('503') ||
                 error.message.includes('504'))
            );

            if (shouldRetry) {
                // 计算指数退避延迟
                const delay = Math.min(BASE_DELAY * Math.pow(2, retryCount), MAX_DELAY);
                console.log(`[API Bridge] ⏳ 网络错误，等待 ${delay/1000} 秒后重试...`);

                // 发送重试通知
                sendToServer(requestId, {
                    retry_info: {
                        attempt: retryCount + 1,
                        max_attempts: MAX_RETRIES,
                        delay: delay,
                        reason: error.message
                    }
                });

                // 等待并重试
                await new Promise(resolve => setTimeout(resolve, delay));
                await executeFetchAndStreamBack(requestId, payload, retryCount + 1);
                return;
            }

            // 清理请求buffer
            if (requestBuffer.queue.length > 0) {
                const remainingData = requestBuffer.queue.join('');
                requestBuffer.queue = [];
                if (requestBuffer.timer) {
                    clearTimeout(requestBuffer.timer);
                    requestBuffer.timer = null;
                }
                sendToServer(requestId, remainingData);
            }

            sendToServer(requestId, {
                error: error.message,
                retry_count: retryCount,
                final_error: true
            });
        } finally {
            // 请求结束后，无论成功与否，都清理资源
            window.isApiBridgeRequest = false;
            // 清理请求级别的buffer和timer
            requestBuffer.queue = [];
            if (requestBuffer.timer) {
                clearTimeout(requestBuffer.timer);
                requestBuffer.timer = null;
            }
        }
    }

    function sendToServer(requestId, data) {
        if (socket && socket.readyState === WebSocket.OPEN) {
            const message = {
                request_id: requestId,
                data: data
            };
            socket.send(JSON.stringify(message));
        } else {
            console.error("[API Bridge] 无法发送数据，WebSocket 连接未打开。");
        }
    }

    // --- 网络请求拦截 ---
    const originalFetch = window.fetch;
    window.fetch = function(...args) {
        const urlArg = args[0];
        let urlString = '';

        // 确保我们总是处理字符串形式的 URL
        if (urlArg instanceof Request) {
            urlString = urlArg.url;
        } else if (urlArg instanceof URL) {
            urlString = urlArg.href;
        } else if (typeof urlArg === 'string') {
            urlString = urlArg;
        }

        // 仅在 URL 是有效字符串时才进行匹配
        if (urlString) {
            const match = urlString.match(/\/nextjs-api\/stream\/retry-evaluation-session-message\/([a-f0-9-]+)\/messages\/([a-f0-9-]+)/);

            // 仅在请求不是由API桥自身发起，且捕获模式已激活时，才更新ID
            if (match && !window.isApiBridgeRequest && isCaptureModeActive) {
                const sessionId = match[1];
                const messageId = match[2];
                console.log(`[API Bridge Interceptor] 🎯 在激活模式下捕获到ID！正在发送...`);

                // 关闭捕获模式，确保只发送一次
                isCaptureModeActive = false;
                if (document.title.startsWith("🎯 ")) {
                    document.title = document.title.substring(2);
                }

                // 异步将捕获到的ID发送到本地的 id_updater.py 脚本
                fetch('http://127.0.0.1:5103/update', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ sessionId, messageId })
                })
                .then(response => {
                    if (!response.ok) throw new Error(`Server responded with status: ${response.status}`);
                    console.log(`[API Bridge] ✅ ID 更新成功发送。捕获模式已自动关闭。`);
                })
                .catch(err => {
                    console.error('[API Bridge] 发送ID更新时出错:', err.message);
                    // 即使发送失败，捕获模式也已关闭，不会重试。
                });
            }
        }

        // 调用原始的 fetch 函数，确保页面功能不受影响
        return originalFetch.apply(this, args);
    };


    // --- 页面源码发送 ---
    async function sendPageSource() {
        try {
            const htmlContent = document.documentElement.outerHTML;
            await fetch('http://localhost:5102/internal/update_available_models', { // 新的端点
                method: 'POST',
                headers: {
                    'Content-Type': 'text/html; charset=utf-8'
                },
                body: htmlContent
            });
             console.log("[API Bridge] 页面源码已成功发送。");
        } catch (e) {
            console.error("[API Bridge] 发送页面源码失败:", e);
        }
    }

    // --- 启动连接 ---
    console.log("========================================");
    console.log("  LMArena API Bridge v2.7 正在运行。");
    console.log(`  📋 标签页ID: ${TAB_ID}`);
    console.log("  ✅ 支持多标签页并发（绕过6连接限制）");
    console.log("  ✅ 修复了标签页后台时的流式响应冻结问题");
    console.log("  ✅ 新增：自动重试机制处理空响应");
    console.log("  - 聊天功能已连接到 ws://localhost:5102");
    console.log("  - ID 捕获器将发送到 http://localhost:5103");
    console.log("  💡 提示：打开多个标签页可提升并发能力");
    console.log("     - 1个标签页 = 6并发");
    console.log("     - 2个标签页 = 12并发");
    console.log("     - 3个标签页 = 18并发");
    console.log("========================================");

    connect(); // 建立 WebSocket 连接

})();
