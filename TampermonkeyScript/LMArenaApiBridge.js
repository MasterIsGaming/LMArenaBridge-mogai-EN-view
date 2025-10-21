// ==UserScript==
// @name LMArena API Bridge test.3
// @namespace http://tampermonkey.net/
// @version 2.7
// @description Bridges LMArena to a local API server via WebSocket. Supports multi-tab concurrent requests to bypass browser HTTP/1.1 limits.
// @author Lianues
// @match https://lmarena.ai/*
// @match https://*.lmarena.ai/*
// @icon https://www.google.com/s2/favicons?sz=64&domain=lmarena.ai
// @grant none
// @run-at document-end
// ==/UserScript==

(function () {
 'use strict';

 // --- Configuration ---
 const SERVER_URL = "ws://localhost:5102/ws"; // Match the port in api_server.py
 let socket;
 let isCaptureModeActive = false; // Switch for ID capture mode

 // --- Generate a unique tab ID ---
 const TAB_ID = `tab_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
 console.log(`[API Bridge] This tab's ID: ${TAB_ID}`);

 // --- Page Visibility Management ---
 const visibilityManager = {
 isHidden: document.hidden,
 bufferQueue: [],
 bufferTimer: null,

 init() {
 document.addEventListener('visibilitychange', () => {
 this.isHidden = document.hidden;
 console.log(`[API Bridge] Page visibility changed: ${document.visibilityState} (hidden=${this.isHidden})`);

 // When the page becomes visible, immediately send the buffered data
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

 // Send the combined data directly
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

 // --- Initialize Page Visibility Management ---
 visibilityManager.init();

 // --- Core Logic ---
 function connect() {
 console.log(`[API Bridge] Connecting to local server: ${SERVER_URL}…`);
 socket = new WebSocket(SERVER_URL);

 socket.onopen = () => {
 console.log("[API Bridge] ✅ WebSocket connection with local server established.");
 console.log(`[API Bridge] Sending tab ID: ${TAB_ID}`);

 // Immediately send the tab ID to the server
 socket.send(JSON.stringify({ tab_id: TAB_ID }));

 document.title = "✅ " + document.title;
 };

 socket.onmessage = async (event) => {
 try {
 const message = JSON.parse(event.data);

 // Check if it's a command, not a standard chat request
 if (message.command) {
 console.log(`[API Bridge] ⬇️ Received command: ${message.command}`);
 if (message.command === 'refresh' || message.command === 'reconnect') {
 console.log(`[API Bridge] Received '${message.command}' command, executing page refresh…`);
 location.reload();
 } else if (message.command === 'activate_id_capture') {
 console.log("[API Bridge] ✅ ID capture mode activated. Please trigger a 'Retry' action on the page.");
 isCaptureModeActive = true;
 // Optionally provide a visual cue to the user
 document.title = "🎯 " + document.title;
 } else if (message.command === 'send_page_source') {
 console.log("[API Bridge] Received command to send page source, sending…");
 sendPageSource();
 }
 return;
 }

 const { request_id, payload } = message;

 if (!request_id || !payload) {
 console.error("[API Bridge] Received invalid message from server:", message);
 return;
 }

 console.log(`[API Bridge] ⬇️ Received chat request ${request_id.substring(0, 8)}. Preparing to execute fetch operation.`);
 await executeFetchAndStreamBack(request_id, payload);

 } catch (error) {
 console.error("[API Bridge] Error processing server message:", error);
 }
 };

 socket.onclose = () => {
 console.warn("[API Bridge] 🔌 Connection to local server lost. Will attempt to reconnect in 5 seconds…");
 if (document.title.startsWith("✅ ")) {
 document.title = document.title.substring(2);
 }
 setTimeout(connect, 5000);
 };

 socket.onerror = (error) => {
 console.error("[API Bridge] ❌ WebSocket error occurred:", error);
 socket.close(); // This will trigger the reconnection logic in onclose
 };
 }

 async function executeFetchAndStreamBack(requestId, payload, retryCount = 0) {
 console.log(`[API Bridge] Current operating domain: ${window.location.hostname}`);
 const { is_image_request, message_templates, target_model_id, session_id, message_id } = payload;

 // Retry configuration
 const MAX_RETRIES = 5;
 const BASE_DELAY = 1000; // 1-second base delay
 const MAX_DELAY = 30000; // Max delay 30 seconds

 if (retryCount > 0) {
 console.log(`[API Bridge] 🔄 Retrying request ${requestId.substring(0, 8)}, retry count: ${retryCount}/${MAX_RETRIES}`);
 }

 // Key fix: Create an independent buffer for each request to avoid content crosstalk during concurrency
 const requestBuffer = {
 queue: [],
 timer: null
 };

 // --- Use session information passed from the backend ---
 if (!session_id || !message_id) {
 const errorMsg = "Session information (session_id or message_id) received from the backend is empty. Please run the `id_updater.py` script first to set it up.";
 console.error(`[API Bridge] ${errorMsg}`);
 sendToServer(requestId, { error: errorMsg });
 sendToServer(requestId, "[DONE]");
 return;
 }

 // The URL is the same for chat and text-to-image generation
 const apiUrl = `/nextjs-api/stream/retry-evaluation-session-message/${session_id}/messages/${message_id}`;
 const httpMethod = 'PUT';

 console.log(`[API Bridge] Using API endpoint: ${apiUrl}`);

 const newMessages = [];
 let lastMsgIdInChain = null;

 if (!message_templates || message_templates.length === 0) {
 const errorMsg = "The message list received from the backend is empty.";
 console.error(`[API Bridge] ${errorMsg}`);
 sendToServer(requestId, { error: errorMsg });
 sendToServer(requestId, "[DONE]");
 return;
 }

 // This loop logic is common for both chat and text-to-image, as the backend has already prepared the correct message_templates
 for (let i = 0; i < message_templates.length; i++) {
 const template = message_templates[i];
 const currentMsgId = crypto.randomUUID();
 const parentIds = lastMsgIdInChain ? [lastMsgIdInChain] : [];

 // If it's a text-to-image request, the status is always 'success'
 // Otherwise, only the last message is 'pending'
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

 console.log("[API Bridge] Final payload ready to be sent to LMArena API:", JSON.stringify(body, null, 2));

 // Set a flag so our fetch interceptor knows this request was initiated by the script itself
 window.isApiBridgeRequest = true;
 try {
 const response = await fetch(apiUrl, {
 method: httpMethod,
 headers: {
 'Content-Type': 'text/plain;charset=UTF-8', // LMArena uses text/plain
 'Accept': '*/*',
 },
 body: JSON.stringify(body),
 credentials: 'include' // Must include cookies
 });

 if (!response.ok || !response.body) {
 const errorBody = await response.text();
 throw new Error(`Network response was not ok. Status: ${response.status}. Content: ${errorBody}`);
 }

 const reader = response.body.getReader();
 const decoder = new TextDecoder();
 let buffer = '';
 let chunkCount = 0;
 let totalBytes = 0;
 let hasReceivedContent = false; // Flag to mark if actual content has been received
 let emptyResponseDetected = false; // Flag to mark if an empty response was detected
 const startTime = Date.now();

 // Optimized stream processing function - uses a request-level buffer
 const processAndSend = (requestId, data) => {
 if (visibilityManager.isHidden) {
 // When the page is in the background, batch buffer data into the request-specific buffer
 requestBuffer.queue.push(data);

 // Clear the old timer and set a new one
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
 // When the page is in the foreground, send immediately
 // First, send data from the request buffer (if any)
 if (requestBuffer.queue.length > 0) {
 const bufferedData = requestBuffer.queue.join('');
 requestBuffer.queue = [];
 if (requestBuffer.timer) {
 clearTimeout(requestBuffer.timer);
 requestBuffer.timer = null;
 }
 sendToServer(requestId, bufferedData);
 }
 // Then send the current data
 sendToServer(requestId, data);
 }
 };

 while (true) {
 const { value, done } = await reader.read();
 if (done) {
 // Detect empty response
 if (!hasReceivedContent || totalBytes === 0) {
 emptyResponseDetected = true;
 console.warn(`[API Bridge] ⚠️ Empty response detected! Request ${requestId.substring(0, 8)}, total bytes: ${totalBytes}`);

 // If there are still retry attempts left
 if (retryCount < MAX_RETRIES) {
 // Calculate exponential backoff delay
 const delay = Math.min(BASE_DELAY * Math.pow(2, retryCount), MAX_DELAY);
 console.log(`[API Bridge] ⏳ Retrying after ${delay/1000} seconds…`);

 // Send retry notification to the server
 sendToServer(requestId, {
 retry_info: {
 attempt: retryCount + 1,
 max_attempts: MAX_RETRIES,
 delay: delay,
 reason: "Empty response detected"
 }
 });

 // Wait for the specified time
 await new Promise(resolve => setTimeout(resolve, delay));

 // Recursive retry
 await executeFetchAndStreamBack(requestId, payload, retryCount + 1);
 return; // Important: return to avoid sending [DONE]
 } else {
 // Exceeded max retry attempts
 console.error(`[API Bridge] ❌ Exceeded max retries (${MAX_RETRIES}), request failed: ${requestId.substring(0, 8)}`);
 sendToServer(requestId, {
 error: `Empty response after ${MAX_RETRIES} retries. Server may be overloaded.`,
 final_attempt: true
 });
 sendToServer(requestId, "[DONE]");
 return;
 }
 }

 // Normal response end
 console.log(`[API Bridge] ✅ Stream for request ${requestId.substring(0, 8)} has ended successfully.`);
 console.log(`[API Bridge Debug] Stream stats: ${chunkCount} chunks, ${totalBytes} bytes, duration ${(Date.now() - startTime) / 1000} seconds`);

 if (retryCount > 0) {
 console.log(`[API Bridge] 🎉 Retry successful! Got a valid response on attempt ${retryCount + 1}.`);
 }

 // Send remaining data from the request buffer
 if (requestBuffer.queue.length > 0) {
 const remainingData = requestBuffer.queue.join('');
 requestBuffer.queue = [];
 if (requestBuffer.timer) {
 clearTimeout(requestBuffer.timer);
 requestBuffer.timer = null;
 }
 sendToServer(requestId, remainingData);
 }

 // If there is still unsent buffer data
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

 // Parse text blocks and chain-of-thought blocks (delay mechanism removed)
 // Match [ab]0: (body), ag: (chain-of-thought), [ab]2: (image), [ab]d: (done)
 const contentPattern = /(?:[ab]0|ag|[ab]2|[ab]d):"((?:\\.|[^"\\])*)"|(?:[ab]d:\{[^}]*\})/g;
 let match;
 let lastIndex = 0;

 while ((match = contentPattern.exec(buffer)) !== null) {
 const matchedBlock = match[0];
 const blockToSend = buffer.substring(lastIndex, match.index + matchedBlock.length);

 // Mark that content has been received
 hasReceivedContent = true;

 // Use the optimized sending mechanism (no delay)
 processAndSend(requestId, blockToSend);

 lastIndex = match.index + matchedBlock.length;
 }

 // Update the buffer
 if (lastIndex > 0) {
 buffer = buffer.substring(lastIndex);
 }

 // If the buffer is too large, send it out
 if (buffer.length > 10000) {
 processAndSend(requestId, buffer);
 buffer = '';
 }

 // Use requestAnimationFrame instead of setTimeout (only when in foreground)
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
 console.error(`[API Bridge] ❌ Error executing fetch for request ${requestId.substring(0, 8)}:`, error);

 // Determine if a retry is needed
 const shouldRetry = (
 retryCount < MAX_RETRIES &&
 (error.message.includes('NetworkError') ||
 error.message.includes('Failed to fetch') ||
 error.message.includes('502') ||
 error.message.includes('503') ||
 error.message.includes('504'))
 );

 if (shouldRetry) {
 // Calculate exponential backoff delay
 const delay = Math.min(BASE_DELAY * Math.pow(2, retryCount), MAX_DELAY);
 console.log(`[API Bridge] ⏳ Network error, retrying after ${delay/1000} seconds…`);

 // Send retry notification
 sendToServer(requestId, {
 retry_info: {
 attempt: retryCount + 1,
 max_attempts: MAX_RETRIES,
 delay: delay,
 reason: error.message
 }
 });

 // Wait and retry
 await new Promise(resolve => setTimeout(resolve, delay));
 await executeFetchAndStreamBack(requestId, payload, retryCount + 1);
 return;
 }

 // Clear the request buffer
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
 // After the request ends, clean up resources regardless of success or failure
 window.isApiBridgeRequest = false;
 // Clean up the request-level buffer and timer
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
 console.error("[API Bridge] Cannot send data, WebSocket connection is not open.");
 }
 }

 // --- Network Request Interception ---
 const originalFetch = window.fetch;
 window.fetch = function(…args) {
 const urlArg = args[0];
 let urlString = '';

 // Ensure we always handle the URL as a string
 if (urlArg instanceof Request) {
 urlString = urlArg.url;
 } else if (urlArg instanceof URL) {
 urlString = urlArg.href;
 } else if (typeof urlArg === 'string') {
 urlString = urlArg;
 }

 // Only match if the URL is a valid string
 if (urlString) {
 const match = urlString.match(/\/nextjs-api\/stream\/retry-evaluation-session-message\/([a-f0-9-]+)\/messages\/([a-f0-9-]+)/);

 // Only update the ID if the request was not initiated by the API bridge itself and capture mode is active
 if (match && !window.isApiBridgeRequest && isCaptureModeActive) {
 const sessionId = match[1];
 const messageId = match[2];
 console.log(`[API Bridge Interceptor] 🎯 Captured IDs in active mode! Sending…`);

 // Deactivate capture mode to ensure it's only sent once
 isCaptureModeActive = false;
 if (document.title.startsWith("🎯 ")) {
 document.title = document.title.substring(2);
 }

 // Asynchronously send the captured IDs to the local id_updater.py script
 fetch('http://127.0.0.1:5103/update', {
 method: 'POST',
 headers: { 'Content-Type': 'application/json' },
 body: JSON.stringify({ sessionId, messageId })
 })
 .then(response => {
 if (!response.ok) throw new Error(`Server responded with status: ${response.status}`);
 console.log(`[API Bridge] ✅ ID update sent successfully. Capture mode has been automatically deactivated.`);
 })
 .catch(err => {
 console.error('[API Bridge] Error sending ID update:', err.message);
 // Even if sending fails, capture mode is deactivated and will not be retried.
 });
 }
 }

 // Call the original fetch function to ensure page functionality is not affected
 return originalFetch.apply(this, args);
 };

 // --- Send Page Source ---
 async function sendPageSource() {
 try {
 const htmlContent = document.documentElement.outerHTML;
 await fetch('http://localhost:5102/internal/update_available_models', { // New endpoint
 method: 'POST',
 headers: {
 'Content-Type': 'text/html; charset=utf-8'
 },
 body: htmlContent
 });
 console.log("[API Bridge] Page source sent successfully.");
 } catch (e) {
 console.error("[API Bridge] Failed to send page source:", e);
 }
 }

 // --- Start Connection ---
 console.log("========================================");
 console.log(" LMArena API Bridge v2.7 is running.");
 console.log(` 📋 Tab ID: ${TAB_ID}`);
 console.log(" ✅ Supports multi-tab concurrency (bypasses 6-connection limit)");
 console.log(" ✅ Fixed streaming response freeze when tab is in the background");
 console.log(" ✅ New: Automatic retry mechanism to handle empty responses");
 console.log(" - Chat functionality connected to ws://localhost:5102");
 console.log(" - ID capturer will send to http://localhost:5103");
 console.log(" 💡 Tip: Open multiple tabs to increase concurrency");
 console.log(" - 1 tab = 6 concurrency");
 console.log(" - 2 tabs = 12 concurrency");
 console.log(" - 3 tabs = 18 concurrency");
 console.log("========================================");

 connect(); // Establish WebSocket connection

})();
