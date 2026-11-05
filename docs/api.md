# API 与客户端

OpenAI SDK 的 base URL 为 `http://127.0.0.1:8787/v1`，API key 用 `PROXY_API_KEY`。图片通过 user 消息里的 `image_url.url` 传入 PNG、JPEG 或 WebP 的 base64 data URL，单请求最多 16 张、解码后合计 10 MiB。请用支持图片输入的模型。代理在 `CODEX_HOST_IMAGE_DIR` 下创建临时文件，并将实际文件路径交给 Codex。建议两个进程使用同一普通用户，以便 Codex 读取临时文件。只处理文本时可省略该变量。

新对话请求创建临时 Codex thread。系统和开发者消息按请求顺序合并到 Codex 的 `developerInstructions`，普通历史消息按角色标记后作为对话上下文传入。代理一次处理一个 Codex turn，工作目录由宿主机配置，使用 Codex 的只读沙箱。正文增量以 SSE 发送，客户端在生成期间断开时代理尝试取消 turn。支持 `GET /v1/models`、`POST /v1/chat/completions`、`POST /v1/responses` 和进程探针 `GET /healthz`。

## Responses API

DSH 自定义模型 API 可以选择 **OpenAI Responses**（`openai-responses`），Base URL 仍填 `http://127.0.0.1:8787/v1`。可用下面的 PowerShell 请求检查 Responses SSE。沿用启动时设置的密钥，并将模型 ID 换成 `/v1/models` 返回的 ID。

```powershell
'{"model":"gpt-6.1-sol","input":"Reply with LOCAL_OK.","stream":true,"store":false}' |
    curl.exe -N --fail-with-body -H "Authorization: Bearer $env:PROXY_API_KEY" -H 'Content-Type: application/json' --data-binary '@-' http://127.0.0.1:8787/v1/responses
```

支持字符串或消息项形式的 `input`、`instructions`、base64 图片、SSE 和 JSON 响应。普通多轮对话可重发完整历史，也可传上一次的 `previous_response_id` 加新增输入。客户端收到 `function_call` 后，通过 `function_call_output.call_id` 提交文本结果。此时可带上发出调用的 `previous_response_id`，代理会验证它与等待中的调用匹配。

代理默认在内存中保留最近 64 个响应及其重放历史，合计最多 8 MiB。可设置 `RESPONSES_STATE_FILE`，将该缓存原子写入本地文件，以便代理重启后保留引用。开启后，请把文件放在私有目录中，其中可能包含消息、工具结果和图片数据，即使客户端发送 `store: false`。重启会中断正在生成或等待工具的 turn。引用被淘汰时，客户端需要重发完整历史。历史 `reasoning` 项只保留文本摘要。OpenAI 的 `store: true`、后台请求、Conversations 和结构化输出超出当前实现范围。

单条完整历史超过状态容量时，只保留能放入缓存的输出项供 `item_reference` 使用，后续增量请求需重发历史。如果输出本身也超过容量，代理正常返回本次结果并跳过缓存。状态文件写入失败时，代理返回 `response_state_write_failed`，流式请求通过 `response.failed` 报告。

Responses 的 `additional_tools` 可声明函数工具。代理把这些定义与顶层 `tools` 合并，在新 Codex thread 启动时一起注册。同名工具使用最后一份定义。工具的逐项加载时机以及在挂起的 turn 中新增工具超出当前实现范围。

Codex 报告的输入、输出、缓存和推理 token 会出现在 JSON 响应的 `usage` 或 SSE 的 `response.completed.response.usage`。Chat Completions 流式客户端可以用 `stream_options.include_usage: true` 获取结束前的用量帧。工具续接按收到的累计用量增量计算，避免重复计数。token/s 由客户端根据用量和耗时计算。

## 客户端工具调用

OpenCode 的自定义 provider 可使用 `@ai-sdk/openai-compatible`（新版配置用对应的 OpenAI-compatible provider），base URL 和 API key 与上面相同。将 `/v1/models` 返回的模型 ID 加到客户端配置。

代理把请求中的 function `tools` 注册成 Codex App Server 的实验性动态工具。客户端收到 `tool_calls` 后，在自己的环境执行工具，然后发送末尾带有 `role: "tool"`、原 `tool_call_id` 和文本结果的请求。代理把结果交回原来的 Codex turn。工具等待包含在 `REQUEST_TIMEOUT_SECONDS` 内（默认 300 秒）。等待期间占用一个 turn 槽位。

当前支持串行工具调用及 `auto`、`none` 两种 `tool_choice`。并行工具调用、强制选择特定工具和非文本工具结果超出当前实现范围。
