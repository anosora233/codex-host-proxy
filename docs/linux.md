# Linux 部署

在同一台机器上运行 Codex App Server 和代理。以下示例使用普通用户 `alice`。请替换为实际用户名和 HOME，并以该用户确认 `codex login status` 成功。两个进程使用同一用户，以便 Codex 读取代理创建的临时图片。部署前确认本机端口 4500 和 8787 可用。

在项目目录编译并安装代理：

```sh
cargo build --release --locked
sudo install -D -m 0755 target/release/codex-host-proxy /usr/local/bin/codex-host-proxy
sudo install -d -o alice -g "$(id -gn alice)" -m 0700 /var/lib/codex-host-proxy
```

服务文件假定 Codex 可执行文件在 `/usr/local/bin/codex`。如果安装在别处，请使用其绝对路径。

## 密钥与代理配置

分别运行两次 `openssl rand -hex 32`，生成 WebSocket token 和代理 API key。用 `sudoedit /etc/codex-host-proxy.env` 写入配置，并替换占位值：

```sh
CODEX_HOST_CWD='/home/alice'
CODEX_HOST_IMAGE_DIR='/var/lib/codex-host-proxy'
RESPONSES_STATE_FILE='/var/lib/codex-host-proxy/responses.json'
CODEX_WS_TOKEN='REPLACE_WITH_WEBSOCKET_TOKEN'
PROXY_API_KEY='REPLACE_WITH_API_KEY'
```

```sh
sudo chown root:root /etc/codex-host-proxy.env
sudo chmod 0600 /etc/codex-host-proxy.env
sudo sh -c '. /etc/codex-host-proxy.env && printf "%s" "$CODEX_WS_TOKEN" | sha256sum'
```

将最后一条命令输出的 64 位十六进制摘要填入 **App Server** 服务文件的 `REPLACE_WITH_WS_TOKEN_SHA256`。代理读取原始 token，App Server 读取摘要。

只处理文本时可省略 `CODEX_HOST_IMAGE_DIR`。省略 `RESPONSES_STATE_FILE` 时，会话引用只在内存中保留。

安装服务时选择 [OpenRC](openrc.md) 或 [systemd](systemd.md)。启动后，在另一终端将 `PROXY_API_KEY` 设置为同一个值，再检查模型列表：

```sh
curl --fail -H "Authorization: Bearer $PROXY_API_KEY" http://127.0.0.1:8787/v1/models
```

模型列表请求会连到 App Server。`GET /healthz` 只检查代理进程。服务依赖负责启动顺序，代理在收到 API 请求时连接 App Server，刚启动时可稍等片刻再验证。
