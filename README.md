# Codex Host Proxy

在同一台机器上运行已登录的 Codex App Server 和 Rust 代理。代理通过本机 WebSocket 连接 Codex，提供 OpenAI Chat Completions、Responses 和模型列表。

## 文档

- [Linux 部署与密钥配置](docs/linux.md)
- [OpenRC 服务](docs/openrc.md)
- [systemd 服务](docs/systemd.md)
- [Windows 启动](docs/windows.md)
- [API 与客户端接入](docs/api.md)

代理默认监听 `127.0.0.1:8787`。OpenAI SDK 的 Base URL 为 `http://127.0.0.1:8787/v1`，API key 使用 `PROXY_API_KEY`。

## 发布

GitHub Actions 在分支推送和拉取请求上运行测试与 Release 模式编译。将版本标签推送到 GitHub 后，工作流会创建 Release，上传 Linux x86_64 和 ARM64（musl）、Windows x86_64 压缩包及 `SHA256SUMS`。

```sh
git tag v0.1.0
git push origin v0.1.0
```
