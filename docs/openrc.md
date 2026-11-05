# OpenRC 服务

先完成 [Linux 部署准备](linux.md)。两个服务均由 `supervise-daemon` 直接监督。代理通过 `need codex-app-server` 声明依赖，启动代理时 OpenRC 会先启动 App Server。

- [codex-app-server](openrc/codex-app-server)
- [codex-host-proxy](openrc/codex-host-proxy)

从仓库根目录安装服务：

```sh
sudo install -m 0755 docs/openrc/codex-app-server /etc/init.d/codex-app-server
sudo install -m 0755 docs/openrc/codex-host-proxy /etc/init.d/codex-host-proxy
```

在安装后的文件中替换 `alice`、App Server 的 `HOME`、Codex 可执行文件路径和 `REPLACE_WITH_WS_TOKEN_SHA256`。如果使用自定义登录目录，在 App Server 服务中设置 `CODEX_HOME`。

```sh
sudo rc-update add codex-host-proxy default
sudo rc-service codex-host-proxy start
sudo rc-service codex-app-server status
sudo rc-service codex-host-proxy status
```

只重启代理可执行 `sudo rc-service codex-host-proxy restart`。维护整套服务时，先停止代理，再重启 App Server，然后启动代理。要停止整套服务，按代理、App Server 的顺序停止。启动后按 [Linux 部署说明](linux.md) 查询模型列表。

[OpenRC 进程监督指南](https://github.com/OpenRC/openrc/blob/master/supervise-daemon-guide.md)
