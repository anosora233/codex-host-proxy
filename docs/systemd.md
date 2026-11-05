# systemd 服务

先完成 [Linux 部署准备](linux.md)。systemd 直接监督两个前台进程。启动代理会拉起 App Server，并按依赖顺序启动。显式重启 App Server 时，代理通过 `PartOf=` 一并重启。

- [codex-app-server.service](systemd/codex-app-server.service)
- [codex-host-proxy.service](systemd/codex-host-proxy.service)

从仓库根目录安装服务：

```sh
sudo install -m 0644 docs/systemd/codex-app-server.service /etc/systemd/system/codex-app-server.service
sudo install -m 0644 docs/systemd/codex-host-proxy.service /etc/systemd/system/codex-host-proxy.service
```

在安装后的文件中替换 `alice`、App Server 的 `HOME`、Codex 可执行文件路径和 `REPLACE_WITH_WS_TOKEN_SHA256`。如果使用自定义登录目录，在 App Server 的 `[Service]` 中添加 `Environment=CODEX_HOME=/实际路径`。

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now codex-host-proxy.service
sudo systemctl status codex-app-server.service codex-host-proxy.service
sudo journalctl -u codex-app-server.service -u codex-host-proxy.service -f
```

用 `sudo systemctl restart codex-host-proxy.service` 单独重启代理，或用 `sudo systemctl restart codex-app-server.service` 重启整套服务。停止整套服务可执行 `sudo systemctl stop codex-host-proxy.service codex-app-server.service`。启动后按 [Linux 部署说明](linux.md) 查询模型列表。

[systemd 单元依赖](https://www.freedesktop.org/software/systemd/man/latest/systemd.unit.html)
