# Windows 启动

以下命令在本项目目录中使用 PowerShell 7 运行。先确认 `codex login status` 成功。

```powershell
cargo build --release --locked
$env:CODEX_HOST_CWD = (Get-Location).Path
$env:CODEX_HOST_IMAGE_DIR = (New-Item -ItemType Directory -Force images).FullName
$env:CODEX_WS_TOKEN = [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(32))
$env:PROXY_API_KEY = [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(32))
$digest = [Convert]::ToHexString([Security.Cryptography.SHA256]::HashData(
    [Text.Encoding]::UTF8.GetBytes($env:CODEX_WS_TOKEN)
)).ToLowerInvariant()

$codexProcess = Start-Process -FilePath (Get-Command codex).Source -WindowStyle Hidden -PassThru -ArgumentList @(
    'app-server', '--listen', 'ws://127.0.0.1:4500',
    '--ws-auth', 'capability-token', '--ws-token-sha256', $digest
)
try {
    .\target\release\codex-host-proxy.exe
} finally {
    Stop-Process -Id $codexProcess.Id -ErrorAction SilentlyContinue
}
```

从另一个 PowerShell 终端使用相同的 `PROXY_API_KEY` 验证代理：

```powershell
$headers = @{ Authorization = "Bearer $env:PROXY_API_KEY" }
$models = Invoke-RestMethod http://127.0.0.1:8787/v1/models -Headers $headers
$models.data | Select-Object id
$model = $models.data[0].id
$body = @{
    model = $model
    stream = $true
    messages = @(@{ role = 'user'; content = '请逐句解释 SSE。' })
} | ConvertTo-Json -Depth 8
$body | curl.exe -N -H "Authorization: Bearer $env:PROXY_API_KEY" -H 'Content-Type: application/json' --data-binary '@-' http://127.0.0.1:8787/v1/chat/completions
```
