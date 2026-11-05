use crate::tools::{Output, OutputSender, PendingTool};
use crate::usage::{Usage, UsageTracker};
use anyhow::{Context, Result, bail};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time::{Instant, timeout, timeout_at};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
    tungstenite::{Message, client::IntoClientRequest, protocol::WebSocketConfig},
};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
const RPC_TIMEOUT: Duration = Duration::from_secs(10);

pub struct Codex {
    socket: Socket,
    next_id: u64,
}

impl Codex {
    pub async fn connect(url: &str, token: &str, tools_enabled: bool) -> Result<Self> {
        let mut request = url.into_client_request()?;
        request
            .headers_mut()
            .insert("Authorization", format!("Bearer {token}").parse()?);
        let config = WebSocketConfig::default()
            .max_message_size(Some(8 * 1024 * 1024))
            .max_frame_size(Some(8 * 1024 * 1024));
        let (socket, _) = timeout(
            RPC_TIMEOUT,
            tokio_tungstenite::connect_async_with_config(request, Some(config), false),
        )
        .await??;
        let mut codex = Self { socket, next_id: 0 };
        codex.call("initialize", json!({
            "clientInfo": { "name": "codex_host_proxy", "version": env!("CARGO_PKG_VERSION") },
            "capabilities": { "experimentalApi": tools_enabled }
        })).await?;
        codex.send(json!({ "method": "initialized" })).await?;
        Ok(codex)
    }

    async fn send(&mut self, value: Value) -> Result<()> {
        timeout(
            RPC_TIMEOUT,
            self.socket.send(Message::Text(value.to_string().into())),
        )
        .await??;
        Ok(())
    }

    async fn receive(&mut self) -> Result<Value> {
        loop {
            match self
                .socket
                .next()
                .await
                .context("Codex WebSocket closed")??
            {
                Message::Text(text) => return Ok(serde_json::from_str(&text)?),
                Message::Ping(_) => self.socket.flush().await?,
                Message::Close(_) => bail!("Codex WebSocket closed"),
                _ => {}
            }
        }
    }

    async fn request(&mut self, method: &str, params: Value) -> Result<u64> {
        self.next_id += 1;
        self.send(json!({ "id": self.next_id, "method": method, "params": params }))
            .await?;
        Ok(self.next_id)
    }

    async fn reject_server_request(&mut self, message: &Value) -> Result<bool> {
        if let (Some(id), Some(_)) = (message.get("id"), message.get("method")) {
            self.send(json!({
                "id": id, "error": { "code": -32601, "message": "Unsupported server request" }
            }))
            .await?;
            return Ok(true);
        }
        Ok(false)
    }

    pub async fn call(&mut self, method: &str, params: Value) -> Result<Value> {
        let id = self.request(method, params).await?;
        timeout(RPC_TIMEOUT, async {
            loop {
                let message = self.receive().await?;
                if !self.reject_server_request(&message).await? && message["id"] == id {
                    return response(&message);
                }
            }
        })
        .await?
    }

    pub async fn models(&mut self) -> Result<Vec<Value>> {
        let mut models = Vec::new();
        let mut cursor = Value::Null;
        for _ in 0..20 {
            let page = self
                .call("model/list", json!({ "limit": 100, "cursor": cursor }))
                .await?;
            models.extend(
                page["data"]
                    .as_array()
                    .context("Invalid model list")?
                    .iter()
                    .cloned(),
            );
            if page["nextCursor"].is_null() {
                return Ok(models);
            }
            cursor = page["nextCursor"].clone();
        }
        bail!("Model catalog exceeded pagination limit")
    }

    pub async fn turn(
        &mut self,
        thread: &str,
        input: Vec<Value>,
        deadline: Instant,
        output: &mut OutputSender,
        mut tools: Option<(&PendingTool, &str, Option<String>)>,
    ) -> Result<Option<Usage>> {
        if output.data.is_closed() {
            return Ok(None);
        }
        let request_id = self
            .request("turn/start", json!({ "threadId": thread, "input": input }))
            .await?;
        let mut turn_id = None;
        let mut completed = false;
        let mut usage = UsageTracker::default();
        let mut streamed_items = std::collections::HashSet::new();
        let result = timeout_at(deadline, async {
            loop {
                let message = tokio::select! {
                    _ = output.data.closed() => return Ok(None),
                    message = self.receive() => message?,
                };
                if let Some((pending, model, response_id)) = &mut tools
                    && message["method"] == "item/tool/call"
                    && message["params"]["threadId"] == thread
                {
                    let id = message.get("id").context("Missing tool RPC ID")?;
                    let params = &message["params"];
                    if turn_id.is_none() {
                        turn_id = params["turnId"].as_str().map(str::to_owned);
                    }
                    if let Some(stats) = usage.take()
                        && output.data.send(Ok(Output::Usage(stats))).await.is_err()
                    {
                        return Ok(None);
                    }
                    let (call, mut result) = pending.register(model, params, response_id.as_deref())?;
                    let (handoff, delivered) = tokio::sync::oneshot::channel();
                    if output.data.send(Ok(Output::Tool(call, handoff))).await.is_err()
                        || delivered.await.is_err()
                    {
                        return Ok(None);
                    }
                    let continuation = loop {
                        tokio::select! {
                            result = &mut result => break result?,
                            event = self.receive() => {
                                let event = event?;
                                if event["params"]["threadId"] == thread {
                                    match event["method"].as_str() {
                                        Some("item/tool/call") => bail!("Parallel dynamic tool calls are unsupported"),
                                        Some("thread/tokenUsage/updated") => usage.update(&event["params"]),
                                        Some("turn/completed") => {
                                            completed = true;
                                            bail!("Codex ended the turn while waiting for a tool result");
                                        }
                                        _ => {}
                                    }
                                }
                                self.reject_server_request(&event).await?;
                            }
                        }
                    };
                    let (text, next_output, next_response_id) = continuation.accept();
                    *response_id = next_response_id;
                    *output = next_output;
                    if output.data.is_closed() {
                        return Ok(None);
                    }
                    self.send(json!({
                        "id": id, "result": {
                            "success": true,
                            "contentItems": [{ "type": "inputText", "text": text }]
                        }
                    })).await?;
                    continue;
                }
                if self.reject_server_request(&message).await? {
                    continue;
                }
                if message["id"] == request_id {
                    let started = response(&message)?;
                    turn_id = Some(
                        started["turn"]["id"]
                            .as_str()
                            .context("Missing turn ID")?
                            .to_owned(),
                    );
                }
                let params = &message["params"];
                if params["threadId"] != thread {
                    continue;
                }
                let text = match message["method"].as_str() {
                    Some("turn/started") => {
                        turn_id = params["turn"]["id"].as_str().map(str::to_owned);
                        None
                    }
                    Some("item/agentMessage/delta") => {
                        if let Some(id) = params["itemId"].as_str() {
                            streamed_items.insert(id.to_owned());
                        }
                        params["delta"].as_str()
                    }
                    Some("item/completed") if params["item"]["type"] == "agentMessage" => {
                        let item = &params["item"];
                        item["id"]
                            .as_str()
                            .filter(|id| !streamed_items.contains(*id))
                            .and_then(|_| item["text"].as_str())
                    }
                    Some("thread/tokenUsage/updated") => {
                        usage.update(params);
                        None
                    }
                    Some("turn/completed") => {
                        completed = true;
                        if params["turn"]["status"] != "completed" {
                            bail!("Codex turn ended with status {}", params["turn"]["status"]);
                        }
                        return Ok(usage.take());
                    }
                    _ => None,
                };
                if let Some(text) = text.filter(|text| !text.is_empty())
                    && output.data.send(Ok(Output::Text(text.to_owned()))).await.is_err()
                {
                    return Ok(None);
                }
            }
        })
        .await;
        if !completed {
            let _ = timeout(Duration::from_secs(3), async {
                // A client can disconnect before Codex has returned the turn ID.
                while turn_id.is_none() {
                    let message = self.receive().await?;
                    if message["id"] == request_id {
                        turn_id = response(&message)?["turn"]["id"]
                            .as_str()
                            .map(str::to_owned);
                    } else if message["method"] == "turn/started"
                        && message["params"]["threadId"] == thread
                    {
                        turn_id = message["params"]["turn"]["id"].as_str().map(str::to_owned);
                    }
                }
                self.call(
                    "turn/interrupt",
                    json!({ "threadId": thread, "turnId": turn_id }),
                )
                .await
            })
            .await;
        }
        result.context("Codex turn timed out")?
    }
}

fn response(message: &Value) -> Result<Value> {
    if let Some(result) = message.get("result") {
        return Ok(result.clone());
    }
    bail!("Codex RPC failed: {}", message["error"]["message"]);
}
