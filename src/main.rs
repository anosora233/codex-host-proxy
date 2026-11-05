mod codex;
mod completions;
mod input;
mod reply;
mod response_state;
mod responses;
#[cfg(test)]
mod tests;
mod tools;
mod usage;

use anyhow::{Context, Result, bail};
use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Request, State, rejection::JsonRejection},
    http::StatusCode,
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use codex::Codex;
use input::Chat;
use serde_json::{Value, json};
use std::{env, path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    net::TcpListener,
    sync::{Semaphore, mpsc},
    time::{Instant, timeout_at},
};
use tools::{Continuation, Output, PendingTool, output_channel};

struct Config {
    ws_url: String,
    ws_token: String,
    api_key: String,
    cwd: String,
    images: Option<(PathBuf, String)>,
    timeout: Duration,
}

impl Config {
    fn from_env() -> Result<Self> {
        let required = |name| {
            env::var(name)
                .ok()
                .filter(|value| !value.trim().is_empty())
                .with_context(|| format!("Set {name}"))
        };
        let cwd = required("CODEX_HOST_CWD")?;
        if !windows_absolute(&cwd) {
            bail!("CODEX_HOST_CWD must be an absolute Windows path");
        }
        let images = match (
            env::var_os("PROXY_IMAGE_DIR"),
            env::var("CODEX_HOST_IMAGE_DIR").ok(),
        ) {
            (None, None) => None,
            (Some(local), Some(host))
                if PathBuf::from(&local).is_absolute() && windows_absolute(&host) =>
            {
                Some((local.into(), host))
            }
            _ => bail!("Set both image directories to absolute paths or leave both unset"),
        };
        let seconds = env::var("REQUEST_TIMEOUT_SECONDS")
            .unwrap_or_else(|_| "300".into())
            .parse::<u64>()?;
        if !(1..=3600).contains(&seconds) {
            bail!("REQUEST_TIMEOUT_SECONDS must be between 1 and 3600");
        }
        Ok(Self {
            ws_url: env::var("CODEX_WS_URL")
                .unwrap_or_else(|_| "ws://host.docker.internal:4500".into()),
            ws_token: required("CODEX_WS_TOKEN")?,
            api_key: required("PROXY_API_KEY")?,
            cwd,
            images,
            timeout: Duration::from_secs(seconds),
        })
    }
}

fn windows_absolute(path: &str) -> bool {
    let bytes = path.as_bytes();
    matches!(bytes, [letter, b':', b'\\' | b'/', ..] if letter.is_ascii_alphabetic())
        || path
            .strip_prefix(r"\\")
            .is_some_and(|tail| tail.split('\\').filter(|part| !part.is_empty()).count() >= 2)
}

struct App {
    config: Config,
    chat_slot: Arc<Semaphore>,
    pending_tool: PendingTool,
    response_items: response_state::State,
}

#[derive(Debug)]
struct ApiError(StatusCode, &'static str, Option<&'static str>);

impl ApiError {
    fn bad_request(message: &'static str) -> Self {
        Self(StatusCode::BAD_REQUEST, message, None)
    }
    fn too_large(message: &'static str) -> Self {
        Self(StatusCode::PAYLOAD_TOO_LARGE, message, None)
    }
    fn missing_reference(message: &'static str, code: &'static str) -> Self {
        Self(StatusCode::BAD_REQUEST, message, Some(code))
    }
    fn internal(error: impl std::fmt::Display) -> Self {
        eprintln!("Proxy error: {error}");
        Self(
            StatusCode::INTERNAL_SERVER_ERROR,
            "Proxy could not prepare request",
            None,
        )
    }
    fn upstream(error: impl std::fmt::Display) -> Self {
        eprintln!("Codex error: {error}");
        Self(
            StatusCode::BAD_GATEWAY,
            "Codex upstream request failed",
            None,
        )
    }
    fn timed_out() -> Self {
        Self(StatusCode::GATEWAY_TIMEOUT, "Codex request timed out", None)
    }
    fn payload(&self) -> Value {
        let kind = if self.0.is_server_error() {
            "server_error"
        } else {
            "invalid_request_error"
        };
        let mut payload = json!({ "error": { "message": self.1, "type": kind } });
        if let Some(code) = self.2 {
            payload["error"]["code"] = json!(code);
        }
        payload
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(self.payload())).into_response()
    }
}

#[cfg(test)]
fn router(config: Config) -> Router {
    router_with_state(config, response_state::State::default())
}

fn router_with_state(config: Config, response_items: response_state::State) -> Router {
    let app = Arc::new(App {
        config,
        chat_slot: Arc::new(Semaphore::new(1)),
        pending_tool: PendingTool::default(),
        response_items,
    });
    Router::new()
        .route("/v1/models", get(models))
        .route("/v1/chat/completions", post(completions::create))
        .route("/v1/responses", post(responses::create))
        .route_layer(middleware::from_fn_with_state(app.clone(), authenticate))
        .route(
            "/healthz",
            get(|| async { Json(json!({ "status": "ok" })) }),
        )
        .layer(DefaultBodyLimit::max(16 * 1024 * 1024))
        .with_state(app)
}

async fn authenticate(State(app): State<Arc<App>>, req: Request, next: Next) -> Response {
    let key = req
        .headers()
        .get("authorization")
        .and_then(|header| header.to_str().ok())
        .and_then(|header| header.strip_prefix("Bearer "));
    if key != Some(app.config.api_key.as_str()) {
        return ApiError(StatusCode::UNAUTHORIZED, "Invalid API key", None).into_response();
    }
    next.run(req).await
}

async fn models(State(app): State<Arc<App>>) -> Result<Json<Value>, ApiError> {
    let data = tokio::time::timeout(app.config.timeout, async {
        let mut codex = Codex::connect(&app.config.ws_url, &app.config.ws_token, false).await?;
        codex.models().await
    })
    .await
    .map_err(|_| ApiError::timed_out())?
    .map_err(ApiError::upstream)?;
    let data: Vec<_> = data
        .iter()
        .filter_map(|model| model["id"].as_str())
        .map(|id| json!({ "id": id, "object": "model", "created": 0, "owned_by": "codex" }))
        .collect();
    Ok(Json(json!({ "object": "list", "data": data })))
}

fn json_error(error: JsonRejection) -> ApiError {
    if error.status() == StatusCode::PAYLOAD_TOO_LARGE {
        ApiError::too_large("Request exceeds 16 MiB")
    } else {
        ApiError::bad_request("Invalid request JSON")
    }
}

async fn start_chat(
    app: Arc<App>,
    body: Chat,
    response_id: Option<String>,
    previous_id: Option<&str>,
) -> Result<mpsc::Receiver<Result<Output>>, ApiError> {
    if body.model.is_empty()
        || body.n.is_some_and(|n| n != 1)
        || body.functions.is_some()
        || body.response_format.is_some()
    {
        return Err(ApiError::bad_request(
            "A model is required; legacy functions, response_format and multiple choices are unsupported",
        ));
    }
    let (sender, receiver) = output_channel();
    if let Some(result) = body
        .messages
        .last()
        .filter(|message| message.role == "tool")
    {
        let id = result
            .tool_call_id
            .as_deref()
            .ok_or_else(|| ApiError::bad_request("Missing tool_call_id"))?;
        app.pending_tool.resume_response(
            id,
            &body.model,
            previous_id,
            Continuation::for_response(result.tool_result()?, sender, response_id),
        )?;
        return Ok(receiver);
    }
    let definitions = tools::definitions(body.tools, body.tool_choice)?;
    let tools_enabled = !definitions.is_empty();
    // The owned permit keeps the slot occupied until cancellation and cleanup finish.
    let permit = app.chat_slot.clone().try_acquire_owned().map_err(|_| {
        ApiError(
            StatusCode::TOO_MANY_REQUESTS,
            "A Codex chat is already running",
            None,
        )
    })?;
    let deadline = Instant::now() + app.config.timeout;
    let config = &app.config;
    let (mut codex, thread_id, input, images) = timeout_at(deadline, async {
        let mut codex = Codex::connect(&config.ws_url, &config.ws_token, tools_enabled)
            .await
            .map_err(ApiError::upstream)?;
        let models = codex.models().await.map_err(ApiError::upstream)?;
        let model = models
            .iter()
            .find(|model| model["id"] == body.model)
            .ok_or(ApiError(
                StatusCode::NOT_FOUND,
                "Model is unavailable in Codex",
                None,
            ))?;
        let (messages, instructions) = input::split_instructions(body.messages)?;
        let (input, images) = input::prepare(messages, model, config).await?;
        let mut params = json!({
            "model": model["model"].as_str().unwrap_or(&body.model),
            "cwd": config.cwd, "ephemeral": true, "approvalPolicy": "never", "sandbox": "read-only"
        });
        if tools_enabled {
            params["dynamicTools"] = json!(definitions);
        }
        if let Some(instructions) = instructions {
            params["developerInstructions"] = json!(instructions);
        }
        let result = codex
            .call("thread/start", params)
            .await
            .map_err(ApiError::upstream)?;
        let id = result["thread"]["id"]
            .as_str()
            .context("Missing thread ID")
            .map_err(ApiError::upstream)?;
        Ok::<_, ApiError>((codex, id.to_owned(), input, images))
    })
    .await
    .map_err(|_| ApiError::timed_out())??;
    tokio::spawn(async move {
        let mut sender = sender;
        let tools = tools_enabled.then_some((&app.pending_tool, body.model.as_str(), response_id));
        let result = codex
            .turn(&thread_id, input, deadline, &mut sender, tools)
            .await;
        app.pending_tool.clear();
        drop(codex);
        drop(images);
        drop(permit);
        match result {
            Ok(Some(usage)) => {
                sender.terminal.send(Ok(Output::Usage(usage)));
            }
            Err(error) => {
                sender.terminal.send(Err(error));
            }
            Ok(None) => {}
        }
    });
    Ok(receiver)
}

#[tokio::main]
async fn main() -> Result<()> {
    let state =
        response_state::State::open(env::var_os("RESPONSES_STATE_FILE").map(PathBuf::from))?;
    let app = router_with_state(Config::from_env()?, state);
    let address = env::var("PROXY_BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:8787".into());
    let listener = TcpListener::bind(&address).await?;
    eprintln!("Proxy listening on {}", listener.local_addr()?);
    axum::serve(listener, app).await?;
    Ok(())
}
