use crate::{ApiError, tools::Output, usage::Usage};
use anyhow::Result;
use axum::{
    Json,
    body::{Body, Bytes},
    http::{HeaderValue, header::CONTENT_TYPE},
    response::{
        IntoResponse, Response, Sse,
        sse::{Event, KeepAlive},
    },
};
use futures_util::{Stream, stream};
use serde_json::Value;
use std::{
    convert::Infallible,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

pub struct Completion {
    pub id: String,
    pub model: String,
    pub created: u64,
}

impl Completion {
    pub fn new(prefix: &str, model: String) -> Self {
        Self {
            id: format!("{prefix}{}", Uuid::new_v4()),
            model,
            created: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }
}

pub struct Collected {
    pub text: String,
    pub image: Option<Value>,
    pub usage: Option<Usage>,
    pub tool: Option<(Value, oneshot::Sender<()>)>,
}

pub async fn collect(mut receiver: mpsc::Receiver<Result<Output>>) -> Result<Collected> {
    let mut reply = Collected {
        text: String::new(),
        image: None,
        usage: None,
        tool: None,
    };
    while let Some(next) = receiver.recv().await {
        match next? {
            Output::Text(text) => append(&mut reply.text, &text)?,
            Output::Image(image) => reply.image = Some(image),
            Output::Usage(usage) => reply.usage = Some(usage),
            Output::Tool(call, delivered) => {
                reply.tool = Some((call, delivered));
                break;
            }
        }
    }
    Ok(reply)
}

pub fn append(target: &mut String, text: &str) -> Result<()> {
    anyhow::ensure!(
        target.len() + text.len() <= 8 * 1024 * 1024,
        "Codex output exceeded 8 MiB"
    );
    target.push_str(text);
    Ok(())
}

pub fn json_response(payload: Value, handoff: Option<oneshot::Sender<()>>) -> Response {
    let Some(delivered) = handoff else {
        return Json(payload).into_response();
    };
    let bytes = Bytes::from(payload.to_string());
    let body = Body::from_stream(stream::once(async move {
        let _ = delivered.send(());
        Ok::<_, Infallible>(bytes)
    }));
    ([(CONTENT_TYPE, "application/json")], body).into_response()
}

pub fn sse_response(
    events: impl Stream<Item = Result<Event, Infallible>> + Send + 'static,
) -> Response {
    let mut response = Sse::new(events)
        .keep_alive(KeepAlive::default())
        .into_response();
    response
        .headers_mut()
        .insert("x-accel-buffering", HeaderValue::from_static("no"));
    response
}

pub fn turn_error(error: anyhow::Error) -> ApiError {
    if error
        .downcast_ref::<tokio::time::error::Elapsed>()
        .is_some()
    {
        ApiError::timed_out()
    } else {
        ApiError::upstream(error)
    }
}
