use crate::reply::{self, Completion, json_response, sse_response, turn_error};
use crate::{ApiError, App, input::Chat, json_error, start_chat, tools::Output, usage};
use anyhow::Result;
use axum::{
    Json,
    extract::{State, rejection::JsonRejection},
    response::{Response, sse::Event},
};
use futures_util::{StreamExt, stream};
use serde_json::{Value, json};
use std::{convert::Infallible, sync::Arc};
use tokio::sync::mpsc;

pub async fn create(
    State(app): State<Arc<App>>,
    body: Result<Json<Chat>, JsonRejection>,
) -> Result<Response, ApiError> {
    let Json(body) = body.map_err(json_error)?;
    let streaming = body.stream;
    let include_usage = body
        .stream_options
        .as_ref()
        .is_some_and(|options| options.include_usage);
    let completion = Completion::new("chatcmpl-", body.model.clone());
    let receiver = start_chat(app, body, None, None).await?;
    respond(completion, receiver, streaming, include_usage).await
}

async fn respond(
    completion: Completion,
    receiver: mpsc::Receiver<Result<Output>>,
    streaming: bool,
    include_usage: bool,
) -> Result<Response, ApiError> {
    if streaming {
        return Ok(stream_response(completion, receiver, include_usage));
    }
    let reply::Collected { text, usage, tool } =
        reply::collect(receiver).await.map_err(turn_error)?;
    let mut message = json!({ "role": "assistant" });
    message["content"] = if text.is_empty() && tool.is_some() {
        Value::Null
    } else {
        json!(text)
    };
    let (finish, handoff) = if let Some((call, delivered)) = tool {
        message["tool_calls"] = json!([call]);
        ("tool_calls", Some(delivered))
    } else {
        ("stop", None)
    };
    let mut payload = json!({
        "id": completion.id, "model": completion.model, "created": completion.created,
        "object": "chat.completion", "choices": [{
            "index": 0, "message": message, "finish_reason": finish
        }]
    });
    if let Some(usage) = usage {
        payload["usage"] = usage.chat();
    }
    Ok(json_response(payload, handoff))
}

impl Completion {
    fn chunk(&self, delta: Value, finish: Option<&str>, include_usage: bool) -> Event {
        let mut payload = json!({
            "id": self.id, "model": self.model, "created": self.created, "object": "chat.completion.chunk",
            "choices": [{ "index": 0, "delta": delta, "finish_reason": finish }]
        });
        if include_usage {
            payload["usage"] = Value::Null;
        }
        event(payload)
    }
}

fn event(value: Value) -> Event {
    Event::default().data(value.to_string())
}

enum Stage {
    Output,
    ToolFinish,
    Footer,
    Done,
}

fn stream_response(
    completion: Completion,
    receiver: mpsc::Receiver<Result<Output>>,
    include_usage: bool,
) -> Response {
    let first = completion.chunk(json!({ "role": "assistant" }), None, include_usage);
    let chunks = stream::unfold(
        (completion, receiver, Stage::Output, None, None),
        move |(completion, mut receiver, stage, mut handoff, mut usage)| async move {
            let (chunk, next) = match stage {
                Stage::Output => loop {
                    match receiver.recv().await {
                        Some(Ok(Output::Text(content))) => {
                            break (
                                completion.chunk(
                                    json!({ "content": content }),
                                    None,
                                    include_usage,
                                ),
                                Stage::Output,
                            );
                        }
                        Some(Ok(Output::Usage(stats))) => usage = Some(stats),
                        Some(Ok(Output::Tool(mut call, delivered))) => {
                            handoff = Some(delivered);
                            call["index"] = json!(0);
                            break (
                                completion.chunk(
                                    json!({ "tool_calls": [call] }),
                                    None,
                                    include_usage,
                                ),
                                Stage::ToolFinish,
                            );
                        }
                        Some(Err(error)) => {
                            break (event(turn_error(error).payload()), Stage::Done);
                        }
                        None => {
                            break (
                                completion.chunk(json!({}), Some("stop"), include_usage),
                                Stage::Footer,
                            );
                        }
                    }
                },
                Stage::Footer if include_usage && usage.is_some() => (
                    event(json!({
                        "id": completion.id, "model": completion.model, "created": completion.created,
                        "object": "chat.completion.chunk", "choices": [], "usage": usage.take().map(usage::Usage::chat)
                    })),
                    Stage::Footer,
                ),
                Stage::Footer => (Event::default().data("[DONE]"), Stage::Done),
                Stage::ToolFinish => {
                    if let Some(delivered) = handoff.take() {
                        let _ = delivered.send(());
                    }
                    (
                        completion.chunk(json!({}), Some("tool_calls"), include_usage),
                        Stage::Footer,
                    )
                }
                Stage::Done => return None,
            };
            Some((
                Ok::<_, Infallible>(chunk),
                (completion, receiver, next, handoff, usage),
            ))
        },
    );
    sse_response(stream::once(async { Ok(first) }).chain(chunks))
}
