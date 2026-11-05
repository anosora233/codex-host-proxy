use crate::input::response_message;
use crate::reply::{self, Completion, json_response, sse_response, turn_error};
use crate::response_state::Record;
use crate::{ApiError, App, Output, TurnInput, json_error, start_chat, tools::Tools};
use anyhow::Result;
use axum::{
    Json,
    extract::{State, rejection::JsonRejection},
    response::{Response, sse::Event},
};
use futures_util::stream;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    convert::Infallible,
    sync::Arc,
};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

#[derive(Deserialize)]
pub struct Create {
    model: String,
    input: Value,
    instructions: Option<String>,
    tools: Option<Vec<Value>>,
    tool_choice: Option<Value>,
    previous_response_id: Option<String>,
    #[serde(default)]
    stream: bool,
    #[serde(default)]
    store: bool,
    #[serde(default)]
    background: bool,
    conversation: Option<Value>,
    text: Option<Value>,
}

pub async fn create(
    State(app): State<Arc<App>>,
    body: Result<Json<Create>, JsonRejection>,
) -> Result<Response, ApiError> {
    let Json(request) = body.map_err(json_error)?;
    if request.store || request.background || request.conversation.is_some() {
        return Err(ApiError::bad_request(
            "Stored responses, background mode and conversations are unsupported",
        ));
    }
    if let Some(format) = request.text.as_ref().and_then(|text| text.get("format"))
        && format["type"] != "text"
    {
        return Err(ApiError::bad_request(
            "Structured response formats are unsupported",
        ));
    }
    let completion = Completion::new("resp_", request.model.clone());
    let mut tools = BTreeMap::new();
    let mut image_generation = false;
    for tool in request.tools.into_iter().flatten() {
        if tool["type"] == "image_generation" {
            if image_generation || tool != json!({"type": "image_generation"}) {
                return Err(ApiError::bad_request(
                    "Only one image_generation tool without options is supported",
                ));
            }
            image_generation = true;
        } else {
            add_function(&mut tools, tool)?;
        }
    }
    let mut messages = Vec::new();
    match request.input {
        Value::String(text) => messages.push(json!({ "role": "user", "content": text })),
        Value::Array(items) => {
            let mut remaining = 8 * 1024 * 1024;
            for item in items {
                let item = app.response_items.resolve(item, &mut remaining)?;
                if item["type"] == "additional_tools" {
                    let mut item = item;
                    if !item["role"].is_null() && item["role"] != "developer" {
                        return Err(ApiError::bad_request(
                            "additional_tools requires the developer role",
                        ));
                    }
                    let Value::Array(additional) = item["tools"].take() else {
                        return Err(ApiError::bad_request(
                            "additional_tools requires a tools array",
                        ));
                    };
                    for tool in additional {
                        add_function(&mut tools, tool)?;
                    }
                    continue;
                }
                if let Some(message) = response_message(item)? {
                    messages.push(message);
                }
            }
        }
        _ => {
            return Err(ApiError::bad_request(
                "input must be a string or an array of input items",
            ));
        }
    }
    if messages
        .iter()
        .rev()
        .take_while(|item| item["role"] == "tool")
        .count()
        > 1
    {
        return Err(ApiError::bad_request(
            "Only one pending tool result can be submitted",
        ));
    }
    let is_tool_result = messages
        .last()
        .is_some_and(|message| message["role"] == "tool");
    let sparse_tool_result =
        is_tool_result && !messages.iter().any(|message| message["role"] == "user");
    let prior = if let Some(id) = &request.previous_response_id {
        match app.response_items.history(id) {
            Ok(prior) => {
                if !is_tool_result
                    && prior
                        .last()
                        .is_some_and(|message| message.get("tool_calls").is_some())
                {
                    return Err(ApiError::bad_request(
                        "Previous response is waiting for a tool result",
                    ));
                }
                Some(prior)
            }
            Err(_) if is_tool_result => {
                // The live tool slot validates the parent and can outlive an evicted record.
                None
            }
            Err(error) => return Err(error),
        }
    } else if sparse_tool_result {
        messages.last().unwrap()["tool_call_id"]
            .as_str()
            .and_then(|id| app.response_items.tool_parent_history(id))
    } else {
        None
    };
    let complete_history =
        prior.is_some() || (request.previous_response_id.is_none() && !sparse_tool_result);
    if let Some(mut prior) = prior {
        if sparse_tool_result {
            prior.push(messages.pop().unwrap());
        } else {
            prior.append(&mut messages);
        }
        messages = prior;
    }
    let history = complete_history.then(|| messages.clone());
    if let Some(instructions) = &request.instructions {
        messages.insert(0, json!({ "role": "developer", "content": instructions }));
    }
    let input = TurnInput {
        model: request.model,
        messages: serde_json::from_value(Value::Array(messages))
            .map_err(|_| ApiError::bad_request("Invalid Responses input"))?,
        tools: Tools::Responses(tools),
        image_generation: match request.tool_choice.as_ref().and_then(Value::as_str) {
            Some("none") => Some(false),
            _ => image_generation.then_some(true),
        },
        tool_choice: request.tool_choice,
    };
    let receiver = start_chat(
        app.clone(),
        input,
        Some(completion.id.clone()),
        request
            .previous_response_id
            .as_deref()
            .filter(|_| is_tool_result),
    )
    .await?;
    let mut encoder = Encoder::new(completion, request.previous_response_id, request.stream);
    if request.stream {
        return Ok(stream_response(encoder, receiver, app, history));
    }
    let reply::Collected {
        text,
        usage,
        tool,
        image,
    } = reply::collect(receiver).await.map_err(turn_error)?;
    if !text.is_empty() {
        encoder.message_id = Some(message_id());
        encoder.text = text;
    }
    encoder.usage = usage;
    encoder.image = image;
    encoder.finish(tool);
    encoder
        .remember(&app, history)
        .await
        .map_err(persistence_error)?;
    Ok(json_response(
        encoder.response("completed"),
        encoder.handoff,
    ))
}

fn persistence_error(error: anyhow::Error) -> ApiError {
    eprintln!("Could not save Responses state: {error}");
    ApiError(
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        "Could not save Responses state",
        Some("response_state_write_failed"),
    )
}

fn add_function(tools: &mut BTreeMap<String, Value>, mut tool: Value) -> Result<(), ApiError> {
    if tool["type"] != "function" {
        let kind = tool["type"]
            .as_str()
            .unwrap_or("<missing>")
            .chars()
            .take(80)
            .collect::<String>();
        eprintln!("Unsupported Responses tool type: {kind:?}");
        return Err(ApiError::bad_request(
            "Only function tools are supported in Responses",
        ));
    }
    let name = tool["name"]
        .as_str()
        .ok_or_else(|| ApiError::bad_request("Responses function tool requires a name"))?
        .to_owned();
    if let Some(fields) = tool.as_object_mut()
        && fields.get("parameters").is_some_and(Value::is_null)
    {
        fields.remove("parameters");
    }
    tools.insert(name, tool);
    Ok(())
}

struct Encoder {
    completion: Completion,
    previous_id: Option<String>,
    streaming: bool,
    sequence: u64,
    events: VecDeque<Event>,
    output: Vec<Value>,
    image: Option<Value>,
    message_id: Option<String>,
    text: String,
    finished: bool,
    handoff: Option<oneshot::Sender<()>>,
    usage: Option<crate::usage::Usage>,
}

impl Encoder {
    async fn remember(&self, app: &Arc<App>, history: Option<Vec<Value>>) -> Result<()> {
        let output = self
            .output
            .iter()
            .filter(|item| item["type"] != "image_generation_call")
            .cloned()
            .collect();
        let record = Record::new(self.completion.id.clone(), history, output);
        let app = app.clone();
        tokio::task::spawn_blocking(move || app.response_items.remember(record)).await?
    }

    fn new(completion: Completion, previous_id: Option<String>, streaming: bool) -> Self {
        let mut encoder = Self {
            completion,
            previous_id,
            streaming,
            sequence: 0,
            events: VecDeque::new(),
            output: Vec::new(),
            image: None,
            message_id: None,
            text: String::new(),
            finished: false,
            handoff: None,
            usage: None,
        };
        encoder.emit(
            "response.created",
            |encoder| json!({ "response": encoder.response("in_progress") }),
        );
        encoder.emit(
            "response.in_progress",
            |encoder| json!({ "response": encoder.response("in_progress") }),
        );
        encoder
    }

    fn response(&self, status: &str) -> Value {
        json!({
            "id": self.completion.id, "object": "response", "model": self.completion.model,
            "created_at": self.completion.created, "status": status, "output": self.output,
            "error": null, "incomplete_details": null, "usage": self.usage.map(crate::usage::Usage::response),
            "previous_response_id": self.previous_id, "store": false, "parallel_tool_calls": false
        })
    }

    fn emit(&mut self, kind: &str, payload: impl FnOnce(&Self) -> Value) {
        if self.streaming {
            let mut payload = payload(self);
            payload["type"] = json!(kind);
            payload["sequence_number"] = json!(self.sequence);
            self.sequence += 1;
            self.events
                .push_back(Event::default().event(kind).data(payload.to_string()));
        }
    }

    fn append(&mut self, text: &str) -> Result<()> {
        reply::append(&mut self.text, text)?;
        if self.message_id.is_none() {
            let id = message_id();
            self.emit("response.output_item.added", |_| json!({
                "output_index": 0,
                "item": { "id": id, "type": "message", "role": "assistant", "status": "in_progress", "content": [] }
            }));
            self.emit("response.content_part.added", |_| {
                json!({
                    "item_id": id, "output_index": 0, "content_index": 0,
                    "part": { "type": "output_text", "text": "", "annotations": [] }
                })
            });
            self.message_id = Some(id);
        }
        self.emit("response.output_text.delta", |encoder| json!({
            "item_id": encoder.message_id, "output_index": 0, "content_index": 0, "delta": text, "logprobs": []
        }));
        Ok(())
    }

    fn finish_message(&mut self) {
        let Some(id) = self.message_id.take() else {
            return;
        };
        let text = std::mem::take(&mut self.text);
        let part = json!({ "type": "output_text", "text": text, "annotations": [] });
        let item = json!({
            "id": id, "type": "message", "role": "assistant", "status": "completed", "content": [part]
        });
        self.emit("response.output_text.done", |_| {
            json!({
                "item_id": id, "output_index": 0, "content_index": 0, "text": text, "logprobs": []
            })
        });
        self.emit("response.content_part.done", |_| {
            json!({
                "item_id": id, "output_index": 0, "content_index": 0, "part": part
            })
        });
        self.emit(
            "response.output_item.done",
            |_| json!({ "output_index": 0, "item": item }),
        );
        self.output.push(item);
    }

    fn finish_tool(&mut self, call: Value) {
        let id = format!("fc_{}", Uuid::new_v4().simple());
        let index = self.output.len();
        let name = &call["function"]["name"];
        let args = &call["function"]["arguments"];
        let mut item = json!({
            "id": id, "type": "function_call", "status": "in_progress",
            "call_id": call["id"], "name": name, "arguments": ""
        });
        self.emit(
            "response.output_item.added",
            |_| json!({ "output_index": index, "item": item }),
        );
        self.emit("response.function_call_arguments.delta", |_| {
            json!({
                "item_id": id, "output_index": index, "delta": args
            })
        });
        self.emit("response.function_call_arguments.done", |_| {
            json!({
                "item_id": id, "output_index": index, "name": name, "arguments": args
            })
        });
        item["arguments"] = args.clone();
        item["status"] = json!("completed");
        self.emit(
            "response.output_item.done",
            |_| json!({ "output_index": index, "item": item }),
        );
        self.output.push(item);
    }

    fn advance(&mut self, next: Option<Result<Output>>) -> Result<()> {
        match next.transpose()? {
            Some(Output::Text(text)) => return self.append(&text),
            Some(Output::Usage(usage)) => {
                self.usage = Some(usage);
                return Ok(());
            }
            Some(Output::Image(image)) => {
                self.image = Some(image);
                return Ok(());
            }
            Some(Output::Tool(call, delivered)) => self.finish(Some((call, delivered))),
            None => self.finish(None),
        }
        Ok(())
    }

    fn finish(&mut self, tool: Option<(Value, oneshot::Sender<()>)>) {
        self.finish_message();
        if let Some(image) = self.image.take() {
            let index = self.output.len();
            self.emit("response.output_item.added", |_| json!({
                "output_index": index,
                "item": {"id": image["id"], "type": "image_generation_call", "status": "in_progress"}
            }));
            self.emit(
                "response.image_generation_call.completed",
                |_| json!({"output_index": index, "item_id": image["id"]}),
            );
            self.emit(
                "response.output_item.done",
                |_| json!({"output_index": index, "item": image}),
            );
            self.output.push(image);
        }
        if let Some((call, delivered)) = tool {
            self.finish_tool(call);
            self.handoff = Some(delivered);
        }
        self.finished = true;
    }
}

fn message_id() -> String {
    format!("msg_{}", Uuid::new_v4().simple())
}

fn stream_response(
    encoder: Encoder,
    receiver: mpsc::Receiver<Result<Output>>,
    app: Arc<App>,
    history: Option<Vec<Value>>,
) -> Response {
    let events = stream::unfold(
        (encoder, receiver, app, history),
        |(mut encoder, mut receiver, app, mut history)| async move {
            loop {
                if let Some(event) = encoder.events.pop_front() {
                    if encoder.finished
                        && encoder.events.is_empty()
                        && let Some(delivered) = encoder.handoff.take()
                    {
                        let _ = delivered.send(());
                    }
                    return Some((
                        Ok::<_, Infallible>(event),
                        (encoder, receiver, app, history),
                    ));
                }
                if encoder.finished {
                    return None;
                }
                let result = match encoder.advance(receiver.recv().await).map_err(turn_error) {
                    Ok(()) if encoder.finished => {
                        let saved = encoder
                            .remember(&app, history.take())
                            .await
                            .map_err(persistence_error);
                        if saved.is_ok() {
                            encoder.emit("response.completed", |encoder| {
                                json!({
                                    "response": encoder.response("completed")
                                })
                            });
                        }
                        saved
                    }
                    result => result,
                };
                if let Err(error) = result {
                    encoder.sequence -= encoder.events.len() as u64;
                    encoder.events.clear();
                    if encoder.handoff.take().is_some() {
                        encoder
                            .output
                            .retain(|item| item["type"] != "function_call");
                    }
                    let mut failed = encoder.response("failed");
                    failed["error"] =
                        json!({ "code": error.2.unwrap_or("server_error"), "message": error.1 });
                    encoder.emit("response.failed", |_| json!({ "response": failed }));
                    encoder.finished = true;
                    receiver.close();
                }
            }
        },
    );
    sse_response(events)
}
