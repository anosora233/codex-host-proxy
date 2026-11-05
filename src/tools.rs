use crate::ApiError;
use anyhow::{Context, Result};
use axum::http::StatusCode;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashSet},
    sync::Mutex,
};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

pub enum Output {
    Text(String),
    Image(Value),
    Tool(Value, oneshot::Sender<()>),
    Usage(crate::usage::Usage),
}

pub struct OutputSender {
    pub data: mpsc::Sender<Result<Output>>,
    pub terminal: mpsc::OwnedPermit<Result<Output>>,
}

pub fn output_channel() -> (OutputSender, mpsc::Receiver<Result<Output>>) {
    let (data, receiver) = mpsc::channel(9);
    // Reserve room for a terminal error even when the client stops reading.
    let terminal = data
        .clone()
        .try_reserve_owned()
        .expect("new output channel has capacity");
    (OutputSender { data, terminal }, receiver)
}

pub struct Continuation {
    text: String,
    output: Option<OutputSender>,
    response_id: Option<String>,
}

impl Continuation {
    pub fn for_response(text: String, output: OutputSender, response_id: Option<String>) -> Self {
        Self {
            text,
            output: Some(output),
            response_id,
        }
    }

    pub fn accept(mut self) -> (String, OutputSender, Option<String>) {
        (
            std::mem::take(&mut self.text),
            self.output.take().expect("continuation is accepted once"),
            self.response_id.take(),
        )
    }
}

impl Drop for Continuation {
    fn drop(&mut self) {
        // A queued continuation can be dropped before the Codex worker takes it.
        if let Some(output) = self.output.take() {
            output.terminal.send(Err(anyhow::anyhow!(
                "Codex turn ended before accepting the tool result"
            )));
        }
    }
}

#[derive(Default)]
pub struct PendingTool(Mutex<Option<Pending>>);

struct Pending {
    id: String,
    model: String,
    response_id: Option<String>,
    image_generation: bool,
    result: oneshot::Sender<Continuation>,
}

impl PendingTool {
    pub fn register(
        &self,
        model: &str,
        params: &Value,
        response_id: Option<&str>,
        image_generation: bool,
    ) -> Result<(Value, oneshot::Receiver<Continuation>)> {
        let name = params["tool"]
            .as_str()
            .context("Missing dynamic tool name")?;
        let args = params
            .get("arguments")
            .context("Missing dynamic tool arguments")?;
        let id = format!("call_{}", Uuid::new_v4().simple());
        let call = json!({
            "id": id, "type": "function",
            "function": { "name": name, "arguments": args.to_string() }
        });
        let (result, receiver) = oneshot::channel();
        *self.0.lock().unwrap() = Some(Pending {
            id,
            model: model.to_owned(),
            response_id: response_id.map(str::to_owned),
            image_generation,
            result,
        });
        Ok((call, receiver))
    }

    pub fn resume_response(
        &self,
        id: &str,
        model: &str,
        previous_id: Option<&str>,
        image_generation: Option<bool>,
        continuation: Continuation,
    ) -> Result<(), ApiError> {
        let conflict = || {
            ApiError(
                StatusCode::CONFLICT,
                "No pending tool call matches this ID and model",
                Some("tool_call_not_found"),
            )
        };
        let mut slot = self.0.lock().unwrap();
        let pending = slot
            .as_ref()
            .filter(|pending| {
                pending.id == id
                    && pending.model == model
                    && previous_id.is_none_or(|id| pending.response_id.as_deref() == Some(id))
            })
            .ok_or_else(conflict)?;
        if image_generation.is_some_and(|enabled| enabled != pending.image_generation) {
            return Err(ApiError::bad_request(
                "Image generation cannot be changed while resuming a pending tool call",
            ));
        }
        slot.take()
            .unwrap()
            .result
            .send(continuation)
            .map_err(|_| conflict())
    }

    pub fn clear(&self) {
        self.0.lock().unwrap().take();
    }
}

pub enum Tools {
    Chat(Option<Value>),
    Responses(BTreeMap<String, Value>),
}

impl Tools {
    pub fn definitions(&self, choice: Option<&Value>) -> Result<Vec<Value>, ApiError> {
        match choice.map(Value::as_str) {
            Some(Some("none")) => return Ok(vec![]),
            Some(Some("auto")) | None => {}
            _ => {
                return Err(ApiError::bad_request(
                    "Only auto and none tool_choice are supported",
                ));
            }
        }
        match self {
            Self::Chat(None) => Ok(vec![]),
            Self::Chat(Some(tools)) => {
                let tools = tools
                    .as_array()
                    .ok_or_else(|| ApiError::bad_request("tools must be an array"))?;
                normalize(tools.iter().map(|tool| {
                    if tool["type"] != "function" {
                        return Err(invalid_definition());
                    }
                    Ok(&tool["function"])
                }))
            }
            Self::Responses(tools) => normalize(tools.values().map(Ok)),
        }
    }
}

fn invalid_definition() -> ApiError {
    ApiError::bad_request("Expected unique function tools with valid names and object schemas")
}

fn normalize<'a>(
    functions: impl Iterator<Item = Result<&'a Value, ApiError>>,
) -> Result<Vec<Value>, ApiError> {
    let mut names = HashSet::new();
    functions
        .map(|function| {
            let function = function?;
            let name = function["name"].as_str().ok_or_else(invalid_definition)?;
            if name.is_empty()
                || name.len() > 64
                || !name
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"_-".contains(&byte))
                || !names.insert(name)
            {
                return Err(invalid_definition());
            }
            let schema = function
                .get("parameters")
                .cloned()
                .unwrap_or_else(|| json!({"type": "object", "properties": {}}));
            if !schema.is_object() {
                return Err(invalid_definition());
            }
            Ok(json!({
                "type": "function", "name": name, "inputSchema": schema,
                "description": function["description"].as_str().unwrap_or("")
            }))
        })
        .collect()
}
