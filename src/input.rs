use crate::{ApiError, Config};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::Deserialize;
use serde_json::{Value, json};
use tempfile::TempDir;
use uuid::Uuid;

#[derive(Deserialize)]
pub struct Chat {
    pub model: String,
    pub messages: Vec<Message>,
    #[serde(default)]
    pub stream: bool,
    pub stream_options: Option<StreamOptions>,
    pub n: Option<u32>,
    pub tools: Option<Value>,
    pub tool_choice: Option<Value>,
    pub functions: Option<Value>,
    pub response_format: Option<Value>,
}

#[derive(Deserialize)]
pub struct StreamOptions {
    #[serde(default)]
    pub include_usage: bool,
}

#[derive(Deserialize)]
pub struct Message {
    pub role: String,
    content: Option<Content>,
    pub tool_call_id: Option<String>,
    tool_calls: Option<Value>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Content {
    Text(String),
    Parts(Vec<Part>),
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Part {
    Text { text: String },
    ImageUrl { image_url: ImageUrl },
}

#[derive(Deserialize)]
struct ImageUrl {
    url: String,
}

impl Message {
    pub fn tool_result(&self) -> Result<String, ApiError> {
        match &self.content {
            Some(Content::Text(text)) => Ok(text.clone()),
            Some(Content::Parts(parts)) => parts
                .iter()
                .map(|part| match part {
                    Part::Text { text } => Ok(text.as_str()),
                    _ => Err(ApiError::bad_request("Tool results must be text")),
                })
                .collect::<Result<String, _>>(),
            None => Err(ApiError::bad_request("Missing tool result content")),
        }
    }
}

pub fn split_instructions(
    messages: Vec<Message>,
) -> Result<(Vec<Message>, Option<String>), ApiError> {
    let mut conversation = Vec::new();
    let mut instructions = Vec::new();
    for message in messages {
        if matches!(message.role.as_str(), "system" | "developer") {
            let text = message.tool_result().map_err(|_| {
                ApiError::bad_request("System and developer instructions must be text")
            })?;
            instructions.push(text);
        } else {
            conversation.push(message);
        }
    }
    let instructions = (!instructions.is_empty()).then(|| instructions.join("\n\n"));
    Ok((conversation, instructions))
}

pub async fn prepare(
    messages: Vec<Message>,
    model: &Value,
    config: &Config,
) -> Result<(Vec<Value>, Option<TempDir>), ApiError> {
    if messages.is_empty() {
        return Err(ApiError::bad_request("messages must be nonempty"));
    }
    let mut inputs = Vec::new();
    let mut directory = None;
    let mut image_count = 0;
    let mut image_bytes = 0;
    for message in messages {
        if !matches!(message.role.as_str(), "assistant" | "user" | "tool") {
            return Err(ApiError::bad_request("Unsupported message role"));
        }
        inputs.push(json!({ "type": "text", "text": format!("[{}]", message.role) }));
        if let Some(calls) = message.tool_calls.filter(|_| message.role == "assistant") {
            inputs.push(json!({ "type": "text", "text": format!("Tool calls: {calls}") }));
        }
        match message.content {
            Some(Content::Text(text)) => inputs.push(json!({ "type": "text", "text": text })),
            Some(Content::Parts(parts)) => {
                for part in parts {
                    match part {
                        Part::Text { text } => inputs.push(json!({ "type": "text", "text": text })),
                        Part::ImageUrl { image_url } => {
                            if message.role != "user" {
                                return Err(ApiError::bad_request(
                                    "Only user messages can include images",
                                ));
                            }
                            let (container, host) = config.images.as_ref().ok_or_else(|| {
                                ApiError::bad_request(
                                    "Image input requires a configured shared directory",
                                )
                            })?;
                            if model["inputModalities"]
                                .as_array()
                                .is_some_and(|types| !types.contains(&json!("image")))
                            {
                                return Err(ApiError::bad_request(
                                    "Selected model does not accept images",
                                ));
                            }
                            let (extension, bytes) = decode_image(&image_url.url)?;
                            image_count += 1;
                            image_bytes += bytes.len();
                            if image_count > 16 || image_bytes > 10 * 1024 * 1024 {
                                return Err(ApiError::too_large(
                                    "Images exceed 16 files or 10 MiB",
                                ));
                            }
                            if directory.is_none() {
                                directory = Some(
                                    tempfile::Builder::new()
                                        .prefix("codex-")
                                        .tempdir_in(container)
                                        .map_err(ApiError::internal)?,
                                );
                            }
                            let dir = directory.as_ref().unwrap().path();
                            let filename = format!("{}.{extension}", Uuid::new_v4());
                            tokio::fs::write(dir.join(&filename), bytes)
                                .await
                                .map_err(ApiError::internal)?;
                            let subdir = dir.file_name().unwrap().to_string_lossy();
                            let path = format!(
                                "{}\\{subdir}\\{filename}",
                                host.trim_end_matches(['\\', '/'])
                            );
                            inputs.push(json!({ "type": "localImage", "path": path }));
                        }
                    }
                }
            }
            None if message.role == "assistant" => {}
            None => return Err(ApiError::bad_request("Missing message content")),
        }
    }
    Ok((inputs, directory))
}

fn decode_image(url: &str) -> Result<(&'static str, Vec<u8>), ApiError> {
    let (mime, encoded) = url.split_once(',').ok_or_else(invalid_image)?;
    if !matches!(
        mime,
        "data:image/png;base64" | "data:image/jpeg;base64" | "data:image/webp;base64"
    ) {
        return Err(invalid_image());
    }
    let bytes = STANDARD.decode(encoded).map_err(|_| invalid_image())?;
    let extension = match mime {
        "data:image/png;base64" if bytes.starts_with(b"\x89PNG\r\n\x1a\n") => "png",
        "data:image/jpeg;base64" if bytes.starts_with(b"\xff\xd8\xff") => "jpeg",
        "data:image/webp;base64"
            if bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WEBP") =>
        {
            "webp"
        }
        _ => return Err(invalid_image()),
    };
    Ok((extension, bytes))
}

fn invalid_image() -> ApiError {
    ApiError::bad_request("Expected a valid PNG, JPEG or WebP base64 data URL")
}

pub fn response_message(item: Value) -> Result<Option<Value>, ApiError> {
    let message =
        match item["type"].as_str() {
            Some("reasoning") => {
                // Codex app-server does not accept another provider's encrypted reasoning state.
                let summary = item["summary"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter(|part| part["type"] == "summary_text")
                    .filter_map(|part| part["text"].as_str())
                    .collect::<Vec<_>>()
                    .join("\n");
                return Ok((!summary.is_empty()).then(|| json!({
                "role": "assistant", "content": format!("Prior reasoning summary:\n{summary}")
            })));
            }
            Some("function_call") => Ok(json!({
                "role": "assistant", "tool_calls": [{
                    "id": item["call_id"], "type": "function",
                    "function": { "name": item["name"], "arguments": item["arguments"] }
                }]
            })),
            Some("function_call_output") => {
                let output = match item["output"].as_array() {
                    Some(parts) => Value::Array(
                        parts
                            .iter()
                            .map(|part| match part["type"].as_str() {
                                Some("input_text" | "text") => {
                                    Ok(json!({"type": "text", "text": part["text"]}))
                                }
                                _ => Err(ApiError::bad_request("Tool results must be text")),
                            })
                            .collect::<Result<Vec<_>, _>>()?,
                    ),
                    None => item["output"].clone(),
                };
                Ok(json!({ "role": "tool", "tool_call_id": item["call_id"], "content": output }))
            }
            Some("message") | None if item["role"].is_string() => {
                let content = if let Some(parts) = item["content"].as_array() {
                    Value::Array(
                        parts
                            .iter()
                            .map(|part| match part["type"].as_str() {
                                Some("input_text" | "output_text") => {
                                    Ok(json!({ "type": "text", "text": part["text"] }))
                                }
                                Some("input_image") => Ok(json!({
                                    "type": "image_url", "image_url": { "url": part["image_url"] }
                                })),
                                _ => Err(ApiError::bad_request(
                                    "Unsupported Responses message content",
                                )),
                            })
                            .collect::<Result<Vec<_>, _>>()?,
                    )
                } else {
                    item["content"].clone()
                };
                Ok(json!({ "role": item["role"], "content": content }))
            }
            _ => {
                let kind = item["type"]
                    .as_str()
                    .unwrap_or("<missing>")
                    .chars()
                    .take(80)
                    .collect::<String>();
                eprintln!("Unsupported Responses input item type: {kind:?}");
                Err(ApiError::bad_request("Unsupported Responses input item"))
            }
        }?;
    Ok(Some(message))
}
