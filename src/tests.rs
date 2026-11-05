use super::*;
use axum::body::{Body, to_bytes};
use futures_util::{SinkExt, StreamExt};
use tokio::{net::TcpStream, sync::oneshot, time::timeout};
use tokio_tungstenite::{
    WebSocketStream, accept_hdr_async,
    tungstenite::{Message, handshake::server},
};
use tower::ServiceExt;

#[tokio::test]
async fn responses_restore_references_and_previous_response_after_restart() {
    timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_url = format!("ws://{}", listener.local_addr().unwrap());
        let upstream =
            tokio::spawn(async move {
                for expected in [
                    vec!["[user]", "Remember OAK"],
                    vec![
                        "[user]",
                        "Remember OAK",
                        "[assistant]",
                        "OAK",
                        "[user]",
                        "Recall it",
                    ],
                    vec!["[user]", "Write failure"],
                    vec!["[user]", "Write failure"],
                ] {
                    let (tcp, _) = listener.accept().await.unwrap();
                    let mut socket = tokio_tungstenite::accept_async(tcp).await.unwrap();
                    let init = read(&mut socket).await;
                    send(&mut socket, json!({"id": init["id"], "result": {}})).await;
                    assert_eq!(read(&mut socket).await["method"], "initialized");
                    let models = read(&mut socket).await;
                    send(
                        &mut socket,
                        json!({"id": models["id"], "result": {"data": [{"id": "test-model"}]}}),
                    )
                    .await;
                    let thread = read(&mut socket).await;
                    send(
                        &mut socket,
                        json!({"id": thread["id"], "result": {"thread": {"id": "thread"}}}),
                    )
                    .await;
                    let turn = read(&mut socket).await;
                    let input = turn["params"]["input"].as_array().unwrap();
                    assert_eq!(
                        input
                            .iter()
                            .map(|part| part["text"].as_str().unwrap())
                            .collect::<Vec<_>>(),
                        expected
                    );
                    send(
                        &mut socket,
                        json!({"id": turn["id"], "result": {"turn": {"id": "turn"}}}),
                    )
                    .await;
                    send(&mut socket, json!({"method": "item/agentMessage/delta", "params": {
                    "threadId": "thread", "turnId": "turn", "itemId": "reply", "delta": "OAK"
                }})).await;
                    send(
                        &mut socket,
                        json!({"method": "turn/completed", "params": {
                            "threadId": "thread", "turn": {"id": "turn", "status": "completed"}
                        }}),
                    )
                    .await;
                }
            });
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");
        let make_router = |state| {
            router_with_state(
                Config {
                    ws_url: ws_url.clone(),
                    ws_token: "host-token".into(),
                    api_key: "client-key".into(),
                    cwd: r"C:\repo".into(),
                    images: None,
                    timeout: Duration::from_secs(2),
                },
                state,
            )
        };
        let open = || make_router(response_state::State::open(Some(path.clone())).unwrap());
        let request =
            |input, previous_id, stream| {
                Request::post("/v1/responses")
            .header("authorization", "Bearer client-key").header("content-type", "application/json")
            .body(Body::from(json!({
                "model": "test-model", "input": input, "previous_response_id": previous_id, "stream": stream
            }).to_string())).unwrap()
            };
        let first = open()
            .oneshot(request(json!("Remember OAK"), None, false))
            .await
            .unwrap();
        let first = read_response(first, false).await;
        let id = first["id"].as_str().unwrap();
        let item = first["output"][0]["id"].as_str().unwrap();
        let reopened = response_state::State::open(Some(path.clone())).unwrap();
        assert_eq!(
            reopened
                .resolve(json!({"type": "item_reference", "id": item}), &mut 4096)
                .unwrap(),
            first["output"][0]
        );
        let missing = open()
            .oneshot(request(json!("test"), Some("resp_missing"), false))
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::BAD_REQUEST);
        let bytes = to_bytes(missing.into_body(), 8192).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&bytes).unwrap()["error"]["code"],
            "previous_response_not_found"
        );
        let second = open()
            .oneshot(request(json!("Recall it"), Some(id), false))
            .await
            .unwrap();
        assert_eq!(
            read_response(second, false).await["output"][0]["content"][0]["text"],
            "OAK"
        );
        let bad_path = dir.path().join("blocked.json");
        let bad_state = response_state::State::open(Some(bad_path.clone())).unwrap();
        std::fs::create_dir(&bad_path).unwrap();
        let failing = make_router(bad_state);
        for streaming in [false, true] {
            let response = failing.clone().oneshot(request(json!("Write failure"), None, streaming)).await.unwrap();
            let expected = if streaming { StatusCode::OK } else { StatusCode::INTERNAL_SERVER_ERROR };
            assert_eq!(response.status(), expected);
            let body = to_bytes(response.into_body(), 8192).await.unwrap();
            let body = std::str::from_utf8(&body).unwrap();
            assert!(body.contains("response_state_write_failed"));
            assert!(!body.contains("response.completed"));
            if streaming { assert!(body.contains("response.failed")); }
        }
        upstream.await.unwrap();
    })
    .await
    .unwrap();
}

#[test]
fn oversized_history_keeps_small_output_reference_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.json");
    let output = json!({"id": "msg_small", "type": "message", "role": "assistant", "content": "short reply"});
    response_state::State::open(Some(path.clone()))
        .unwrap()
        .remember(response_state::Record::new(
            "resp_large".into(),
            Some(vec![
                json!({"role": "user", "content": "a".repeat(9 * 1024 * 1024)}),
            ]),
            vec![output.clone()],
        ))
        .unwrap();
    let reopened = response_state::State::open(Some(path)).unwrap();
    assert_eq!(
        reopened
            .resolve(
                json!({"type": "item_reference", "id": "msg_small"}),
                &mut 4096
            )
            .unwrap(),
        output
    );
    assert_eq!(
        reopened.history("resp_large").unwrap_err().2,
        Some("previous_response_not_found")
    );
}

#[test]
fn oversized_encoded_output_does_not_fail_or_evict_existing_state() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.json");
    let state = response_state::State::open(Some(path.clone())).unwrap();
    state
        .remember(response_state::Record::new(
            "resp_kept".into(),
            Some(vec![]),
            vec![],
        ))
        .unwrap();
    state
        .remember(response_state::Record::new(
            "resp_too_large".into(),
            None,
            vec![json!({"id": "msg_large", "text": "\\".repeat(5 * 1024 * 1024)})],
        ))
        .unwrap();
    let reopened = response_state::State::open(Some(path)).unwrap();
    assert!(reopened.history("resp_kept").is_ok());
    assert_eq!(
        reopened
            .resolve(
                json!({"type": "item_reference", "id": "msg_large"}),
                &mut 4096
            )
            .unwrap_err()
            .2,
        Some("item_reference_not_found")
    );
}

#[test]
fn response_state_replays_legacy_and_large_single_copy_outputs() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.json");
    let legacy = json!([{
        "id": "resp_legacy",
        "history": [
            {"role": "user", "content": "question"},
            {"role": "assistant", "content": "old answer"}
        ],
        "output": [{"id": "msg_old", "type": "message", "role": "assistant", "content": "old answer"}]
    }]);
    std::fs::write(&path, legacy.to_string()).unwrap();
    let state = response_state::State::open(Some(path.clone())).unwrap();
    assert_eq!(state.history("resp_legacy").unwrap().len(), 2);
    let text = "a".repeat(9 * 1024 * 1024 / 2);
    state
        .remember(response_state::Record::new(
            "resp_large".into(),
            Some(vec![json!({"role": "user", "content": "long answer"})]),
            vec![json!({
                "id": "msg_large", "type": "message", "role": "assistant",
                "content": [{"type": "output_text", "text": text}]
            })],
        ))
        .unwrap();
    let reopened = response_state::State::open(Some(path)).unwrap();
    assert_eq!(reopened.history("resp_legacy").unwrap().len(), 2);
    let history = reopened.history("resp_large").unwrap();
    assert_eq!(history.len(), 2);
    assert_eq!(
        history[1]["content"][0]["text"].as_str(),
        Some(text.as_str())
    );
}

#[tokio::test]
async fn tool_result_resumes_the_same_codex_turn() {
    exercise_tool_call(ToolStream::ReadAll).await;
}

#[tokio::test]
async fn disconnect_before_tool_handoff_interrupts_turn() {
    exercise_tool_call(ToolStream::Abandon).await;
}

#[tokio::test]
async fn client_can_resume_after_closing_at_tool_finish_reason() {
    exercise_tool_call(ToolStream::StopAtFinish).await;
}

#[tokio::test]
async fn responses_stream_tool_call_and_resume_by_response_id() {
    exercise_tool_call(ToolStream::Responses(true)).await;
}

#[tokio::test]
async fn responses_return_json_for_tool_call_and_text() {
    exercise_tool_call(ToolStream::Responses(false)).await;
}

#[derive(Clone, Copy)]
enum ToolStream {
    ReadAll,
    Abandon,
    StopAtFinish,
    Responses(bool),
}

async fn exercise_tool_call(mode: ToolStream) {
    timeout(Duration::from_secs(10), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_url = format!("ws://{}", listener.local_addr().unwrap());
        let upstream = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(tcp).await.unwrap();
            let init = read(&mut socket).await;
            assert_eq!(init["params"]["capabilities"]["experimentalApi"], true);
            send(&mut socket, json!({"id": init["id"], "result": {}})).await;
            assert_eq!(read(&mut socket).await["method"], "initialized");
            let models = read(&mut socket).await;
            send(&mut socket, json!({"id": models["id"], "result": {"data": [{"id": "test-model"}]}})).await;
            let thread = read(&mut socket).await;
            assert_eq!(thread["params"]["developerInstructions"], "System constraint\n\nDeveloper constraint");
            let image_config = if matches!(mode, ToolStream::Responses(true)) {
                Value::Null
            } else {
                json!(false)
            };
            assert_eq!(thread["params"]["config"]["features"]["image_generation"], image_config);
            let schema = if matches!(mode, ToolStream::Responses(true)) {
                json!({"type": "object", "properties": {}})
            } else {
                json!({"type": "object"})
            };
            assert_eq!(thread["params"]["dynamicTools"], json!([{
                "type": "function", "name": "read", "description": "Read a file",
                "inputSchema": schema
            }]));
            send(&mut socket, json!({"id": thread["id"], "result": {"thread": {"id": "thread"}}})).await;
            let turn = read(&mut socket).await;
            assert_eq!(turn["method"], "turn/start");
            assert_eq!(turn["params"]["input"], json!([
                {"type": "text", "text": "[user]"},
                {"type": "text", "text": "Read README.md"}
            ]));
            send(&mut socket, json!({"id": turn["id"], "result": {"turn": {"id": "turn"}}})).await;
            send(&mut socket, usage_update(100, 20, 10, 5)).await;
            send(&mut socket, json!({
                "id": "rpc-tool-1", "method": "item/tool/call", "params": {
                    "threadId": "thread", "turnId": "turn", "callId": "upstream-call",
                    "tool": "read", "arguments": {"path": "README.md"}
                }
            })).await;
            if matches!(mode, ToolStream::Abandon) {
                let interrupt = read(&mut socket).await;
                assert_eq!(interrupt["method"], "turn/interrupt");
                assert_eq!(interrupt["params"], json!({"threadId": "thread", "turnId": "turn"}));
                send(&mut socket, json!({"id": interrupt["id"], "result": {}})).await;
                return;
            }
            assert_eq!(read(&mut socket).await, json!({
                "id": "rpc-tool-1", "result": {
                    "success": true, "contentItems": [{"type": "inputText", "text": "File contents"}]
                }
            }));
            send(&mut socket, json!({"method": "item/agentMessage/delta", "params": {
                "threadId": "thread", "turnId": "turn", "itemId": "reply", "delta": "Got the file."
            }})).await;
            send(&mut socket, usage_update(150, 35, 25, 9)).await;
            send(&mut socket, json!({"method": "turn/completed", "params": {
                "threadId": "thread", "turn": {"id": "turn", "status": "completed"}
            }})).await;
        });
        let state_dir = tempfile::tempdir().unwrap();
        let state_path = state_dir.path().join("responses.json");
        let app = router_with_state(Config {
            ws_url, ws_token: "host-token".into(), api_key: "client-key".into(),
            cwd: r"C:\repo".into(), images: None, timeout: Duration::from_secs(5),
        }, response_state::State::open(Some(state_path.clone())).unwrap());
        if let ToolStream::Responses(stream) = mode {
            exercise_responses(app, stream, state_path).await;
            upstream.await.unwrap();
            return;
        }
        let chat_request = |messages, stream| {
            Request::post("/v1/chat/completions")
                .header("authorization", "Bearer client-key")
                .header("content-type", "application/json")
                .body(Body::from(json!({
                    "model": "test-model", "stream": stream, "messages": messages,
                    "stream_options": {"include_usage": true},
                    "tools": [{"type": "function", "function": {
                        "name": "read", "description": "Read a file", "parameters": {"type": "object"}
                    }}]
                }).to_string())).unwrap()
        };
        let response = app.clone().oneshot(chat_request(json!([
            {"role": "system", "content": "System constraint"},
            {"role": "developer", "content": [{"type": "text", "text": "Developer constraint"}]},
            {"role": "user", "content": "Read README.md"}
        ]), true)).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        if matches!(mode, ToolStream::Abandon) {
            let mut events = response.into_body().into_data_stream();
            assert!(String::from_utf8_lossy(&events.next().await.unwrap().unwrap()).contains("assistant"));
            assert!(String::from_utf8_lossy(&events.next().await.unwrap().unwrap()).contains("tool_calls"));
            drop(events);
            timeout(Duration::from_secs(2), upstream).await.unwrap().unwrap();
            return;
        }
        let sse = if matches!(mode, ToolStream::StopAtFinish) {
            let mut events = response.into_body().into_data_stream();
            let mut sse = String::new();
            while let Some(chunk) = events.next().await {
                sse.push_str(std::str::from_utf8(&chunk.unwrap()).unwrap());
                if sse.contains("\"finish_reason\":\"tool_calls\"") {
                    break;
                }
            }
            drop(events);
            assert!(!sse.contains("data: [DONE]"));
            sse
        } else {
            let body = to_bytes(response.into_body(), 8192).await.unwrap();
            let sse = String::from_utf8(body.to_vec()).unwrap();
            assert!(sse.contains("data: [DONE]"));
            sse
        };
        assert!(sse.contains("\"finish_reason\":\"tool_calls\""));
        let call = sse.lines().filter_map(|line| line.strip_prefix("data: "))
            .filter_map(|data| serde_json::from_str::<Value>(data).ok())
            .find_map(|chunk| chunk["choices"][0]["delta"]["tool_calls"][0].as_object().cloned())
            .unwrap();
        assert_eq!(call["function"]["name"], "read");
        assert_eq!(call["index"], 0);
        let continuation = |id| chat_request(json!([
            {"role": "user", "content": "Read README.md"},
            {"role": "assistant", "content": null, "tool_calls": [call]},
            {"role": "tool", "tool_call_id": id, "content": "File contents"}
        ]), true);
        let wrong = app.clone().oneshot(continuation("wrong-id")).await.unwrap();
        assert_eq!(wrong.status(), StatusCode::CONFLICT);
        let next = app.clone().oneshot(continuation(call["id"].as_str().unwrap())).await.unwrap();
        let answer = to_bytes(next.into_body(), 8192).await.unwrap();
        let answer = std::str::from_utf8(&answer).unwrap();
        assert!(answer.contains("Got the file."));
        assert!(answer.contains("\"finish_reason\":\"stop\""));
        assert!(answer.contains("\"choices\":[],"));
        assert!(answer.contains("\"completion_tokens\":15"));
        assert!(answer.contains("\"prompt_tokens\":50"));
        assert!(answer.contains("data: [DONE]"));
        for data in answer.lines().filter_map(|line| line.strip_prefix("data: ")).filter(|data| *data != "[DONE]") {
            let chunk: Value = serde_json::from_str(data).unwrap();
            if !chunk["choices"].as_array().unwrap().is_empty() {
                assert_eq!(chunk.get("usage"), Some(&Value::Null));
            }
        }
        let duplicate = app.oneshot(continuation(call["id"].as_str().unwrap())).await.unwrap();
        assert_eq!(duplicate.status(), StatusCode::CONFLICT);
        upstream.await.unwrap();
    }).await.unwrap();
}

#[tokio::test]
async fn completed_turn_reports_usage_when_output_queue_is_full() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_url = format!("ws://{}", listener.local_addr().unwrap());
    let upstream = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(tcp).await.unwrap();
        let init = read(&mut socket).await;
        send(&mut socket, json!({"id": init["id"], "result": {}})).await;
        assert_eq!(read(&mut socket).await["method"], "initialized");
        let models = read(&mut socket).await;
        send(
            &mut socket,
            json!({"id": models["id"], "result": {"data": [{"id": "test-model"}]}}),
        )
        .await;
        let thread = read(&mut socket).await;
        send(
            &mut socket,
            json!({"id": thread["id"], "result": {"thread": {"id": "thread"}}}),
        )
        .await;
        let turn = read(&mut socket).await;
        send(
            &mut socket,
            json!({"id": turn["id"], "result": {"turn": {"id": "turn"}}}),
        )
        .await;
        for _ in 0..8 {
            send(
                &mut socket,
                json!({"method": "item/agentMessage/delta", "params": {
                    "threadId": "thread", "turnId": "turn", "itemId": "reply", "delta": "x"
                }}),
            )
            .await;
        }
        send(&mut socket, usage_update(20, 8, 0, 0)).await;
        send(
            &mut socket,
            json!({"method": "turn/completed", "params": {
                "threadId": "thread", "turn": {"id": "turn", "status": "completed"}
            }}),
        )
        .await;
    });
    let deadline = Duration::from_millis(600);
    let app = router(Config {
        ws_url,
        ws_token: "host-token".into(),
        api_key: "client-key".into(),
        cwd: r"C:\repo".into(),
        images: None,
        timeout: deadline,
    });
    let request = Request::post("/v1/chat/completions")
        .header("authorization", "Bearer client-key")
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "model": "test-model", "stream": true, "stream_options": {"include_usage": true},
                "messages": [{"role": "user", "content": "test"}]
            })
            .to_string(),
        ))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    timeout(Duration::from_secs(2), upstream)
        .await
        .unwrap()
        .unwrap();
    // Leave all eight text chunks queued until after the turn deadline.
    tokio::time::sleep(deadline + Duration::from_millis(50)).await;
    let bytes = timeout(Duration::from_secs(2), to_bytes(response.into_body(), 8192))
        .await
        .unwrap()
        .unwrap();
    let sse = std::str::from_utf8(&bytes).unwrap();
    assert_eq!(sse.matches("\"content\":\"x\"").count(), 8);
    assert!(sse.contains("\"completion_tokens\":8"));
    assert!(sse.contains("data: [DONE]"));
}

async fn exercise_responses(app: Router, streaming: bool, state_path: PathBuf) {
    let tool = json!({"type": "function", "name": "read", "description": "Read a file",
                      "parameters": {"type": "object"}});
    let request = |input, previous_id, image_tool, tool_choice| {
        let mut tools = if streaming {
            vec![json!({"type": "function", "name": "read", "description": "Overridden"})]
        } else {
            vec![tool.clone()]
        };
        if image_tool {
            tools.push(json!({"type": "image_generation"}));
        }
        Request::post("/v1/responses")
            .header("authorization", "Bearer client-key")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "model": "test-model", "stream": streaming, "input": input,
                    "previous_response_id": previous_id, "store": false,
                    "instructions": "System constraint",
                    "tools": tools, "tool_choice": tool_choice
                })
                .to_string(),
            ))
            .unwrap()
    };
    let mut input = vec![
        json!({"role": "developer", "content": [{"type": "input_text", "text": "Developer constraint"}]}),
        json!({"type": "reasoning", "summary": [], "encrypted_content": "opaque-history"}),
        json!({"role": "user", "content": [{"type": "input_text", "text": "Read README.md"}]}),
    ];
    if streaming {
        let mut additional = tool.clone();
        additional["parameters"] = Value::Null;
        input.push(json!({"type": "additional_tools", "role": "developer", "tools": [additional]}));
    }
    let first = app
        .clone()
        .oneshot(request(json!(input), None, streaming, None::<&str>))
        .await
        .unwrap();
    let first = read_response(first, streaming).await;
    let id = first["id"].as_str().unwrap();
    assert!(id.starts_with("resp_"));
    assert_eq!(first["usage"]["output_tokens"], 20);
    let call = &first["output"][0];
    assert_eq!(call["type"], "function_call");
    assert_eq!(call["name"], "read");
    assert_eq!(
        serde_json::from_str::<Value>(call["arguments"].as_str().unwrap()).unwrap(),
        json!({"path": "README.md"})
    );
    let result = json!([
        {"type": "item_reference", "id": call["id"]},
        {"type": "function_call_output", "call_id": call["call_id"], "output": [
            {"type": "input_text", "text": "File contents"}
        ]},
        {"type": "additional_tools", "role": "developer", "tools": [tool]}
    ]);
    let wrong = app
        .clone()
        .oneshot(request(result.clone(), Some("resp_wrong"), false, None))
        .await
        .unwrap();
    assert_eq!(wrong.status(), StatusCode::CONFLICT);
    let previous = (!streaming).then_some(id);
    let changed = app
        .clone()
        .oneshot(request(
            result.clone(),
            previous,
            !streaming,
            streaming.then_some("none"),
        ))
        .await
        .unwrap();
    assert_eq!(changed.status(), StatusCode::BAD_REQUEST);
    let answer = app
        .oneshot(request(result, previous, false, None))
        .await
        .unwrap();
    let answer = read_response(answer, streaming).await;
    assert_eq!(answer["previous_response_id"], json!(previous));
    let state = response_state::State::open(Some(state_path)).unwrap();
    let history = state.history(answer["id"].as_str().unwrap()).unwrap();
    assert_eq!(
        history
            .iter()
            .filter(|message| message["role"] == "user")
            .count(),
        1
    );
    assert_eq!(
        history
            .iter()
            .filter(|message| message.get("tool_calls").is_some())
            .count(),
        1
    );
    assert_eq!(
        history.last().unwrap()["content"][0]["text"],
        "Got the file."
    );
    assert_eq!(
        answer["usage"],
        json!({
            "input_tokens": 50, "output_tokens": 15, "total_tokens": 65,
            "input_tokens_details": {"cached_tokens": 15},
            "output_tokens_details": {"reasoning_tokens": 4}
        })
    );
    assert_eq!(answer["output"][0]["content"][0]["text"], "Got the file.");
}

fn usage_update(input: u64, output: u64, cached: u64, reasoning: u64) -> Value {
    json!({
        "method": "thread/tokenUsage/updated", "params": {
            "threadId": "thread", "turnId": "turn", "tokenUsage": {
                "total": {
                    "inputTokens": input, "outputTokens": output, "totalTokens": input + output,
                    "cachedInputTokens": cached, "reasoningOutputTokens": reasoning
                }
            }
        }
    })
}

async fn read_response(response: Response, streaming: bool) -> Value {
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = to_bytes(response.into_body(), 16_384).await.unwrap();
    if !streaming {
        return serde_json::from_slice(&bytes).unwrap();
    }
    let sse = std::str::from_utf8(&bytes).unwrap();
    let events = sse
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .map(|data| serde_json::from_str::<Value>(data).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(events[0]["type"], "response.created");
    for (sequence, event) in events.iter().enumerate() {
        assert_eq!(event["sequence_number"], sequence);
        assert!(sse.contains(&format!("event: {}", event["type"].as_str().unwrap())));
    }
    let last = events.last().unwrap();
    assert_eq!(last["type"], "response.completed");
    let item = &last["response"]["output"][0];
    let delta = if item["type"] == "function_call" {
        "response.function_call_arguments.delta"
    } else {
        "response.output_text.delta"
    };
    assert!(events.iter().any(|event| event["type"] == delta));
    for (index, image) in last["response"]["output"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
    {
        if image["type"] == "image_generation_call" {
            assert!(events.iter().any(|event| {
                event["type"] == "response.image_generation_call.completed"
                    && event["item_id"] == image["id"]
                    && event["output_index"] == index
            }));
        }
    }
    last["response"].clone()
}

#[tokio::test]
async fn discarded_queued_tool_result_reports_an_error() {
    let pending = PendingTool::default();
    let (call, upstream) = pending
        .register(
            "test-model",
            &json!({"tool": "read", "arguments": {}}),
            None,
            false,
        )
        .unwrap();
    let (output, mut receiver) = output_channel();
    pending
        .resume_response(
            call["id"].as_str().unwrap(),
            "test-model",
            None,
            None,
            Continuation::for_response("result".into(), output, None),
        )
        .unwrap();
    drop(upstream);
    let error = receiver.recv().await.unwrap().err().unwrap();
    assert_eq!(
        error.to_string(),
        "Codex turn ended before accepting the tool result"
    );
    assert!(receiver.recv().await.is_none());
}

#[tokio::test]
async fn terminal_error_fits_while_client_is_not_reading() {
    let (sender, mut receiver) = output_channel();
    while sender
        .data
        .try_send(Ok(Output::Text("delta".into())))
        .is_ok()
    {}
    let data = sender.terminal.send(Err(anyhow::anyhow!("timed out")));
    drop(data);
    drop(sender.data);
    for _ in 0..8 {
        assert!(matches!(receiver.recv().await, Some(Ok(Output::Text(_)))));
    }
    assert_eq!(
        receiver.recv().await.unwrap().err().unwrap().to_string(),
        "timed out"
    );
    assert!(receiver.recv().await.is_none());
}

async fn read(socket: &mut WebSocketStream<TcpStream>) -> Value {
    let message = socket.next().await.unwrap().unwrap();
    serde_json::from_str(message.to_text().unwrap()).unwrap()
}

async fn send(socket: &mut WebSocketStream<TcpStream>, data: Value) {
    socket.send(Message::text(data.to_string())).await.unwrap();
}

async fn mock(listener: TcpListener, image_root: PathBuf, resume: oneshot::Receiver<bool>) {
    let (tcp, _) = listener.accept().await.unwrap();
    // tungstenite's handshake callback fixes the error type.
    #[allow(clippy::result_large_err)]
    let mut socket = accept_hdr_async(tcp, |req: &server::Request, res: server::Response| {
        assert_eq!(req.headers()["authorization"], "Bearer host-token");
        Ok(res)
    })
    .await
    .unwrap();
    let initialize = read(&mut socket).await;
    assert_eq!(initialize["method"], "initialize");
    send(&mut socket, json!({ "id": initialize["id"], "result": {} })).await;
    assert_eq!(read(&mut socket).await["method"], "initialized");
    let models = read(&mut socket).await;
    assert_eq!(models["method"], "model/list");
    send(&mut socket, json!({ "id": models["id"], "result": {
        "data": [{ "id": "vision-model", "model": "vision-upstream", "inputModalities": ["text", "image"] }],
        "nextCursor": null
    }})).await;
    let thread = read(&mut socket).await;
    assert_eq!(thread["method"], "thread/start");
    assert_eq!(thread["params"]["model"], "vision-upstream");
    assert_eq!(thread["params"]["sandbox"], "read-only");
    send(
        &mut socket,
        json!({ "id": thread["id"], "result": { "thread": { "id": "thread" } } }),
    )
    .await;
    let turn = read(&mut socket).await;
    assert_eq!(turn["method"], "turn/start");
    let image = &turn["params"]["input"][2];
    assert_eq!(image["type"], "localImage");
    let file = std::path::Path::new(image["path"].as_str().unwrap());
    assert!(file.is_absolute() && file.starts_with(image_root));
    assert_eq!(tokio::fs::read(file).await.unwrap(), b"\x89PNG\r\n\x1a\n");
    // Send the first delta before the turn/start response to exercise notification ordering.
    send(
        &mut socket,
        json!({ "method": "item/agentMessage/delta", "params": {
            "threadId": "thread", "turnId": "turn", "itemId": "item", "delta": "hello"
        }}),
    )
    .await;
    send(
        &mut socket,
        json!({ "id": turn["id"], "result": { "turn": { "id": "turn" } } }),
    )
    .await;
    if resume.await.unwrap() {
        send(
            &mut socket,
            json!({ "method": "item/completed", "params": {
                "threadId": "thread", "turnId": "turn", "item": {
                    "id": "item", "type": "agentMessage", "text": "hello"
                }
            }}),
        )
        .await;
        send(
            &mut socket,
            json!({ "method": "turn/completed", "params": {
                "threadId": "thread", "turn": { "id": "turn", "status": "completed" }
            }}),
        )
        .await;
    } else {
        let interrupt = read(&mut socket).await;
        assert_eq!(interrupt["method"], "turn/interrupt");
        assert_eq!(
            interrupt["params"],
            json!({ "threadId": "thread", "turnId": "turn" })
        );
        send(&mut socket, json!({ "id": interrupt["id"], "result": {} })).await;
    }
}

async fn exercise(cancel: bool) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_url = format!("ws://{}", listener.local_addr().unwrap());
    let dir = tempfile::tempdir().unwrap();
    let (resume, wait) = oneshot::channel();
    let server = tokio::spawn(mock(listener, dir.path().to_owned(), wait));
    let app = router(Config {
        ws_url,
        ws_token: "host-token".into(),
        api_key: "client-key".into(),
        cwd: r"C:\repo".into(),
        images: Some(dir.path().to_owned()),
        timeout: Duration::from_secs(5),
    });
    let body = json!({ "stream": true, "model": "vision-model", "messages": [{
        "role": "user", "content": [
            { "type": "text", "text": "Describe this" },
            { "type": "image_url", "image_url": { "url": "data:image/png;base64,iVBORw0KGgo=" } }
        ]
    }]});
    let request = Request::post("/v1/chat/completions")
        .header("authorization", "Bearer client-key")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut events = response.into_body().into_data_stream();
    let first = events.next().await.unwrap().unwrap();
    assert!(String::from_utf8_lossy(&first).contains("assistant"));
    let delta = events.next().await.unwrap().unwrap();
    assert!(String::from_utf8_lossy(&delta).contains("hello"));
    resume.send(!cancel).unwrap();
    if cancel {
        drop(events);
    } else {
        let tail = events.collect::<Vec<_>>().await;
        let tail = tail
            .into_iter()
            .map(|part| String::from_utf8(part.unwrap().to_vec()).unwrap())
            .collect::<String>();
        assert!(tail.contains("\"finish_reason\":\"stop\""));
        assert!(tail.contains("data: [DONE]"));
        assert!(!tail.contains("hello"));
    }
    server.await.unwrap();
    while dir.path().read_dir().unwrap().next().is_some() {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test]
async fn streams_image_response_before_turn_finishes() {
    timeout(Duration::from_secs(10), exercise(false))
        .await
        .unwrap();
}

#[tokio::test]
async fn interrupts_turn_and_cleans_image_on_disconnect() {
    timeout(Duration::from_secs(10), exercise(true))
        .await
        .unwrap();
}

#[tokio::test]
async fn responses_returns_generated_image_in_json_and_sse() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("generated.png");
    tokio::fs::write(&path, b"\x89PNG\r\n\x1a\n").await.unwrap();
    for (streaming, disabled) in [(false, false), (true, false), (true, true)] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let app = router(Config {
            ws_url: format!("ws://{}", listener.local_addr().unwrap()),
            ws_token: "host-token".into(),
            api_key: "client-key".into(),
            cwd: dir.path().to_str().unwrap().into(),
            images: None,
            timeout: Duration::from_secs(5),
        });
        let path = path.clone();
        let upstream = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(tcp).await.unwrap();
            let init = read(&mut socket).await;
            send(&mut socket, json!({"id": init["id"], "result": {}})).await;
            assert_eq!(read(&mut socket).await["method"], "initialized");
            let models = read(&mut socket).await;
            send(
                &mut socket,
                json!({"id": models["id"], "result": {"data": [{"id": "test-model"}]}}),
            )
            .await;
            let thread = read(&mut socket).await;
            assert!(thread["params"].get("dynamicTools").is_none());
            assert_eq!(
                thread["params"]["config"]["features"]["image_generation"],
                disabled.then_some(false).map_or(Value::Null, Value::Bool)
            );
            send(
                &mut socket,
                json!({"id": thread["id"], "result": {"thread": {"id": "thread"}}}),
            )
            .await;
            let turn = read(&mut socket).await;
            send(
                &mut socket,
                json!({"id": turn["id"], "result": {"turn": {"id": "turn"}}}),
            )
            .await;
            let mut item = json!({"type": "imageGeneration", "status": "completed"});
            if streaming {
                item["savedPath"] = json!(path);
            } else {
                item["result"] = json!("iVBORw0KGgo=");
            }
            send(
                &mut socket,
                json!({"method": "item/completed", "params": {
                    "threadId": "thread", "turnId": "turn", "item": item
                }}),
            )
            .await;
            send(
                &mut socket,
                json!({"method": "item/agentMessage/delta", "params": {
                    "threadId": "thread", "turnId": "turn", "itemId": "reply", "delta": "Image ready."
                }}),
            )
            .await;
            send(
                &mut socket,
                json!({"method": "turn/completed", "params": {
                    "threadId": "thread", "turn": {"id": "turn", "status": "completed"}
                }}),
            )
            .await;
        });
        let request = Request::post("/v1/responses")
            .header("authorization", "Bearer client-key")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({
                    "model": "test-model", "input": "Draw a red circle",
                    "stream": streaming, "tools": [{"type": "image_generation"}],
                    "tool_choice": disabled.then_some("none")
                })
                .to_string(),
            ))
            .unwrap();
        let response = read_response(app.oneshot(request).await.unwrap(), streaming).await;
        assert_eq!(response["output"][0]["content"][0]["text"], "Image ready.");
        let item = &response["output"][1];
        if disabled {
            assert!(item.is_null());
        } else {
            assert_eq!(item["type"], "image_generation_call");
            assert_eq!(item["result"], "iVBORw0KGgo=");
            assert!(item.get("savedPath").is_none());
        }
        upstream.await.unwrap();
    }
}

#[tokio::test]
async fn rejects_missing_client_key_before_connecting() {
    let app = router(Config {
        ws_url: "ws://127.0.0.1:1".into(),
        ws_token: "host-token".into(),
        api_key: "client-key".into(),
        cwd: r"C:\repo".into(),
        images: None,
        timeout: Duration::from_secs(1),
    });
    let request = Request::get("/v1/models").body(Body::empty()).unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let body = to_bytes(response.into_body(), 1024).await.unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["error"]["message"],
        "Invalid API key"
    );
}
