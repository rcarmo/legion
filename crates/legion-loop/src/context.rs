//! Context window builder: converts stored `TurnEnvelope`s into rs-ai `Message`s.

use legion_core::types::{TurnEnvelope, TurnEventKind};
use rs_ai::types::{ContentBlock, Message, Role};

/// Build an rs-ai message list from the recent turn history.
///
/// Only `UserMessage`, `AssistantMessage`, and `ToolResult` turns are included;
/// write-ahead intents and lifecycle events are skipped.
pub fn build_messages(envelopes: &[TurnEnvelope]) -> Vec<Message> {
    let mut messages = Vec::new();

    for env in envelopes {
        match &env.event.kind {
            TurnEventKind::UserMessage => {
                let text = extract_text_content(&env.event.payload);
                messages.push(user_message(text));
            }
            TurnEventKind::AssistantMessage => {
                messages.push(assistant_message_from_payload(&env.event.payload));
            }
            TurnEventKind::ToolResult { call_id } => {
                let result = env
                    .event
                    .payload
                    .as_ref()
                    .map(|v| v.to_string())
                    .unwrap_or_default();
                let is_error = env
                    .event
                    .payload
                    .as_ref()
                    .is_some_and(|payload| payload.get("error").is_some());
                let tool_name = env.event.model.as_deref().unwrap_or_else(|| {
                    envelopes
                        .iter()
                        .rev()
                        .filter(|candidate| candidate.seq < env.seq)
                        .find_map(|candidate| match &candidate.event.kind {
                            TurnEventKind::ToolCallIntent {
                                tool_name,
                                call_id: intent_id,
                                ..
                            } if intent_id == call_id => Some(tool_name.as_str()),
                            _ => None,
                        })
                        .unwrap_or("")
                });
                messages.push(tool_result_message(call_id, tool_name, result, is_error));
            }
            // Skip intents and lifecycle events — not LLM-visible
            _ => {}
        }
    }
    messages
}

fn extract_text_content(payload: &Option<serde_json::Value>) -> String {
    match payload {
        Some(v) => {
            if let Some(s) = v.get("content").and_then(|c| c.as_str()) {
                s.to_string()
            } else {
                v.to_string()
            }
        }
        None => String::new(),
    }
}

fn blank_message(role: Role, text: String) -> Message {
    Message {
        role,
        content: vec![ContentBlock::Text {
            text,
            text_signature: None,
        }],
        timestamp: 0,
        api: None,
        provider: None,
        model: None,
        response_id: None,
        response_model: None,
        diagnostics: Vec::new(),
        usage: None,
        stop_reason: None,
        deferred: None,
        error_message: None,
        raw_stop_reason: None,
        end_turn: None,
        tool_call_id: None,
        tool_name: None,
        is_error: false,
        details: None,
        added_tool_names: Vec::new(),
    }
}

pub fn user_message(text: String) -> Message {
    blank_message(Role::User, text)
}

pub fn assistant_message(text: String) -> Message {
    blank_message(Role::Assistant, text)
}

fn assistant_message_from_payload(payload: &Option<serde_json::Value>) -> Message {
    let Some(payload) = payload else {
        return assistant_message(String::new());
    };
    if let Some(message) = payload
        .get("message")
        .and_then(|value| serde_json::from_value::<Message>(value.clone()).ok())
    {
        return message;
    }
    assistant_message(extract_text_content(&Some(payload.clone())))
}

pub fn tool_result_message(
    call_id: &str,
    tool_name: &str,
    result: String,
    is_error: bool,
) -> Message {
    Message {
        role: Role::ToolResult,
        content: vec![ContentBlock::Text {
            text: result,
            text_signature: None,
        }],
        timestamp: 0,
        api: None,
        provider: None,
        model: None,
        response_id: None,
        response_model: None,
        diagnostics: Vec::new(),
        usage: None,
        stop_reason: None,
        deferred: None,
        error_message: None,
        raw_stop_reason: None,
        end_turn: None,
        tool_call_id: Some(call_id.to_string()),
        tool_name: Some(tool_name.to_string()),
        is_error,
        details: None,
        added_tool_names: Vec::new(),
    }
}
