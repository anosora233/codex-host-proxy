use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Usage {
    input_tokens: u64,
    output_tokens: u64,
    total_tokens: u64,
    #[serde(default)]
    cached_input_tokens: u64,
    #[serde(default)]
    reasoning_output_tokens: u64,
}

#[derive(Default)]
pub struct UsageTracker {
    reported: Usage,
    latest: Option<Usage>,
}

impl UsageTracker {
    pub fn update(&mut self, params: &Value) {
        if let Some(total) = params["tokenUsage"].get("total")
            && let Ok(usage) = Usage::deserialize(total)
        {
            self.latest = Some(usage);
        }
    }

    pub fn take(&mut self) -> Option<Usage> {
        let latest = self.latest.take()?;
        let previous = std::mem::replace(&mut self.reported, latest);
        Some(Usage {
            input_tokens: latest.input_tokens.saturating_sub(previous.input_tokens),
            output_tokens: latest.output_tokens.saturating_sub(previous.output_tokens),
            total_tokens: latest.total_tokens.saturating_sub(previous.total_tokens),
            cached_input_tokens: latest
                .cached_input_tokens
                .saturating_sub(previous.cached_input_tokens),
            reasoning_output_tokens: latest
                .reasoning_output_tokens
                .saturating_sub(previous.reasoning_output_tokens),
        })
    }
}

impl Usage {
    pub fn chat(self) -> Value {
        json!({
            "prompt_tokens": self.input_tokens, "completion_tokens": self.output_tokens,
            "total_tokens": self.total_tokens,
            "prompt_tokens_details": { "cached_tokens": self.cached_input_tokens },
            "completion_tokens_details": { "reasoning_tokens": self.reasoning_output_tokens }
        })
    }

    pub fn response(self) -> Value {
        json!({
            "input_tokens": self.input_tokens, "output_tokens": self.output_tokens,
            "total_tokens": self.total_tokens,
            "input_tokens_details": { "cached_tokens": self.cached_input_tokens },
            "output_tokens_details": { "reasoning_tokens": self.reasoning_output_tokens }
        })
    }
}
