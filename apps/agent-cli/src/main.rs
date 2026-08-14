use observability_core::{
    model_observation, DeterministicModelProvider, ModelProvider, ModelRequest, ModelResponse,
    Observation, TenantId,
};
use std::env;
use uuid::Uuid;

fn value_after(flag: &str, args: &[String]) -> Option<String> {
    args.windows(2)
        .find(|pair| pair[0] == flag)
        .map(|pair| pair[1].clone())
}

struct OpenAiCompatibleProvider {
    endpoint: String,
    api_key: String,
    client: reqwest::blocking::Client,
}

impl ModelProvider for OpenAiCompatibleProvider {
    fn complete(&self, request: &ModelRequest) -> Result<ModelResponse, String> {
        let response = self
            .client
            .post(&self.endpoint)
            .bearer_auth(&self.api_key)
            .json(&serde_json::json!({
                "model": request.model,
                "messages": [{"role": "user", "content": request.prompt}],
                "metadata": {"evidence_ids": request.evidence_ids}
            }))
            .send()
            .map_err(|error| error.to_string())?;
        if !response.status().is_success() {
            return Err(format!("model provider returned {}", response.status()));
        }
        let body: serde_json::Value = response.json().map_err(|error| error.to_string())?;
        let text = body["choices"][0]["message"]["content"]
            .as_str()
            .ok_or_else(|| "provider response missing choices[0].message.content".to_string())?
            .to_string();
        Ok(ModelResponse {
            model: request.model.clone(),
            text,
            input_tokens: body["usage"]["prompt_tokens"].as_u64().unwrap_or_default(),
            output_tokens: body["usage"]["completion_tokens"]
                .as_u64()
                .unwrap_or_default(),
        })
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let prompt = value_after("--prompt", &args).unwrap_or_else(|| {
        eprintln!(
            "usage: observability-agent --prompt <text> [--model <name>] [--evidence <json-file>]"
        );
        std::process::exit(2);
    });
    let model = value_after("--model", &args).unwrap_or_else(|| "local-deterministic".into());
    let evidence_ids = value_after("--evidence", &args)
        .map(|path| {
            let content = std::fs::read_to_string(path).expect("read evidence file");
            let observations: Vec<Observation> =
                serde_json::from_str(&content).expect("parse evidence JSON");
            observations
                .into_iter()
                .map(|observation| observation.id)
                .collect()
        })
        .unwrap_or_default();
    let request = ModelRequest {
        tenant_id: TenantId(Uuid::new_v4()),
        model,
        prompt,
        evidence_ids,
    };
    let response = if let Some(endpoint) = value_after("--endpoint", &args) {
        let key_env = value_after("--api-key-env", &args).unwrap_or_else(|| "MODEL_API_KEY".into());
        let api_key = env::var(key_env).expect("model API key environment variable");
        OpenAiCompatibleProvider {
            endpoint,
            api_key,
            client: reqwest::blocking::Client::new(),
        }
        .complete(&request)
        .expect("model provider request")
    } else {
        DeterministicModelProvider
            .complete(&request)
            .expect("valid prompt")
    };
    let observation = model_observation(&request, &response, "cli-trace", 0, 0);
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "response": response,
            "observation": observation
        }))
        .expect("serialize response")
    );
}
