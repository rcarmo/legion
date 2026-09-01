use extism_pdk::*;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Args { name: Option<String> }
#[derive(Serialize)]
struct Output { greeting: String, runtime: &'static str }

#[plugin_fn]
pub fn run(input: String) -> FnResult<Json<Output>> {
    let args: Args = serde_json::from_str(&input).unwrap_or(Args { name: None });
    Ok(Json(Output {
        greeting: format!("Hello, {}!", args.name.unwrap_or_else(|| "world".into())),
        runtime: "wasm",
    }))
}
