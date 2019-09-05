use crate::http::Context;
use crate::recommend::Request as RecommendRequest;
use crate::storage::Store;
use rouille::{Request, Response};
use serde_json::json;

pub fn apply(request: &Request, context: &Context<impl Store>) -> Response {
    let request: RecommendRequest = try_or_400!(rouille::input::json_input(request));
    match context.core.recommend(&request) {
        Ok(response) => Response::json(&json!({ "result": response })),
        Err(_) => Response::json(&json!({"_err": true})).with_status_code(500),
    }
}
