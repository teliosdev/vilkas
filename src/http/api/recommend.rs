use crate::http::Context;
use crate::recommend::Request as RecommendRequest;
use rouille::{Request, Response};
use serde_json::json;

pub fn apply(request: &Request, context: &Context) -> Response {
    let request: RecommendRequest = try_or_400!(rouille::input::json_input(request));
    match context.core.recommend(&request) {
        Ok(response) => Response::json(&response),
        Err(_) => Response::json(&json!({"_err": true})).with_status_code(500),
    }
}
