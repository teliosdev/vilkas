use crate::http::Context;
use crate::recommend::Request as RecommendRequest;
use crate::storage::Store;
use failure::Error;
use rouille::{Request, Response};
use serde_json::json;

pub fn apply(request: &Request, context: &Context<impl Store>) -> Result<Response, Error> {
    let request: RecommendRequest = rouille::input::json_input(request)?;
    let response = context.core.recommend(&request)?;
    Ok(Response::json(&json!({ "result": response })))
}
