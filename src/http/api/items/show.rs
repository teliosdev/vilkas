use crate::http::Context;
use crate::storage::Store;
use failure::Error;
use rouille::{Request, Response};
use uuid::Uuid;

pub fn apply(request: &Request, context: &Context<impl Store>) -> Result<Response, Error> {
    let id = request.get_param("id").and_then(|v| v.parse::<Uuid>().ok());
    let part = request.get_param("part");
    let id = match id {
        None => {
            return Ok(Response::json(&json!({"description": "missing id"})).with_status_code(400));
        }
        Some(id) => id,
    };
    let part = match part {
        None => {
            return Ok(Response::json(&json!({"description":"missing part"})).with_status_code(400));
        }
        Some(part) => part,
    };
    let result = context.storage.find_item(&part, id)?;
    Ok(Response::json(&json!({ "result": result })))
}
