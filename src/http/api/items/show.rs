use crate::http::Context;
use crate::storage::Store;
use rouille::try_or_400::ErrJson;
use rouille::{Request, Response};
use uuid::Uuid;

pub fn apply(request: &Request, context: &Context<impl Store>) -> Response {
    let id = request.get_param("id").and_then(|v| v.parse::<Uuid>().ok());
    let part = request.get_param("part");
    let id = match id {
        None => {
            return Response::json(&json!({"description": "missing id"})).with_status_code(400);
        }
        Some(id) => id,
    };
    let part = match part {
        None => {
            return Response::json(&json!({"description":"missing part"})).with_status_code(400);
        }
        Some(part) => part,
    };
    match context.storage.find_item(&part, id) {
        Ok(result) => Response::json(&json!({ "result": result })),
        Err(err) => {
            error!("{:#?}", err);
            Response::json(&ErrJson::from_err(&err.compat())).with_status_code(500)
        }
    }
}
