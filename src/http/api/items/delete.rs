use crate::http::Context;
use crate::storage::Store;
use rouille::{Request, Response};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteItem {
    part: String,
    id: Uuid,
}

pub fn apply(request: &Request, context: &Context<impl Store>) -> Response {
    let item: DeleteItem = try_or_400!(rouille::input::json_input(request));
    match context.storage.items_delete(&item.part, item.id) {
        Ok(_) => Response::empty_204(),
        Err(_) => Response::json(&json!({"_err": true})).with_status_code(500),
    }
}
