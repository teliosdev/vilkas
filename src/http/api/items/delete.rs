use crate::http::Context;
use crate::storage::Store;
use failure::Error;
use rouille::{Request, Response};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteItem {
    part: String,
    id: Uuid,
}

pub fn apply(request: &Request, context: &Context<impl Store>) -> Result<Response, Error> {
    let item: DeleteItem = rouille::input::json_input(request)?;
    context.storage.items_delete(&item.part, item.id)?;
    Ok(Response::empty_204())
}
