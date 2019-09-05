use crate::http::Context;
use crate::storage::{Item, Store};
use failure::Error;
use rouille::{Request, Response};

pub fn apply(request: &Request, context: &Context<impl Store>) -> Result<Response, Error> {
    let item: Item = rouille::input::json_input(request)?;
    context.storage.items_insert(&item)?;
    Ok(Response::empty_204())
}
