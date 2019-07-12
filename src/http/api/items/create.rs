use crate::http::Context;
use crate::storage::{Item, Storage};
use rouille::{Request, Response};

pub fn apply(request: &Request, context: &Context<impl Storage>) -> Response {
    eprintln!("attempting to load item...");
    let item: Item = try_or_400!(rouille::input::json_input(request));
    eprintln!("item loaded: {:?}", item);
    match context.storage.items_insert(&item) {
        Ok(_) => Response::empty_204(),
        Err(_) => Response::json(&json!({"_err": true})).with_status_code(500),
    }
}
