use crate::http::Context;
use crate::storage::Store;
use failure::Error;
use rouille::{Request, Response};

pub fn show(
    _request: &Request,
    which: String,
    context: &Context<impl Store>,
) -> Result<Response, Error> {
    let model = context.storage.find_model(&which).and_then(|m| {
        m.map(Ok)
            .unwrap_or_else(|| context.storage.find_default_model())
    })?;

    Ok(Response::json(&json!({ "result": model })))
}

pub fn train(
    _request: &Request,
    _which: String,
    context: &Context<impl Store>,
) -> Result<Response, Error> {
    context.core.load_train()?;
    Ok(Response::empty_204())
}
