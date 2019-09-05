use crate::http::Context;
use crate::storage::Store;
use rouille::{Request, Response};

pub fn show(_request: &Request, which: String, context: &Context<impl Store>) -> Response {
    let model = context.storage.find_model(&which).and_then(|m| {
        m.map(Ok)
            .unwrap_or_else(|| context.storage.find_default_model())
    });

    match model {
        Ok(m) => Response::json(&json!({ "result": m })),
        Err(_) => Response::json(&json!({"_err": true})).with_status_code(500),
    }
}

pub fn train(_request: &Request, _which: String, context: &Context<impl Store>) -> Response {
    match context.core.load_train() {
        Ok(_) => Response::empty_204(),
        Err(e) => {
            error!("{:#?}", e);
            Response::json(&json!({"_err": true})).with_status_code(500)
        }
    }
}
