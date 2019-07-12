use crate::http::Context;
use crate::storage::Storage;
use rouille::{Request, Response};

pub fn show(request: &Request, which: String, context: &Context<impl Storage>) -> Response {
    let model = context.storage.find_model(&which).and_then(|m| {
        m.map(Ok)
            .unwrap_or_else(|| context.storage.find_default_model())
    });

    match model {
        Ok(m) => Response::json(&json!({ "result": m })),
        Err(_) => Response::json(&json!({"_err": true})).with_status_code(500),
    }
}
