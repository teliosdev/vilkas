use failure::Error;
use rouille::{Request, Response};
use uuid::Uuid;

use crate::http::Context;
use crate::storage::UserStorage;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ViewRequest {
    #[serde(alias = "u")]
    pub user: String,
    #[serde(alias = "i")]
    pub item: Uuid,
    #[serde(alias = "a")]
    pub actid: Option<Uuid>,
}

pub fn apply_get(request: &Request, context: &Context) -> Response {
    let user = request.get_param("user").or_else(|| request.get_param("u"));
    let item = request.get_param("item").or_else(|| request.get_param("i"));
    let item = item.and_then(|i| Uuid::from_str(&i).ok());
    let actid = request
        .get_param("actid")
        .or_else(|| request.get_param("a"));
    let actid = actid.and_then(|i| Uuid::from_str(&i).ok());
    let pair = user.and_then(|u| item.map(|i| (u, i)));

    match pair {
        Some((user, item)) => {
            let view = ViewRequest { user, item, actid };
            apply(request, &view, context)
        }

        None => Response::empty_400(),
    }
}

pub fn apply_post(request: &Request, context: &Context) -> Response {
    let view: ViewRequest = try_or_400!(rouille::input::json_input(request));
    apply(request, &view, context)
}

fn apply(request: &Request, view: &ViewRequest, context: &Context) -> Response {
    let user = context.storage.find_user(&view.user)?;
}
