use crate::http::Context;
use crate::storage::{ItemStorage, UserStorage};
use failure::Error;
use rouille::{Request, Response};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ViewRequest {
    #[serde(alias = "p")]
    pub part: String,
    #[serde(alias = "u")]
    pub user: String,
    #[serde(alias = "i")]
    pub item: Uuid,
    #[serde(alias = "a")]
    pub actid: Option<Uuid>,
}

pub fn apply_get(request: &Request, context: &Context) -> Response {
    let part = request.get_param("part").or_else(|| request.get_param("p"));
    let user = request.get_param("user").or_else(|| request.get_param("u"));
    let item = request.get_param("item").or_else(|| request.get_param("i"));
    let item = item.and_then(|i| i.parse::<Uuid>().ok());
    let actid = request
        .get_param("actid")
        .or_else(|| request.get_param("a"));
    let actid = actid.and_then(|i| i.parse::<Uuid>().ok());
    let pair = user.and_then(|u| item.and_then(|i| part.map(|p| (u, i, p))));

    match pair {
        Some((user, item, part)) => {
            let view = ViewRequest {
                part,
                user,
                item,
                actid,
            };
            apply(request, &view, context)
                .unwrap_or_else(|_| Response::empty_204().with_status_code(500))
        }

        None => Response::empty_400(),
    }
}

pub fn apply_post(request: &Request, context: &Context) -> Response {
    let view: ViewRequest = try_or_400!(rouille::input::json_input(request));
    apply(request, &view, context).unwrap_or_else(|_| Response::empty_204().with_status_code(500))
}

fn apply(_request: &Request, view: &ViewRequest, context: &Context) -> Result<Response, Error> {
    let user = context.storage.find_user(&view.part, &view.user)?;

    let nears = std::iter::once((
        view.item,
        Box::new(user.history.iter().cloned()) as Box<dyn Iterator<Item = _>>,
    ))
    .chain(user.history.iter().map(|his| {
        (
            *his,
            Box::new(std::iter::once(view.item)) as Box<dyn Iterator<Item = _>>,
        )
    }));

    context.storage.items_add_bulk_near(&view.part, nears)?;
    context
        .storage
        .items_view(&view.part, view.item, context.last.push_view())?;

    Ok(Response::empty_204())
}
