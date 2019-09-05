use crate::http::Context;
use crate::storage::{Store, UserData};
use failure::Error;
use rouille::{Request, Response};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewRequest {
    #[serde(alias = "p")]
    pub part: String,
    #[serde(alias = "u")]
    pub user: String,
    #[serde(alias = "i")]
    pub item: Uuid,
    #[serde(alias = "a")]
    pub actid: Option<Uuid>,
}

pub fn apply_get(request: &Request, context: &Context<impl Store>) -> Response {
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

pub fn apply_post(request: &Request, context: &Context<impl Store>) -> Response {
    let view: ViewRequest = try_or_400!(rouille::input::json_input(request));
    apply(request, &view, context).unwrap_or_else(|_| Response::empty_204().with_status_code(500))
}

fn apply(
    _request: &Request,
    view: &ViewRequest,
    context: &Context<impl Store>,
) -> Result<Response, Error> {
    let user = context.storage.find_user(&view.part, &view.user)?;

    calculate_near(view, &user, context)?;
    context
        .storage
        .items_view(&view.part, view.item, context.last.push_view())?;
    context
        .storage
        .user_push_history(&view.part, &view.user, view.item)?;

    if let Some(activity) = view.actid {
        complete_activity(activity, view, context)?;
    }

    Ok(Response::empty_204())
}

fn calculate_near(
    view: &ViewRequest,
    user: &UserData,
    context: &Context<impl Store>,
) -> Result<(), Error> {
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

    context.storage.items_add_bulk_near(&view.part, nears)
}

fn complete_activity(
    activity: Uuid,
    view: &ViewRequest,
    context: &Context<impl Store>,
) -> Result<(), Error> {
    let chosen = [view.item];
    context
        .storage
        .model_activity_choose(&view.part, activity, &chosen)
}
