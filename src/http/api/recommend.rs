use crate::http::Context;
use rouille::{Request, Response};
use uuid::Uuid;

pub fn apply(request: &Request, context: &Context<'_>) -> Response {
    let request: RecommendRequest = try_or_400!(rouille::input::json_input(request));
    unimplemented!()
}

#[derive(Debug, Serialize, Deserialize)]
struct RecommendRequest {
    #[serde(alias = "u")]
    user: Uuid,
    #[serde(alias = "c")]
    current: Uuid,
    #[serde(alias = "w")]
    whitelist: Option<Vec<Uuid>>,
}
