use crate::http::{handle_request, Context};
use crate::recommend::Core;
use crate::storage::{Item, ItemStore, Store};
use rouille::{Request, Response, ResponseBody};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use std::sync::Arc;
use uuid::Uuid;

fn context() -> Context<impl Store> {
    let storage = crate::storage::mem::tests::TemporaryFileWrap::load();
    let storage = Arc::new(storage);
    let core = Core {
        storage: storage.clone(),
        parameters: Default::default(),
        part_config: Default::default(),
        default_config: Default::default(),
    };
    Context {
        core: Arc::new(core),
        storage,
        last: Default::default(),
    }
}

fn request<T: Serialize, M: Into<String>, U: Into<String>>(
    method: M,
    url: U,
    body: Option<&T>,
    mut headers: Vec<(String, String)>,
) -> Request {
    let body = body.map(|b| serde_json::to_vec(&b).unwrap());
    if body.is_some() {
        headers.push(("Content-Type".into(), "application/json".into()));
    }
    Request::fake_http(method, url, headers, body.unwrap_or_default())
}

fn read_all<T: DeserializeOwned>(response: &mut Response) -> T {
    let mut empty = ResponseBody::empty();
    std::mem::swap(&mut empty, &mut response.data);
    let (reader, _) = empty.into_reader_and_size();
    serde_json::from_reader(reader).expect("could not read data")
}

#[test]
fn it_inserts_items() {
    let context = context();
    let id = Uuid::new_v4();
    let item = Item {
        id,
        part: "default".to_string(),
        views: 0,
        meta: Default::default(),
    };
    let insert_request = request("POST", "/api/items", Some(&item), vec![]);
    let response = handle_request(&insert_request, &context).expect("could not perform request");
    assert_eq!(response.status_code, 204);
    let show_request = request(
        "GET",
        format!("/api/items?id={}&part=default", id),
        None as Option<&()>,
        vec![],
    );
    let mut response = handle_request(&show_request, &context).expect("could not perform request");
    assert_eq!(response.status_code, 200);
    let data = read_all::<Value>(&mut response);
    let data = serde_json::from_value::<Item>(data["result"].clone()).unwrap();
    assert_eq!(item, data);
}

fn gen_item() -> Item {
    Item {
        id: Uuid::new_v4(),
        part: "default".to_string(),
        views: 0,
        meta: Default::default(),
    }
}

#[test]
fn it_generates_recommendations() {
    use crate::http::api::view::ViewRequest;
    use crate::recommend::{Request as RecommendRequest, Response as RecommendResponse};

    let context = context();
    let items = (0..20).map(|_| gen_item()).collect::<Vec<_>>();

    for item in items.iter() {
        context.storage.items_insert(item).unwrap();
    }

    let mut view = ViewRequest {
        part: "default".to_string(),
        user: "me".to_string(),
        item: Default::default(),
        actid: None,
    };
    for item in items.iter() {
        view.item = item.id;
        let request = request("POST", "/api/view", Some(&view), vec![]);
        let response = handle_request(&request, &context).expect("could not perform request");
        assert_eq!(response.status_code, 204);
    }

    let recreq = RecommendRequest {
        part: "default".to_string(),
        user: "me".to_string(),
        current: items.first().unwrap().id,
        whitelist: None,
        count: 5,
    };

    let request = request("POST", "/api/recommend", Some(&recreq), vec![]);
    let mut response = handle_request(&request, &context).expect("could not perform request");
    assert_eq!(response.status_code, 200);
    let data = read_all::<RecommendResponse>(&mut response);
    assert_eq!(data.items.len(), 5);
    dbg!(data);
    assert!(false);
}
