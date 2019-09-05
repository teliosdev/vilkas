use crate::recommend::Core;
use crate::storage::{DefaultStorage, Store};
use config::Config;
use failure::Error;
use rouille::{start_server, Request, Response};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

mod api;
#[cfg(all(test, feature = "lmdb"))]
mod tests;

pub fn run(config: Config) -> ! {
    let addr = config
        .get_str("http.addr")
        .unwrap_or_else(|_| "0.0.0.0:3000".into());
    let context = Context::load(config);
    Core::train_loop(&context.core);
    eprintln!("listening on address {}...", addr);
    start_server(addr, move |request| handle(request, &context))
}

fn handle(request: &Request, context: &Context<impl Store>) -> Response {
    debug!("{} {}", request.method(), request.url());
    match handle_request(request, context) {
        Ok(r) => r,
        Err(e) => {
            error!("received error during request: {}", e);
            error!("{:#?}", e);

            Response::json(&json!({ "_err": true })).with_status_code(500)
        }
    }
}

fn handle_request(request: &Request, context: &Context<impl Store>) -> Result<Response, Error> {
    router!(request,
        (POST)["/api/recommend"] => {  api::recommend::apply(request, &context) },
        (GET)["/api/view"] => { api::view::apply_get(request, &context) },
        (POST)["/api/view"] => { api::view::apply_post(request, &context) },
        (POST)["/api/items"] => { api::items::create::apply(request, &context) },
        (DELETE)["/api/items"] => { api::items::delete::apply(request, &context) },
        (GET)["/api/items"] => { api::items::show::apply(request, &context) },
        (GET)["/api/model/{name}", name: String] => { api::model::show(request, name, context) },
        (POST)["/api/model/{name}/train", name: String] => { api::model::train(request, name, context) },
        _ => { Ok(Response::empty_404()) })
}

#[derive(Debug)]
pub struct Context<T: Store + 'static> {
    core: Arc<Core<T>>,
    storage: Arc<T>,
    last: LastView,
}

impl Context<DefaultStorage> {
    pub fn load(config: Config) -> Context<DefaultStorage> {
        let storage = Arc::new(DefaultStorage::load(&config));
        let core = Arc::new(Core::of(&storage, &config));
        Context {
            core,
            storage,
            last: LastView::default(),
        }
    }
}

#[derive(Debug)]
pub struct LastView {
    instant: Mutex<Instant>,
    count: AtomicU64,
    average: AtomicU64,
}

impl Default for LastView {
    fn default() -> LastView {
        LastView {
            instant: Mutex::new(Instant::now()),
            count: AtomicU64::new(0),
            average: AtomicU64::new(0),
        }
    }
}

impl LastView {
    pub fn push_view(&self) -> f64 {
        let value = self.push();
        let value = value as f64;
        1.0 / (1.0 + (-value / 60.0).exp())
    }

    pub fn push(&self) -> u64 {
        let current = Instant::now();
        let since = {
            let mut instant = self.instant.lock().unwrap();
            let since = current.duration_since(*instant);
            *instant = current;
            since
        };

        let since = since.as_millis() as u64;
        let count = self.count.fetch_add(1, Ordering::SeqCst);
        let mut prev = self.average.load(Ordering::SeqCst);
        loop {
            let new = if count == 0 {
                since
            } else {
                (prev * count + since) / count
            };

            match self
                .average
                .compare_exchange(prev, new, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => {
                    return new;
                }
                Err(e) => {
                    prev = e;
                }
            }
        }
    }
}
