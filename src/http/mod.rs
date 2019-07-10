use crate::recommend::Core;
use crate::storage::DefaultStorage;
use config::Config;
use rouille::{router, start_server, Response};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

mod api;

pub fn run(config: Config) -> ! {
    let addr = config
        .get_str("http.addr")
        .unwrap_or_else(|_| "0.0.0.0:3000".into());
    let context = Context::load(config);
    eprintln!("listening on address {}...", addr);
    start_server(addr, move |request| {
        router!(request,
            (POST)["/api/recommend"] => {  api::recommend::apply(request, &context) },
            (GET)["/api/view"] => { api::view::apply_get(request, &context) },
            (POST)["/api/view"] => { api::view::apply_post(request, &context) },
            _ => { Response::empty_404() }
        )
    })
}

#[derive(Debug)]
pub struct Context {
    config: Config,
    core: Core<DefaultStorage>,
    storage: Arc<DefaultStorage>,
    last: LastView,
}

impl Context {
    pub fn load(config: Config) -> Context {
        let storage = Arc::new(DefaultStorage::load(&config));
        let core = Core::of(&storage, &config);
        Context {
            config,
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
            let new = (prev * (count - 1) + since) / count;

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
