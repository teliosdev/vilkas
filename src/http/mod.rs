use config::Config;
use rouille::{router, start_server, Response};

use crate::recommend::Core;
use crate::storage::DefaultStorage;
use std::sync::Arc;

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
            (POST)["/api/view"] => { api::view::apply(request, &context) },
            _ => { Response::empty_404() }
        )
    })
}

#[derive(Debug)]
pub struct Context {
    config: Config,
    core: Core<DefaultStorage>,
    storage: Arc<DefaultStorage>,
}

impl Context {
    pub fn load(config: Config) -> Context {
        let storage = Arc::new(DefaultStorage::load(&config));
        let core = Core::of(&storage, &config);
        Context {
            config,
            core,
            storage,
        }
    }
}
