#![warn(clippy::all)]

#[cfg(feature = "aerospike")]
#[macro_use]
extern crate aerospike;
#[macro_use]
extern crate rouille;
#[macro_use]
extern crate serde;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_json;

pub mod http;
pub mod learn;
mod ord;
pub mod recommend;
pub mod storage;

fn main() {
    env_logger::init();
    let mut config = config::Config::new();
    let env = config::Environment::with_prefix("VILKAS");
    let file = config::File::with_name("vilkas").required(false);
    config.merge(env).expect("could not load env config");
    config.merge(file).expect("could not load file config");
    http::run(config);
}
