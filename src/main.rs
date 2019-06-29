#![warn(clippy::all)]

#[macro_use]
extern crate serde;

// #[cfg(feature = "aerospike")]
#[macro_use]
extern crate aerospike;

use crate::learn::metrics::roc_auc_score;
use crate::learn::{logistic, Algorithm, Vector};
use rand::Rng;

pub mod http;
pub mod learn;
pub mod storage;

fn main() {
    const TOP: usize = 1000;
    let mut r = ::rand::thread_rng();
    let data_set: Vec<(Vector<f64>, f64)> = (0..TOP)
        .into_iter()
        .map(move |i| {
            let positive = i % 2 == 0;
            let associated = r.gen_bool(0.80);
            let associated = if positive { associated } else { !associated };
            let features = (0..5)
                .into_iter()
                .map(|_| r.gen_range(0u32, 10u32) as f64)
                .chain(Some(associated as u32 as f64).into_iter())
                .collect::<Vector<f64>>();
            (features, positive as u32 as f64)
        })
        .collect::<Vec<_>>();
    println!("done generating data.");

    let mut log = logistic::Parameters::new().gradient_cap(0.1).build();

    println!("training...");
    log.train(&data_set[..]);
    println!("done!");

    dbg!(&log);

    let predictions = log
        .predict(data_set.iter().map(|(a, _)| a))
        .collect::<Vec<_>>();
    let targets = data_set.iter().map(|(_, v)| *v).collect::<Vec<_>>();

    dbg!(roc_auc_score(&targets, &predictions));
}
