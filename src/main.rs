#![warn(clippy::all)]

#[cfg(feature = "aerospike")]
#[macro_use]
extern crate aerospike;
#[macro_use]
extern crate rouille;
#[macro_use]
extern crate serde;

use rand::Rng;

use crate::learn::metrics::roc_auc_score;
use crate::learn::{logistic, Algorithm, Vector};

pub mod http;
pub mod learn;
pub mod recommend;
pub mod storage;

fn main() {
    const TOP: usize = 1000;
    let mut r = ::rand::thread_rng();
    let data_set: Vec<(Vector<f64>, f64)> = (0..TOP)
        .map(move |i| {
            let positive = i % 2 == 0;
            let associated = r.gen_bool(0.80);
            let associated = if positive { associated } else { !associated };
            let features = (0..5)
                .map(|_| f64::from(r.gen_range(0u32, 10u32)))
                .chain(Some(f64::from(u32::from(associated))).into_iter())
                .collect::<Vector<f64>>();
            (features, f64::from(u32::from(positive)))
        })
        .collect::<Vec<_>>();
    println!("done generating data.");

    let mut log = logistic::Parameters::default().gradient_cap(0.1).build();

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
