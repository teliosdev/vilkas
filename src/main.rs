use crate::learn::logistic::LogisticRegression;
use crate::learn::Features;
use rand::Rng;
use crate::learn::metrics::ConfusionMatrix;

pub mod learn;

fn main() {
    const TOP: usize = 1000;
    let mut r = ::rand::thread_rng();
    let data_set: Vec<(Features<f64>, f64)> = (0..TOP)
        .into_iter()
        .map(move |i| {
            let positive = i % 2 == 0;
            let associated = r.gen_bool(0.80);
            let associated = if positive { associated } else { !associated };
            let features = (0..5)
                .into_iter()
                .map(|_| r.gen_range(0u32, 10u32) as f64)
                .chain(Some(associated as u32 as f64).into_iter())
                .enumerate()
                .map(|(v, i)| (v.to_string(), i))
                .collect::<Features<f64>>();
            (features, positive as u32 as f64)
        })
        .collect::<Vec<_>>();
    println!("done generating data.");

    let mut log = LogisticRegression::new();

    println!("training...");
    log.train(&data_set[..]);
    println!("done!");

    dbg!(&log);

    let predictions = log.predict(data_set.iter().map(|(a, _)| a)).collect::<Vec<_>>();
    let targets = data_set.iter().map(|(_, v)| *v).collect::<Vec<_>>();
    let matrix = ConfusionMatrix::with(0.5, &predictions, &targets);

    dbg!(matrix);
}
