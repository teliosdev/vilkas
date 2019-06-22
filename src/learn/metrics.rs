use num_traits::Float;
use std::cell::Cell;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::iter::from_fn;
use std::rc::Rc;
use std::vec::IntoIter;

fn positive_counts<'r, T: Float + Debug>(
    ytrue: &'r [T],
    yhat: &'r [T],
) -> impl Iterator<Item=(T, T)> {
    let mut pairs = yhat
        .iter()
        .cloned()
        .zip(ytrue.iter().cloned())
        .collect::<Vec<_>>();
    pairs.sort_by(|(a, _), (b, _)| b.partial_cmp(a).unwrap_or(Ordering::Equal));
    let mut score_prev = T::neg_infinity();
    // gotta do this because we need them in both the flat_map closure and the
    // chain(from_fn()) closure.
    let tp = Rc::new(Cell::new(T::zero()));
    let tpc = tp.clone();
    let fp = Rc::new(Cell::new(T::zero()));
    let fpc = fp.clone();
    pairs
        .into_iter()
        .flat_map(move |(score, label)| {
            let value = if score != score_prev {
                score_prev = score;
                Some((fp.get(), tp.get()))
            } else {
                None
            };
            tp.set(tp.get() + label);
            fp.set(fp.get() + T::one() - label);
            value
        })
        .chain(from_fn(move || Some((fpc.get(), tpc.get()))).take(1))
}

//fn positive_rates<'r, T: Float + Default>(ytrue: &'r [T], yhat: &'r [T]) -> impl Iterator<Item = (T, T)>{
//    let counts = positive_counts(ytrue, yhat).collect::<Vec<_>>();
//    let (total_true, total_false) = counts.last().cloned().unwrap_or_default();
//
//    counts.into_iter().map(|(tp, fp)| {
//        (tp / total_true, fp / total_false)
//    })
//}

fn trapezoidal_area<T: Float>((x1, y1): (T, T), (x2, y2): (T, T)) -> T {
    (x1 - x2).abs() * ((y1 + y2) / (T::one() + T::one()))
}

fn trapezoidal<T: Float + Debug + Default>(xy: impl Iterator<Item=(T, T)>) -> T {
    let init = (T::zero(), (T::zero(), T::zero()));
    let (integral, (x, y)) = xy.fold(init, |(integral, prev), cur| {
        let integral = integral + trapezoidal_area(cur, prev);
        (integral, cur)
    });

    integral / (x * y)
}

pub fn roc_auc_score<T: Float + Debug + Default>(ytrue: &[T], yhat: &[T]) -> T {
    trapezoidal(positive_counts(ytrue, yhat))
}

#[cfg(test)]
mod tests {
    use super::roc_auc_score;

    fn close(x: f32, y: f32, e: f32) -> bool {
        let abs_x = x.abs();
        let abs_y = y.abs();
        let diff = (x - y).abs();

        if x == y {
            true
        } else if x == 0.0 || y == 0.0 || (abs_x + abs_y < ::std::f32::MIN_POSITIVE) {
            diff < (e * ::std::f32::MIN_POSITIVE)
        } else {
            diff / (abs_x + abs_y) < e
        }
    }

    #[test]
    fn test_basic_auc() {
        let ytrue = vec![1.0, 1.0, 0.0, 0.0];
        let yhat = vec![0.5, 0.2, 0.3, -1.0];
        let auc = roc_auc_score(&ytrue, &yhat);
        assert!(close(auc, 0.75, 0.00001));
    }

    #[test]
    fn test_basic_other() {
        let ytrue = vec![1.0, 1.0, 0.0, 0.0];
        let yhat = vec![0.5, 0.5, -1.0, 0.5];

        let auc = roc_auc_score(&ytrue, &yhat);
        dbg!(auc);
        assert!(close(auc, 0.75, 0.00001));
    }
}
