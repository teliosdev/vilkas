use std::cmp::Ordering;

#[derive(Debug, PartialOrd, PartialEq)]
struct FloatOrd<F>(F);

impl<F: PartialOrd> Ord for FloatOrd<F> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

impl<F: PartialEq> Eq for FloatOrd<F> {}

#[derive(Debug, PartialOrd, PartialEq, Eq)]
struct ReverseOrd<F>(F);

impl<F: Ord> Ord for ReverseOrd<F> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0).reverse()
    }
}

pub fn sort_float<T, F: FnMut(&T) -> f64>(list: &mut Vec<T>, mut f: F) -> () {
    list.sort_unstable_by(|(a, b)| {
        let a = f(a);
        let b = f(b);

        a.partial_cmp(&b).unwrap_or(Ordering::Equal).reverse()
    });
}

pub fn sort_cached_float<T, F: FnMut(&T) -> f64>(list: &mut Vec<T>, mut f: F) -> () {
    list.sort_by_cached_key(|a| ReverseOrd(FloatOrd(f(a))));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_sorts_floats_descending() {
        let mut floats = vec![1.0, 5.0, 10.0];
        sort_float(&mut floats, |a| *a);
        assert_eq!(floats, vec![10.0, 5.0, 1.0]);
    }
}
