use std::cmp::Ordering;
use std::fmt::Debug;

#[derive(Debug)]
struct FloatOrd<F>(F);

impl<F: PartialOrd> PartialOrd for FloatOrd<F> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<F: PartialOrd> Ord for FloatOrd<F> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.partial_cmp(&other.0).unwrap_or(Ordering::Equal)
    }
}

impl<F: PartialEq> PartialEq for FloatOrd<F> {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl<F: PartialEq> Eq for FloatOrd<F> {}

#[derive(Debug, PartialEq, Eq)]
struct ReverseOrd<F>(F);

impl<F: PartialOrd> PartialOrd for ReverseOrd<F> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.0.partial_cmp(&other.0).map(Ordering::reverse)
    }
}

impl<F: Ord + Debug> Ord for ReverseOrd<F> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0).reverse()
    }
}

pub fn sort_float<T, F: FnMut(&T) -> f64>(list: &mut Vec<T>, mut f: F) {
    list.sort_unstable_by_key(|a| ReverseOrd(FloatOrd(f(a))));
}

pub fn sort_cached_float<T, F: FnMut(&T) -> f64>(list: &mut Vec<T>, mut f: F) {
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

    #[test]
    fn it_sorts_cached_floats_descending() {
        let mut floats = vec![1.0, 5.0, 10.0];
        println!("precheck");
        sort_cached_float(&mut floats, |a| *a);
        assert_eq!(floats, vec![10.0, 5.0, 1.0]);
;    }
}
