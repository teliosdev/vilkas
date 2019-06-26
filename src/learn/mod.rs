pub mod logistic;
pub mod metrics;
pub mod vector;

use num_traits::Float;

pub use self::vector::{combine, Vector};

pub trait Algorithm<T: Float + Default + 'static> {
    fn fit(&mut self, examples: &[(Vector<T>, T)]);
    fn train(&mut self, examples: &[(Vector<T>, T)]);
    fn predict_iter<'o>(
        &'o self,
        iter: Box<dyn Iterator<Item = &'o Vector<T>> + 'o>,
    ) -> Box<dyn Iterator<Item = T> + 'o>;
    fn predict_slice(&self, examples: &[Vector<T>]) -> Vec<T>;
}
