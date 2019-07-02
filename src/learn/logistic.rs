use std::mem::swap;

use num_traits::Float;

use crate::learn::Algorithm;

use super::Vector;

#[derive(Debug, Copy, Clone)]
pub struct Parameters<T: Float + Default + 'static> {
    learning_rate: T,
    gradient_cap: T,
    iteration_cap: usize,
    regularization: Option<(T, T)>,
}

impl<T: Float + Default + 'static> Default for Parameters<T> {
    fn default() -> Parameters<T> {
        Parameters {
            learning_rate: T::one(),
            gradient_cap: T::one(),
            iteration_cap: 10_000,
            regularization: None,
        }
    }
}

impl<T: Float + Default + 'static> Parameters<T> {
    pub fn learning_rate(self, learning_rate: T) -> Self {
        Parameters {
            learning_rate,
            ..self
        }
    }

    pub fn gradient_cap(self, gradient_cap: T) -> Self {
        Parameters {
            gradient_cap,
            ..self
        }
    }

    pub fn iteration_cap(self, iteration_cap: usize) -> Self {
        Parameters {
            iteration_cap,
            ..self
        }
    }

    pub fn l1_regularization(self, l1: T) -> Self {
        let regularization = match self.regularization {
            Some((_, l2)) => Some((l1, l2)),
            None => Some((l1, Default::default())),
        };

        Parameters {
            regularization,
            ..self
        }
    }

    pub fn l2_regularization(self, l2: T) -> Self {
        let regularization = match self.regularization {
            Some((l1, _)) => Some((l1, l2)),
            None => Some((Default::default(), l2)),
        };

        Parameters {
            regularization,
            ..self
        }
    }

    pub fn build(self) -> LogisticRegression<T> {
        let learning_rate = Some(self.learning_rate);
        LogisticRegression {
            parameters: self,
            learning_rate,
            loss: T::infinity(),
            previous: None,
            weights: Vector::empty(),
            gradients: Vector::empty(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LogisticRegression<T: Float + Default + 'static> {
    parameters: Parameters<T>,
    learning_rate: Option<T>,
    loss: T,
    previous: Option<(Vector<T>, Vector<T>)>,
    weights: Vector<T>,
    pub(crate) gradients: Vector<T>,
}

impl<T: Float + Default + 'static> LogisticRegression<T> {
    pub fn weights(&self) -> &Vector<T> {
        &self.weights
    }

    pub fn loss(&self) -> T {
        self.loss
    }

    pub fn predict<'o>(
        &'o self,
        examples: impl Iterator<Item = &'o Vector<T>> + 'o,
    ) -> impl Iterator<Item = T> + 'o {
        predict(&self.weights, examples)
    }

    fn adjust_learning_rate(&mut self) {
        match self.previous.as_ref() {
            None => {
                self.learning_rate = Some(self.parameters.learning_rate);
            }
            Some((pw, pg)) => {
                let gs = self
                    .gradients
                    .combine(pg)
                    .map(|(c, p)| c - p)
                    .collect::<Vector<_>>();
                let gw = self
                    .weights
                    .combine(pw)
                    .map(|(c, p)| c - p)
                    .collect::<Vector<_>>();

                let gs_dot = gs.dot(&gs);
                if gs_dot.is_zero() {
                    self.learning_rate = None;
                } else {
                    self.learning_rate = Some(gw.dot(&gs).abs() / gs_dot);
                }
            }
        }
    }
}

impl<T: Float + Default + 'static> Algorithm<T> for LogisticRegression<T> {
    fn fit(&mut self, examples: &[(Vector<T>, T)]) {
        match self.learning_rate {
            None => {}
            Some(learn) => {
                let weights = new_weight_step(&self.weights, &self.gradients, learn);
                let new_loss = loss(examples, &weights, self.parameters.regularization);
                let new_learn = Some(learn / (T::one() + T::one())).filter(|v| *v > T::zero());

                if new_loss > self.loss {
                    self.learning_rate = new_learn;
                    self.fit(examples);
                } else {
                    let mut new_weights = weights;
                    let mut new_gradients =
                        loss_gradient(examples, &new_weights, self.parameters.regularization);
                    swap(&mut self.weights, &mut new_weights);
                    swap(&mut self.gradients, &mut new_gradients);
                    self.previous = Some((new_weights, new_gradients));
                    self.loss = loss(examples, &self.weights, self.parameters.regularization);
                }
            }
        }
    }

    fn train(&mut self, examples: &[(Vector<T>, T)]) {
        self.gradients = loss_gradient(examples, &self.weights, self.parameters.regularization);
        self.loss = loss(examples, &self.weights, self.parameters.regularization);
        let mut iterations = 0usize;

        while self.gradients.magnitude() > self.parameters.gradient_cap
            && iterations < self.parameters.iteration_cap
            && self.learning_rate.is_some()
        {
            self.adjust_learning_rate();
            self.fit(examples);
            iterations += 1;
        }
    }

    fn predict_iter<'o>(
        &'o self,
        iter: Box<dyn Iterator<Item = &'o Vector<T>> + 'o>,
    ) -> Box<dyn Iterator<Item = T> + 'o> {
        Box::new(predict(self.weights(), iter))
    }

    fn predict_slice(&self, examples: &[Vector<T>]) -> Vec<T> {
        predict(self.weights(), examples.iter()).collect()
    }
}

pub fn predict<'o, T: Float + Default + 'static, E: Iterator<Item = &'o Vector<T>> + 'o>(
    weights: &'o Vector<T>,
    examples: E,
) -> impl Iterator<Item = T> + 'o {
    examples.map(move |example| sigmoid(example.dot(&weights)))
}

fn new_weight_step<'c, T: Float + Default>(
    weights: &'c Vector<T>,
    gradient: &'c Vector<T>,
    step: T,
) -> Vector<T> {
    weights
        .combine(gradient)
        .map(|(weight, gradient)| weight - gradient * step)
        .collect()
}

/// Calculates the loss of a set of examples (with their targets) against the
/// current weights.  The loss function works by taking every example, and
/// scoring the example with the given weights (which, for logistic models,
/// is done by [`sigmoid`] ([`Features::dot`] (examples, weights)).  Then,
/// the loss for each prediction is calculated; if the actual value is false,
/// then the loss is `-ln(1-pred)`, where `pred` is the prediction (for
/// logistic models, a number between 0 and 1); if the actual value is true,
/// then the loss is `-ln(pred)`, where `pred` is the prediction.  These are
/// all summed together to calculate the total loss on all of the examples.
fn loss<T: Float + Default + 'static>(
    examples: &[(Vector<T>, T)],
    weights: &Vector<T>,
    regularization: Option<(T, T)>,
) -> T {
    let sum = examples
        .iter()
        .map(|(example, target)| {
            let prediction = sigmoid(example.dot(weights));
            if target.is_zero() {
                // ln_1p = ln(1+n), and we need ln(1-n), so we negate n, before
                // calling ln_1p.
                prediction.neg().ln_1p()
            } else {
                prediction.ln()
            }
        })
        .fold(T::zero(), T::add);
    let base = (T::one().neg() / T::from(examples.len()).unwrap()) * sum;
    let reg = match regularization {
        None => T::zero(),
        Some((l1, l2)) => {
            let l1_loss = weights.iter().cloned().map(T::abs).fold(T::zero(), T::add);
            let l2_loss = weights.iter().map(|v| v.powi(2)).fold(T::zero(), T::add);

            l1 * l1_loss + l2 * l2_loss
        }
    };

    base + reg
}

/// This calculates the loss gradient of a set of examples (with their targets)
/// against the current weights.  This, essentially, applies the partial
/// derivative of the loss function against each of the weights, summing them,
/// and outputting the result.  This **should not** take the place of weights -
/// this is only one step in the process of gradient descent.
fn loss_gradient<'l, T: Float + Default + 'static>(
    examples: &'l [(Vector<T>, T)],
    weights: &Vector<T>,
    regularization: Option<(T, T)>,
) -> Vector<T> {
    let examples_len = T::from(examples.len()).unwrap();
    let mut list = Vector::empty();

    for (example, target) in examples {
        let delta = sigmoid(example.dot(weights)) - *target;
        list.resize_to(example.len() - 1);
        for (el, v) in list.iter_mut().zip(example.iter()) {
            *el = *el + (delta * *v) / examples_len;
        }
    }

    match regularization {
        None => {}
        Some((l1, l2)) => {
            for (el, w) in list.iter_mut().zip(weights.iter()) {
                let l1_mod = l1 * w.signum();
                let l2_mod = (T::one() + T::one()) * l2 * *w;

                *el = *el + l1_mod + l2_mod;
            }
        }
    }

    list
}

// S(x) = 1/(1+e^(-x))

/// Calculates the sigmoid function against the input.  This is critical to the
/// logistic regression machine learning model.  This is essentially the
/// function `S(x) = 1/(1+e^(-x))`.
fn sigmoid<T: Float>(input: T) -> T {
    T::one() / (T::one() + input.neg().exp())
}
