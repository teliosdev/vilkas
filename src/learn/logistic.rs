use super::Vector;
use num_traits::Float;
use std::fmt::Debug;
use std::mem::swap;

#[derive(Debug, Clone)]
pub struct LogisticRegression<T: Float + Default + 'static> {
    initial_learning_rate: T,
    learning_rate: T,
    gradient_cap: T,
    loss: T,
    regularization: Option<(T, T)>,
    previous: Option<(Vector<T>, Vector<T>)>,
    weights: Vector<T>,
    gradients: Vector<T>,
}

impl LogisticRegression<f64> {
    pub fn new() -> LogisticRegression<f64> {
        LogisticRegression {
            initial_learning_rate: 0.5,
            learning_rate: 0.5,
            gradient_cap: 0.1,
            loss: f64::infinity(),
            regularization: None,
            previous: None,
            weights: Vector::empty(),
            gradients: Vector::empty(),
        }
    }
}

impl<T: Float + Default + Debug + 'static> LogisticRegression<T> {
    pub fn fit(&mut self, examples: &[(Vector<T>, T)]) {
        let weights = new_weight_step(&self.weights, &self.gradients, self.learning_rate);
        let new_loss = loss(examples, &weights, self.regularization);

        if new_loss > self.loss {
            self.learning_rate =  self.learning_rate / (T::one() + T::one());
            self.fit(examples);
        } else {
            let mut new_weights = weights;
            let mut new_gradients = loss_gradient(examples, &new_weights, self.regularization);
            swap(&mut self.weights, &mut new_weights);
            swap(&mut self.gradients, &mut new_gradients);
            self.previous = Some((new_weights, new_gradients));
            self.loss = loss(examples, &self.weights, self.regularization);
        }
    }

    pub fn train(&mut self, examples: &[(Vector<T>, T)]) {
        self.gradients = loss_gradient(examples, &self.weights, self.regularization);
        self.loss = loss(examples, &self.weights, self.regularization);

        while self.gradients.magnitude() > self.gradient_cap {
            self.adjust_learning_rate();
            self.fit(examples);
            dbg!(self.gradients.magnitude());
        }
    }

    pub fn predict<'o>(
        &'o self,
        examples: impl Iterator<Item = &'o Vector<T>> + 'o,
    ) -> impl Iterator<Item = T> + 'o {
        examples.map(move |example| sigmoid(example.dot(&self.weights)))
    }

    fn adjust_learning_rate(&mut self) {
        match self.previous.as_ref() {
            None => {
                self.learning_rate = self.initial_learning_rate;
            }
            Some((pw, pg)) => {
                let gs = self.gradients.combine(pg).map(|(c, p)| c - p).collect::<Vector<_>>();
                let gw = self.weights.combine(pw).map(|(c, p)| c - p).collect::<Vector<_>>();

                let gs_dot = gs.dot(&gs);
                if gs_dot.is_zero() {
                    self.learning_rate = self.initial_learning_rate;
                } else {
                    self.learning_rate = gw.dot(&gs).abs() / gs_dot
                }
            }
        }
    }
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
pub fn loss<T: Float + Default + Debug + 'static>(
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
pub fn loss_gradient<'e, 'l: 'e, T: Float + Default + 'static>(
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
        None => {},
        Some((l1, l2)) => {
            let two = T::one() + T::one();
            for (el, w) in list.iter_mut().zip(weights.iter()) {
                let l1_mod = two * l1 * w.signum();
                let l2_mod = two * l2 * *w;

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
pub fn sigmoid<T: Float>(input: T) -> T {
    T::one() / (T::one() + input.neg().exp())
}
