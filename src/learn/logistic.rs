use super::Features;
use num_traits::Float;
use std::fmt::Debug;

#[derive(Debug, Clone)]
pub struct LogisticRegression<T: Float + 'static> {
    step_start: T,
    gradient_cap: T,
    weights: Features<'static, T>,
}

impl LogisticRegression<f64> {
    pub fn new() -> LogisticRegression<f64> {
        LogisticRegression {
            step_start: 0.5,
            gradient_cap: 0.1,
            weights: Features::new(),
        }
    }
}

impl<T: Float + Debug + 'static> LogisticRegression<T> {
    pub fn train(&mut self, examples: &[(Features<'_, T>, T)]) {
        let mut step = self.step_start;
        let mut gradient = loss_gradient(examples, &self.weights);
        let mut magnitude = gradient.magnitude();

        let mut current_loss = loss(examples, &self.weights);

        while magnitude > self.gradient_cap {
            let mut new_weights = new_weight_step(&self.weights, &gradient, step);
            let mut new_loss = loss(examples, &new_weights);
            while (current_loss - new_loss) < T::zero() {
                step = adjusted_step(step, magnitude);
                new_weights = new_weight_step(&self.weights, &gradient, step);
                new_loss = loss(examples, &new_weights);
            }

            //            step = self.step_start;
            step = step + step;
            self.weights = new_weights.to_owned();
            current_loss = loss(examples, &self.weights);
            gradient = loss_gradient(examples, &self.weights);
            magnitude = gradient.magnitude();

            dbg!(magnitude);
        }
    }

    pub fn predict<'o>(
        &'o self,
        examples: impl Iterator<Item=&'o Features<'o, T>> + 'o,
    ) -> impl Iterator<Item=T> + 'o {
        examples.map(move |example| sigmoid(example.dot(&self.weights)))
    }
}

fn adjusted_step<T: Float>(prev: T, _slope: T) -> T {
    prev / (T::one() + T::one())
}

fn new_weight_step<'c, T: Float>(
    weights: &'c Features<'c, T>,
    gradient: &'c Features<'c, T>,
    step: T,
) -> Features<'c, T> {
    weights
        .zip(gradient)
        .map(|(name, weight, gradient)| (name, weight - gradient * step))
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
pub fn loss<T: Float + Debug + 'static>(
    examples: &[(Features<'_, T>, T)],
    weights: &Features<'_, T>,
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
    (T::one().neg() / T::from(examples.len()).unwrap()) * sum
}

/// This calculates the loss gradient of a set of examples (with their targets)
/// against the current weights.  This, essentially, applies the partial
/// derivative of the loss function against each of the weights, summing them,
/// and outputting the result.  This **should not** take the place of weights -
/// this is only one step in the process of gradient descent.
pub fn loss_gradient<'e, 'l: 'e, T: Float + 'static>(
    examples: &'l [(Features<'e, T>, T)],
    weights: &Features<'_, T>,
) -> Features<'e, T> {
    let mut list = Features::<T>::new();
    let inner = list.inner_mut();
    examples
        .iter()
        .flat_map(|(example, target)| {
            let delta = sigmoid(example.dot(weights)) - *target;
            example
                .iter()
                .map(move |(name, value)| (name, delta * value))
        })
        .for_each(|(name, gradient)| {
            inner
                .entry(name.into())
                .and_modify(|content| *content = *content + gradient)
                .or_insert(gradient);
        });

    list
}

// S(x) = 1/(1+e^(-x))

/// Calculates the sigmoid function against the input.  This is critical to the
/// logistic regression machine learning model.  This is essentially the
/// function `S(x) = 1/(1+e^(-x))`.
pub fn sigmoid<T: Float>(input: T) -> T {
    T::one() / (T::one() + input.neg().exp())
}
