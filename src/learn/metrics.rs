use num_traits::Float;

#[derive(Debug, Copy, Clone)]
pub struct ConfusionMatrix {
    // condition positive, prediction positive
    pub true_positive: u32,
    // condition negative, prediction negative
    pub true_negative: u32,
    // condition negative, prediction positive
    pub false_positive: u32,
    // condition positive, prediction negative
    pub false_negative: u32,
}

impl ConfusionMatrix {
    pub fn with<T: Float>(threshold: T, predictions: &[T], targets: &[T]) -> ConfusionMatrix {
        assert_eq!(
            predictions.len(),
            targets.len(),
            "expected arrays to have same length"
        );

        let mut matrix = ConfusionMatrix::default();
        let bool_targets = targets.iter().map(|value| !value.is_zero());
        let bool_predictions = predictions.iter().map(|value| *value > threshold);

        for (prediction, actual) in bool_predictions.zip(bool_targets) {
            match (prediction, actual) {
                (true, true) => matrix.true_positive += 1,
                (false, false) => matrix.true_negative += 1,
                (true, false) => matrix.false_positive += 1,
                (false, true) => matrix.false_negative += 1,
            }
        }

        matrix
    }

    pub fn condition_positive(&self) -> u32 {
        self.true_positive + self.false_negative
    }

    pub fn condition_negative(&self) -> u32 {
        self.true_negative + self.false_positive
    }

    pub fn prediction_positive(&self) -> u32 {
        self.true_positive + self.false_positive
    }

    pub fn prediction_negative(&self) -> u32 {
        self.true_negative + self.false_negative
    }

    pub fn true_positive_rate(&self) -> f64 {
        self.true_positive as f64 / self.condition_positive() as f64
    }

    #[inline]
    pub fn recall(&self) -> f64 {
        self.true_positive_rate()
    }

    pub fn false_positive_rate(&self) -> f64 {
        self.false_positive as f64 / self.condition_negative() as f64
    }

    pub fn precision(&self) -> f64 {
        self.true_positive as f64 / self.prediction_positive() as f64
    }

    pub fn roc_auc(&self) -> f64 {
        self.true_positive_rate() / self.false_positive_rate()
    }
}

impl Default for ConfusionMatrix {
    fn default() -> ConfusionMatrix {
        ConfusionMatrix {
            true_positive: 0,
            true_negative: 0,
            false_positive: 0,
            false_negative: 0,
        }
    }
}
