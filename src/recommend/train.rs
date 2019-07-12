use crate::learn::logistic::LogisticRegression;
use crate::learn::metrics::roc_auc_score;
use crate::learn::{Algorithm, Vector};
use crate::recommend::{Core, PartConfig};
use crate::storage::{Activity, FeatureList, Storage};
use failure::Error;

impl<T: Storage + 'static> Core<T> {
    pub fn load_train(&self) -> Result<(), Error> {
        let activities = self.storage.model_activity_pluck()?;
        self.train(&activities)?;
        self.storage
            .model_activity_delete_all(activities.iter().map(|act| (&act.part[..], act.id)))?;
        Ok(())
    }

    pub fn train(&self, activities: &[Activity]) -> Result<(), Error> {
        let mut list = self.storage.find_default_model()?;

        let features = complete_generate_features(&self, activities, &mut list);

        if features.len() < 64 {
            return Ok(());
        }

        let keys = compute_keys(&list);
        let (model, features) = convert_model_examples(&keys, &list, &features);

        let mut lr = self.parameters.build_with_weights(model);
        let (training, holdout) = normal_split(&features);
        let old_performance = check_performance(&lr, holdout);
        lr.train(&training);
        let new_performance = check_performance(&lr, holdout);

        if new_performance > old_performance {
            let model = lr.weights();
            let result = keys
                .iter()
                .zip(model.iter())
                .map(|(k, v)| (*k, *v))
                .collect::<FeatureList<'_>>();
            self.storage.set_default_model(result)?;
        }

        Ok(())
    }
}

fn normal_split<T>(complete: &[T]) -> (&[T], &[T]) {
    let split = complete.len() * 2 / 3;
    (&complete[0..split], &complete[split..])
}

fn check_performance(lr: &LogisticRegression<f64>, examples: &[(Vector<f64>, f64)]) -> f64 {
    let hat = lr
        .predict(examples.iter().map(|(a, _)| a))
        .collect::<Vec<_>>();
    let tru = examples.iter().map(|(_, b)| *b).collect::<Vec<_>>();
    roc_auc_score(&tru, &hat)
}

fn compute_keys<'l>(list: &'l FeatureList<'static>) -> Vec<&'l str> {
    let mut keys = list.keys().map(|k| k.as_ref()).collect::<Vec<_>>();
    keys.sort_unstable();
    keys
}

fn convert_model_examples(
    keys: &[&str],
    list: &FeatureList<'static>,
    examples: &[(FeatureList<'static>, f64)],
) -> (Vector<f64>, Vec<(Vector<f64>, f64)>) {
    let model = list.to_vector(&keys);
    let features = examples
        .into_iter()
        .map(|(f, v)| (f.to_vector(&keys), *v))
        .collect::<Vec<_>>();
    (model, features)
}

fn complete_generate_features<T: Storage + 'static>(
    core: &Core<T>,
    activities: &[Activity],
    list: &mut FeatureList<'static>,
) -> Vec<(FeatureList<'static>, f64)> {
    let features = activities
        .iter()
        .flat_map(|activity| {
            let part = core.config_for(&activity.part);
            generate_features(activity, part).into_iter()
        })
        .collect::<Vec<_>>();

    for (f, _) in features.iter() {
        for key in f.keys() {
            list.ensure_has(key);
        }
    }

    features
}

fn generate_features<'v>(
    activity: &'v Activity,
    part: &'v PartConfig,
) -> impl Iterator<Item = (FeatureList<'static>, f64)> + 'v {
    activity.visible.iter().map(move |example| {
        let positive = activity
            .chosen
            .as_ref()
            .map(|c| c.contains(&example.item.id))
            .unwrap_or(false);
        let features = example.features(&activity.current, part);
        let value = if positive { 1.0 } else { 0.0 };
        (features, value)
    })
    //    let features = activity
    //        .visible
    //        .iter()
    //        .map(|ex| (ex, ex.features(&activity.current, part)))
    //        .collect::<Vec<_>>();
    //    activity
    //        .chosen
    //        .iter()
    //        .flatten()
    //        .flat_map(|chosen| features.iter().find(|(ex, _)| ex.item.id == *chosen))
    //        .flat_map(|(ex, feats): &(&Example, FeatureList<'static>)| {
    //            features
    //                .iter()
    //                .filter(move |(e, _)| e.item.id != ex.item.id)
    //                .map(move |(_, f)| f - feats)
    //        })
    //        .collect()
}
