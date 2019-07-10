pub use self::conf::PartConfig;
pub use self::request::Request;
use crate::storage::{Activity, BasicExample, Example, FeatureList, Storage};
use config::Config;
use failure::Error;
use rand::Rng;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use uuid::Uuid;

mod conf;
mod request;

#[derive(Debug, Clone)]
pub struct Core<T: Storage + 'static> {
    storage: Arc<T>,

    part_config: HashMap<String, PartConfig>,
    default_config: PartConfig,
}

impl<T: Storage + 'static> Core<T> {
    pub fn of(storage: &Arc<T>, config: &Config) -> Core<T> {
        let default_config = config
            .get("recommend.core.default")
            .expect("could not load default config");
        let part_config = config
            .get("recommend.core.parts")
            .expect("could not load part configs");

        Core {
            storage: storage.clone(),
            part_config,
            default_config,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub result: Vec<(Uuid, f64)>,
    pub id: Uuid,
}

impl<T: Storage + 'static> Core<T> {
    pub fn config_for<Q>(&self, name: &Q) -> &PartConfig
    where
        String: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.part_config
            .get(name)
            .unwrap_or_else(|| &self.default_config)
    }

    pub fn recommend(&self, request: &Request) -> Result<Response, Error> {
        let current = request.current(self)?;
        let current = Example::new(BasicExample::new(current.id), current);
        let config = self.config_for(&request.part);
        let model = pluck_model(self.storage.as_ref(), &request.part)?;
        let examples = request.examples(self)?;
        let mut scored = score_examples(examples, &current, &model, config).collect::<Vec<_>>();
        crate::ord::sort_float(&mut scored, |(_, a)| *a);
        resort_examples(&mut scored, request.count, config);
        scored.truncate(request.count);

        let id = build_activity(self.storage.as_ref(), request, current, &scored[..])?;

        Ok(Response {
            result: scored.into_iter().map(|(v, s)| (v.item.id, s)).collect(),
            id,
        })
    }
}

fn pluck_model<T: Storage>(storage: &T, part: &str) -> Result<FeatureList<'static>, Error> {
    let model = storage.find_model(part)?;
    match model {
        Some(model) => Ok(model),
        None => storage.find_default_model(),
    }
}

fn score_examples<'v, I>(
    examples: I,
    current: &'v Example,
    model: &'v FeatureList<'static>,
    config: &'v PartConfig,
) -> impl Iterator<Item = (Example, f64)> + 'v
where
    I: Iterator<Item = Example> + 'v,
{
    use crate::learn::logistic::predict_iter;
    examples.map(move |example| {
        let features = example.features(&current, config);
        let iter = features.combine(&model).map(|(_, a, b)| (a, b));
        let score = predict_iter::<f64, _>(iter);
        (example, score)
    })
}

fn build_activity<T: Storage>(
    storage: &T,
    request: &Request,
    current: Example,
    visible: &[(Example, f64)],
) -> Result<Uuid, Error> {
    let activity_id = Uuid::new_v4();

    let visible = visible.iter().map(|(e, _)| e.clone()).collect::<Vec<_>>();

    let activity = Activity {
        id: activity_id,
        part: request.part.clone(),
        current,
        visible,
        chosen: None,
    };

    storage.model_activity_save(&request.part, &activity)?;
    Ok(activity_id)
}

fn resort_examples(examples: &mut Vec<(Example, f64)>, max: usize, config: &PartConfig) {
    if max >= examples.len() {
        return;
    }

    let mut rng = rand::thread_rng();

    if !rng.gen_bool(config.upgrade_chance) {
        return;
    }

    let from = rng.gen_range(max, examples.len());
    let to = rng.gen_range(0, max);

    examples.swap(to, from);
}
