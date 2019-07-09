pub use self::config::PartConfig;
pub use self::request::Request;
use crate::storage::{BasicExample, Example, Storage};
use failure::Error;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

mod config;
mod request;

pub struct Core<T: Storage + 'static> {
    storage: Arc<T>,

    part_config: HashMap<String, PartConfig>,
    default_config: PartConfig,
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

    pub fn recommend(&self, request: &Request) -> Result<(), Error> {
        use crate::learn::logistic::predict_iter;

        let current = request.current(self)?;
        let current = Example::new(BasicExample::new(current.id), current);
        let config = self.config_for(&request.part);
        let examples = request.examples(self)?;
        let model = self.storage.find_model(&request.part)?;
        let model = if let Some(m) = model {
            m
        } else {
            self.storage.find_default_model()?
        };

        let scored = examples.map(|example| {
            let features = example.features(&current, config);
            let iter = features.combine(&model).map(|(_, a, b)| (a, b));
            let score = predict_iter::<f64, _>(iter);
            (example.item.id, score)
        });

        let mut scored = scored.collect::<Vec<_>>();
        crate::ord::sort_float(&mut scored, |(_, a)| *a);

        unimplemented!()
    }
}
