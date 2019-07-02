pub use self::config::PartConfig;
pub use self::request::Request;
use crate::storage::Storage;
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
        let examples = request.examples(self)?;

        unimplemented!()
    }
}
