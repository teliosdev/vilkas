use self::keys::Keys;
use super::items::{ItemListDecay, NearListDecay};
use super::{Sealed, Storage};
use config::Config;
use failure::Error;
use lmdb::{
    Database, DatabaseFlags, Environment, EnvironmentFlags, RoTransaction, RwTransaction,
    Transaction,
};
use std::path::PathBuf;

mod ext;
mod item;
mod keys;
mod model;
#[cfg(test)]
mod tests;
mod user;

#[derive(Debug)]
pub struct MemStorage {
    env: Environment,
    keys: Keys,
    user_history_size: usize,
    near_decay: NearListDecay,
    top_decay: ItemListDecay,
    pop_decay: ItemListDecay,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct MemStorageConfiguration {
    pub path: PathBuf,
    #[serde(default = "defaults::max_readers")]
    pub max_readers: u32,
    #[serde(default = "defaults::map_size")]
    pub map_size: usize,
    #[serde(default)]
    keys: Keys,
    #[serde(default)]
    pub near_decay: NearListDecay,
    #[serde(default = "ItemListDecay::top_default")]
    pub top_decay: ItemListDecay,
    #[serde(default = "ItemListDecay::pop_default")]
    pub pop_decay: ItemListDecay,
    #[serde(default = "defaults::user_history_size")]
    pub user_history_size: usize,
}

mod defaults {
    pub const fn max_readers() -> u32 {
        126
    }
    pub const fn map_size() -> usize {
        // 4096 is page size, so page aligned
        4096 * 1024
    }
    pub const fn user_history_size() -> usize {
        16
    }
}

impl Default for MemStorageConfiguration {
    fn default() -> MemStorageConfiguration {
        MemStorageConfiguration {
            path: Default::default(),
            max_readers: defaults::max_readers(),
            map_size: defaults::map_size(),
            keys: Default::default(),
            near_decay: Default::default(),
            top_decay: ItemListDecay::top_default(),
            pop_decay: ItemListDecay::pop_default(),
            user_history_size: defaults::user_history_size(),
        }
    }
}

impl Into<MemStorage> for MemStorageConfiguration {
    fn into(self) -> MemStorage {
        let env = Environment::new()
            .set_max_readers(self.max_readers)
            .set_max_dbs(8)
            .set_map_size(self.map_size)
            .set_flags(EnvironmentFlags::WRITE_MAP | EnvironmentFlags::NO_TLS)
            .open(&self.path)
            .expect("could not open memory-mapped file");

        MemStorage {
            env,
            keys: self.keys,
            user_history_size: self.user_history_size,
            near_decay: self.near_decay,
            top_decay: self.top_decay,
            pop_decay: self.pop_decay,
        }
    }
}

impl MemStorage {
    pub fn load(config: &Config) -> MemStorage {
        let configuration = config
            .get::<MemStorageConfiguration>("storage.memory")
            .expect("could not load memory configuration");
        let storage: MemStorage = configuration.into();
        storage.initialize().expect("could not initialize database");
        storage
    }

    pub(crate) fn initialize(&self) -> Result<(), Error> {
        self.env
            .create_db(Some(self.keys.activity_database()), DatabaseFlags::empty())?;
        self.env
            .create_db(Some(self.keys.item_database()), DatabaseFlags::empty())?;
        self.env
            .create_db(Some(self.keys.model_database()), DatabaseFlags::empty())?;
        self.env
            .create_db(Some(self.keys.user_database()), DatabaseFlags::empty())?;
        Ok(())
    }

    pub fn read_transaction<'e, T, F>(&'e self, db: &str, f: F) -> Result<T, Error>
    where
        F: FnOnce(&RoTransaction<'e>, Database) -> Result<T, Error>,
    {
        let db = self.env.open_db(Some(db))?;
        let transaction = self.env.begin_ro_txn()?;
        match f(&transaction, db) {
            Ok(v) => {
                transaction.commit()?;
                Ok(v)
            }
            Err(e) => {
                transaction.abort();
                Err(e)
            }
        }
    }

    pub fn write_transaction<'e, T, F>(&'e self, db: &str, f: F) -> Result<T, Error>
    where
        F: FnOnce(&mut RwTransaction<'e>, Database) -> Result<T, Error>,
    {
        let db = self.env.open_db(Some(db))?;
        let mut transaction = self.env.begin_rw_txn()?;
        match f(&mut transaction, db) {
            Ok(v) => {
                transaction.commit()?;
                Ok(v)
            }
            Err(e) => {
                transaction.abort();
                Err(e)
            }
        }
    }
}

impl Sealed for MemStorage {}

impl Storage for MemStorage {}
