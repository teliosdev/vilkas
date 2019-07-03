use self::keys::Keys;
use super::items::{ItemListDecay, NearListDecay};
use super::{Sealed, Storage};
use config::Config;
use failure::Error;
use lmdb::{Database, Environment, EnvironmentFlags, RoTransaction, RwTransaction};
use std::path::PathBuf;

mod ext;
mod item;
mod keys;
mod model;
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
struct MemStorageConfiguration {
    path: PathBuf,
    #[serde(default = "defaults::max_readers")]
    max_readers: u32,
    #[serde(default = "defaults::max_dbs")]
    max_dbs: u32,
    #[serde(default = "defaults::map_size")]
    map_size: usize,
    #[serde(default)]
    keys: Keys,
    #[serde(default)]
    near_decay: NearListDecay,
    #[serde(default = "ItemListDecay::top_default")]
    top_decay: ItemListDecay,
    #[serde(default = "ItemListDecay::pop_default")]
    pop_decay: ItemListDecay,
    #[serde(default = "defaults::user_history_size")]
    user_history_size: usize,
}

mod defaults {
    pub const fn max_readers() -> u32 {
        126
    }
    pub const fn max_dbs() -> u32 {
        4
    }
    pub const fn map_size() -> usize {
        // 4096 is page size, so page aligned
        4096 * 1024
    }
    pub const fn user_history_size() -> usize {
        16
    }
}

impl Into<MemStorage> for MemStorageConfiguration {
    fn into(self) -> MemStorage {
        let env = Environment::new()
            .set_max_readers(self.max_readers)
            .set_max_dbs(self.max_dbs)
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
        configuration.into()
    }

    pub fn read_transaction<'e, T, F>(&'e self, db: &str, f: F) -> Result<T, Error>
    where
        F: FnOnce(RoTransaction<'e>, Database) -> Result<T, Error>,
    {
        let db = self.env.open_db(Some(db))?;
        let transaction = self.env.begin_ro_txn()?;
        f(transaction, db)
    }

    pub fn write_transaction<'e, T, F>(&'e self, db: &str, f: F) -> Result<T, Error>
    where
        F: FnOnce(RwTransaction<'e>, Database) -> Result<T, Error>,
    {
        let db = self.env.open_db(Some(db))?;
        let transaction = self.env.begin_rw_txn()?;
        f(transaction, db)
    }
}

impl Sealed for MemStorage {}

impl Storage for MemStorage {}
