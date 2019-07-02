use self::ext::*;
use self::keys::Keys;
use super::items::{ItemListDecay, NearListDecay};
use super::{Item, ItemList, ItemStorage, ModelStorage, Sealed, Storage, TimeScope};
use config::Config;
use failure::Error;
use lmdb::{Database, Environment, EnvironmentFlags, RoTransaction, RwTransaction};
use std::path::PathBuf;
use uuid::Uuid;

mod ext;
mod keys;

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

// impl Storage for MemStorage {}

impl ItemStorage for MemStorage {
    fn find_item(&self, part: &str, item: Uuid) -> Result<Option<Item>, Error> {
        self.read_transaction(self.keys.item_database(), |txn, db| {
            let key = self.keys.item_key(part, item);
            txn.deget::<Item, _>(db, &key)
        })
    }

    fn find_items<'i>(
        &self,
        part: &str,
        items: Box<dyn Iterator<Item = Uuid> + 'i>,
    ) -> Result<Vec<Option<Item>>, Error> {
        self.read_transaction(self.keys.item_database(), |txn, db| {
            let items = items
                .map(|item| {
                    let key = self.keys.item_key(part, item);
                    txn.deget::<Item, _>(db, &key)
                        .ok()
                        .and_then(core::convert::identity)
                })
                .collect::<Vec<_>>();
            Ok(items)
        })
    }

    fn find_items_near(&self, part: &str, item: Uuid) -> Result<ItemList, Error> {
        self.read_transaction(self.keys.item_database(), |txn, db| {
            let key = self.keys.item_near_key(part, item);
            let result = txn.deget::<ItemList, _>(db, &key)?.unwrap_or_default();
            Ok(result)
        })
    }

    fn find_items_top(&self, part: &str, scope: TimeScope) -> Result<ItemList, Error> {
        self.read_transaction(self.keys.item_database(), |txn, db| {
            let key = self.keys.item_top_key(part, scope);
            let result = txn.deget::<ItemList, _>(db, &key)?.unwrap_or_default();
            Ok(result)
        })
    }

    fn find_items_popular(&self, part: &str, scope: TimeScope) -> Result<ItemList, Error> {
        self.read_transaction(self.keys.item_database(), |txn, db| {
            let key = self.keys.item_pop_key(part, scope);
            let result = txn.deget::<ItemList, _>(db, &key)?.unwrap_or_default();
            Ok(result)
        })
    }

    fn items_add_near(&self, part: &str, item: Uuid, near: Uuid) -> Result<(), Error> {
        self.write_transaction(self.keys.item_database(), |mut txn, db| {
            let key = self.keys.item_near_key(part, item);
            item_list_decay(&mut txn, db, &key, near, 1.0, |list| {
                self.near_decay.decay(list)
            })
        })
    }

    fn items_view(&self, part: &str, item: Uuid, view_cost: f64) -> Result<(), Error> {
        self.write_transaction(self.keys.item_database(), |mut txn, db| {
            for scope in TimeScope::variants() {
                let key = self.keys.item_top_key(part, scope);
                item_list_decay(&mut txn, db, &key, item, 1.0, |list| {
                    self.top_decay.decay(scope, list)
                })?;
                let key = self.keys.item_pop_key(part, scope);
                item_list_decay(&mut txn, db, &key, item, view_cost, |list| {
                    self.pop_decay.decay(scope, list)
                })?;
            }

            Ok(())
        })
    }

    fn items_list_flush(&self, _part: &str) -> Result<(), Error> {
        Ok(())
    }
}

fn item_list_decay<F>(
    txn: &mut RwTransaction<'_>,
    db: Database,
    key: &str,
    id: Uuid,
    by: f64,
    decay: F,
) -> Result<(), Error>
where
    F: FnOnce(&mut ItemList),
{
    let mut result: ItemList = txn.deget::<ItemList, _>(db, &key)?.unwrap_or_default();

    if let Some((_, count)) = result.items.iter_mut().find(|(i, _)| *i == id) {
        *count += by;
    } else {
        result.items.push((id, by));
    }

    result.nmods += 1;
    decay(&mut result);
    let size = bincode::serialized_size(&result)? as usize;
    let buffer = txn.reserve(db, &key, size, Default::default())?;
    let writer = std::io::Cursor::new(buffer);
    bincode::serialize_into(writer, &result)?;
    Ok(())
}
