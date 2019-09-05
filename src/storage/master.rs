use super::core::items::{Item, ItemList, TimeScope};
#[cfg(feature = "lmdb")]
use super::mem::MemStorage;
#[cfg(feature = "redis")]
use super::redis::RedisStorage;
#[cfg(feature = "aerospike")]
use super::spike::SpikeStorage;
use super::{Activity, FeatureList, ItemStore, ModelStore, Sealed, Store, UserData, UserStore};
use config::Config;

use failure::Error;
use uuid::Uuid;
#[derive(Debug)]
pub enum MasterStorage {
    #[cfg(feature = "aerospike")]
    Spike(SpikeStorage),
    #[cfg(feature = "lmdb")]
    Memory(MemStorage),
    #[cfg(feature = "redis")]
    Redis(RedisStorage),
}

impl MasterStorage {
    pub fn load(config: &Config) -> MasterStorage {
        let kind = config
            .get_str("storage.type")
            .expect("unexpected storage type");
        match &kind[..] {
            #[cfg(feature = "aerospike")]
            "aerospike" => MasterStorage::Spike(SpikeStorage::load(config)),
            #[cfg(feature = "lmdb")]
            "memory" => MasterStorage::Memory(MemStorage::load(config)),
            #[cfg(feature = "redis")]
            "redis" => MasterStorage::Redis(RedisStorage::load(config)),

            store => panic!("unknown storage type {}", store),
        }
    }
}

macro_rules! expand_storage {
    ($base:expr, $v:ident, $act:expr) => {
        #[allow(unreachable_patterns)]
        match $base {
            #[cfg(feature = "aerospike")]
            MasterStorage::Spike($v) => $act,
            #[cfg(feature = "lmdb")]
            MasterStorage::Memory($v) => $act,
            #[cfg(feature = "redis")]
            MasterStorage::Redis($v) => $act,
            _ => unreachable!(),
        }
    };
}

impl Sealed for MasterStorage {}
impl Store for MasterStorage {}

#[allow(unused_variables)]
impl UserStore for MasterStorage {
    fn find_user(&self, part: &str, id: &str) -> Result<UserData, Error> {
        expand_storage!(self, storage, storage.find_user(part, id))
    }

    fn user_push_history(&self, part: &str, id: &str, history: Uuid) -> Result<(), Error> {
        expand_storage!(self, storage, storage.user_push_history(part, id, history))
    }
}

#[allow(unused_variables)]
impl ModelStore for MasterStorage {
    fn set_default_model(&self, list: FeatureList) -> Result<(), Error> {
        expand_storage!(self, storage, storage.set_default_model(list))
    }

    fn find_default_model(&self) -> Result<FeatureList<'static>, Error> {
        expand_storage!(self, storage, storage.find_default_model())
    }

    fn find_model(&self, part: &str) -> Result<Option<FeatureList<'static>>, Error> {
        expand_storage!(self, storage, storage.find_model(part))
    }

    fn model_activity_save(&self, part: &str, activity: &Activity) -> Result<(), Error> {
        expand_storage!(self, storage, storage.model_activity_save(part, activity))
    }

    fn model_activity_load(&self, part: &str, id: Uuid) -> Result<Option<Activity>, Error> {
        expand_storage!(self, storage, storage.model_activity_load(part, id))
    }

    fn model_activity_choose(&self, part: &str, id: Uuid, chosen: &[Uuid]) -> Result<(), Error> {
        expand_storage!(
            self,
            storage,
            storage.model_activity_choose(part, id, chosen)
        )
    }

    fn model_activity_pluck(&self) -> Result<Vec<Activity>, Error> {
        expand_storage!(self, storage, storage.model_activity_pluck())
    }

    fn model_activity_delete_all<'p, Ids>(&self, id: Ids) -> Result<(), Error>
    where
        Ids: IntoIterator<Item = (&'p str, Uuid)>,
    {
        expand_storage!(self, storage, storage.model_activity_delete_all(id))
    }
}

#[allow(unused_variables)]
impl ItemStore for MasterStorage {
    fn find_item(&self, part: &str, item: Uuid) -> Result<Option<Item>, Error> {
        expand_storage!(self, storage, storage.find_item(part, item))
    }

    fn find_items<Items>(&self, part: &str, items: Items) -> Result<Vec<Option<Item>>, Error>
    where
        Items: IntoIterator<Item = Uuid>,
    {
        expand_storage!(self, storage, storage.find_items(part, items))
    }

    fn find_items_near(&self, part: &str, item: Uuid) -> Result<ItemList, Error> {
        expand_storage!(self, storage, storage.find_items_near(part, item))
    }

    fn find_items_top(&self, part: &str, scope: TimeScope) -> Result<ItemList, Error> {
        expand_storage!(self, storage, storage.find_items_top(part, scope))
    }

    fn find_items_popular(&self, part: &str, scope: TimeScope) -> Result<ItemList, Error> {
        expand_storage!(self, storage, storage.find_items_popular(part, scope))
    }

    fn find_items_recent(&self, part: &str) -> Result<ItemList, Error> {
        expand_storage!(self, storage, storage.find_items_recent(part))
    }

    fn items_insert(&self, item: &Item) -> Result<(), Error> {
        expand_storage!(self, storage, storage.items_insert(item))
    }

    fn items_delete(&self, part: &str, item: Uuid) -> Result<(), Error> {
        expand_storage!(self, storage, storage.items_delete(part, item))
    }

    fn items_add_near(&self, part: &str, item: Uuid, near: Uuid) -> Result<(), Error> {
        expand_storage!(self, storage, storage.items_add_near(part, item, near))
    }

    fn items_add_bulk_near<Inner, Bulk>(&self, part: &str, bulk: Bulk) -> Result<(), Error>
    where
        Inner: IntoIterator<Item = Uuid>,
        Bulk: IntoIterator<Item = (Uuid, Inner)>,
    {
        expand_storage!(self, storage, storage.items_add_bulk_near(part, bulk))
    }

    fn items_view(&self, part: &str, item: Uuid, view_cost: f64) -> Result<(), Error> {
        expand_storage!(self, storage, storage.items_view(part, item, view_cost))
    }

    fn items_list_flush(&self, part: &str) -> Result<(), Error> {
        expand_storage!(self, storage, storage.items_list_flush(part))
    }
}
