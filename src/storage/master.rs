use super::items::{Item, ItemList, TimeScope};
#[cfg(feature = "lmdb")]
use super::mem::MemStorage;
#[cfg(feature = "aerospike")]
use super::spike::SpikeStorage;
use super::{
    Activity, FeatureList, ItemStorage, ModelStorage, Sealed, Storage, UserData, UserStorage,
};
use config::Config;

use failure::Error;
use uuid::Uuid;
#[derive(Debug)]
pub enum MasterStorage {
    #[cfg(feature = "aerospike")]
    Spike(SpikeStorage),
    #[cfg(feature = "lmdb")]
    Memory(MemStorage),
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
            // #[cfg(feature = "lmdb")]
            // MasterStorage::Memory($v) => $act,
            _ => unreachable!(),
        }
    };
}

impl Sealed for MasterStorage {}
impl Storage for MasterStorage {}

#[allow(unused_variables)]
impl UserStorage for MasterStorage {
    fn find_user(&self, part: &str, id: &str) -> Result<UserData, Error> {
        expand_storage!(self, storage, storage.find_user(part, id))
    }

    fn user_push_history(&self, part: &str, id: &str, history: Uuid) -> Result<(), Error> {
        expand_storage!(self, storage, storage.user_push_history(part, id, history))
    }
}

#[allow(unused_variables)]
impl ModelStorage for MasterStorage {
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
}

#[allow(unused_variables)]
impl ItemStorage for MasterStorage {
    fn find_item(&self, part: &str, item: Uuid) -> Result<Option<Item>, Error> {
        expand_storage!(self, storage, storage.find_item(part, item))
    }

    fn find_items<'i>(
        &self,
        part: &str,
        items: Box<dyn Iterator<Item = Uuid> + 'i>,
    ) -> Result<Vec<Option<Item>>, Error> {
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

    fn items_add_near(&self, part: &str, item: Uuid, near: Uuid) -> Result<(), Error> {
        expand_storage!(self, storage, storage.items_add_near(part, item, near))
    }

    fn items_view(&self, part: &str, item: Uuid, view_cost: f64) -> Result<(), Error> {
        expand_storage!(self, storage, storage.items_view(part, item, view_cost))
    }

    fn items_list_flush(&self, part: &str) -> Result<(), Error> {
        expand_storage!(self, storage, storage.items_list_flush(part))
    }
}
