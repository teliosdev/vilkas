use crate::storage::mem::{MemStorage, MemStorageConfiguration};
use crate::storage::sealed::Sealed;
use crate::storage::{
    Activity, FeatureList, Item, ItemList, ItemStore, ModelStore, Store, TimeScope, UserData,
    UserStore,
};
use failure::Error;
use rand::distributions::Alphanumeric;
use rand::Rng;
use std::path::PathBuf;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct TemporaryFileWrap<T>(T, PathBuf);

impl TemporaryFileWrap<MemStorage> {
    pub fn load() -> TemporaryFileWrap<MemStorage> {
        let name = ::rand::thread_rng()
            .sample_iter(Alphanumeric)
            .take(16)
            .collect::<String>();
        let file = std::env::temp_dir().join(name);
        let _ = std::fs::create_dir(&file).unwrap();
        let mut config = MemStorageConfiguration::default();
        config.path = file.clone();
        let storage: MemStorage = config.into();
        storage
            .initialize()
            .expect("could not initialize databases");

        TemporaryFileWrap(storage, file)
    }
}

impl<T> Drop for TemporaryFileWrap<T> {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.1);
    }
}

impl<T> std::ops::Deref for TemporaryFileWrap<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> std::ops::DerefMut for TemporaryFileWrap<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T: Store> Sealed for TemporaryFileWrap<T> {}
impl<T: Store> Store for TemporaryFileWrap<T> {}

impl<T: Store> ItemStore for TemporaryFileWrap<T> {
    fn find_item(&self, part: &str, item: Uuid) -> Result<Option<Item>, Error> {
        self.0.find_item(part, item)
    }

    fn find_items<Items>(&self, part: &str, items: Items) -> Result<Vec<Option<Item>>, Error>
    where
        Items: IntoIterator<Item = Uuid>,
    {
        self.0.find_items(part, items)
    }

    fn find_items_near(&self, part: &str, item: Uuid) -> Result<ItemList, Error> {
        self.0.find_items_near(part, item)
    }

    fn find_items_top(&self, part: &str, scope: TimeScope) -> Result<ItemList, Error> {
        self.0.find_items_top(part, scope)
    }

    fn find_items_popular(&self, part: &str, scope: TimeScope) -> Result<ItemList, Error> {
        self.0.find_items_popular(part, scope)
    }

    fn find_items_recent(&self, part: &str) -> Result<ItemList, Error> {
        self.0.find_items_recent(part)
    }

    fn items_insert(&self, item: &Item) -> Result<(), Error> {
        self.0.items_insert(item)
    }

    fn items_add_near(&self, part: &str, item: Uuid, near: Uuid) -> Result<(), Error> {
        self.0.items_add_near(part, item, near)
    }

    fn items_add_bulk_near<Inner, Bulk>(&self, part: &str, bulk: Bulk) -> Result<(), Error>
    where
        Inner: IntoIterator<Item = Uuid>,
        Bulk: IntoIterator<Item = (Uuid, Inner)>,
    {
        self.0.items_add_bulk_near(part, bulk)
    }

    fn items_view(&self, part: &str, item: Uuid, view_cost: f64) -> Result<(), Error> {
        self.0.items_view(part, item, view_cost)
    }

    fn items_list_flush(&self, part: &str) -> Result<(), Error> {
        self.0.items_list_flush(part)
    }
}

impl<T: Store> ModelStore for TemporaryFileWrap<T> {
    fn set_default_model(&self, list: FeatureList<'_>) -> Result<(), Error> {
        self.0.set_default_model(list)
    }

    fn find_default_model(&self) -> Result<FeatureList<'static>, Error> {
        self.0.find_default_model()
    }

    fn find_model(&self, part: &str) -> Result<Option<FeatureList<'static>>, Error> {
        self.0.find_model(part)
    }

    fn model_activity_save(&self, part: &str, activity: &Activity) -> Result<(), Error> {
        self.0.model_activity_save(part, activity)
    }

    fn model_activity_load(&self, part: &str, id: Uuid) -> Result<Option<Activity>, Error> {
        self.0.model_activity_load(part, id)
    }

    fn model_activity_choose(&self, part: &str, id: Uuid, chosen: &[Uuid]) -> Result<(), Error> {
        self.0.model_activity_choose(part, id, chosen)
    }

    fn model_activity_pluck(&self) -> Result<Vec<Activity>, Error> {
        self.0.model_activity_pluck()
    }

    fn model_activity_delete_all<'p, Ids>(&self, id: Ids) -> Result<(), Error>
    where
        Ids: IntoIterator<Item = (&'p str, Uuid)>,
    {
        self.0.model_activity_delete_all(id)
    }
}

impl<T: Store> UserStore for TemporaryFileWrap<T> {
    fn find_user(&self, part: &str, id: &str) -> Result<UserData, Error> {
        self.0.find_user(part, id)
    }

    fn user_push_history(&self, part: &str, id: &str, history: Uuid) -> Result<(), Error> {
        self.0.user_push_history(part, id, history)
    }
}
