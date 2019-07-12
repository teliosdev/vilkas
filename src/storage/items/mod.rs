use std::collections::{HashMap, HashSet};

use failure::Error;
use uuid::Uuid;

use crate::storage::Sealed;

pub use self::decay::{DecayFunction, ItemListDecay, NearListDecay};
pub use self::scope::TimeScope;

mod decay;
mod scope;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Item {
    pub id: Uuid,
    pub part: String,
    pub views: u64,
    pub meta: HashMap<String, HashSet<String>>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ItemList {
    pub items: Vec<(Uuid, f64)>,
    pub nmods: u64,
    pub epoch: u128,
}

pub trait ItemStorage: Sealed {
    fn find_item(&self, part: &str, item: Uuid) -> Result<Option<Item>, Error>;
    fn find_items<Items>(&self, part: &str, items: Items) -> Result<Vec<Option<Item>>, Error>
    where
        Items: IntoIterator<Item = Uuid>;
    fn find_items_near(&self, part: &str, item: Uuid) -> Result<ItemList, Error>;
    fn find_items_top(&self, part: &str, scope: TimeScope) -> Result<ItemList, Error>;
    fn find_items_popular(&self, part: &str, scope: TimeScope) -> Result<ItemList, Error>;

    fn items_insert(&self, item: &Item) -> Result<(), Error>;
    fn items_add_near(&self, part: &str, item: Uuid, near: Uuid) -> Result<(), Error>;
    fn items_add_bulk_near<Inner, Bulk>(&self, part: &str, bulk: Bulk) -> Result<(), Error>
    where
        Inner: IntoIterator<Item = Uuid>,
        Bulk: IntoIterator<Item = (Uuid, Inner)>,
    {
        for (item, nears) in bulk {
            for near in nears {
                self.items_add_near(part, item, near)?;
            }
        }

        Ok(())
    }
    fn items_view(&self, part: &str, item: Uuid, view_cost: f64) -> Result<(), Error>;
    fn items_list_flush(&self, part: &str) -> Result<(), Error>;
}
