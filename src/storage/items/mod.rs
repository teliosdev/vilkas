use crate::storage::Sealed;
use failure::Error;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

mod decay;
mod scope;

pub use self::decay::{DecayFunction, ItemListDecay, NearListDecay};
pub use self::scope::TimeScope;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Item {
    id: Uuid,
    part: String,
    views: u64,
    popularity: f64,
    meta: HashMap<String, HashSet<String>>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ItemList {
    pub items: Vec<(Uuid, f64)>,
    pub nmods: u64,
}

pub trait ItemStorage: Sealed {
    fn find_item(&self, part: &str, item: Uuid) -> Result<Option<Item>, Error>;
    fn find_items<'i>(&self, part: &str, items: Box<dyn Iterator<Item = Uuid> + 'i>) -> Result<Vec<Option<Item>>, Error>;
    fn find_items_near(&self, part: &str, item: Uuid) -> Result<ItemList, Error>;
    fn find_items_top(&self, part: &str, scope: TimeScope) -> Result<ItemList, Error>;
    fn find_items_popular(&self, part: &str, scope: TimeScope) -> Result<ItemList, Error>;

    fn items_add_near(&self, part: &str, item: Uuid, near: Uuid) -> Result<(), Error>;
    fn items_view(&self, part: &str, item: Uuid, view_cost: f64) -> Result<(), Error>;
    fn items_list_flush(&self, part: &str) -> Result<(), Error>;
}
