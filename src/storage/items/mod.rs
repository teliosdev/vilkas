use std::collections::{HashMap, HashSet};

use failure::Error;
use uuid::Uuid;

use crate::storage::Sealed;

pub use self::decay::{DecayFunction, ItemListDecay, NearListDecay};
pub use self::scope::TimeScope;

mod decay;
mod scope;

#[derive(Debug, Clone, Serialize, Deserialize)]
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
}

pub trait ItemStorage: Sealed {
    fn find_item(&self, part: &str, item: Uuid) -> Result<Option<Item>, Error>;
    fn find_items<'i>(
        &self,
        part: &str,
        items: Box<dyn Iterator<Item = Uuid> + 'i>,
    ) -> Result<Vec<Option<Item>>, Error>;
    fn find_items_near(&self, part: &str, item: Uuid) -> Result<ItemList, Error>;
    fn find_items_top(&self, part: &str, scope: TimeScope) -> Result<ItemList, Error>;
    fn find_items_popular(&self, part: &str, scope: TimeScope) -> Result<ItemList, Error>;

    fn items_add_near(&self, part: &str, item: Uuid, near: Uuid) -> Result<(), Error>;
    fn items_view(&self, part: &str, item: Uuid, view_cost: f64) -> Result<(), Error>;
    fn items_list_flush(&self, part: &str) -> Result<(), Error>;
}
