use crate::storage::Sealed;
use failure::Error;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Item {
    id: Uuid,
    views: u64,
    popularity: f64,
    meta: HashMap<String, HashSet<String>>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ItemList {
    pub items: Vec<(Uuid, f64)>,
    pub nmods: u32,
}

impl Item {
    pub(super) fn new(
        id: Uuid,
        views: u64,
        popularity: f64,
        meta: HashMap<String, HashSet<String>>,
    ) -> Item {
        Item {
            id,
            views,
            popularity,
            meta,
        }
    }
}

pub trait ItemStorage: Sealed {
    fn find_item(&self, item: Uuid) -> Result<Option<Item>, Error>;
    fn find_items_near(&self, item: Uuid) -> Result<Vec<Uuid>, Error>;
    fn find_items_top(&self) -> Result<Vec<Uuid>, Error>;
    fn find_items_popular(&self) -> Result<Vec<Uuid>, Error>;

    fn items_add_near(&self, item: Uuid, near: Uuid) -> Result<(), Error>;
}
