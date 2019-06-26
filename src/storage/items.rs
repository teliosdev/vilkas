use crate::storage::Sealed;
use failure::Error;
use futures::{Future, Stream};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Item {
    id: Uuid,
    views: u64,
    popularity: f64,
    meta: HashMap<String, HashSet<String>>,
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
}
