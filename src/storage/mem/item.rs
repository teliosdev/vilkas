use super::ext::*;
use super::MemStorage;
use crate::storage::{Item, ItemList, ItemStorage, TimeScope};
use failure::Error;
use lmdb::{Database, RwTransaction};
use uuid::Uuid;

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
    txn.serput(db, &key, &result)?;
    Ok(())
}
