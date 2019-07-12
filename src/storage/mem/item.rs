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

    fn find_items<Items>(&self, part: &str, items: Items) -> Result<Vec<Option<Item>>, Error>
    where
        Items: IntoIterator<Item = Uuid>,
    {
        self.read_transaction(self.keys.item_database(), |txn, db| {
            let items = items
                .into_iter()
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

    fn items_insert(&self, item: &Item) -> Result<(), Error> {
        self.write_transaction(self.keys.item_database(), |txn, db| {
            let key = self.keys.item_key(&item.part, item.id);
            txn.serput(db, &key, item)
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

    fn items_add_bulk_near<Inner, Bulk>(&self, part: &str, bulk: Bulk) -> Result<(), Error>
    where
        Inner: IntoIterator<Item = Uuid>,
        Bulk: IntoIterator<Item = (Uuid, Inner)>,
    {
        self.write_transaction(self.keys.item_database(), |txn, db| {
            for (item, nears) in bulk.into_iter() {
                let key = self.keys.item_near_key(part, item);
                item_list_decay_bulk(txn, db, &key, nears.into_iter(), 1.0, |list| {
                    self.near_decay.decay(list)
                })?;
            }
            Ok(())
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

fn item_list_decay_bulk<F, Ids>(
    txn: &mut RwTransaction<'_>,
    db: Database,
    key: &str,
    ids: Ids,
    by: f64,
    decay: F,
) -> Result<(), Error>
where
    F: FnOnce(&mut ItemList),
    Ids: Iterator<Item = Uuid>,
{
    let mut result: ItemList = txn.deget::<ItemList, _>(db, &key)?.unwrap_or_default();
    let mut ids_count = 0;

    for id in ids {
        ids_count += 1;
        if let Some((_, count)) = result.items.iter_mut().find(|(i, _)| *i == id) {
            *count += by;
        } else {
            result.items.push((id, by));
        }
    }

    result.nmods += ids_count;
    decay(&mut result);
    crate::ord::sort_float(&mut result.items, |(_, a)| *a);
    txn.serput(db, &key, &result)?;
    Ok(())
}

#[inline]
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
    item_list_decay_bulk(txn, db, key, std::iter::once(id), by, decay)
}
