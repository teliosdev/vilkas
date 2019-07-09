use crate::storage::spike::ext::{RecordExt, ValueExt};
use crate::storage::spike::{read_modify_write, SpikeStorage};
use crate::storage::{Item, ItemList, ItemStorage, TimeScope};
use aerospike::{BatchPolicy, BatchRead, Bin, Bins, Client, Key, Record, Value, WritePolicy};
use byteorder::{ByteOrder, LittleEndian};
use failure::{Error, SyncFailure};
use std::collections::HashMap;
use uuid::Uuid;

impl ItemStorage for SpikeStorage {
    fn find_item(&self, part: &str, item: Uuid) -> Result<Option<Item>, Error> {
        let key = self.keys.item_key(part, item);
        self.get(&key, ["data"])?.deserialize_bin::<Item>("data")
    }

    fn find_items<Items>(&self, part: &str, items: Items) -> Result<Vec<Option<Item>>, Error>
    where
        Items: Iterator<Item = Uuid>,
    {
        let bins = Bins::Some(vec!["data".into()]);
        let keys = items
            .map(|key| self.keys.item_key(part, key))
            .map(|key| BatchRead::new(key, &bins))
            .collect();
        let results = self
            .client
            .batch_get(&BatchPolicy::default(), keys)
            .map_err(SyncFailure::new)?
            .into_iter()
            .map(|read| {
                read.record
                    .deserialize_bin::<Item>("data")
                    .ok()
                    .and_then(|v| v)
            })
            .collect();

        Ok(results)
    }

    fn find_items_near(&self, part: &str, item: Uuid) -> Result<ItemList, Error> {
        let key = self.keys.item_near_key(part, item);
        Ok(self
            .get(&key, ["list", "nmods", "epoch"])?
            .as_ref()
            .map(build_item_list)
            .unwrap_or_default())
    }

    fn find_items_top(&self, part: &str, scope: TimeScope) -> Result<ItemList, Error> {
        let key = self.keys.item_top_key(part, scope);
        Ok(self
            .get(&key, ["list", "nmods", "epoch"])?
            .as_ref()
            .map(build_item_list)
            .unwrap_or_default())
    }

    fn find_items_popular(&self, part: &str, scope: TimeScope) -> Result<ItemList, Error> {
        let key = self.keys.item_pop_key(part, scope);
        Ok(self
            .get(&key, ["list", "nmods", "epoch"])?
            .as_ref()
            .map(build_item_list)
            .unwrap_or_default())
    }

    fn items_insert(&self, item: &Item) -> Result<(), Error> {
        let key = self.keys.item_key(&item.part, item.id);
        let data = bincode::serialize(&item)?;
        let bins = [Bin::new("data", data.into())];
        self.client
            .put(&Default::default(), &key, &bins)
            .map_err(SyncFailure::new)?;
        Ok(())
    }

    fn items_add_near(&self, part: &str, item: Uuid, near: Uuid) -> Result<(), Error> {
        let key = self.keys.item_near_key(part, item);
        let nmods = increment_item_list_map(&self.client, &key, near, 1.0)?;
        // If the number of modifications is not sufficient, nothing
        // further needs to happen.
        if nmods < self.near_decay.max_modifications {
            return Ok(());
        }
        item_list_decay(&self, &key, |_, list| self.near_decay.decay(list))
    }

    fn items_add_bulk_near<Inner, Bulk>(&self, part: &str, bulk: Bulk) -> Result<(), Error>
    where
        Inner: Iterator<Item = Uuid>,
        Bulk: Iterator<Item = (Uuid, Inner)>,
    {
        for (item, nears) in bulk {
            let key = self.keys.item_near_key(part, item);
            let nmods = increment_item_list_map_bulk(&self.client, &key, nears, 1.0)?;
            if nmods >= self.near_decay.max_modifications {
                item_list_decay(&self, &key, |_, list| self.near_decay.decay(list))?;
            }
        }

        Ok(())
    }

    fn items_view(&self, part: &str, item: Uuid, view_cost: f64) -> Result<(), Error> {
        let top_keys = TimeScope::variants().map(|s| self.keys.item_top_key(part, s));
        let pop_keys = TimeScope::variants().map(|s| self.keys.item_pop_key(part, s));

        for key in top_keys {
            increment_item_list_map(&self.client, &key, item, 1.0)?;
        }

        for key in pop_keys {
            increment_item_list_map(&self.client, &key, item, view_cost)?;
        }

        Ok(())
    }

    fn items_list_flush(&self, part: &str) -> Result<(), Error> {
        let top_keys = TimeScope::variants().map(|s| (self.keys.item_top_key(part, s), s));
        let pop_keys = TimeScope::variants().map(|s| (self.keys.item_pop_key(part, s), s));
        let current = millis_epoch();
        for (key, scope) in top_keys {
            let record = self.get(&key, ["nmods"])?;
            let nmods = record
                .as_ref()
                .and_then(|record| record.bins.get("nmods"))
                .and_then(|v| v.as_u64())
                .unwrap_or_default();
            if nmods > self.top_decay.max_modifications {
                item_list_decay(&self, &key, |epoch, list| self.top_decay.decay(scope, list))?;
            }
        }

        for (key, scope) in pop_keys {
            let record = self.get(&key, ["nmods"])?;
            let nmods = record
                .as_ref()
                .and_then(|record| record.bins.get("nmods"))
                .and_then(|v| v.as_u64())
                .unwrap_or_default();
            if nmods > self.top_decay.max_modifications {
                item_list_decay(&self, &key, |list| self.pop_decay.decay(scope, list))?;
            }
        }

        Ok(())
    }
}

fn build_item_list(record: &Record) -> ItemList {
    let items = record
        .bins
        .get("list")
        .and_then(|list| list.as_hash())
        .map(|list| {
            list.iter()
                .flat_map(|(key, value)| {
                    let key = key.as_str().and_then(|k| k.parse::<Uuid>().ok());
                    let value = value.as_f64();
                    key.and_then(|k| value.map(|v| (k, v)))
                })
                .collect()
        })
        .unwrap_or_default();
    let nmods = record
        .bins
        .get("nmods")
        .and_then(|v| v.as_u64())
        .unwrap_or_default();
    let epoch = record
        .bins
        .get("epoch")
        .and_then(|v| v.as_blob())
        .and_then(|data| LittleEndian::read_u128(data))
        .unwrap_or(0u128);

    ItemList {
        items,
        nmods,
        epoch,
    }
}

fn increment_item_list_map_bulk<Ids>(
    client: &Client,
    key: &Key,
    ids: Ids,
    by: f64,
) -> Result<u64, Error>
where
    Ids: Iterator<Item = Uuid>,
{
    // We'll have to do some weird stuff to get this to work.
    // First, do our imports...
    use aerospike::operations::{self as ops, MapPolicy};
    // Now, we'll say to increment the value at the given key
    // (our near item) in the list; this should create a key
    // if it did not exist previously.
    let map_policy_default = MapPolicy::default();
    // Create our keys list.  This is a list of keys to add to
    // in the near list for the given key.
    let map_keys = ids
        .map(|id| Value::from(id.to_string()))
        .collect::<Vec<_>>();
    // Create our operations list.  We'll reserve enough capacity
    // for the increment values as well as the bin add and
    // retrieval.
    let mut ops = Vec::with_capacity(map_keys.len() + 2);
    // The amount to add by.
    let one_value = Value::from(by);
    // Now, we'll push all of these generated operations into
    // our ops list.
    ops.extend(map_keys.iter().map(|map_key| {
        ops::maps::increment_value(&map_policy_default, "list", map_key, &one_value)
    }));
    // Now, create the addition operation.  This will add the
    // number of keys above to the number of modifications of
    // the list, which...
    let add_bin = Bin::new("nmods", Value::UInt(map_keys.len() as u64));
    ops.push(ops::add(&add_bin));
    // We then retrieve, and return.
    ops.push(ops::get_bin("nmods"));

    // The resulting "record" should contain the nmods bin,
    // which should, at this point, be at least 1; but we'll
    // handle it gracefully in case something funky happens.
    let nmods = client
        .operate(&WritePolicy::default(), &key, &ops[..])
        .map_err(SyncFailure::new)?
        .bins
        .get("nmods")
        .and_then(|v| v.as_u64())
        .unwrap_or_default();
    Ok(nmods)
}

#[inline]
fn increment_item_list_map(client: &Client, key: &Key, id: Uuid, by: f64) -> Result<u64, Error> {
    increment_item_list_map_bulk(client, key, std::iter::once(id), by)
}

fn item_list_decay<F>(spike: &SpikeStorage, key: &Key, decay: F) -> Result<(), Error>
where
    F: Fn(&mut ItemList),
{
    read_modify_write(&spike.client, key, ["list", "nmods", "epoch"], |record| {
        // Load the list from aerospike.
        let mut list = record.as_ref().map(build_item_list).unwrap_or_default();
        // Now, calculate the decays, as well as capping the list.
        // self.near_decay.decay(&mut list);
        decay(&mut list);
        // Now, collect the items into a proper hashmap for
        // aerospike.  At the same time, we'll also reset the nmods
        // counter to zero.
        let items = list
            .items
            .into_iter()
            .map(|(k, v)| (Value::from(k.to_string()), Value::from(v)))
            .collect::<HashMap<_, _>>();

        Ok(vec![
            Bin::new("list", items.into()),
            Bin::new("nmods", 0.into()),
        ])
    })
}
