use crate::storage::spike::ext::{RecordExt, ValueExt};
use crate::storage::spike::{read_modify_write, SpikeStorage};
use crate::storage::{Item, ItemList, ItemStorage, TimeScope};
use aerospike::{
    BatchPolicy, BatchRead, Bin, Bins, Client, Key, ReadPolicy, Record, Value, WritePolicy,
};
use byteorder::{ByteOrder, LittleEndian};
use failure::{Error, SyncFailure};
use std::collections::HashMap;
use uuid::Uuid;

impl ItemStorage for SpikeStorage {
    fn find_item(&self, part: &str, item: Uuid) -> Result<Option<Item>, Error> {
        let key = self.keys.item_key(part, item);
        self.get(&key, ["data"])?.deserialize_bin::<Item>("data")
    }

    fn find_items<'i>(
        &self,
        part: &str,
        items: Box<dyn Iterator<Item = Uuid> + 'i>,
    ) -> Result<Vec<Option<Item>>, Error> {
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
            .get(&key, ["list", "nmods"])?
            .as_ref()
            .map(build_item_list)
            .unwrap_or_default())
    }

    fn find_items_top(&self, part: &str, scope: TimeScope) -> Result<ItemList, Error> {
        let key = self.keys.item_top_key(part, scope);
        Ok(self
            .get(&key, ["list", "nmods"])?
            .as_ref()
            .map(build_item_list)
            .unwrap_or_default())
    }

    fn find_items_popular(&self, part: &str, scope: TimeScope) -> Result<ItemList, Error> {
        let key = self.keys.item_pop_key(part, scope);
        Ok(self
            .get(&key, ["list", "nmods"])?
            .as_ref()
            .map(build_item_list)
            .unwrap_or_default())
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
                item_list_decay(&self, &key, |epoch, list| {
                    self.top_decay
                        .decay(scope, current - epoch.unwrap_or(current - 360_000), list)
                })?;
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
                item_list_decay(&self, &key, |epoch, list| {
                    self.pop_decay
                        .decay(scope, current - epoch.unwrap_or(current - 360_000), list)
                })?;
            }
        }

        Ok(())
    }
}

fn millis_epoch() -> u128 {
    std::time::UNIX_EPOCH.elapsed().unwrap().as_millis()
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

    ItemList { items, nmods }
}

fn increment_item_list_map(client: &Client, key: &Key, id: Uuid, by: f64) -> Result<u64, Error> {
    // We'll have to do some weird stuff to get this to work.
    // First, do our imports...
    use aerospike::operations::{self as ops, MapPolicy};
    // Now, we'll say to increment the value at the given key
    // (our near item) in the list; this should create a key
    // if it did not exist previously.
    let map_policy_default = MapPolicy::default();
    let map_key = id.to_string().into();
    let one_value = by.into();
    let incr = ops::maps::increment_value(&map_policy_default, "list", &map_key, &one_value);
    // Then, we'll increment the nmods key...
    let add_bin = Bin::new("nmods", Value::UInt(1));
    let add = ops::add(&add_bin);
    // and retrieve it, for use.
    let get = ops::get_bin("nmods");
    let op = [incr, add, get];

    // The resulting "record" should contain the nmods bin,
    // which should, at this point, be at least 1; but we'll
    // handle it gracefully in case something funky happens.
    let nmods = client
        .operate(&WritePolicy::default(), &key, &op)
        .map_err(SyncFailure::new)?
        .bins
        .get("nmods")
        .and_then(|v| v.as_u64())
        .unwrap_or_default();
    Ok(nmods)
}

fn item_list_decay<F>(spike: &SpikeStorage, key: &Key, decay: F) -> Result<(), Error>
where
    F: Fn(Option<u128>, &mut ItemList),
{
    read_modify_write(&spike.client, key, ["list", "nmods", "since"], |record| {
        // Load the list from aerospike.
        let mut list = record.as_ref().map(build_item_list).unwrap_or_default();
        let epoch = record
            .as_ref()
            .and_then(|r| r.bins.get("since"))
            .and_then(|v| v.as_blob())
            .map(|v| LittleEndian::read_u128(&v[..]));
        // Now, calculate the decays, as well as capping the list.
        // self.near_decay.decay(&mut list);
        decay(epoch, &mut list);
        // Now, collect the items into a proper hashmap for
        // aerospike.  At the same time, we'll also reset the nmods
        // counter to zero.
        let items = list
            .items
            .into_iter()
            .map(|(k, v)| (Value::from(k.to_string()), Value::from(v)))
            .collect::<HashMap<_, _>>();

        let mut epoch = vec![0u8; 16];
        LittleEndian::write_u128(&mut epoch[..], millis_epoch());
        Ok(vec![
            Bin::new("list", items.into()),
            Bin::new("nmods", 0.into()),
            Bin::new("epoch", epoch.into()),
        ])
    })
}
