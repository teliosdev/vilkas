use self::ext::{RecordExt, ResultExt, ValueExt};
use self::keys::Keys;
use super::items::{Item, ItemList, ItemListDecay, NearListDecay, TimeScope};
use super::{ItemStorage, Sealed, Storage, UserStorage};
use aerospike::errors::{Error as AerospikeError, ErrorKind as AerospikeErrorKind};
use aerospike::{
    BatchPolicy, BatchRead, Bin, Bins, Client, ClientPolicy, GenerationPolicy, Key, ReadPolicy,
    Record, ResultCode, Value, WritePolicy,
};
use byteorder::{ByteOrder, LittleEndian};
use config::Config;
use failure::{Error, SyncFailure};
use std::collections::HashMap;
use uuid::Uuid;

mod ext;
mod keys;

pub struct SpikeStorage {
    client: Client,
    keys: Keys,
    near_decay: NearListDecay,
    top_decay: ItemListDecay,
    pop_decay: ItemListDecay,
}

impl SpikeStorage {
    pub fn load(config: &Config) -> SpikeStorage {
        let user_password = config.get("aerospike.login").ok().and_then(|v| v);
        let thread_pool_size = config
            .get("aerospike.thread_pool")
            .expect("could not load aerospike thread pool count");
        let use_services_alternate = config
            .get_bool("aerospike.services_alternate")
            .ok()
            .unwrap_or(false);
        let policy = ClientPolicy {
            user_password,
            thread_pool_size,
            use_services_alternate,
            ..ClientPolicy::default()
        };
        let hosts = config
            .get_str("aerospike.hosts")
            .expect("could not load aerospike hosts");
        let client = Client::new(&policy, &hosts).expect("could not connect to aerospike");
        let keys: Keys = config
            .get("aerospike.keys")
            .expect("could not load aerospike key information");
        let near_decay: NearListDecay = config.get("aerospike.near_decay").ok().unwrap_or_default();
        let top_decay: ItemListDecay = config
            .get("aerospike.top_decay")
            .ok()
            .unwrap_or_else(ItemListDecay::top_default);
        let pop_decay: ItemListDecay = config
            .get("aerospike.pop_decay")
            .ok()
            .unwrap_or_else(ItemListDecay::pop_default);

        SpikeStorage {
            client,
            keys,
            near_decay,
            top_decay,
            pop_decay,
        }
    }

    pub fn get(&self, key: &Key, bins: impl Into<Bins>) -> Result<Option<Record>, Error> {
        self.client
            .get(&ReadPolicy::default(), &key, bins)
            .optional()
            .map_err(SyncFailure::new)
            .map_err(Error::from)
    }
}

struct DebugClient<'r>(&'r Client);

impl<'r> std::fmt::Debug for DebugClient<'r> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("connected", &self.0.is_connected())
            .field(
                "nodes",
                &self
                    .0
                    .nodes()
                    .into_iter()
                    .map(|node| (node.name().to_owned(), node.address().to_owned()))
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl std::fmt::Debug for SpikeStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("SpikeStorage")
            .field("client", &DebugClient(&self.client))
            .field("keys", &self.keys)
            .field("near_decay", &self.near_decay)
            .field("top_decay", &self.top_decay)
            .field("pop_decay", &self.pop_decay)
            .finish()
    }
}

impl Sealed for SpikeStorage {}

impl Storage for SpikeStorage {}

impl UserStorage for SpikeStorage {}

impl ItemStorage for SpikeStorage {
    fn find_item(&self, part: &str, item: Uuid) -> Result<Option<Item>, Error> {
        let key = self.keys.item_key(part, item);
        self.get(&key, ["data"])?.deserialize_bin::<Item>("data")
    }

    fn find_items<'i>(&self, part: &str, items: Box<dyn Iterator<Item = Uuid> + 'i>) -> Result<Vec<Option<Item>>, Error> {
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
    // Otherwise, we'll first set the nmods count to zero, since
    // we are flushing the list, and we'll recalculate the new
    // values.
    loop {
        let record = spike.get(&key, ["list", "nmods", "since"])?;
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
        // We need to use our own write policy, because...
        let policy = WritePolicy {
            // We need to ensure that the generation we're writing
            // to is the same generation we've read from.  This is
            // a form of optimistic locking.  If the generation
            // doesn't equal, then we need to try again, because
            // it's been written to before we've had a chance to
            // write to it.
            generation_policy: GenerationPolicy::ExpectGenEqual,
            generation: record.map(|r| r.generation).unwrap_or(0),
            ..WritePolicy::default()
        };
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
        let bins = [
            Bin::new("list", items.into()),
            Bin::new("nmods", 0.into()),
            Bin::new("epoch", epoch.into()),
        ];

        match spike.client.put(&policy, &key, &bins) {
            // We've successfully written.  Return.
            Ok(()) => {
                return Ok(());
            }
            // We tried to write, but the generations don't match.
            // Try again.
            Err(AerospikeError(
                AerospikeErrorKind::ServerError(ResultCode::GenerationError),
                _,
            )) => {
                continue;
            }
            // We tried to write, but we got an unknown error.
            // Bail out completely.  If we bail out at this point,
            // ideally the item added to the map from earlier will
            // still be there, just... the map wasn't completed.
            Err(e) => {
                return Err(SyncFailure::new(e).into());
            }
        }
    }
}
