use std::collections::HashMap;

use aerospike::errors::{Error as AerospikeError, ErrorKind as AerospikeErrorKind};
use aerospike::{
    BatchPolicy, BatchRead, Bin, Bins, Client, ClientPolicy, Expiration, GenerationPolicy, Key,
    ReadPolicy, Record, ResultCode, Value, WritePolicy,
};
use byteorder::{ByteOrder, LittleEndian};
use config::Config;
use failure::{Error, SyncFailure};
use uuid::Uuid;

use crate::storage::{FeatureList, UserData};

use super::items::{Item, ItemList, ItemListDecay, NearListDecay, TimeScope};
use super::{ItemStorage, ModelStorage, Sealed, Storage, UserStorage};

use self::ext::{RecordExt, ResultExt, ValueExt};
use self::keys::Keys;
use crate::storage::models::Activity;

mod ext;
mod keys;

mod item;
mod user;

pub struct SpikeStorage {
    client: Client,
    keys: Keys,
    user_history_size: usize,
    short_activity_lifetime: u32,
    long_activity_lifetime: u32,
    list_activity_lifetime: u32,
    near_decay: NearListDecay,
    top_decay: ItemListDecay,
    pop_decay: ItemListDecay,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct SpikeStorageConfiguration {
    aerospike_login: Option<(String, String)>,
    aerospike_thread_pool: usize,
    aerospike_services_alternate: bool,
    aerospike_hosts: String,
    keys: Keys,
    near_decay: NearListDecay,
    top_decay: ItemListDecay,
    pop_decay: ItemListDecay,
    user_history_size: usize,
    short_activity_lifetime: u32,
    long_activity_lifetime: u32,
    list_activity_lifetime: u32,
}

impl Into<SpikeStorage> for SpikeStorageConfiguration {
    fn into(self) -> SpikeStorage {
        let policy = ClientPolicy {
            user_password: self.aerospike_login,
            thread_pool_size: self.aerospike_thread_pool,
            use_services_alternate: self.aerospike_services_alternate,
            ..Default::default()
        };
        let client =
            Client::new(&policy, &self.aerospike_hosts).expect("could not connect to aerospike");

        SpikeStorage {
            client,
            keys: self.keys,
            user_history_size: self.user_history_size,
            short_activity_lifetime: self.short_activity_lifetime,
            long_activity_lifetime: self.long_activity_lifetime,
            list_activity_lifetime: self.list_activity_lifetime,
            near_decay: self.near_decay,
            top_decay: self.top_decay,
            pop_decay: self.pop_decay,
        }
    }
}

impl SpikeStorage {
    pub fn load(config: &Config) -> SpikeStorage {
        let configuration = config
            .get::<SpikeStorageConfiguration>("storage.aerospike")
            .expect("could not load aerospike configuration");
        configuration.into()
    }

    pub fn get(&self, key: &Key, bins: impl Into<Bins>) -> Result<Option<Record>, Error> {
        simple_get(&self.client, key, bins)
    }
}

fn simple_get(client: &Client, key: &Key, bins: impl Into<Bins>) -> Result<Option<Record>, Error> {
    client
        .get(&ReadPolicy::default(), &key, bins)
        .optional()
        .map_err(SyncFailure::new)
        .map_err(Error::from)
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

impl ModelStorage for SpikeStorage {
    fn set_default_model(&self, list: FeatureList<'_>) -> Result<(), Error> {
        let key = self.keys.default_model_key();
        let bin = bincode::serialize(&list)?;
        self.client
            .put(&Default::default(), &key, &[Bin::new("data", bin.into())])
            .map_err(SyncFailure::new)?;
        Ok(())
    }

    fn find_default_model(&self) -> Result<FeatureList<'static>, Error> {
        let key = self.keys.default_model_key();
        let list = self
            .get(&key, ["data"])?
            .deserialize_bin::<FeatureList<'static>>("data")?;
        Ok(list.unwrap_or_default())
    }

    fn find_model(&self, part: &str) -> Result<Option<FeatureList<'static>>, Error> {
        let key = self.keys.model_key(part);
        self.get(&key, ["data"])?
            .deserialize_bin::<FeatureList<'static>>("data")
    }

    fn model_activity_save(&self, part: &str, activity: &Activity) -> Result<(), Error> {
        let key = self.keys.activity_key(part, activity.id);
        let data = bincode::serialize(activity)?;
        self.client
            .put(&Default::default(), &key, &[Bin::new("data", data.into())])
            .map_err(SyncFailure::new)?;
        Ok(())
    }

    fn model_activity_load(&self, part: &str, id: Uuid) -> Result<Option<Activity>, Error> {
        let key = self.keys.activity_key(part, id);
        self.get(&key, ["data"])?
            .deserialize_bin::<Activity>("data")
    }

    fn model_activity_choose(&self, part: &str, id: Uuid, chosen: &[Uuid]) -> Result<(), Error> {
        let key = self.keys.activity_key(part, id);
        let record = self.get(&key, ["data"])?;
        let data = record.deserialize_bin::<Activity>("data")?;
        let mut data = if let Some(d) = data {
            d
        } else {
            return Ok(());
        };

        data.chosen = Some(chosen.to_owned());
        let data = bincode::serialize(&data)?;
        let bins = [Bin::new("data", data.into())];

        self.client
            .put(&Default::default(), &key, &bins)
            .map_err(SyncFailure::new)?;
        Ok(())
    }
}

fn read_modify_write<F>(
    client: &Client,
    key: &Key,
    bins: impl Into<Bins>,
    mut modify: F,
) -> Result<(), Error>
where
    F: FnMut(&Option<Record>) -> Result<Vec<Bin>, Error>,
{
    let bins = bins.into();
    loop {
        let record = simple_get(client, key, bins.clone())?;
        let output = modify(&record)?;
        // We need to use our own write policy, because...
        let policy = WritePolicy {
            // We need to ensure that the generation we're writing
            // to is the same generation we've read from.  This is
            // a form of optimistic locking.  If the generation
            // doesn't equal, then we need to try again, because
            // it's been written to before we've had a chance to
            // write to it.
            generation_policy: GenerationPolicy::ExpectGenEqual,
            generation: record.as_ref().map(|r| r.generation).unwrap_or(0),
            ..WritePolicy::default()
        };

        match client.put(&policy, &key, &output) {
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

fn push_activity_list(
    client: &Client,
    keys: (Key, Key),
    part: &str,
    id: Uuid,
) -> Result<(), Error> {
    use aerospike::operations as ops;

    let value = id.to_string().into();
    let push = ops::lists::append("list", &value);
    let list = [push];

    let _ = client
        .operate(&WritePolicy::default(), &keys.0, &list)
        .map_err(SyncFailure::new)?;
    let _ = client
        .operate(&WritePolicy::default(), &keys.1, &list)
        .map_err(SyncFailure::new)?;
    Ok(())
}
