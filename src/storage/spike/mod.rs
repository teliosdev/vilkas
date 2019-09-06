use self::ext::ResultExt;
use self::keys::Keys;
use super::{ItemListDecay, NearListDecay};
use super::{ModelStore, Sealed, Store};
use aerospike::errors::{Error as AerospikeError, ErrorKind as AerospikeErrorKind};
use aerospike::{
    Bin, Bins, Client, ClientPolicy, GenerationPolicy, Key, ReadPolicy, Record, ResultCode,
    WritePolicy,
};
use config::Config;
use failure::{Error, SyncFailure};

mod ext;
mod item;
mod keys;
mod model;
mod user;

pub struct SpikeStorage {
    client: Client,
    keys: Keys,
    user_history_length: usize,
    short_activity_lifetime: u32,
    long_activity_lifetime: u32,
    list_activity_lifetime: u32,
    list_activity_length: u32,
    list_recent_length: u32,
    near_decay: NearListDecay,
    top_decay: ItemListDecay,
    pop_decay: ItemListDecay,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct SpikeStorageConfiguration {
    #[serde(default)]
    aerospike_login: Option<(String, String)>,
    #[serde(default = "defaults::aerospike_thread_pool")]
    aerospike_thread_pool: usize,
    #[serde(default = "defaults::aerospike_services_alternate")]
    aerospike_services_alternate: bool,
    aerospike_hosts: String,
    #[serde(default)]
    keys: Keys,
    #[serde(default)]
    near_decay: NearListDecay,
    #[serde(default = "ItemListDecay::top_default")]
    top_decay: ItemListDecay,
    #[serde(default = "ItemListDecay::pop_default")]
    pop_decay: ItemListDecay,
    #[serde(default = "defaults::user_history_length")]
    user_history_length: usize,
    #[serde(default = "defaults::short_activity_lifetime")]
    short_activity_lifetime: u32,
    #[serde(default = "defaults::long_activity_lifetime")]
    long_activity_lifetime: u32,
    #[serde(default = "defaults::list_activity_lifetime")]
    list_activity_lifetime: u32,
    #[serde(default = "defaults::list_activity_length")]
    list_activity_length: u32,
    #[serde(default = "defaults::list_recent_length")]
    list_recent_length: u32,
}

mod defaults {
    pub const fn aerospike_thread_pool() -> usize {
        16
    }
    pub const fn aerospike_services_alternate() -> bool {
        false
    }
    pub const fn user_history_length() -> usize {
        16
    }
    // ten minutes
    pub const fn short_activity_lifetime() -> u32 {
        60 * 10
    }
    // two hours
    pub const fn long_activity_lifetime() -> u32 {
        60 * 60 * 2
    }
    // ditto
    pub const fn list_activity_lifetime() -> u32 {
        60 * 60 * 2
    }

    pub const fn list_activity_length() -> u32 {
        256
    }

    pub const fn list_recent_length() -> u32 {
        256
    }
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
            user_history_length: self.user_history_length,
            short_activity_lifetime: self.short_activity_lifetime,
            long_activity_lifetime: self.long_activity_lifetime,
            list_activity_lifetime: self.list_activity_lifetime,
            list_activity_length: self.list_activity_length,
            list_recent_length: self.list_recent_length,
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

impl Store for SpikeStorage {}

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

fn simple_get(client: &Client, key: &Key, bins: impl Into<Bins>) -> Result<Option<Record>, Error> {
    client
        .get(&ReadPolicy::default(), &key, bins)
        .optional()
        .map_err(SyncFailure::new)
        .map_err(Error::from)
}
