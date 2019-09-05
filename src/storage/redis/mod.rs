use self::keys::Keys;
use super::core::items::{ItemListDecay, NearListDecay};
use crate::storage::sealed::Sealed;
use crate::storage::Store;
use config::Config;
use redis::Client;

mod ext;
mod item;
mod keys;
mod model;
mod user;

#[derive(Debug)]
pub struct RedisStorage {
    client: Client,
    keys: Keys,
    user_history_length: usize,
    short_activity_lifetime: u32,
    long_activity_lifetime: u32,
    activity_list_lifetime: u32,
    activity_list_length: u32,
    recent_list_length: u32,
    near_decay: NearListDecay,
    top_decay: ItemListDecay,
    pop_decay: ItemListDecay,
}

mod defaults {
    pub const fn user_history_length() -> usize {
        16
    }
    pub const fn activity_list_length() -> u32 {
        256
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
    pub const fn activity_list_lifetime() -> u32 {
        60 * 60 * 2
    }
    pub const fn recent_list_length() -> u32 {
        256
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct RedisStorageConfiguration {
    pub url: String,
    #[serde(default)]
    pub near_decay: NearListDecay,
    #[serde(default = "ItemListDecay::top_default")]
    pub top_decay: ItemListDecay,
    #[serde(default = "ItemListDecay::pop_default")]
    pub pop_decay: ItemListDecay,
    #[serde(default = "defaults::user_history_length")]
    pub user_history_length: usize,
    #[serde(default = "defaults::short_activity_lifetime")]
    pub short_activity_lifetime: u32,
    #[serde(default = "defaults::long_activity_lifetime")]
    pub long_activity_lifetime: u32,
    #[serde(default = "defaults::activity_list_lifetime")]
    pub activity_list_lifetime: u32,
    #[serde(default = "defaults::activity_list_length")]
    pub activity_list_length: u32,
    #[serde(default = "defaults::recent_list_length")]
    pub recent_list_length: u32,
}

impl Into<RedisStorage> for RedisStorageConfiguration {
    fn into(self) -> RedisStorage {
        RedisStorage {
            client: redis::Client::open(&self.url[..]).expect("could not connect to redis"),
            keys: Keys,
            user_history_length: self.user_history_length,
            short_activity_lifetime: self.short_activity_lifetime,
            long_activity_lifetime: self.long_activity_lifetime,
            activity_list_lifetime: self.activity_list_lifetime,
            activity_list_length: self.activity_list_length,
            recent_list_length: self.recent_list_length,
            near_decay: self.near_decay,
            top_decay: self.top_decay,
            pop_decay: self.pop_decay,
        }
    }
}

impl Default for RedisStorageConfiguration {
    fn default() -> RedisStorageConfiguration {
        RedisStorageConfiguration {
            url: "redis://localhost/0".to_string(),
            near_decay: Default::default(),
            top_decay: ItemListDecay::top_default(),
            pop_decay: ItemListDecay::pop_default(),
            user_history_length: defaults::user_history_length(),
            short_activity_lifetime: defaults::short_activity_lifetime(),
            long_activity_lifetime: defaults::long_activity_lifetime(),
            activity_list_lifetime: defaults::activity_list_lifetime(),
            activity_list_length: defaults::activity_list_length(),
            recent_list_length: defaults::recent_list_length(),
        }
    }
}

impl RedisStorage {
    pub fn load(config: &Config) -> RedisStorage {
        let configuration = config
            .get::<RedisStorageConfiguration>("storage.redis")
            .expect("could not load memory configuration");
        let storage: RedisStorage = configuration.into();
        storage
    }
}

impl Sealed for RedisStorage {}
impl Store for RedisStorage {}
