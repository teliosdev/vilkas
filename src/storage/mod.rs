use config::Config;

mod items;
pub mod redis;

use self::sealed::Sealed;
use crate::storage::items::ItemStorage;

pub trait Storage: ItemStorage + Sealed {}

pub type DefaultStorage = redis::RedisStorage;

mod sealed {
    pub(super) trait Sealed {}
}
