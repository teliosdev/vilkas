pub use self::core::items::{Item, ItemList, ItemListDecay, ItemStore, NearListDecay, TimeScope};
pub use self::core::models::{Activity, BasicExample, Example, FeatureList, ModelStore};
pub use self::core::users::{UserData, UserStore};
use self::sealed::Sealed;

mod core;
mod master;

#[cfg(feature = "lmdb")]
pub mod mem;
#[cfg(feature = "redis")]
pub mod redis;
#[cfg(feature = "aerospike")]
pub mod spike;

pub trait Store: ItemStore + UserStore + ModelStore + Sealed {}

pub type DefaultStorage = master::MasterStorage;

mod sealed {
    pub trait Sealed {}
}
