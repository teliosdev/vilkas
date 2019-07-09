pub use self::items::{Item, ItemList, ItemStorage, TimeScope};
pub use self::models::{Activity, BasicExample, Example, FeatureList, ModelStorage};
use self::sealed::Sealed;
pub use self::users::{UserData, UserStorage};

mod items;
mod master;
mod models;
mod users;

#[cfg(feature = "lmdb")]
pub mod mem;
#[cfg(feature = "aerospike")]
pub mod spike;
pub trait Storage: ItemStorage + UserStorage + ModelStorage + Sealed {}

pub type DefaultStorage = master::MasterStorage;

mod sealed {
    pub trait Sealed {}
}
