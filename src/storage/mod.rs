pub use self::items::{Item, ItemList, ItemStorage, TimeScope};
pub use self::models::{BasicExample, Example, FeatureList, ModelStorage};
use self::sealed::Sealed;
pub use self::users::{UserData, UserStorage};

mod items;
mod models;
mod spike;
mod users;

pub trait Storage: ItemStorage + UserStorage + ModelStorage + Sealed {}

pub type DefaultStorage = spike::SpikeStorage;

mod sealed {
    pub trait Sealed {}
}
