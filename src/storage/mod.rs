use self::sealed::Sealed;

mod items;
mod spike;

pub use self::items::{Item, ItemList, ItemStorage, TimeScope};

pub trait UserStorage: Sealed {}

pub trait Storage: ItemStorage + UserStorage + Sealed {}

pub type DefaultStorage = spike::SpikeStorage;

mod sealed {
    pub trait Sealed {}
}
