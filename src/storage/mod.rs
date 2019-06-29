use config::Config;

mod items;
mod spike;

use self::sealed::Sealed;
use self::items::ItemStorage;

pub trait Storage: ItemStorage + Sealed {}

pub type DefaultStorage = spike::SpikeStorage;

mod sealed {
    pub(super) trait Sealed {}
}
