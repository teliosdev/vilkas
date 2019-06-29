use aerospike::{Client, ReadPolicy, Bin, Bins,Record};
use config::Config;
use super::{ItemStorage, Storage, Sealed};
use super::items::{Item, ItemList};
use self::ext::ResultExt;
use failure::Error;
use uuid::Uuid;


pub struct SpikeStorage {
    client: Client,
    item_namespace: String,

}

impl SpikeStorage {
    pub fn load(config: &Config) -> SpikeStorage {
        unimplemented!()
    }
}

impl Sealed for SpikeStorage {}
impl Storage for SpikeStorage {}
impl ItemStorage for SpikeStorage {
    fn find_item(&self, item: Uuid) -> Result<Option<Item>, Error> {
        let key = as_key![&self.item_namespace[..], "item-definitions", item.to_string()];
        let record = self.client.get(&ReadPolicy::default(), &key, Bins::All).optional()?;
    }
    fn find_items_near(&self, item: Uuid) -> Result<Vec<Uuid>, Error> {
        unimplemented!()
    }

    fn find_items_top(&self) -> Result<Vec<Uuid>, Error> {
        unimplemented!()
    }

    fn find_items_popular(&self) -> Result<Vec<Uuid>, Error> {
        unimplemented!()
    }

    fn items_add_near(&self, item: Uuid, near: Uuid) -> Result<(), Error> {
        unimplemented!()
    }
}

mod ext {
    use aerospike::{ResultCode};
    use aerospike::errors::{Error as AerospikeError, ErrorKind as AerospikeErrorKind};

    pub trait ResultExt<T> {
        fn optional(self) -> Result<Option<T>, AerospikeError>;
    }

    impl<T> ResultExt<T> for Result<T, AerospikeError> {
        fn optional(self) -> Result<Option<T>, AerospikeError> {
            match self {
                Ok(v) => Ok(Some(v)),
                Err(AerospikeError(AerospikeErrorKind::ServerError(ResultCode::KeyNotFoundError), _)) =>
                    Ok(None),
                Err(e) => Err(e)
            }
        }
    }
}