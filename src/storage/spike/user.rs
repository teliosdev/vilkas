use super::ext::{RecordExt, ResultExt, ValueExt};
use super::keys::Keys;
use super::SpikeStorage;
use crate::storage::items::{Item, ItemList, ItemListDecay, NearListDecay, TimeScope};
use crate::storage::models::Activity;
use crate::storage::spike::read_modify_write;
use crate::storage::{FeatureList, UserData};
use crate::storage::{ItemStorage, ModelStorage, Sealed, Storage, UserStorage};
use aerospike::errors::{Error as AerospikeError, ErrorKind as AerospikeErrorKind};
use aerospike::{
    BatchPolicy, BatchRead, Bin, Bins, Client, ClientPolicy, Expiration, GenerationPolicy, Key,
    ReadPolicy, Record, ResultCode, Value, WritePolicy,
};
use byteorder::{ByteOrder, LittleEndian};
use config::Config;
use failure::{Error, SyncFailure};
use std::collections::HashMap;
use uuid::Uuid;

impl UserStorage for SpikeStorage {
    fn find_user(&self, part: &str, id: &str) -> Result<UserData, Error> {
        let key = self.keys.user_key(part, id);
        self.get(&key, ["data"])?
            .deserialize_bin::<UserData>("data")
            .map(|r| {
                r.unwrap_or_else(|| UserData {
                    id: id.to_owned(),
                    history: vec![],
                })
            })
    }

    fn user_push_history(&self, part: &str, id: &str, item: Uuid) -> Result<(), Error> {
        let key = self.keys.user_key(part, id);
        read_modify_write(&self.client, &key, ["data"], |record| {
            let mut data = record
                .deserialize_bin::<UserData>("data")
                .ok()
                .and_then(core::convert::identity)
                .unwrap_or_else(|| UserData {
                    id: id.to_owned(),
                    history: vec![],
                });
            let mut history = vec![];
            std::mem::swap(&mut history, &mut data.history);
            data.history = std::iter::once(item)
                .chain(history.into_iter())
                .take(self.user_history_size)
                .collect();

            let data = bincode::serialize(&data)?;

            Ok(vec![Bin::new("data", data.into())])
        })
    }
}
