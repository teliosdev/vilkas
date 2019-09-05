use super::ext::RecordExt;
use super::SpikeStorage;
use crate::storage::spike::read_modify_write;
use crate::storage::UserData;
use crate::storage::UserStore;
use aerospike::Bin;
use failure::Error;
use uuid::Uuid;

impl UserStore for SpikeStorage {
    fn find_user(&self, part: &str, id: &str) -> Result<UserData, Error> {
        let key = self.keys.user_key(part, id);
        self.get(&key, ["data"])?
            .deserialize_bin::<UserData>("data")
            .map(|r| r.unwrap_or_else(|| UserData::new(id)))
    }

    fn user_push_history(&self, part: &str, id: &str, item: Uuid) -> Result<(), Error> {
        let key = self.keys.user_key(part, id);
        read_modify_write(&self.client, &key, ["data"], |record| {
            let mut data = record
                .deserialize_bin::<UserData>("data")
                .ok()
                .and_then(core::convert::identity)
                .unwrap_or_else(|| UserData::new(id));
            let mut history = vec![];
            std::mem::swap(&mut history, &mut data.history);
            data.history = std::iter::once(item)
                .chain(history.into_iter())
                .take(self.user_history_length)
                .collect();

            let data = bincode::serialize(&data)?;

            Ok(vec![Bin::new("data", data.into())])
        })
    }
}
