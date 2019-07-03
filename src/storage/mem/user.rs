use super::ext::*;
use super::MemStorage;
use crate::storage::users::{UserData, UserStorage};
use failure::Error;
use uuid::Uuid;

impl UserStorage for MemStorage {
    fn find_user(&self, part: &str, id: &str) -> Result<UserData, Error> {
        self.read_transaction(self.keys.user_database(), |txn, db| {
            let key = self.keys.user_key(part, id);
            let data = txn.deget::<UserData, _>(db, &key)?;
            Ok(data.unwrap_or_else(|| UserData::new(id)))
        })
    }

    fn user_push_history(&self, part: &str, id: &str, item: Uuid) -> Result<(), Error> {
        self.write_transaction(self.keys.user_database(), |mut txn, db| {
            let key = self.keys.user_key(part, id);
            let data = txn.deget::<UserData, _>(db, &key)?;
            let mut data = data.unwrap_or_else(|| UserData::new(id));
            let mut history = vec![];
            std::mem::swap(&mut history, &mut data.history);
            data.history = std::iter::once(item)
                .chain(history.into_iter())
                .take(self.user_history_size)
                .collect();
            txn.serput(db, &key, &data)?;
            Ok(())
        })
    }
}
