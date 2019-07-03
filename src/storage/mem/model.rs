use super::ext::*;
use super::MemStorage;
use crate::storage::models::*;
use failure::Error;
use uuid::Uuid;

impl ModelStorage for MemStorage {
    fn set_default_model(&self, list: FeatureList) -> Result<(), Error> {
        self.write_transaction(self.keys.model_database(), |mut txn, db| {
            let key = self.keys.default_model_key();
            txn.serput(db, &key, &list)?;
            Ok(())
        })
    }

    fn find_default_model(&self) -> Result<FeatureList<'static>, Error> {
        self.read_transaction(self.keys.model_database(), |txn, db| {
            let key = self.keys.default_model_key();
            let list = txn.deget(db, &key)?;
            Ok(list.unwrap_or_default())
        })
    }

    fn find_model(&self, part: &str) -> Result<Option<FeatureList<'static>>, Error> {
        self.read_transaction(self.keys.model_database(), |txn, db| {
            let key = self.keys.model_key(part);
            let list = txn.deget(db, &key)?;
            Ok(list)
        })
    }

    fn model_activity_save(&self, part: &str, activity: &Activity) -> Result<(), Error> {
        self.write_transaction(self.keys.model_database(), |mut txn, db| {
            let key = self.keys.activity_key(part, activity.id);
            txn.serput(db, &key, activity)?;
            Ok(())
        })
    }

    fn model_activity_load(&self, part: &str, id: Uuid) -> Result<Option<Activity>, Error> {
        self.read_transaction(self.keys.model_database(), |txn, db| {
            let key = self.keys.activity_key(part, id);
            txn.deget(db, &key)
        })
    }

    fn model_activity_choose(&self, part: &str, id: Uuid, chosen: &[Uuid]) -> Result<(), Error> {
        self.write_transaction(self.keys.model_database(), |mut txn, db| {
            let key = self.keys.activity_key(part, id);
            let mut item = txn.deget::<Activity, _>(db, &key)?;
            if let Some(mut item) = item {
                item.chosen = Some(chosen.to_owned());
                txn.serput(db, &key, &item)?;
                Ok(())
            } else {
                Ok(())
            }
        })
    }
}
