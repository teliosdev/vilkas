use super::ext::*;
use super::MemStorage;
use crate::storage::models::*;
use failure::Error;
use lmdb::{Database, RwTransaction};
use std::collections::VecDeque;
use uuid::Uuid;

impl ModelStorage for MemStorage {
    fn set_default_model(&self, list: FeatureList) -> Result<(), Error> {
        self.write_transaction(self.keys.model_database(), |txn, db| {
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
        self.write_transaction(self.keys.model_database(), |txn, db| {
            let key = self.keys.activity_key(part, activity.id);
            txn.serput(db, &key, activity)?;
            push_activity(
                txn,
                db,
                &self.keys.default_activity_list_key(),
                self.activity_list_length,
                part,
                activity.id,
            )?;
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
        self.write_transaction(self.keys.model_database(), |txn, db| {
            let key = self.keys.activity_key(part, id);
            let item = txn.deget::<Activity, _>(db, &key)?;
            if let Some(mut item) = item {
                item.chosen = Some(chosen.to_owned());
                txn.serput(db, &key, &item)?;
                Ok(())
            } else {
                Ok(())
            }
        })
    }

    fn model_activity_pluck(&self) -> Result<Vec<Activity>, Error> {
        self.write_transaction(self.keys.model_database(), |txn, db| {
            let key = self.keys.default_activity_list_key();
            let result = txn
                .deget::<Vec<(String, Uuid)>, _>(db, &key)?
                .unwrap_or_default();
            txn.serput::<Vec<(String, Uuid)>, _>(db, &key, &vec![])?;
            let result = result
                .into_iter()
                .flat_map(|(part, id)| {
                    txn.deget::<Activity, _>(db, self.keys.activity_key(&part, id))
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>();

            Ok(result)
        })
    }

    fn model_activity_delete_all<'p, Ids>(&self, ids: Ids) -> Result<(), Error>
    where
        Ids: IntoIterator<Item = (&'p str, Uuid)>,
    {
        self.write_transaction(self.keys.model_database(), |txn, db| {
            for (part, id) in ids.into_iter() {
                let key = self.keys.activity_key(part, id);
                match txn.del(db, &key, None) {
                    Ok(_) => {}
                    // We don't care if it's not found.
                    Err(lmdb::Error::NotFound) => {}
                    Err(e) => Err(e)?,
                }
            }

            Ok(())
        })
    }
}

fn push_activity(
    txn: &mut RwTransaction<'_>,
    db: Database,
    key: &str,
    cap: u32,
    part: &str,
    id: Uuid,
) -> Result<(), Error> {
    let mut result = txn
        .deget::<VecDeque<(String, Uuid)>, _>(db, key)?
        .unwrap_or_default();
    result.push_front((part.to_owned(), id));
    result.truncate(cap as usize);
    txn.serput(db, key, &result)?;

    Ok(())
}
