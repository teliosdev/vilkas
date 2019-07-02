use super::ext::RecordExt;
use super::ModelStorage;
use super::SpikeStorage;
use crate::storage::models::Activity;
use crate::storage::FeatureList;
use aerospike::{Bin, Client, Expiration, Key, Value, WritePolicy};
use failure::{Error, SyncFailure};
use uuid::Uuid;

impl ModelStorage for SpikeStorage {
    fn set_default_model(&self, list: FeatureList<'_>) -> Result<(), Error> {
        let key = self.keys.default_model_key();
        let bin = bincode::serialize(&list)?;
        self.client
            .put(&Default::default(), &key, &[Bin::new("data", bin.into())])
            .map_err(SyncFailure::new)?;
        Ok(())
    }

    fn find_default_model(&self) -> Result<FeatureList<'static>, Error> {
        let key = self.keys.default_model_key();
        let list = self
            .get(&key, ["data"])?
            .deserialize_bin::<FeatureList<'static>>("data")?;
        Ok(list.unwrap_or_default())
    }

    fn find_model(&self, part: &str) -> Result<Option<FeatureList<'static>>, Error> {
        let key = self.keys.model_key(part);
        self.get(&key, ["data"])?
            .deserialize_bin::<FeatureList<'static>>("data")
    }

    fn model_activity_save(&self, part: &str, activity: &Activity) -> Result<(), Error> {
        let key = self.keys.activity_key(part, activity.id);
        let data = bincode::serialize(activity)?;
        let lifetime = if activity.chosen.is_some() {
            self.long_activity_lifetime
        } else {
            self.short_activity_lifetime
        };
        let policy = WritePolicy::new(0, Expiration::Seconds(lifetime));
        self.client
            .put(&policy, &key, &[Bin::new("data", data.into())])
            .map_err(SyncFailure::new)?;

        if activity.chosen.is_some() {
            let local_key = self.keys.activity_list_key(part);
            let default_key = self.keys.default_activity_list_key();
            push_activity_list(
                &self.client,
                (local_key, default_key),
                self.list_activity_lifetime,
                part,
                activity.id,
            )?;
        }
        Ok(())
    }

    fn model_activity_load(&self, part: &str, id: Uuid) -> Result<Option<Activity>, Error> {
        let key = self.keys.activity_key(part, id);
        self.get(&key, ["data"])?
            .deserialize_bin::<Activity>("data")
    }

    fn model_activity_choose(&self, part: &str, id: Uuid, chosen: &[Uuid]) -> Result<(), Error> {
        let key = self.keys.activity_key(part, id);
        let record = self.get(&key, ["data"])?;
        let data = record.deserialize_bin::<Activity>("data")?;
        let mut data = if let Some(d) = data {
            d
        } else {
            return Ok(());
        };

        data.chosen = Some(chosen.to_owned());
        let data = bincode::serialize(&data)?;
        let bins = [Bin::new("data", data.into())];

        let policy = WritePolicy::new(
            record.map(|r| r.generation).unwrap_or(0),
            Expiration::Seconds(self.long_activity_lifetime),
        );

        self.client
            .put(&policy, &key, &bins)
            .map_err(SyncFailure::new)?;

        let local_key = self.keys.activity_list_key(part);
        let default_key = self.keys.default_activity_list_key();
        push_activity_list(
            &self.client,
            (local_key, default_key),
            self.list_activity_lifetime,
            part,
            id,
        )?;
        Ok(())
    }
}

fn push_activity_list(
    client: &Client,
    keys: (Key, Key),
    lifetime: u32,
    part: &str,
    id: Uuid,
) -> Result<(), Error> {
    use aerospike::operations as ops;
    let value: Value = vec![
        Value::String(part.to_owned()),
        Value::String(id.to_string()),
    ]
    .into();
    let push = ops::lists::append("list", &value);
    let list = [push];

    let policy = WritePolicy::new(0, Expiration::Seconds(lifetime));
    let _ = client
        .operate(&policy, &keys.0, &list)
        .map_err(SyncFailure::new)?;
    let _ = client
        .operate(&policy, &keys.1, &list)
        .map_err(SyncFailure::new)?;
    Ok(())
}
