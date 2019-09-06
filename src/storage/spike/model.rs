use super::ext::RecordExt;
use super::ext::{ResultExt, ValueExt};
use super::ModelStore;
use super::SpikeStorage;
use crate::storage::Activity;
use crate::storage::FeatureList;
use aerospike::{
    BatchPolicy, BatchRead, Bin, Bins, Client, Expiration, Key, ReadPolicy, Value, WritePolicy,
};
use failure::{Error, SyncFailure};
use uuid::Uuid;

impl ModelStore for SpikeStorage {
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
                self.list_activity_length,
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
        Ok(())
    }

    fn model_activity_pluck(&self) -> Result<Vec<Activity>, Error> {
        let default_key = self.keys.default_activity_list_key();
        let result = self
            .client
            .get(&ReadPolicy::default(), &default_key, ["list"])
            .optional()
            .map_err(SyncFailure::new)?;
        let bins = Bins::from(["data"]);
        let list = result.as_ref().and_then(|r| r.bins.get("list"));
        let list = list.and_then(|v| v.as_list());
        let items = list
            .iter()
            .flat_map(|i| i.iter())
            .flat_map(|v: &Value| v.as_list())
            .flat_map(|item| {
                let part = item.get(0).and_then(|part| part.as_str());
                let id = item
                    .get(1)
                    .and_then(|id| id.as_str())
                    .and_then(|v| v.parse::<Uuid>().ok());
                part.and_then(|p| id.map(|i| (p, i)))
            })
            .map(|(part, id)| {
                let key = self.keys.activity_key(part, id);
                BatchRead {
                    key,
                    bins: &bins,
                    record: None,
                }
            })
            .collect::<Vec<_>>();
        self.client
            .delete(&WritePolicy::default(), &default_key)
            .map_err(SyncFailure::new)?;
        let result = self
            .client
            .batch_get(&BatchPolicy::default(), items)
            .map_err(SyncFailure::new)?;

        let result = result
            .into_iter()
            .flat_map(|read| read.record)
            .flat_map(|record| {
                record
                    .bins
                    .get("data")
                    .and_then(|data| data.as_blob())
                    .and_then(|data| bincode::deserialize::<Activity>(data).ok())
            })
            .collect::<Vec<_>>();
        Ok(result)
    }

    fn model_activity_delete_all<'p, Ids>(&self, id: Ids) -> Result<(), Error>
    where
        Ids: IntoIterator<Item = (&'p str, Uuid)>,
    {
        let keys = id
            .into_iter()
            .map(|(part, id)| self.keys.activity_key(part, id));
        for key in keys {
            self.client
                .delete(&WritePolicy::default(), &key)
                .map_err(SyncFailure::new)?;
        }
        Ok(())
    }
}

fn push_activity_list(
    client: &Client,
    keys: (Key, Key),
    lifetime: u32,
    cap: u32,
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
    let cap = ops::lists::remove_range_from("list", cap as i64);
    let list = [push, cap];

    let policy = WritePolicy::new(0, Expiration::Seconds(lifetime));
    //    let _ = client
    //        .operate(&policy, &keys.0, &list)
    //        .map_err(SyncFailure::new)?;
    let _ = client
        .operate(&policy, &keys.1, &list)
        .map_err(SyncFailure::new)?;
    Ok(())
}
