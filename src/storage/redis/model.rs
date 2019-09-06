use super::ext::*;
use super::RedisStorage;
use crate::storage::{Activity, FeatureList, ModelStore};
use failure::Error;
use redis::{Commands, Connection, PipelineCommands};
use uuid::Uuid;

impl ModelStore for RedisStorage {
    fn set_default_model(&self, list: FeatureList<'_>) -> Result<(), Error> {
        let mut conn = self.client.get_connection()?;
        let key = self.keys.default_model_key();
        conn.serput(key, &list)?;
        Ok(())
    }

    fn find_default_model(&self) -> Result<FeatureList<'static>, Error> {
        let mut conn = self.client.get_connection()?;
        let key = self.keys.default_model_key();
        conn.deget(key).map(Option::unwrap_or_default)
    }

    fn find_model(&self, part: &str) -> Result<Option<FeatureList<'static>>, Error> {
        let mut conn = self.client.get_connection()?;
        let key = self.keys.model_key(part);
        conn.deget(key)
    }

    fn model_activity_save(&self, part: &str, activity: &Activity) -> Result<(), Error> {
        let mut conn = self.client.get_connection()?;
        let key = self.keys.activity_key(part, activity.id);
        let data = bincode::serialize(activity)?;
        let lifetime = if activity.chosen.is_some() {
            self.long_activity_lifetime
        } else {
            self.short_activity_lifetime
        };

        let _: () = conn.set_ex(&key, data, lifetime as usize)?;

        if activity.chosen.is_some() {
            let local_key = self.keys.activity_list_key(part);
            let default_key = self.keys.default_activity_list_key();
            push_activity_list(
                &mut conn,
                (local_key, default_key),
                self.activity_list_lifetime,
                self.activity_list_length,
                part,
                activity.id,
            )?;
        }
        Ok(())
    }

    fn model_activity_load(&self, part: &str, id: Uuid) -> Result<Option<Activity>, Error> {
        self.client
            .get_connection()?
            .deget(self.keys.activity_key(part, id))
    }

    fn model_activity_choose(&self, part: &str, id: Uuid, chosen: &[Uuid]) -> Result<(), Error> {
        let mut conn = self.client.get_connection()?;
        let key = self.keys.activity_key(part, id);
        redis::transaction(&mut conn, &[&key], |conn, pipe| {
            let data: Option<Activity> = conn
                .get::<_, Option<Vec<u8>>>(&key)?
                .and_then(|data| bincode::deserialize(&data).ok());

            let mut data = if let Some(d) = data {
                d
            } else {
                return Ok(Some(()));
            };
            data.chosen = Some(chosen.to_owned());
            let data = bincode::serialize(&data).expect("could not serialize activity?");
            pipe.set_ex(&key, data, self.long_activity_lifetime as usize)
                .ignore()
                .query(conn)
        })?;

        let activity: Activity = bincode::deserialize(&conn.get::<_, Vec<u8>>(&key)?)?;

        let local_key = self.keys.activity_list_key(part);
        let default_key = self.keys.default_activity_list_key();
        push_activity_list(
            &mut conn,
            (local_key, default_key),
            self.activity_list_lifetime,
            self.activity_list_length,
            part,
            activity.id,
        )?;

        Ok(())
    }

    fn model_activity_pluck(&self) -> Result<Vec<Activity>, Error> {
        let mut conn = self.client.get_connection()?;
        let default_key = self.keys.default_activity_list_key();
        let mut items: Vec<Vec<Vec<u8>>> =
            redis::transaction(&mut conn, &[&default_key], |conn, pipe| {
                pipe.lrange(&default_key, 0, -1)
                    .del(&default_key)
                    .ignore()
                    .query(conn)
            })?;
        let items = items.pop().unwrap_or_default();
        let mut buf = Vec::with_capacity(items.len());

        for item in items {
            let (part, id) = bincode::deserialize::<(String, Uuid)>(&item)?;
            let key = self.keys.activity_key(&part, id);
            if let Some(activity) = conn.deget::<Activity, _>(key)? {
                buf.push(activity);
            }
        }

        buf.shrink_to_fit();
        Ok(buf)
    }

    fn model_activity_delete_all<'p, Ids>(&self, id: Ids) -> Result<(), Error>
    where
        Ids: IntoIterator<Item = (&'p str, Uuid)>,
    {
        let keys = id
            .into_iter()
            .map(|(p, u)| self.keys.activity_key(p, u))
            .collect::<Vec<_>>();
        self.client.get_connection()?.del(keys)?;
        Ok(())
    }
}

fn push_activity_list(
    conn: &mut Connection,
    keys: (String, String),
    lifetime: u32,
    cap: u32,
    part: &str,
    id: Uuid,
) -> Result<(), Error> {
    let content = bincode::serialize::<(&str, Uuid)>(&(part, id))?;
    redis::transaction(conn, &[&keys.0, &keys.1], |conn, pipe| {
        pipe.lpush(&keys.0, &content[..])
            .ignore()
            .ltrim(&keys.0, 0, cap as isize)
            .ignore()
            .expire(&keys.0, lifetime as usize)
            .ignore()
            .lpush(&keys.1, &content[..])
            .ignore()
            .ltrim(&keys.1, 0, cap as isize)
            .ignore()
            .expire(&keys.1, lifetime as usize)
            .ignore()
            .query(conn)
    })
    .map_err(Error::from)
}
