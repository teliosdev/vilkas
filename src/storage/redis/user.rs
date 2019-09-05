//use super::ext::RecordExt;
//use super::SpikeStorage;
//use crate::storage::spike::read_modify_write;
//use crate::storage::UserData;
//use crate::storage::UserStorage;
//use aerospike::Bin;
//use failure::Error;
//use uuid::Uuid;
use super::RedisStorage;
use crate::storage::{UserData, UserStore};
use failure::Error;
use redis::{Commands, PipelineCommands};
use uuid::Uuid;

impl UserStore for RedisStorage {
    fn find_user(&self, part: &str, id: &str) -> Result<UserData, Error> {
        let mut conn = self.client.get_connection()?;
        let key = self.keys.user_key(part, id);
        let list: Vec<String> = conn.lrange::<_, Vec<String>>(&key, 0, -1)?;
        let list = list
            .into_iter()
            .flat_map(|i| i.parse::<Uuid>().ok())
            .collect::<Vec<_>>();

        Ok(UserData {
            id: id.to_string(),
            history: list,
        })
    }

    fn user_push_history(&self, part: &str, id: &str, history: Uuid) -> Result<(), Error> {
        let mut conn = self.client.get_connection()?;
        let key = self.keys.user_key(part, id);
        redis::transaction(&mut conn, &[&key], |conn, pipe| {
            pipe.lpush(&key, history.to_string())
                .ignore()
                .ltrim(&key, 0, self.user_history_length as isize)
                .ignore()
                .query(conn)
        })
        .map_err(Error::from)
    }
}
