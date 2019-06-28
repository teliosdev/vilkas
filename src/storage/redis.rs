use crate::storage::items::{Item, ItemList, ItemStorage};
use crate::storage::sealed::Sealed;
use crate::storage::Storage;
use config::Config;
use failure::Error;
use r2d2::{Pool, PooledConnection};
use r2d2_redis::redis::{Commands, PipelineCommands};
use r2d2_redis::RedisConnectionManager;
use uuid::Uuid;
use std::ops::DerefMut;

struct RedisKeys;

impl RedisKeys {
    fn item(id: Uuid) -> String {
        format!("item:{}", id)
    }

    fn item_near(id: Uuid) -> String {
        format!("item:{}:near", id)
    }

    const fn all_items_top() -> &'static str {
        "item:!:top"
    }

    const fn all_items_popular() -> &'static str {
        "item:!:pop"
    }
}

pub struct RedisStorage {
    pool: Pool<RedisConnectionManager>,
    near_modification_max: u32,
    near_list_size_max: u32,
}

impl RedisStorage {
    pub fn load(config: &Config) -> RedisStorage {
        unimplemented!()
    }

    fn calc_near(&self, item_list: &mut ItemList) -> Result<(), Error> {
        if item_list.nmods > self.near_modification_max {
            item_list.items = item_list.items.into_iter().take(self.near_list_size_max as usize).map(|(item, count)| {
                (item, count.ln())
            }).collect();
        }

        Ok(())
    }
}

impl Sealed for RedisStorage {}
impl Storage for RedisStorage {}

impl ItemStorage for RedisStorage {
    fn find_item(&self, item: Uuid) -> Result<Option<Item>, Error> {
        let conn = self.pool.get()?;
        let contents: Option<Vec<u8>> = conn.get(RedisKeys::item(item))?;
        contents
            .map(|content| bincode::deserialize::<Item>(&content))
            .transpose()
            .map_err(Error::from)
    }

    fn find_items_near(&self, item: Uuid) -> Result<Vec<Uuid>, Error> {
        let conn = self.pool.get()?;
        let item_list = load_item_near(conn.get(RedisKeys::item_near(item))?)?;
        Ok(item_list.items.into_iter().map(|(item, _)| item).collect())
    }

    fn items_add_near(&self, current: Uuid, other: Uuid) -> Result<(), Error> {
                let conn = self.pool.get()?;
        let key = RedisKeys::item_near(current);
        loop {
            redis::cmd("WATCH").arg(&key).query(conn)?;
            // TODO
        }
    }

    fn find_items_top(&self) -> Result<Vec<Uuid>, Error> {
        let conn = self.pool.get()?;
        Ok(conn
            .zscan::<_, String>(RedisKeys::all_items_top())?
            .flat_map(|item| item.parse::<Uuid>().ok())
            .collect())
    }

    fn find_items_popular(&self) -> Result<Vec<Uuid>, Error> {
        let conn = self.pool.get()?;
        Ok(conn
            .zscan::<_, String>(RedisKeys::all_items_popular())?
            .flat_map(|item| item.parse::<Uuid>().ok())
            .collect())
    }
}

fn load_item_near(contents: Option<Vec<u8>>) -> Result<ItemList, Error> {
        Ok(contents
            .map(|content| bincode::deserialize::<ItemList>(&content))
            .transpose()?
            .unwrap_or_default())
}

fn inner_add_near(this: &RedisStorage, conn: &mut PooledConnection<RedisConnectionManager>, key: &str, other: Uuid) -> Result<Option<()>, Error> {
    let list = load_item_near(conn.get(key)?)?;
    if let Some((_, count)) = list.items.iter_mut().find(|(id, _)| other == *id) {
        *count += 1.0;
    } else {
        list.items.push((other, 1.0));
    }

    list.nmods += 1;
    this.calc_near(&mut list)?;
    let content = bincode::serialize(&list)?;
    redis::pipe().atomic().set(key, content).query(conn.deref_mut()).map_err(Error::from)
}
