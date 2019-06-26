use crate::storage::items::{Item, ItemStorage};
use crate::storage::sealed::Sealed;
use crate::storage::Storage;
use config::Config;
use failure::Error;
use r2d2::Pool;
use r2d2_redis::redis::Commands;
use r2d2_redis::RedisConnectionManager;
use uuid::Uuid;

pub struct RedisStorage(Pool<RedisConnectionManager>);

impl RedisStorage {
    pub fn load(config: &Config) -> RedisStorage {
        unimplemented!()
    }
}

impl Sealed for RedisStorage {}
impl Storage for RedisStorage {}

impl ItemStorage for RedisStorage {
    fn find_item(&self, item: Uuid) -> Result<Option<Item>, Error> {
        let conn = self.0.get()?;
        let contents: Option<Vec<u8>> = conn.get(format!("item:{}", item))?;
        contents
            .map(|content| bincode::deserialize::<Item>(&content))
            .transpose()
            .map_err(Error::from)
    }

    fn find_items_near(&self, item: Uuid) -> Result<Vec<Uuid>, Error> {
        let conn = self.0.get()?;
        Ok(conn
            .zscan::<_, String>("item:{}:near")?
            .flat_map(|item| Uuid::from_str(&item).ok()))
    }

    fn add_near(&self, current: Uuid, other: Uuid) -> Result<(), Error> {
        let conn = self.0.get()?;

    }

    fn find_items_top(&self) -> Result<Vec<Uuid>, Error> {
        let conn = self.0.get()?;
        Ok(conn
            .zscan::<_, String>("item:!:top")?
            .flat_map(|item| Uuid::from_str(&item).ok()))
    }

    fn find_items_popular(&self) -> Result<Vec<Uuid>, Error> {
        let conn = self.0.get()?;
        Ok(conn
            .zscan::<_, String>("item:!:pop")?
            .flat_map(|item| Uuid::from_str(&item).ok()))
    }
}

fn calc_near(conn: impl Commands, )
