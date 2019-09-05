use failure::Error;
use redis::ToRedisArgs;
use serde::de::DeserializeOwned;
use serde::Serialize;

pub trait CommandsExt {
    fn deget<T: DeserializeOwned, K>(&mut self, key: K) -> Result<Option<T>, Error>
    where
        K: ToRedisArgs;
    fn serput<T: Serialize, K>(&mut self, key: K, data: &T) -> Result<(), Error>
    where
        K: ToRedisArgs;
}

impl<C: redis::Commands> CommandsExt for C {
    fn deget<T: DeserializeOwned, K>(&mut self, key: K) -> Result<Option<T>, Error>
    where
        K: ToRedisArgs,
    {
        self.get::<_, Option<Vec<u8>>>(key)?
            .as_ref()
            .map(Vec::as_slice)
            .map(bincode::deserialize)
            .transpose()
            .map_err(Error::from)
    }

    fn serput<T: Serialize, K>(&mut self, key: K, data: &T) -> Result<(), Error>
    where
        K: ToRedisArgs,
    {
        let data = bincode::serialize(data)?;
        let _: () = self.set(key, data)?;
        Ok(())
    }
}
