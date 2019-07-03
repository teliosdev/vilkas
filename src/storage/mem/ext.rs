use failure::Error;
use lmdb::{Database, RwTransaction, Transaction};
use serde::{Deserialize, Serialize};

pub trait ResultExt<T, E> {
    fn optional(self) -> Result<Option<T>, E>;
}

impl<T> ResultExt<T, lmdb::Error> for Result<T, lmdb::Error> {
    fn optional(self) -> Result<Option<T>, lmdb::Error> {
        match self {
            Ok(v) => Ok(Some(v)),
            Err(lmdb::Error::NotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

pub trait TransactionExt {
    fn deget<'s, T: Deserialize<'s>, K>(&'s self, db: Database, key: K) -> Result<Option<T>, Error>
    where
        K: AsRef<[u8]>;
}

pub trait WriteTransactionExt {
    fn serput<T: Serialize, K>(&mut self, db: Database, key: K, data: &T) -> Result<(), Error>
    where
        K: AsRef<[u8]>;
}

impl<T> TransactionExt for T
where
    T: Transaction,
{
    fn deget<'s, D: Deserialize<'s>, K>(&'s self, db: Database, key: K) -> Result<Option<D>, Error>
    where
        K: AsRef<[u8]>,
    {
        self.get(db, &key)
            .optional()?
            .map(bincode::deserialize)
            .transpose()
            .map_err(Error::from)
    }
}
impl WriteTransactionExt for RwTransaction<'_> {
    fn serput<T: Serialize, K>(&mut self, db: Database, key: K, data: &T) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
    {
        let size = bincode::serialized_size(data)? as usize;
        let buffer = self.reserve(db, &key, size, Default::default())?;
        let writer = std::io::Cursor::new(buffer);
        bincode::serialize_into(writer, data)?;
        Ok(())
    }
}
