use failure::Error;
use lmdb::{Database, Transaction};
use serde::Deserialize;

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
