use super::ext::*;
use super::RedisStorage;
use crate::storage::{Item, ItemList, ItemStore, TimeScope};
use failure::Error;
use redis::{Commands, Connection, PipelineCommands, RedisResult};

use crate::storage::redis::keys::ListKey;
use byteorder::{ByteOrder, LittleEndian};
use uuid::Uuid;

impl ItemStore for RedisStorage {
    fn find_item(&self, part: &str, item: Uuid) -> Result<Option<Item>, Error> {
        let mut conn = self.client.get_connection()?;
        let key = self.keys.item_key(part, item);
        conn.deget(key)
    }

    fn find_items<Items>(&self, part: &str, items: Items) -> Result<Vec<Option<Item>>, Error>
    where
        Items: IntoIterator<Item = Uuid>,
    {
        let mut conn = self.client.get_connection()?;
        let keys = items
            .into_iter()
            .map(|i| self.keys.item_key(part, i))
            .collect::<Vec<_>>();
        let result: Vec<Option<Vec<u8>>> = conn.get::<_, Vec<Option<Vec<u8>>>>(keys)?;
        let mut output = Vec::with_capacity(result.len());
        for item in result {
            output.push(
                item.as_ref()
                    .map(Vec::as_slice)
                    .map(bincode::deserialize)
                    .transpose()?,
            );
        }

        Ok(output)
    }

    fn find_items_near(&self, part: &str, item: Uuid) -> Result<ItemList, Error> {
        build_item_list(
            &mut self.client.get_connection()?,
            &self.keys.item_near_key(part, item),
        )
        .map_err(Error::from)
    }

    fn find_items_top(&self, part: &str, scope: TimeScope) -> Result<ItemList, Error> {
        build_item_list(
            &mut self.client.get_connection()?,
            &self.keys.item_top_key(part, scope),
        )
        .map_err(Error::from)
    }

    fn find_items_popular(&self, part: &str, scope: TimeScope) -> Result<ItemList, Error> {
        build_item_list(
            &mut self.client.get_connection()?,
            &self.keys.item_pop_key(part, scope),
        )
        .map_err(Error::from)
    }

    fn find_items_recent(&self, part: &str) -> Result<ItemList, Error> {
        let mut conn = self.client.get_connection()?;
        let key = self.keys.item_recent_key(part);
        let items: Vec<String> = conn.lrange(&key, 0, -1)?;
        let items = items
            .into_iter()
            .flat_map(|v| v.parse::<Uuid>().ok())
            .map(|v| (v, 1.0))
            .collect::<Vec<_>>();
        Ok(ItemList {
            items,
            nmods: 0,
            epoch: 0,
        })
    }

    fn items_insert(&self, item: &Item) -> Result<(), Error> {
        let mut conn = self.client.get_connection()?;
        let data = bincode::serialize(item)?;
        let item_key = self.keys.item_key(&item.part, item.id);
        let recent_key = self.keys.item_recent_key(&item.part);
        let id_string = item.id.to_string();

        redis::transaction(&mut conn, &[&item_key, &recent_key], |conn, pipe| {
            pipe.set(&item_key, &data[..])
                .ignore()
                .lpush(&recent_key, &id_string)
                .ignore()
                .ltrim(&recent_key, 0, self.recent_list_length as isize)
                .ignore()
                .query(conn)
        })
        .map_err(Error::from)
    }

    fn items_delete(&self, part: &str, id: Uuid) -> Result<(), Error> {
        let mut conn = self.client.get_connection()?;
        let item_key = self.keys.item_key(&part, id);
        conn.del(item_key)?;
        Ok(())
    }

    fn items_add_near(&self, part: &str, item: Uuid, near: Uuid) -> Result<(), Error> {
        let mut conn = self.client.get_connection()?;
        let key = self.keys.item_near_key(part, item);
        let nmods = increment_item_list_map(&mut conn, &key, near, 1.0)?;
        // If the number of modifications is not sufficient, nothing
        // further needs to happen.
        if nmods < self.near_decay.max_modifications {
            return Ok(());
        }
        item_list_decay(&mut conn, &key, |list| self.near_decay.decay(list))
    }

    fn items_add_bulk_near<Inner, Bulk>(&self, part: &str, bulk: Bulk) -> Result<(), Error>
    where
        Inner: IntoIterator<Item = Uuid>,
        Bulk: IntoIterator<Item = (Uuid, Inner)>,
    {
        let mut conn = self.client.get_connection()?;
        for (item, nears) in bulk.into_iter() {
            let key = self.keys.item_near_key(part, item);
            let nmods = increment_item_list_map_bulk(&mut conn, &key, nears.into_iter(), 1.0)?;
            if nmods >= self.near_decay.max_modifications {
                item_list_decay(&mut conn, &key, |list| self.near_decay.decay(list))?;
            }
        }

        Ok(())
    }

    fn items_view(&self, part: &str, item: Uuid, view_cost: f64) -> Result<(), Error> {
        let mut conn = self.client.get_connection()?;
        let top_keys = TimeScope::variants().map(|s| self.keys.item_top_key(part, s));
        let pop_keys = TimeScope::variants().map(|s| self.keys.item_pop_key(part, s));

        for key in top_keys {
            increment_item_list_map(&mut conn, &key, item, 1.0)?;
        }

        for key in pop_keys {
            increment_item_list_map(&mut conn, &key, item, view_cost)?;
        }

        Ok(())
    }

    fn items_list_flush(&self, part: &str) -> Result<(), Error> {
        let mut conn = self.client.get_connection()?;
        let top_keys = TimeScope::variants().map(|s| (self.keys.item_top_key(part, s), s));
        let pop_keys = TimeScope::variants().map(|s| (self.keys.item_pop_key(part, s), s));
        for (key, scope) in top_keys {
            let nmods = conn.get::<_, i64>(key.nmods_key())? as u64;
            if nmods > self.top_decay.max_modifications {
                item_list_decay(&mut conn, &key, |list| self.top_decay.decay(scope, list))?;
            }
        }

        for (key, scope) in pop_keys {
            let nmods = conn.get::<_, i64>(key.nmods_key())? as u64;
            if nmods > self.top_decay.max_modifications {
                item_list_decay(&mut conn, &key, |list| self.pop_decay.decay(scope, list))?;
            }
        }

        Ok(())
    }
}

fn read_epoch(conn: &mut Connection, key: &ListKey<'_>) -> RedisResult<u128> {
    let vec: Option<Vec<u8>> = conn.get::<_, Option<Vec<u8>>>(key.epoch_key())?;
    Ok(vec
        .as_ref()
        .map(Vec::as_slice)
        .map(LittleEndian::read_u128)
        .unwrap_or(0))
}

fn build_item_list(conn: &mut Connection, key: &ListKey<'_>) -> RedisResult<ItemList> {
    let list = conn
        .zscan(key.list_key())?
        .flat_map(|(key, value): (String, f64)| key.parse::<Uuid>().ok().map(|k| (k, value)))
        .collect::<Vec<_>>();
    let nmods = conn.get::<_, i64>(key.nmods_key())? as u64;
    let epoch = read_epoch(conn, key)?;

    Ok(ItemList {
        items: list,
        nmods,
        epoch,
    })
}

fn increment_item_list_map_bulk<Ids>(
    client: &mut Connection,
    key: &ListKey<'_>,
    ids: Ids,
    by: f64,
) -> Result<u64, Error>
where
    Ids: Iterator<Item = Uuid>,
{
    let mut pipe = redis::pipe();
    let mut incby = 1u64;
    for id in ids {
        pipe.zincr(key.list_key(), id.to_string(), by).ignore();
        incby += 1;
    }
    pipe.incr(key.nmods_key(), incby as i64)
        .query(client)
        .map_err(Error::from)
}

#[inline]
fn increment_item_list_map(
    conn: &mut Connection,
    key: &ListKey<'_>,
    id: Uuid,
    by: f64,
) -> Result<u64, Error> {
    increment_item_list_map_bulk(conn, key, std::iter::once(id), by)
}

fn item_list_decay<F>(conn: &mut Connection, key: &ListKey<'_>, decay: F) -> Result<(), Error>
where
    F: Fn(&mut ItemList),
{
    let keys = [key.list_key(), key.nmods_key()];
    redis::transaction(conn, &keys, |conn, pipe| {
        let mut list = build_item_list(conn, key)?;
        decay(&mut list);
        pipe.del(&keys).ignore();
        let lkey = key.list_key();
        for (member, score) in list.items {
            pipe.zadd(&lkey, member.to_string(), score).ignore();
        }
        let mut epoch = [0u8; 16];
        LittleEndian::write_u128(&mut epoch, list.epoch);

        pipe.set(key.nmods_key(), list.nmods as i64)
            .ignore()
            .set(key.epoch_key(), &epoch)
            .ignore()
            .query(conn)
    })
    .map_err(Error::from)
}
