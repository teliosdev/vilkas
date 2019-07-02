use std::collections::HashMap;

use aerospike::errors::{Error as AerospikeError, ErrorKind as AerospikeErrorKind};
use aerospike::{FloatValue, Record, ResultCode, Value};
use failure::Error;
use serde::Deserialize;

pub(super) trait ResultExt<T> {
    fn optional(self) -> Result<Option<T>, AerospikeError>;
}

impl<T> ResultExt<T> for Result<T, AerospikeError> {
    fn optional(self) -> Result<Option<T>, AerospikeError> {
        match self {
            Ok(v) => Ok(Some(v)),
            Err(AerospikeError(
                AerospikeErrorKind::ServerError(ResultCode::KeyNotFoundError),
                _,
            )) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

pub(super) trait ValueExt {
    fn as_str(&self) -> Option<&str>;
    fn as_u64(&self) -> Option<u64>;
    fn as_f64(&self) -> Option<f64>;
    fn as_hash(&self) -> Option<&HashMap<Value, Value>>;
    fn as_list(&self) -> Option<&[Value]>;
    fn as_blob(&self) -> Option<&[u8]>;
}

impl ValueExt for Value {
    fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(ref s) => Some(s),
            _ => None,
        }
    }
    fn as_u64(&self) -> Option<u64> {
        match self {
            Value::UInt(v) => Some(*v),
            Value::Int(i) if *i > 0 => Some(*i as u64),
            _ => None,
        }
    }
    fn as_f64(&self) -> Option<f64> {
        match self {
            // this has to be done because... aerospike?
            Value::Float(FloatValue::F64(v)) => Some(f64::from_bits(*v)),
            Value::Float(FloatValue::F32(v)) => Some(f64::from(f32::from_bits(*v))),
            _ => None,
        }
    }
    fn as_hash(&self) -> Option<&HashMap<Value, Value>> {
        match self {
            Value::HashMap(ref map) => Some(map),
            _ => None,
        }
    }
    fn as_list(&self) -> Option<&[Value]> {
        match self {
            Value::List(list) => Some(&list[..]),
            _ => None,
        }
    }
    fn as_blob(&self) -> Option<&[u8]> {
        match self {
            Value::Blob(v) => Some(&v[..]),
            _ => None,
        }
    }
}

pub(super) trait RecordExt {
    fn deserialize_bin<'s, T: Deserialize<'s>>(&'s self, bin: &str) -> Result<Option<T>, Error>;
}

impl RecordExt for Record {
    fn deserialize_bin<'s, T: Deserialize<'s>>(&'s self, bin: &str) -> Result<Option<T>, Error> {
        self.bins
            .get(bin)
            .and_then(|bin| bin.as_blob())
            .map(|bin| bincode::deserialize(bin))
            .transpose()
            .map_err(Error::from)
    }
}

impl RecordExt for Option<Record> {
    fn deserialize_bin<'s, T: Deserialize<'s>>(&'s self, bin: &str) -> Result<Option<T>, Error> {
        match self {
            Some(r) => r.deserialize_bin(bin),
            None => Ok(None),
        }
    }
}
