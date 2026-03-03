use crate::Entry;
use chrono::{DateTime, Utc};
use image_cache::image_type::ImageType;
use rocksdb::{DB, IteratorMode, Options};
use std::borrow::Cow;
use std::path::Path;
use std::sync::Arc;

type DefaultConfig =
    bincode::config::Configuration<bincode::config::BigEndian, bincode::config::Fixint>;

const ERROR_DIGEST: [u8; 16] = [0; 16];

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("RocksDB error")]
    Db(#[from] rocksdb::Error),
    #[error("Decoding error")]
    Decode(#[from] bincode::error::DecodeError),
    #[error("Encoding error")]
    Encode(#[from] bincode::error::EncodeError),
    #[error("Invalid key")]
    InvalidKeyBytes(Vec<u8>),
    #[error("Extra key bytes")]
    ExtraKeyBytes(Vec<u8>),
    #[error("Extra value bytes")]
    ExtraValueBytes(Vec<u8>),
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Key<'a> {
    pub url: Cow<'a, str>,
    pub timestamp: DateTime<Utc>,
}

impl Key<'_> {
    /// Decode a key from the given bytes.
    ///
    /// The representation is a null-terminated string for the URL, followed by four big-endian
    /// bytes for the epoch second timestamp.
    fn from_bytes<B: AsRef<[u8]>>(bytes: B) -> Result<Self, Error> {
        let bytes = bytes.as_ref();

        if bytes.len() < 5 {
            Err(Error::InvalidKeyBytes(bytes.to_vec()))
        } else {
            let timestamp_s = u32::from_be_bytes(
                bytes[bytes.len() - 4..bytes.len()]
                    .try_into()
                    .map_err(|_| Error::InvalidKeyBytes(bytes.to_vec()))?,
            );

            let timestamp = DateTime::from_timestamp(timestamp_s.into(), 0)
                .ok_or_else(|| Error::InvalidKeyBytes(bytes.to_vec()))?;

            let url = std::str::from_utf8(&bytes[0..bytes.len() - 5])
                .map_err(|_| Error::InvalidKeyBytes(bytes.to_vec()))?;

            Ok(Key {
                url: url.to_string().into(),
                timestamp,
            })
        }
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.url.len() + 5);

        // This should always fit, but just in case.
        let timestamp_s = u32::try_from(self.timestamp.timestamp()).unwrap_or(u32::MAX);

        bytes.extend_from_slice(self.url.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(&timestamp_s.to_be_bytes());

        bytes
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, bincode::BorrowDecode, bincode::Encode)]
struct Value {
    pub digest: [u8; 16],
    pub image_type: ImageType,
}

#[derive(Clone)]
pub struct Database<C = DefaultConfig> {
    db: Arc<DB>,
    config: C,
}

impl Database<DefaultConfig> {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.set_compression_type(rocksdb::DBCompressionType::Zstd);

        let db = DB::open(&options, path)?;
        let config = bincode::config::standard();

        Ok(Self {
            db: Arc::new(db),
            config: config.with_big_endian().with_fixed_int_encoding(),
        })
    }

    pub fn lookup(&self, url: &str) -> Result<Vec<Result<Entry, DateTime<Utc>>>, Error> {
        let mut entries = vec![];

        for result in self.db.iterator(IteratorMode::From(
            url.as_bytes(),
            rocksdb::Direction::Forward,
        )) {
            let (key_bytes, value_bytes) = result?;

            let key = Key::from_bytes(&key_bytes)?;

            if key.url != url {
                break;
            }

            let (value, value_read) =
                bincode::borrow_decode_from_slice::<Value, _>(&value_bytes, self.config)?;

            if value_read == value_bytes.len() {
                match value.image_type.value() {
                    Some(image_type) => {
                        entries.push(Ok(Entry {
                            timestamp: key.timestamp,
                            digest: md5::Digest(value.digest),
                            image_type,
                        }));
                    }
                    None => {
                        entries.push(Err(key.timestamp));
                    }
                }

                Ok(())
            } else {
                Err(Error::ExtraValueBytes(value_bytes.to_vec()))
            }?;
        }

        entries.sort_by_key(|result| {
            std::cmp::Reverse(match result {
                Ok(entry) => entry.timestamp,
                Err(timestamp) => *timestamp,
            })
        });

        Ok(entries)
    }

    pub fn add(&self, url: &str, entry: Entry) -> Result<(), Error> {
        let key = Key {
            url: url.into(),
            timestamp: entry.timestamp,
        };

        let value = Value {
            digest: entry.digest.0,
            image_type: entry.image_type.into(),
        };

        let key_bytes = key.to_bytes();
        let value_bytes = bincode::encode_to_vec(value, self.config)?;

        Ok(self.db.put(&key_bytes, &value_bytes)?)
    }

    pub fn add_failed(&self, url: &str, timestamp: DateTime<Utc>) -> Result<(), Error> {
        let key = Key {
            url: url.into(),
            timestamp,
        };

        let value = Value {
            digest: ERROR_DIGEST,
            image_type: ImageType::empty(),
        };

        let key_bytes = key.to_bytes();
        let value_bytes = bincode::encode_to_vec(value, self.config)?;

        Ok(self.db.put(&key_bytes, &value_bytes)?)
    }

    pub fn iter(
        &self,
    ) -> impl Iterator<Item = Result<(String, Result<Entry, DateTime<Utc>>), Error>> {
        self.db.iterator(IteratorMode::Start).map(|result| {
            let (key_bytes, value_bytes) = result?;

            let key = Key::from_bytes(&key_bytes)?;
            let (value, value_read) =
                bincode::borrow_decode_from_slice::<Value, _>(&value_bytes, self.config)?;

            if value_read == value_bytes.len() {
                Ok((
                    key.url.to_string(),
                    match value.image_type.value() {
                        Some(image_type) => Ok(Entry {
                            timestamp: key.timestamp,
                            digest: md5::Digest(value.digest),
                            image_type,
                        }),
                        None => Err(key.timestamp),
                    },
                ))
            } else {
                Err(Error::ExtraValueBytes(value_bytes.to_vec()))
            }
        })
    }
}
