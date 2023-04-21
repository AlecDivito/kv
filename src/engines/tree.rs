use std::{convert::TryInto, sync::mpsc};

use crate::KvsEngine;

use super::UpdateResult;

#[derive(Clone)]
pub struct Tree<Kvs: KvsEngine> {
    name_index: u64,
    name: String,
    inner: Kvs,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Metadata {
    index: u64,
    name: String,
}

impl<Kvs: KvsEngine> Tree<Kvs> {
    pub fn new(inner: Kvs, name: String) -> crate::Result<Self> {
        let schema_name = format!("__schema.{}", name);
        if let Some(schema) = inner.get(schema_name.as_bytes())? {
            let metadata: Metadata = bincode::deserialize(&schema)?;
            Ok(Self {
                name_index: metadata.index,
                name: metadata.name,
                inner,
            })
        } else {
            let index_name = b"__database.schema.index".to_vec();
            let value = inner
                .get(&index_name)?
                .unwrap_or(0_u64.to_be_bytes().to_vec());
            let index = u64::from_be_bytes(value.try_into().unwrap());
            inner.set(index_name, (index + 1).to_be_bytes().to_vec())?;

            let metadata = bincode::serialize(&Metadata {
                index,
                name: name.clone(),
            })?;
            inner.set(schema_name.into_bytes(), metadata)?;

            Ok(Self {
                name_index: index,
                name,
                inner,
            })
        }
    }
}

impl<Kvs: KvsEngine> KvsEngine for Tree<Kvs> {
    fn restore(folder: impl Into<std::path::PathBuf>) -> crate::Result<Self>
    where
        Self: Sized,
    {
        todo!()
    }

    fn set(&self, key: Vec<u8>, value: Vec<u8>) -> crate::Result<()> {
        todo!()
    }

    fn get(&self, key: &[u8]) -> crate::Result<Option<Vec<u8>>> {
        todo!()
    }

    fn remove(&self, key: Vec<u8>) -> crate::Result<()> {
        todo!()
    }

    fn find(&self, pattern: Vec<u8>) -> crate::Result<Vec<Vec<u8>>> {
        todo!()
    }

    fn subscribe(&self, pattern: Vec<u8>) -> crate::Result<mpsc::Receiver<UpdateResult>> {
        todo!()
    }
}
