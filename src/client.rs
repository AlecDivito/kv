use crate::common::{FindResponse, GetResponse, RemoveResponse, Request, SetResponse};
use crate::{KvError, Result};
use serde_json::de::IoRead;
use serde_json::Deserializer;
use std::io::{BufReader, BufWriter, Write};
use std::net::{TcpStream, ToSocketAddrs};

/// Key value store client
pub struct KvClient {
    reader: Deserializer<IoRead<BufReader<TcpStream>>>,
    writer: BufWriter<TcpStream>,
}

impl KvClient {
    /// Connect to `addr` to access `KvsServer`
    pub fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        let tcp_reader = TcpStream::connect(addr)?;
        let tcp_writer = tcp_reader.try_clone()?;
        Ok(KvClient {
            reader: Deserializer::from_reader(BufReader::new(tcp_reader)),
            writer: BufWriter::new(tcp_writer),
        })
    }

    /// Get the value of a given key from the server.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.write(&Request::Get { key })? {
            GetResponse::Ok(value) => Ok(value),
            GetResponse::Err(msg) => Err(KvError::StringError(msg.into())),
        }
    }

    /// Set the value of a string key in the server.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        match self.write(&Request::Set { key, value })? {
            SetResponse::Ok(_) => Ok(()),
            SetResponse::Err(msg) => Err(KvError::StringError(msg.into())),
        }
    }

    /// Find a list of keys given a pattern from the server.
    pub fn find(&mut self, pattern: String) -> Result<Vec<String>> {
        match self.write(&Request::Find { pattern })? {
            FindResponse::Ok(mut list) => Ok(list
                .drain(..)
                .map(|b| {
                    String::from_utf8(b).unwrap_or_else(|err| format!("<from_utf8_error> {}", err))
                })
                .collect::<Vec<_>>()),
            FindResponse::Err(err) => Err(KvError::StringError(err.into())),
        }
    }

    /// Remove a value from the key value store
    pub fn remove(&mut self, key: String) -> Result<()> {
        match self.write(&Request::Remove { key })? {
            RemoveResponse::Ok(_) => Ok(()),
            RemoveResponse::Err(msg) => Err(KvError::StringError(msg.into())),
        }
    }

    fn write<T, R>(&mut self, t: &T) -> Result<R>
    where
        T: ?Sized + serde::Serialize,
        R: serde::de::DeserializeOwned,
    {
        serde_json::to_writer(&mut self.writer, &t)?;
        self.writer.flush()?;
        let resp = R::deserialize(&mut self.reader)?;
        Ok(resp)
    }
}
