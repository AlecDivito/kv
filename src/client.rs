use crate::command::get::Get;
use crate::command::remove::Remove;
use crate::command::set::Set;
use crate::command::{GetResponse, RemoveResponse, Request, SetResponse};
use crate::{KvError, Result};
use serde::Deserialize;
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
        serde_json::to_writer(&mut self.writer, &Request::Get(Get::new(key)))?;
        self.writer.flush()?;
        let response = GetResponse::deserialize(&mut self.reader)?;
        match response {
            GetResponse::Ok(value) => Ok(value),
            GetResponse::Err(msg) => Err(KvError::StringError(msg.into())),
        }
    }

    /// Set the value of a string key in the server.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        serde_json::to_writer(&mut self.writer, &Request::Set(Set::new(key, value)))?;
        self.writer.flush()?;
        let response = SetResponse::deserialize(&mut self.reader)?;
        match response {
            SetResponse::Ok(_) => Ok(()),
            SetResponse::Err(msg) => Err(KvError::StringError(msg.into())),
        }
    }

    /// Remove a value from the key value store
    pub fn remove(&mut self, key: String) -> Result<()> {
        serde_json::to_writer(&mut self.writer, &Request::Remove(Remove::new(key)))?;
        self.writer.flush()?;
        let resp = RemoveResponse::deserialize(&mut self.reader)?;
        match resp {
            RemoveResponse::Ok(_) => Ok(()),
            RemoveResponse::Err(msg) => Err(KvError::StringError(msg.into())),
        }
    }
}
