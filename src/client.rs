use crate::common::{GetResponse, RemoveResponse, Request, SetResponse};
use crate::{KvsError, Result};
use serde::Deserialize;
use serde_json::de::{Deserializer, IoRead};
use std::io::{BufReader, BufWriter, Write};
use std::net::{TcpStream, ToSocketAddrs};

/// Key value store client
pub struct KvsClient {
    reader: Deserializer<IoRead<BufReader<TcpStream>>>,
    writer: BufWriter<TcpStream>,
}

impl KvsClient {
    /// Connect to `addr` to access `KvsServer`
    pub fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        let tcp_reader = TcpStream::connect(addr)?;
        let tcp_writer = tcp_reader.try_clone()?;
        Ok(KvsClient {
            reader: Deserialize::from_reader(BufReader::new(tcp_reader)),
            writer: BufWriter::new(tcp_writer),
        })
    }

    /// Get the value of a given key from the server.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let buffer = bincode::serialize(&Request::Get { key })?;
        self.writer.write(&buffer)?;
        self.writer.flush()?;
        let response = GetResponse::deserialize(&mut self.reader)?;
        match response {
            GetResponse::Ok(value) => Ok(value),
            GetResponse::Err(msg) => Err(KvError::StringError(msg)),
        }
    }

    /// Set the value of a string key in the server.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {}
}
