use crate::command::get::Get;
use crate::command::remove::Remove;
use crate::command::set::Set;
use crate::command::{GetResponse, RemoveResponse, Request, Response, SetResponse};
use crate::connection::Connection;
use crate::{KvError, Result};
use tokio::net::TcpStream;

/// Key value store client
pub struct KvClient {
    connection: Connection,
    flush_after_request: bool,
}

impl KvClient {
    /// Connect to `addr` to access `KvsServer`
    pub async fn connect(addr: String) -> Result<Self> {
        let socket = TcpStream::connect(addr).await?;
        let connection = Connection::new(socket);
        Ok(KvClient {
            connection,
            flush_after_request: true,
        })
    }

    /// Set the option if the client should flush their request to the server
    /// after sending the initial data. This is mainly used for testing the
    /// server.
    pub fn flush_after_request(&mut self, status: bool) {
        self.flush_after_request = status;
    }

    /// Get the value of a given key from the server.
    pub async fn get(&mut self, key: String) -> Result<Option<String>> {
        self.connection.send(Request::Get(Get::new(key))).await?;
        if self.flush_after_request {
            self.connection.flush().await?;
        }
        let response = self.connection.recieve().await?;
        if let Some(response) = response {
            if let Response::Get(get) = response {
                match get {
                    GetResponse::Ok(result) => return Ok(result),
                    GetResponse::Err(e) => return Err(KvError::StringError(e.into())),
                }
            }
        }
        self.connection.close().await?;
        Ok(None)
    }

    /// Set the value of a string key in the server.
    pub async fn set(&mut self, key: String, value: String) -> Result<()> {
        self.connection
            .send(Request::Set(Set::new(key, value)))
            .await?;
        if self.flush_after_request {
            self.connection.flush().await?;
        }
        let response = self.connection.recieve().await?;
        if let Some(response) = response {
            if let Response::Set(set) = response {
                match set {
                    SetResponse::Ok(result) => return Ok(result),
                    SetResponse::Err(e) => return Err(KvError::StringError(e.into())),
                }
            }
        }
        self.connection.close().await?;
        Ok(())
    }

    /// Remove a value from the key value store
    pub async fn remove(&mut self, key: String) -> Result<()> {
        self.connection
            .send(Request::Remove(Remove::new(key)))
            .await?;
        if self.flush_after_request {
            self.connection.flush().await?;
        }
        let response = self.connection.recieve().await?;
        if let Some(response) = response {
            if let Response::Remove(remove) = response {
                match remove {
                    RemoveResponse::Ok(result) => return Ok(result),
                    RemoveResponse::Err(e) => return Err(KvError::StringError(e.into())),
                }
            }
        }
        self.connection.close().await?;
        Ok(())
    }

    /// Test the api by sending large continous requests
    pub async fn test(&mut self, operation: &str, amount: usize) -> Result<()> {
        // self.flush_after_request(false);
        match operation {
            "get" => {
                for number in 0..amount {
                    if number == amount - 1 {
                        self.flush_after_request(true);
                    }
                    let key = format!("Key{}", number);
                    if let Some(value) = self.get(key.clone()).await? {
                        println!("{}: {} = {}", number, key, value);
                    } else {
                        println!("{}: {} could not be found", number, key);
                    }
                }
            }
            "set" => {
                for number in 0..amount {
                    if number == amount - 1 {
                        self.flush_after_request(true);
                    }
                    let key = format!("Key{}", number);
                    let value = format!("Value{}", number);
                    // println!("{}: Set {} and {}", number, key, value);
                    self.set(key, value).await?;
                }
            }
            "rm" => {
                for number in 0..amount {
                    if number == amount - 1 {
                        self.flush_after_request(true);
                    }
                    let key = format!("Key{}", number);
                    println!("{}: Removed {}", number, key);
                    self.remove(key).await?;
                }
            }
            _ => {
                println!("This shouldn't execte. Exitting...");
                std::process::exit(1);
            }
        }
        Ok(())
    }
}
