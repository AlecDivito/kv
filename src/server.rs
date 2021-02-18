use std::{
    io::{BufReader, BufWriter, Write},
    net::{TcpListener, TcpStream, ToSocketAddrs},
};

use serde_json::Deserializer;

use crate::error::Result;
use crate::{
    common::{GetResponse, RemoveResponse, Request, SetResponse},
    KvsEngine,
};

/// Wrapper class to hold the current context of the key value server
pub struct KvServer<E: KvsEngine> {
    engine: E,
}

impl<E: KvsEngine> KvServer<E> {
    /// Create a `KvServer` with a given storage engine
    pub fn new(engine: E) -> Self {
        KvServer { engine }
    }

    /// Run the server listening on the given address
    pub fn run<A: ToSocketAddrs>(mut self, addr: A) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    if let Err(e) = self.serve(stream) {
                        error!("Error on serving client: {}", e);
                    }
                }
                Err(e) => error!("Connection failed: {}", e),
            }
        }
        Ok(())
    }

    fn serve(&mut self, tcp: TcpStream) -> Result<()> {
        let peer_addr = tcp.peer_addr()?;
        let reader = BufReader::new(&tcp);
        let mut writer = BufWriter::new(&tcp);
        let req_reader = Deserializer::from_reader(reader).into_iter::<Request>();

        macro_rules! send_response {
            ($resp:expr) => {{
                let response = $resp;
                serde_json::to_writer(&mut writer, &response)?;
                writer.flush()?;
                debug!("Response sent to {}: {:?}", peer_addr, response);
            };};
        }

        for req in req_reader {
            let req = req?;
            debug!("Receive request from {}: {:?}", peer_addr, req);
            match req {
                Request::Get { key } => send_response!(match self.engine.get(key) {
                    Ok(v) => GetResponse::Ok(v),
                    Err(e) => GetResponse::Err(format!("{}", e)),
                }),
                Request::Set { key, value } => send_response!(match self.engine.set(key, value) {
                    Ok(_) => SetResponse::Ok(()),
                    Err(e) => SetResponse::Err(format!("{}", e)),
                }),
                Request::Remove { key } => send_response!(match self.engine.remove(key) {
                    Ok(_) => RemoveResponse::Ok(()),
                    Err(e) => RemoveResponse::Err(format!("{}", e)),
                }),
            }
        }

        Ok(())
    }
}
