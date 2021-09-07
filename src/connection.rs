use std::io::Cursor;

use serde_json::error::Category;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    net::TcpStream,
};

use crate::{
    command::{GetResponse, RemoveResponse, Request, Response, SetResponse},
    KvError,
};

/// Send and receive values from a remote peer.
#[derive(Debug)]
pub struct Connection {
    // The `TcpStream` is decorated with `BufWriter` which allows
    // for a buffer to be filled before sending out a request. Overall
    // it is more effecient then writing to the socket all of the time.
    stream: BufWriter<TcpStream>,

    // The buffer where data is read into
    buffer: Vec<u8>,
}

impl Connection {
    /// Create a new `Connection`
    pub fn new(socket: TcpStream) -> Connection {
        Connection {
            stream: BufWriter::new(socket),
            // by default, our connections will have a 4KB buffer which will
            // read in data from the socket.
            buffer: Vec::with_capacity(4 * 1024),
        }
    }

    pub async fn read(&mut self) -> crate::Result<Option<Request>> {
        loop {
            // Attempt to parse the buffer to retrieve the request.
            // If enough data has been buffered, the request is returned
            if let Some(request) = self.parse_request()? {
                return Ok(Some(request));
            }

            // The buffer is still too empty to parse, read more data from the
            // socket.
            //
            // `0` indicated the end of the stream.
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err(KvError::Connection("Connection was reset by peer".into()));
                }
            }
        }
    }

    /// Tries to parse the buffer. If the buffer contains enough data. If
    /// there is enough data, that is removed from the buffer.
    fn parse_request(&mut self) -> crate::Result<Option<Request>> {
        // create a cursor which will access our buffer.
        let mut buf = Cursor::new(&self.buffer[..]);

        match serde_json::from_reader(&mut buf) {
            Ok(request) => Ok(Some(request)),
            Err(e) => match e.classify() {
                Category::Io => Err(KvError::Json(e)),
                Category::Syntax => Ok(None),
                Category::Data => Err(KvError::Json(e)),
                Category::Eof => Ok(None),
            },
        }
    }

    pub async fn write(&mut self, response: Response) -> crate::Result<()> {
        let src = match response {
            Response::Get(get) => serde_json::to_vec(&match get {
                GetResponse::Ok(v) => GetResponse::Ok(v),
                GetResponse::Err(e) => GetResponse::Err(format!("{}", e)),
            }),
            Response::Set(set) => serde_json::to_vec(&match set {
                SetResponse::Ok(_) => SetResponse::Ok(()),
                SetResponse::Err(e) => SetResponse::Err(format!("{}", e)),
            }),
            Response::Remove(rm) => serde_json::to_vec(&match rm {
                RemoveResponse::Ok(_) => RemoveResponse::Ok(()),
                RemoveResponse::Err(e) => RemoveResponse::Err(format!("{}", e)),
            }),
        }?;
        self.stream.write_all(&src).await?;
        Ok(self.stream.flush().await?)
    }
}
