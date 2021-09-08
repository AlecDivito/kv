use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::{command::Response, connection::Connection, KvsEngine};

/// Get the value of the key.
#[derive(Debug, Serialize, Deserialize)]
pub struct Get {
    /// Name of the key to get
    key: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum GetResponse {
    Ok(Option<String>),
    Err(String),
}

impl Get {
    /// Create a new `Get` command which fetches the `value` using the `key`.
    pub fn new(key: impl ToString) -> Get {
        Get {
            key: key.to_string(),
        }
    }

    #[instrument(skip(self, engine, connection))]
    pub(crate) async fn apply<E: KvsEngine>(
        self,
        engine: &E,
        connection: &mut Connection,
    ) -> crate::Result<()> {
        let response = match engine.get(self.key).await {
            Ok(v) => GetResponse::Ok(v),
            Err(e) => match e {
                _ => GetResponse::Err(format!("{}", e)),
                // crate::KvError::Io(_) => todo!(),
                // crate::KvError::Serialize(_) => todo!(),
                // crate::KvError::Json(_) => todo!(),
                // crate::KvError::KeyNotFound(_) => todo!(),
                // crate::KvError::UnexpectedCommandType(_) => todo!(),
                // crate::KvError::Parse(_) => todo!(),
                // crate::KvError::Utf8(_) => todo!(),
                // crate::KvError::Compact(_) => todo!(),
                // crate::KvError::Sled(_) => todo!(),
                // crate::KvError::Lock(_) => todo!(),
                // crate::KvError::StringError(_) => todo!(),
                // crate::KvError::Connection(_) => todo!(),
            },
        };
        debug!(?response);
        connection.write(Response::Get(response)).await?;
        Ok(())
    }
}
