use serde::{Deserialize, Serialize};
use tracing::instrument;

use crate::{command::Response, connection::Connection, KvsEngine};

/// Set the value of the key.
#[derive(Debug, Serialize, Deserialize)]
pub struct Set {
    /// Name of the key to Set
    key: String,

    /// Value of the key
    value: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum SetResponse {
    Ok(()),
    Err(String),
}

impl Set {
    /// Create a new `Set` command which fetches the `value` using the `key`.
    pub fn new(key: impl ToString, value: impl ToString) -> Set {
        Set {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    #[instrument(skip(self, engine, connection))]
    pub(crate) async fn apply<E: KvsEngine>(
        self,
        engine: &E,
        connection: &mut Connection,
    ) -> crate::Result<()> {
        let response = match engine.set(self.key, self.value).await {
            Ok(v) => SetResponse::Ok(v),
            Err(e) => match e {
                _ => SetResponse::Err(format!("{}", e)),
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
        connection.write(Response::Set(response)).await?;
        Ok(())
    }
}
