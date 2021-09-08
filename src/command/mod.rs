use serde::{Deserialize, Serialize};

use crate::{connection::Connection, shutdown::Shutdown, KvsEngine};

use self::{get::Get, remove::Remove, set::Set};

pub mod get;
pub mod remove;
pub mod set;

pub use self::{get::GetResponse, remove::RemoveResponse, set::SetResponse};

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    Get(Get),
    Set(Set),
    Remove(Remove),
}

impl Request {
    /// Apply the command to the specificed kvs engine instance.
    ///
    /// The response is then written to the connection
    pub(crate) async fn apply<E: KvsEngine>(
        self,
        engine: &E,
        connection: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> crate::Result<()> {
        match self {
            Request::Get(cmd) => cmd.apply(engine, connection).await,
            Request::Set(cmd) => cmd.apply(engine, connection).await,
            Request::Remove(cmd) => cmd.apply(engine, connection).await,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Get(GetResponse),
    Set(SetResponse),
    Remove(RemoveResponse),
}
