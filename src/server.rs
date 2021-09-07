use std::{future::Future, sync::Arc, time::Duration};

use tokio::{
    net::{TcpListener, TcpStream},
    sync::{broadcast, mpsc, Semaphore},
};
use tracing::instrument;

use crate::{connection::Connection, shutdown::Shutdown, KvsEngine};

/// Server listerner state. This is created in the `run` call. It includes a
/// `run` method which will perform TCP listening and initialize network state.
#[derive(Debug)]
struct Listener<E: KvsEngine + 'static> {
    /// Shared Key Value Server handle.
    ///
    /// Contains Key Value state as well as broadcast channels for followers
    ///
    /// This is a wrapper around `Arc`. It enableds `KvServer` to be cloned and
    /// passed into the per connection state (`Handler`).
    engine: E,

    /// TCP listener supplied by the `run` caller.
    listener: TcpListener,

    /// Limit the max number of connections
    limit_connections: Arc<Semaphore>,

    /// Broadcast a shutdown signal to all active connections
    notify_shutdown: broadcast::Sender<()>,

    /// Used as part of the graceful shutdown process to wait for client
    /// connections to complete processing
    shutdown_complete_rx: mpsc::Receiver<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
}

const MAX_CONNECTIONS: usize = 250;

/// Run the kv server
///
/// Accepts a connection suppiled by a listener. For each inbound connection, a
/// task is spawned to handle the connection. It will gracefully shutdown once
/// it has been completed.
pub async fn run<E: KvsEngine + 'static>(
    engine: E,
    listener: TcpListener,
    shutdown: impl Future,
) -> crate::Result<()> {
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);

    // Initialize the kv listener state
    let mut server = Listener {
        engine,
        listener,
        limit_connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        notify_shutdown,
        shutdown_complete_tx,
        shutdown_complete_rx,
    };

    // Concurrently run the server and listen for the `shutdown` signal. The
    // server task runs until an error is encountered, so normally, this
    // `select!` statement runs until the `shutdown` signal is recieved.
    //
    // The `select!` macro is a foundational building block for writing
    // asynchronous Rust. See the API docs for more details:
    //
    // https://docs.rs/tokio/*/tokio/macro.select.html
    tokio::select! {
        res = server.run() => {
            // if error received here, accepting TCP connection failed multiple
            // times and the server is shutting down
            //
            // Errors encountered handling the individual connection do not
            // bubble up to this point.
            if let Err(err) = res {
                error!(cause = %err, "failed to accept conneciton");
            }
        }
        _ = shutdown => {
            // the shutdown signal has been received.
            info!("shutting down")
        }
    }

    // Extract `shutdown_copmlete` receeiver and transmitter and explicitly drop
    // them. This is important as the `.await` will never complete
    let Listener {
        mut shutdown_complete_rx,
        shutdown_complete_tx,
        notify_shutdown,
        ..
    } = server;

    // When `notify_shutdown` is dropped, all tasks which have `subscribe`d will
    // receive the shutdown signal and can exit.
    drop(notify_shutdown);
    // Drop final `Sender` so the `Receiver` can complete.
    drop(shutdown_complete_tx);

    // Wait for all active connections to finish processing. As the `Sender`
    // handle held by the listener has been dropped above, the only remaining
    // `Sender` instances are held by connection handler tasks. When those drop,
    // the `mpsc` channel will close and `recv()` will return `None`.
    let _ = shutdown_complete_rx.recv().await;

    Ok(())
}

impl<E: KvsEngine> Listener<E> {
    /// Run the server
    ///
    /// Listen for inbound connections, for each, spawn a task
    ///
    /// # Errors
    ///
    /// Returns `Err` if accepting returns an error.
    async fn run(&mut self) -> crate::Result<()> {
        info!("accepting inbound connections");

        loop {
            // Wait for a permit to become available
            //
            // `acquire` receives a permit that is bound via a lifetime to the
            // semaphore. Once dropped, it is returned to the semaphore.
            // However, in this case, the permit must be returned in a different
            // task than it was acquired (in the handler task). For this, we
            // "forget" the permit which drops the permit value **without**
            // incrementing the semaphore's permit. In the handler task,
            // we manually add a new permit when processing is complete.
            //
            // `acquire()` returns `Err` when the semaphore has been closed. We
            // don't ever close the sempahore, so `unwrap()` is safe here.
            self.limit_connections.acquire().await.unwrap().forget();

            // Accept a new socket.
            let socket = self.accept().await?;

            // Create a handler for the incoming connection
            let mut handler = Handler {
                // Get a handle to the key value store
                engine: self.engine.clone(),
                connection: Connection::new(socket),
                limit_connections: self.limit_connections.clone(),
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                _shutdown_complete: self.shutdown_complete_tx.clone(),
            };

            tokio::spawn(async move {
                if let Err(err) = handler.run().await {
                    error!(cause = ?err, "connection error");
                }
            });
        }
    }

    /// Accept an inbound connection
    ///
    /// Errors are handled by backing off and retrying. An exponential backoff
    /// strategy is used. After the first failure, the task waits for 200ms.
    /// After the second, it waits 400ms and so on. If accepting failed 6 times
    /// in a row, the connection is dropped.
    async fn accept(&mut self) -> crate::Result<TcpStream> {
        let mut backoff = 200;

        // Try to accept the connection
        loop {
            // perform accept operation
            match self.listener.accept().await {
                Ok((socket, _)) => return Ok(socket),
                Err(e) => {
                    if backoff > 64 * 200 {
                        // Accept has failed, return the error
                        return Err(e.into());
                    }
                }
            }

            // Pause execution until the backoff period elapses.
            tokio::time::sleep(Duration::from_millis(backoff)).await;
            // Double back off
            backoff = backoff * 2;
        }
    }
}

struct Handler<E: KvsEngine> {
    /// Shared key value store handle
    ///
    /// Commands recieved from the connection are forwarded to the database
    /// handle. Each command will interact with the key value store.
    engine: E,

    /// Encasulation around the TCP connection with the ability to encode
    /// responses and decode requests.
    connection: Connection,

    /// Max connection semaphore.
    limit_connections: Arc<Semaphore>,

    /// Listen for a shutdown notification.
    shutdown: Shutdown,

    // Not used directly. Only during when the `Handler` is dropped.
    _shutdown_complete: mpsc::Sender<()>,
}

impl<E: KvsEngine> Handler<E> {
    /// Process a single connection.
    ///
    /// Requests are read from the socket and processed. Responses are written back.
    ///
    /// Pipelining is not implements. It is the ability to process more than one
    /// request concurrently per connection. Read more: https://redis.io/topics/pipelining
    ///
    /// When the shutdown signal is received, the connection is processed until
    /// it reaches a safe state, at which point it terminates.
    #[instrument(skip(self))]
    async fn run(&mut self) -> crate::Result<()> {
        while !self.shutdown.is_shutdown() {
            // Read frame until shutdown is signalled.
            let request = tokio::select! {
                res = self.connection.read() => res?,
                _ = self.shutdown.recv() => {
                    // By returning ok, the server knows to stop listening and
                    // shutdown.
                    return Ok(())
                }
            };

            // if we don't have a request, just return early.
            if request.is_none() {
                return Ok(());
            }

            request
                .unwrap()
                .apply(&self.engine, &mut self.connection, &mut self.shutdown)
                .await?;
        }
        Ok(())
    }
}

impl<E: KvsEngine> Drop for Handler<E> {
    fn drop(&mut self) {
        // Remove subscription on semaphore and return it. This unlocks a
        // connection.
        self.limit_connections.add_permits(1);
    }
}
