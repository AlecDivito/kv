use crate::error::Result;

/// ThreadPool is a trait to be used for threading our applications
pub trait ThreadPool {
    
    /// Creates a new thread pool, immediately spawns the specificed number
    /// of threads.
    ///
    /// # Errors
    ///
    /// If any thread fails to spawn. All previously-spawned threads are
    /// terminated.
    fn new(threads: u32) -> Result<Self> where Self: Sized;

    /// Spawn a function into the threadpool. Spawning should always succeed but
    /// if the function panics the threadpool continues to operate with the same
    /// number of threads.
    /// The thread count is not reduced nor is the thread pool destroyed,
    /// corrupted or invalidated.
    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static;
}

pub use naive::NaiveThreadPool;
pub use shared::SharedQueueThreadPool;
pub use rayon::RayonThreadPool;

mod naive;
mod shared;
mod rayon;