use super::ThreadPool;

/// A Naive implementation of a thread pool
pub struct SharedQueueThreadPool;

impl ThreadPool for SharedQueueThreadPool {
    fn new(_: u32) -> crate::Result<Self> where Self: Sized {
        Ok(SharedQueueThreadPool { })
    }

    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        std::thread::spawn(job);
    }
}  