use super::ThreadPool;

/// A Naive implementation of a thread pool
pub struct RayonThreadPool;

impl ThreadPool for RayonThreadPool {
    fn new(_: u32) -> crate::Result<Self> where Self: Sized {
        Ok(RayonThreadPool { })
    }

    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        std::thread::spawn(job);
    }
}  