use super::ThreadPool;


/// A Naive implementation of a thread pool
pub struct NaiveThreadPool;

impl ThreadPool for NaiveThreadPool {
    fn new(_: u32) -> crate::Result<Self> where Self: Sized {
        Ok(NaiveThreadPool { })
    }

    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        std::thread::spawn(job);
    }
}  