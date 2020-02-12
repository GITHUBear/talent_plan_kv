use super::ThreadPool;

use crate::Result;

use std::thread;

/// A naive implement for trait `ThreadPool`.
pub struct RayonThreadPool;

impl ThreadPool for RayonThreadPool {
    fn new(_threads: u32) -> Result<RayonThreadPool> {
        Ok(RayonThreadPool)
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        thread::spawn(job);
    }
}
