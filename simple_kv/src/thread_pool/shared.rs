use super::ThreadPool;

use crate::Result;

use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;

type Job = Box<dyn FnOnce() + Send + 'static>;

/// `SharedQueueThreadPool` is a thread pool based on mpsc::channel.
///
/// `threads` is the number of threads.
/// `job_sender` sends `Msg` to threads.
/// `handles` is maintained for exiting.
pub struct SharedQueueThreadPool {
    job_sender: Sender<Job>,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<SharedQueueThreadPool> {
        let (tx, rx) = mpsc::channel::<Job>();
        let rx = Arc::new(Mutex::new(rx));
        for _ in 0..threads {
            let rx_clone = RxWrapper(Arc::clone(&rx));
            thread::spawn(move || {
                loop {
                    let job = rx_clone.0.lock().unwrap().recv();
                    match job {
                        Ok(f) => {
                            f();
                            //  I try `catch_unwind` then the compile error occurs
                            //  ``` rust
                            //  let res = panic::catch_unwind(f);
                            //  match res {
                            //      Ok(_) => {},
                            //      Err(e) => {
                            //          error!("[SharedThreadPool]Thread {} panics", id);
                            //      }
                            //  }
                            //  ```
                            //      the type `dyn std::ops::FnOnce() + std::marker::Send`
                            //  may not be safely transferred across an unwind boundary
                            //      So I can't catch the panic and keep the existing thread
                            //  running.
                        }
                        Err(e) => {
                            debug!("[SharedThreadPool]Maybe Sender was destroyed: {}", e);
                        }
                    }
                }
            });
        }

        Ok(SharedQueueThreadPool { job_sender: tx })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.job_sender.send(Box::new(job)).unwrap();
    }
}

// So I determined to use thread::panicking
// to let the thread die and spawn another.
// We can't impl Drop for Arc<Mutex<Receiver<Msg>>>
// because of the orphan rule.
struct RxWrapper(Arc<Mutex<Receiver<Job>>>);

impl Drop for RxWrapper {
    fn drop(&mut self) {
        if thread::panicking() {
            let rx = RxWrapper(Arc::clone(&self.0));
            thread::spawn(move || loop {
                let job = rx.0.lock().unwrap().recv();
                match job {
                    Ok(f) => {
                        f();
                    }
                    Err(e) => {
                        debug!("[SharedThreadPool]Maybe Sender was destroyed: {}", e);
                    }
                }
            });
        }
    }
}
