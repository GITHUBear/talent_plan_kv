use super::{
    ThreadPool,
};

use crate::{
    Result
};

use std::thread::{ self, JoinHandle };
use std::sync::mpsc::{ self, Sender };
use std::sync::{ Arc, Mutex };

enum Msg {
    Job(Box<dyn FnOnce() + Send + 'static>),
    Stop,
}

/// `SharedQueueThreadPool` is a thread pool based on mpsc::channel.
pub struct SharedQueueThreadPool {
    threads: u32,
    job_sender: Sender<Msg>,
    handles: Vec<Option<JoinHandle<()>>>,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<SharedQueueThreadPool> {
        let mut handles = Vec::new();
        let (tx, rx) = mpsc::channel::<Msg>();
        let rx = Arc::new(Mutex::new(rx));
        for id in 0..threads {
            let rx_clone = Arc::clone(&rx);
            let handle = thread::spawn(move || {
                loop {
                    let job = rx_clone.lock().unwrap().recv();
                    match job {
                        Ok(msg) => {
                            match msg {
                                Msg::Job(f) => {
                                    debug!("[SharedThreadPool]Thread {} get a job", id);
                                    f();
                                },
                                Msg::Stop => {
                                    debug!("[SharedThreadPool]Thread {} exits", id);
                                    break;
                                },
                            }
                        },
                        Err(e) => {
                            debug!("[SharedThreadPool]Maybe Sender was destroyed: {}", e);
                        }
                    }
                }
            });
            handles.push(Some(handle));
        }

        Ok(SharedQueueThreadPool {
            threads,
            job_sender: tx,
            handles,
        })
    }

    fn spawn<F>(&self, job: F)
        where F: FnOnce() + Send + 'static {
        self.job_sender.send(Msg::Job(Box::new(job))).unwrap();
    }
}

impl Drop for SharedQueueThreadPool {
    fn drop(&mut self) {
        for _ in 0..self.threads {
            self.job_sender.send(Msg::Stop).unwrap();
        }

        for handle in &mut self.handles {
            let handle = handle.take();
            if let Some(h) = handle {
                h.join().unwrap();
            }
        }
    }
}