use crate::thread_pool::ThreadPool;
use crate::{
    protocol::{GetResponse, RemoveResponse, Request, Response, SetResponse},
    KvsEngine, Result,
};
use serde_json::Deserializer;
use std::io::Write;
use std::{
    io::{BufReader, BufWriter},
    net::{SocketAddr, TcpListener, TcpStream},
};

/// `KvServer` is a top level wrapper of various implementation of `KvsEngine`.
pub struct KvServer<E: KvsEngine, P: ThreadPool> {
    engine: E,
    addr: SocketAddr,
    pool: P,
}

impl<E: KvsEngine, P: ThreadPool> KvServer<E, P> {
    /// create a `KvServer`.
    pub fn new(engine: E, addr: SocketAddr, pool: P) -> Self {
        KvServer { engine, addr, pool }
    }

    /// run a `KvServer`.
    pub fn run(&mut self) -> Result<()> {
        // SocketAddr implements Clone
        // There is no matter with ownership
        let listener = TcpListener::bind(self.addr)?;
        for stream in listener.incoming() {
            let engine = self.engine.clone();
            self.pool.spawn(move || {
                match stream {
                    Ok(tcp) => {
                        // serve
                        if let Err(e) = serve(engine, tcp) {
                            error!("[kv_server] Serve Err: {}", e);
                        }
                    }
                    Err(e) => error!("Connection failed: {}", e),
                }
            });
        }
        Ok(())
    }
}

fn serve<E: KvsEngine>(engine: E, tcp: TcpStream) -> Result<()> {
    let peer_addr = tcp.peer_addr()?;
    let mut writer = BufWriter::new(&tcp);
    let req_stream = Deserializer::from_reader(BufReader::new(&tcp)).into_iter::<Request>();

    for req in req_stream {
        let req: Request = req?;
        debug!("[server]: Get request from {}: {:?}", &peer_addr, &req);
        let resp = match req {
            Request::Get { key } => match engine.get(key) {
                Ok(opt_string) => Response::Get(GetResponse::Ok(opt_string)),
                Err(e) => Response::Get(GetResponse::Err(format!("{}", e))),
            },
            Request::Set { key, value } => match engine.set(key, value) {
                Ok(_) => Response::Set(SetResponse::Ok(())),
                Err(e) => Response::Set(SetResponse::Err(format!("{}", e))),
            },
            Request::Remove { key } => match engine.remove(key) {
                Ok(_) => Response::Remove(RemoveResponse::Ok(())),
                Err(e) => Response::Remove(RemoveResponse::Err(format!("{}", e))),
            },
        };
        serde_json::to_writer(&mut writer, &resp)?;
        writer.flush()?;
        debug!("[server]: Send response to {}: {:?}", &peer_addr, &resp);
    }
    Ok(())
}
