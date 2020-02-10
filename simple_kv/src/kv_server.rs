use crate::{
    KvsEngine, Result,
    protocol::{
        Request,
        Response,
        SetResponse,
        GetResponse,
        RemoveResponse,
    },
};
use std::{
    net::{
        SocketAddr, TcpListener, TcpStream,
    },
    io::{
        BufReader, BufWriter,
    },
};
use serde_json::Deserializer;
use std::io::Write;

/// `KvServer` is a top level wrapper of various implementation of `KvsEngine`.
pub struct KvServer<E: KvsEngine> {
    engine: E,
    addr: SocketAddr,
}

impl<E: KvsEngine> KvServer<E> {
    /// create a `KvServer`.
    pub fn new(engine: E, addr: SocketAddr) -> Self {
        KvServer {
            engine,
            addr,
        }
    }

    /// run a `KvServer`.
    pub fn run(&mut self) -> Result<()> {
        // SocketAddr implements Clone
        // There is no matter with ownership
        let listener = TcpListener::bind(self.addr)?;
        for stream in listener.incoming() {
            match stream {
                Ok(tcp) => {
                    // serve
                    self.serve(tcp)?;
                },
                Err(e) => error!("Connection failed: {}", e),
            }
        }
        Ok(())
    }

    fn serve(&mut self, tcp: TcpStream) -> Result<()> {
        let peer_addr = tcp.peer_addr()?;
        let mut writer = BufWriter::new(&tcp);
        let req_stream =
            Deserializer::from_reader(BufReader::new(&tcp))
                .into_iter::<Request>();

        for req in req_stream {
            let req: Request = req?;
            debug!("[server]: Get request from {}: {:?}", &peer_addr, &req);
            let resp = match req {
                Request::Get { key } => {
                    match self.engine.get(key) {
                        Ok(opt_string) =>
                            Response::Get(GetResponse::Ok(opt_string)),
                        Err(e) =>
                            Response::Get(GetResponse::Err(format!("{}", e))),
                    }

                },
                Request::Set { key, value } => {
                    match self.engine.set(key, value) {
                        Ok(_) =>
                            Response::Set(SetResponse::Ok(())),
                        Err(e) =>
                            Response::Set(SetResponse::Err(format!("{}", e))),
                    }
                },
                Request::Remove { key } => {
                    match self.engine.remove(key) {
                        Ok(_) =>
                            Response::Remove(RemoveResponse::Ok(())),
                        Err(e) =>
                            Response::Remove(RemoveResponse::Err(format!("{}", e))),
                    }
                }
            };
            serde_json::to_writer(&mut writer, &resp)?;
            writer.flush()?;
            debug!("[server]: Send response to {}: {:?}",
                   &peer_addr, &resp);
        }
        Ok(())
    }
}