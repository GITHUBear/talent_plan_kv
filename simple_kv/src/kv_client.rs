use crate::{
    protocol::{GetResponse, RemoveResponse, Request, Response, SetResponse},
    KvsError, Result,
};
use serde::Deserialize;
use serde_json::de::{Deserializer, IoRead};
use std::io::Write;
use std::{
    io::{BufReader, BufWriter},
    net::{SocketAddr, TcpStream},
};

/// `KvClient` is a client end of `KvServer`.
pub struct KvClient {
    reader: Deserializer<IoRead<BufReader<TcpStream>>>,
    writer: BufWriter<TcpStream>,
}

impl KvClient {
    /// connect to the `KvServer` binds on addr.
    pub fn connect(addr: SocketAddr) -> Result<Self> {
        let connection = TcpStream::connect(addr)?;
        let writer = connection.try_clone()?;
        Ok(KvClient {
            reader: Deserializer::from_reader(BufReader::new(connection)),
            writer: BufWriter::new(writer),
        })
    }

    /// Get the value of a given key from the server.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let request = Request::Get { key };
        serde_json::to_writer(&mut self.writer, &request)?;
        self.writer.flush()?;
        let resp = Response::deserialize(&mut self.reader)?;
        debug!("[client get] Get response from server {:?}", &resp);
        match resp {
            Response::Get(get) => match get {
                GetResponse::Ok(val) => Ok(val),
                GetResponse::Err(msg) => Err(KvsError::StringErr(msg)),
            },
            _ => {
                panic!("[client get] Reach unreachable code");
            }
        }
    }

    /// Set the value of a string key in the server.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let request = Request::Set { key, value };
        serde_json::to_writer(&mut self.writer, &request)?;
        self.writer.flush()?;
        let resp = Response::deserialize(&mut self.reader)?;
        debug!("[client set] Get response from server {:?}", &resp);
        match resp {
            Response::Set(set) => match set {
                SetResponse::Ok(_) => Ok(()),
                SetResponse::Err(msg) => Err(KvsError::StringErr(msg)),
            },
            _ => {
                panic!("[client set] Reach unreachable code");
            }
        }
    }

    /// Remove a string key in the server.
    pub fn remove(&mut self, key: String) -> Result<()> {
        let request = Request::Remove { key };
        serde_json::to_writer(&mut self.writer, &request)?;
        self.writer.flush()?;
        let resp = Response::deserialize(&mut self.reader)?;
        debug!("[client remove] Get response from server {:?}", &resp);
        match resp {
            Response::Remove(rm) => match rm {
                RemoveResponse::Ok(_) => Ok(()),
                RemoveResponse::Err(msg) => Err(KvsError::StringErr(msg)),
            },
            _ => {
                panic!("[client remove] Reach unreachable code");
            }
        }
    }
}
