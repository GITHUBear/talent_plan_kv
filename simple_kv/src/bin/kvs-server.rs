#[macro_use] extern crate log;

use simple_kv::{KvsError, KvsEngine, Result, KvServer, KvStore, SledStore};
use std::{
    env::current_dir,
    net::SocketAddr,
    str::FromStr,
    process::exit,
    fs,
};
use structopt::StructOpt;

use env_logger::fmt::Target;
use log::LevelFilter;

#[allow(non_camel_case_types)]
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
enum Engine {
    kvs,
    sled,
}

impl FromStr for Engine {
    type Err = KvsError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "kvs" => {
                Ok(Engine::kvs)
            },
            "sled" => {
                Ok(Engine::sled)
            },
            _ => {
                Err(KvsError::ParseEngineNameErr)
            },
        }
    }
}

#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(long,
    required = false,
    default_value = "127.0.0.1:4000",
    parse(try_from_str))]
    addr: SocketAddr,
    #[structopt(long,
    parse(try_from_str))]
    engine: Option<Engine>,
}

fn get_engine_name_from_file() -> Result<Option<Engine>> {
    let path= current_dir()?.join("engine");
    if !path.exists() {
        return Ok(None);
    }

    let engine = fs::read_to_string(&path)?;
    match &*engine {
        "kvs" => Ok(Some(Engine::kvs)),
        "sled" => Ok(Some(Engine::sled)),
        _ => {
            warn!("The content of engine file is invalid");
            Ok(None)
        }
    }
}

fn run_kv_server<E: KvsEngine>(engine: E, addr: SocketAddr) -> Result<()> {
    let mut server = KvServer::new(engine, addr);
    Ok(server.run()?)
}

fn main() -> Result<()> {
    env_logger::builder()
        .target(Target::Stderr)
        .filter_level(LevelFilter::Info)
        .init();
    let mut opt = Opt::from_args();

    let engine = get_engine_name_from_file()?;
    if opt.engine.is_none() {
        opt.engine = engine;
    }
    if opt.engine.is_none() {
        // first time
        opt.engine = Some(Engine::kvs);
    }
    if engine.is_some() && engine != opt.engine {
        error!("Wrong engine!");
        exit(1);
    }

    info!("kvs-server {}", env!("CARGO_PKG_VERSION"));
    info!("Storage engine: {:?}", opt.engine);
    info!("Listening on {}", opt.addr);

    let engine = opt.engine.unwrap();
    fs::write(current_dir()?.join("engine"), format!("{:?}", engine))?;

    match engine {
        Engine::kvs => {
            let engine = KvStore::open(&current_dir()?)?;
            run_kv_server(engine, opt.addr)?;
        },
        Engine::sled => {
            let engine = SledStore::new(sled::open(current_dir()?)?);
            run_kv_server(engine, opt.addr)?;
        },
    }

    Ok(())
}
