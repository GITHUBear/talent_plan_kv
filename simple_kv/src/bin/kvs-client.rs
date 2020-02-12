use simple_kv::{KvClient, Result};
use std::{net::SocketAddr, process::exit};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
enum Opt {
    Set {
        #[structopt(name = "KEY", required = true)]
        key: String,
        #[structopt(name = "VALUE", required = true)]
        value: String,
        #[structopt(long, default_value = "127.0.0.1:4000", parse(try_from_str))]
        addr: SocketAddr,
    },
    Get {
        #[structopt(name = "KEY", required = true)]
        key: String,
        #[structopt(long, default_value = "127.0.0.1:4000", parse(try_from_str))]
        addr: SocketAddr,
    },
    Rm {
        #[structopt(name = "KEY", required = true)]
        key: String,
        #[structopt(long, default_value = "127.0.0.1:4000", parse(try_from_str))]
        addr: SocketAddr,
    },
}

fn main() {
    let opt = Opt::from_args();
    if let Err(e) = run(opt) {
        eprintln!("{}", e);
        exit(1);
    }
}

fn run(opt: Opt) -> Result<()> {
    match opt {
        Opt::Set { key, value, addr } => {
            let mut client = KvClient::connect(addr)?;
            client.set(key, value)?;
        }
        Opt::Get { key, addr } => {
            let mut client = KvClient::connect(addr)?;
            if let Some(value) = client.get(key)? {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        }
        Opt::Rm { key, addr } => {
            let mut client = KvClient::connect(addr)?;
            client.remove(key)?;
        }
    }
    Ok(())
}
