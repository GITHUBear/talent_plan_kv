use simple_kv::{KvStore, KvsError, Result};
use std::env::current_dir;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
enum Opt {
    Set {
        #[structopt(name = "KEY", required = true)]
        key: String,
        #[structopt(name = "VALUE", required = true)]
        value: String,
    },
    Get {
        #[structopt(name = "KEY", required = true)]
        key: String,
    },
    Rm {
        #[structopt(name = "KEY", required = true)]
        key: String,
    },
}

fn main() -> Result<()> {
    let opt = Opt::from_args();
    let mut kvs = KvStore::open(&current_dir()?)?;
    match opt {
        Opt::Set { key, value } => {
            Ok(kvs.set(key, value)?)
        },
        Opt::Get { key } => {
            if let Some(val) =  kvs.get(key)? {
                println!("{}", &val);
            } else {
                println!("Key not found");
            }
            Ok(())
        },
        Opt::Rm { key } => {
            match kvs.remove(key) {
                Err(KvsError::KeyNotFound) => {
                    println!("Key not found");
                    return Err(KvsError::KeyNotFound);
                },
                Ok(()) => {},
                Err(e) => return Err(e),
            }
            Ok(())
        },
    }
}
