use simple_kv::KvStore;
use std::process::exit;
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

fn main() {
    let opt = Opt::from_args();
    match opt {
        Opt::Set { key: _, value: _ } => {
            eprintln!("unimplemented");
            exit(1);
        },
        Opt::Get { key: _ } => {
            eprintln!("unimplemented");
            exit(1);
        },
        Opt::Rm { key: _ } => {
            eprintln!("unimplemented");
            exit(1);
        },
    }
}
