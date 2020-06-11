use kvs::{KvError, KvStore, Result};
use std::env;
use structopt::StructOpt;

#[derive(StructOpt)]
#[structopt(
    name = env!("CARGO_PKG_NAME"),
    about = env!("CARGO_PKG_DESCRIPTION"),
    version = env!("CARGO_PKG_VERSION"),
    author = env!("CARGO_PKG_AUTHORS"),
)]
enum Kvs {
    Set {
        #[structopt(required = true)]
        key: String,
        #[structopt(required = true)]
        value: String,
    },
    Get {
        #[structopt(required = true)]
        key: String,
    },
    Rm {
        #[structopt(required = true)]
        key: String,
    },
}

#[derive(Debug, StructOpt)]
#[structopt(name = env!("CARGO_PKG_NAME"), about = env!("CARGO_PKG_DESCRIPTION"))]
struct Opt {
    #[structopt(short = "")]
    command: String,
    action: String,
    value: Option<String>,
}

fn main() -> Result<()> {
    let opt = Kvs::from_args();
    let mut kvs = KvStore::open(env::current_dir().unwrap())?;
    match opt {
        Kvs::Set { key, value } => kvs.set(key, value),
        Kvs::Rm { key } => match kvs.remove(key) {
            Ok(_) => Ok(()),
            Err(e) => match e {
                KvError::KeyNotFound(_) => {
                    println!("Key not found");
                    std::process::exit(1);
                }
                _ => Err(e),
            },
        },
        Kvs::Get { key } => {
            let value = kvs.get(key)?;
            match value {
                Some(v) => println!("{}", v),
                None => println!("Key not found"),
            };
            Ok(())
        }
        _ => unreachable!(),
    }
}
