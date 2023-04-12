use clap_v3::{App, Arg};
use kvs::*;
use log::LevelFilter;
use log::{error, info};
use std::env::current_dir;
use std::fs;
use std::net::{IpAddr, SocketAddr};
use std::process::exit;
use std::str::FromStr;

const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1";

enum Engine {
    Kvs,
    Sled,
    Memory,
}

impl FromStr for Engine {
    type Err = &'static str;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "kvs" => Ok(Engine::Kvs),
            "sled" => Ok(Engine::Sled),
            "memory" => Ok(Engine::Memory),
            _ => Err("no match"),
        }
    }
}

impl std::fmt::Display for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Engine::Kvs => "kvs",
            Engine::Sled => "sled",
            Engine::Memory => "memory",
        };
        write!(f, "{}", s)
    }
}

fn main() {
    env_logger::builder().filter_level(LevelFilter::Info).init();
    let opt = App::new("kvs-server")
        .version("0.1.0")
        .author("Alec Di Vito")
        .about("Key value store server")
        .arg(
            Arg::with_name("addr")
                .short('a')
                .default_value(DEFAULT_LISTENING_ADDRESS)
                .help("Sets the server address"),
        )
        .arg(
            Arg::with_name("port")
                .short('p')
                .default_value("4000")
                .help("Set the servers port number"),
        )
        .arg(
            Arg::from("<engine> 'The type of engine to use'")
                .short('e')
                .default_value("kvs")
                .possible_values(&["kvs", "sled"]),
        )
        .get_matches();

    let engine_str = opt.value_of("engine").unwrap();
    let engine: Engine = engine_str.parse().unwrap();
    let address = opt.value_of("addr").unwrap();
    let port = opt.value_of("port").unwrap();

    info!("kvs-server {}", env!("CARGO_PKG_VERSION"));
    info!("Storage engine: {}", engine_str);
    info!("Listening on {}", address);

    if let Err(e) = run(engine, address, port) {
        error!("{}", e);
        exit(1);
    }
}

fn run_with_engine<E: KvsEngine>(engine: E, addr: impl Into<SocketAddr>) -> Result<()> {
    let server = KvServer::new(engine);
    server.run(addr.into())
}

fn run(engine: Engine, address: &str, port: &str) -> Result<()> {
    fs::write(current_dir()?.join("engine"), format!("{}", engine))?;
    let ip = SocketAddr::new(IpAddr::from_str(address).unwrap(), port.parse().unwrap());

    match engine {
        Engine::Kvs => run_with_engine(KvStore::open("./.temp")?, ip)?,
        Engine::Sled => run_with_engine(SledKvsEngine::open(current_dir()?.as_path())?, ip)?,
        Engine::Memory => run_with_engine(KvInMemoryStore::open("").unwrap(), ip)?,
    };

    Ok(())
}
