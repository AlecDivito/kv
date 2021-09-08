use clap_v3::{App, Arg};
use kvs::*;
use std::env::current_dir;
use std::fs;
use std::path::PathBuf;
use std::process::exit;
use std::str::FromStr;
use tokio::net::TcpListener;

const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1";

enum Engine {
    Kvs,
    Sled,
}

impl FromStr for Engine {
    type Err = &'static str;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "kvs" => Ok(Engine::Kvs),
            "sled" => Ok(Engine::Sled),
            _ => Err("no match"),
        }
    }
}

impl std::fmt::Display for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Engine::Kvs => "kvs",
            Engine::Sled => "sled",
        };
        write!(f, "{}", s)
    }
}

#[tokio::main]
async fn main() {
    // enable logging
    // see https://docs.rs/tracing for more info
    if let Err(e) = tracing_subscriber::fmt::try_init() {
        eprintln!("Failed to setup tracing: {}", e);
        exit(2);
    };
    let opt = App::new("kvs-server")
        .version("1.0.0")
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

    println!("kvs-server {}", env!("CARGO_PKG_VERSION"));
    println!("Storage engine: {}", engine_str);
    println!("Listening on {}", address);

    if let Err(e) = run(engine, address, port).await {
        eprintln!("{}", e);
        exit(1);
    }
}

async fn run_with_engine<E: KvsEngine + 'static>(
    engine: E,
    ip: &str,
    port: &str,
) -> crate::Result<()> {
    let listener = TcpListener::bind(format!("{}:{}", ip, port)).await?;
    kvs::listen_with(engine, listener, tokio::signal::ctrl_c()).await?;
    Ok(())
}

async fn run(engine: Engine, ip: &str, port: &str) -> crate::Result<()> {
    fs::write(current_dir()?.join("engine"), format!("{}", engine))?;

    match engine {
        Engine::Kvs => {
            run_with_engine(KvStore::open(PathBuf::from("./.temp")).await?, ip, port).await
        }
        Engine::Sled => run_with_engine(SledKvsEngine::open(current_dir()?).await?, ip, port).await,
    }?;

    Ok(())
}
