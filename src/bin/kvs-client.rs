use clap_v3::{App, Arg, ArgMatches};
use kvs::{KvClient, KvError, Result};
use std::process::exit;

const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1";

#[tokio::main(flavor = "current_thread")]
async fn main() -> kvs::Result<()> {
    let opt = App::new("kvs-client")
        .version("1.0.0")
        .author("Alec Di Vito")
        .about("Access key value store server")
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
        .subcommand(
            App::new("get")
                .about("Get the string value of a given string key")
                .arg(Arg::with_name("key").help("A string key").required(true)),
        )
        .subcommand(
            App::new("set")
                .about("Set the value of a string key to a string")
                .arg(Arg::with_name("key").help("A string key").required(true))
                .arg(
                    Arg::with_name("value")
                        .help("The string vallue of the key")
                        .required(true),
                ),
        )
        .subcommand(
            App::new("rm")
                .about("Remove a given string key")
                .arg(Arg::with_name("key").help("A string key").required(true)),
        )
        .subcommand(
            App::new("test")
                .about("Test the key value store")
                .arg(Arg::with_name("operation").help("Operation to test"))
                .arg(Arg::with_name("amount").help("The amount of operations to send the server")),
        )
        .get_matches();

    if let Err(e) = run(opt).await {
        eprintln!("{}", e);
        exit(1);
    }
    Ok(())
}

async fn run(opt: ArgMatches) -> Result<()> {
    let host = opt.value_of("addr").unwrap();
    let port = opt.value_of("port").unwrap();
    let addr = format!("{}:{}", host, port);
    let mut client = KvClient::connect(addr).await?;
    match opt.subcommand() {
        ("get", Some(sub)) => {
            if let Some(value) = client.get(sub.value_of("key").unwrap().to_string()).await? {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        }
        ("set", Some(sub)) => {
            client
                .set(
                    sub.value_of("key").unwrap().to_string(),
                    sub.value_of("value").unwrap().to_string(),
                )
                .await?;
        }
        ("rm", Some(sub)) => {
            client
                .remove(sub.value_of("key").unwrap().to_string())
                .await?;
        }
        ("test", Some(sub)) => {
            let operation = match sub.value_of("operation") {
                Some("get") => "get",
                Some("set") => "set",
                Some("rm") => "rm",
                _ => return Err(KvError::Parse("A valid operation was not found".into())),
            };
            let amount = sub.value_of("amount").ok_or(KvError::Parse(
                "A test amount must be included. Should be a valid number".into(),
            ))?;
            let amount = amount
                .parse::<usize>()
                .map_err(|_| KvError::Parse("The test amount was not a valid number".into()))?;

            client.test(operation, amount).await?;
        }
        (_, _) => return Err(KvError::Parse("Command does not exist".to_string().into())),
    }
    Ok(())
}
