use clap_v3::{App, Arg, ArgMatches};
use kvs::{KvClient, KvError, Result};
use std::net::{IpAddr, SocketAddr};
use std::process::exit;
use std::str::FromStr;

const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1";

fn main() {
    let opt = App::new("kvs-client")
        .version("0.1.0")
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

    if let Err(e) = run(opt) {
        eprintln!("{}", e);
        exit(1);
    }
}

fn run(opt: ArgMatches) -> Result<()> {
    let addr = opt.value_of("addr").unwrap();
    let port = opt.value_of("port").unwrap();
    let ip = SocketAddr::new(IpAddr::from_str(addr).unwrap(), port.parse().unwrap());
    let mut client = KvClient::connect(ip)?;
    match opt.subcommand() {
        ("get", Some(sub)) => {
            if let Some(value) = client.get(sub.value_of("key").unwrap().to_string())? {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        }
        ("set", Some(sub)) => {
            client.set(
                sub.value_of("key").unwrap().to_string(),
                sub.value_of("value").unwrap().to_string(),
            )?;
        }
        ("rm", Some(sub)) => {
            client.remove(sub.value_of("key").unwrap().to_string())?;
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

            for number in 0..amount {
                let key = format!("Key{}", number);
                match operation {
                    "get" => {
                        if let Some(value) = client.get(key.clone())? {
                            println!("{}: {} = {}", number, key, value);
                        } else {
                            println!("{}: {} could not be found", number, key);
                        }
                    }
                    "set" => {
                        let value = format!("Value{}", number);
                        println!("{}: Set {} and {}", number, key, value);
                        client.set(key, value)?;
                    }
                    "rm" => {
                        println!("{}: Removed {}", number, key);
                        client.remove(key)?;
                    }
                    _ => {
                        println!("This shouldn't execte. Exitting...");
                        std::process::exit(1);
                    }
                }
            }
        }
        (_, _) => return Err(KvError::Parse("Command does not exist".to_string().into())),
    }
    Ok(())
}
