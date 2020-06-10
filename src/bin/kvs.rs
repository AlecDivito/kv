use std::process::exit;
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

fn main() {
    let opt = Kvs::from_args();
    match opt {
        Kvs::Set { key, value } => {
            eprintln!("unimplemented");
            exit(1);
        }
        Kvs::Get { key } => {
            eprintln!("unimplemented");
            exit(1);
        }
        Kvs::Rm { key } => {
            eprintln!("unimplemented");
            exit(1);
        }
        _ => unreachable!(),
    }
}
