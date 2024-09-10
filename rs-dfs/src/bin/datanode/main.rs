use tarpc::serde_transport::tcp;
use tarpc::server::BaseChannel;
use tokio_serde::formats::Json;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use futures::prelude::*;
use std::sync::Arc;
use tokio::sync::Mutex;
use clap::{App, Arg};
use crate::dnlib::{Datanode, DatanodeService};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let matches = App::new("Datanode")
        .version("1.0")
        .author("Author Name <author@example.com>")
        .about("Datanode server for distributed file system")
        .arg(
            Arg::new("port")
                .short('p')
                .long("port")
                .value_name("PORT")
                .about("Sets the port to listen on")
                .default_value("4120")
                .takes_value(true),
        )
        .arg(
            Arg::new("datadir")
                .short('d')
                .long("datadir")
                .value_name("DATADIR")
                .about("Sets the directory to store file blocks")
                .default_value(".data")
                .takes_value(true),
        )
        .get_matches();

    let port = matches.value_of("port").unwrap();
    let datadir = matches.value_of("datadir").unwrap();

    let addr = format!("127.0.0.1:{}", port);

    let data = Arc::new(Mutex::new(Vec::new()));
    let server = Datanode { data };

    let listener = TcpListener::bind(&addr).await?;
    println!("Server started at {}", addr);

    TcpListenerStream::new(listener)
        .for_each(|stream| async {
            if let Ok(stream) = stream {
                let transport = tcp::listen(stream, Json::default);
                let channel = BaseChannel::with_defaults(transport);
                channel.execute(server.serve());
            }
        })
        .await;

    Ok(())
}
