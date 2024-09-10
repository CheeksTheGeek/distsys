use clap::{Arg, Command};
mod dnlib;
use crate::dnlib::{new_datanode, DataNodeInstance};
use rs_dfs::NodeAddress;
use tarpc::{
    context,
    server::{self, BaseChannel},
    serde_transport
};
use tokio;
use std::{
    net::{IpAddr, Ipv6Addr, SocketAddr}, thread::spawn, time::Duration
};

fn main() {
    let matches = Command::new("HDFS Datanode")
        .version("0.1.0")
        .about("HDFS Datanode")
        .arg_required_else_help(true)
        .arg(
            Arg::new("port")
                .short('p')
                .long("port")
                .value_name("PORT")
                .help("Sets the port to listen on")
        )
        .arg(
            Arg::new("datadir")
                .short('d')
                .long("datadir")
                .value_name("DATADIR")
                .help("Sets the datadir")
        ).get_matches();

    let port = matches.get_one::<String>("port").map(String::as_str).unwrap_or("8080");
    let datadir = matches.get_one::<String>("datadir").map(String::as_str).unwrap_or("data");

    println!("Port: {}", port);
    println!("Datadir: {}", datadir);
}

