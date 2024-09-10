use clap::{Arg, Command};
mod nnlib;
use crate::nnlib::{new_namenode, add_datanodes};
use rs_dfs::NodeAddress;
fn main() {
    // hdfsn -p 8080 -bs 100 -r 3 -dn localhost:8080,localhost:8081,somenonlocal:8082
    let matches = Command::new("HDFS Namenode")
        .version("0.1.0")
        .about("HDFS Namenode")
        .arg_required_else_help(true)
        .arg(
            Arg::new("port")
                .short('p')
                .long("port")
                .value_name("PORT")
                .help("Sets the port to listen on")
        )
        .arg(
            Arg::new("blockSize")
                .short('b')
                .long("block-size")
                .value_name("BLOCK_SIZE")
                .help("Sets the block size")
        )
        .arg(
            Arg::new("replFactor")
                .short('r')
                .long("repl-factor")
                .value_name("REPL_FACTOR")
                .help("Sets the replication factor")
        )
        .arg(
            Arg::new("dataNodes")
                .short('d')
                .long("data-nodes")
                .value_name("DATA_NODES")
                .help("List of data nodes in cluster")
        )
        .get_matches();

    let port = matches.get_one::<String>("port").map(String::as_str).unwrap_or("8080");
    let block_size = matches.get_one::<String>("blockSize").map(String::as_str).unwrap_or("100");
    let repl_factor = matches.get_one::<String>("replFactor").map(String::as_str).unwrap_or("3");
    let data_nodes = matches.get_one::<String>("dataNodes").map(String::as_str).unwrap_or("localhost:8080,localhost:8081,somenonlocal:8082");
    println!("Port: {}", port);
    println!("Block Size: {}", block_size);
    println!("Replication Factor: {}", repl_factor);
    println!("Data Nodes: {}", data_nodes);
    let mut namenode = new_namenode(port.parse().unwrap(), block_size.parse().unwrap(), repl_factor.parse().unwrap());
    let data_nodes: Vec<(NodeAddress, bool)> = parse_data_nodes(data_nodes);
    add_datanodes(&mut namenode, data_nodes);
    
}


fn parse_data_nodes(data_nodes: &str) -> Vec<(NodeAddress, bool)> {
    data_nodes.split(",").map(|node| {
        let parts: Vec<&str> = node.split(":").collect();
        (NodeAddress {
            host: parts[0].to_string(),
            port: parts[1].parse().unwrap(),
        }, false)
    }).collect()
}