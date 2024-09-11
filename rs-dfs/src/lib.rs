// export ansi module
pub mod ansi;

pub use ansi::{AnsiColor, AnsiStyle, ansi};

use serde::{Serialize, Deserialize};


mod namenode {
    tonic::include_proto!("namenode");
}

mod datanode {
    tonic::include_proto!("datanode");
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeAddress {
    pub host: String,
    pub port: u16,
}
pub struct SerializableError {
    message: String,
}

impl std::fmt::Display for SerializableError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}