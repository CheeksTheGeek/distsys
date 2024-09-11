
use rs_dfs::ansi::{AnsiColor, AnsiStyle, style};
use std::io::{self, Write};
use termion::raw::IntoRawMode;
use std::fs;
use std::path::{Path, PathBuf};
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader};
use crossterm::{
    event::{self, Event, KeyCode, KeyEvent},
};
use std::sync::{Arc, Mutex};
use namenode::name_node_client::NameNodeClient;
use namenode::{ReadFileRequest, ReadFileResponse, WriteFileRequest, WriteFileResponse, BlockSizeRequest, BlockSizeResponse, AssignBlocksForFileRequest, AssignBlocksForFileResponse};
mod namenode{
    tonic::include_proto!("namenode");
}

const DATA_DIR: &str = ".data";
const HISTORY_FILE: &str = ".history";
const CURSOR_OFFSET: usize = 6;

macro_rules! rprintln {
    () => {
        {
            print!("\r\n");
            io::stdout().flush().unwrap();
        }
    };
    ($($arg:tt)*) => {
        {
            print!($($arg)*);
            print!("\r\n");  // Ensures the \r\n is always printed
            io::stdout().flush().unwrap();
        }
    };
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("{}", style(AnsiStyle::BoldHighIntensityText, AnsiColor::Green, "Distributed File System Client starting...\n"));
    fs::create_dir_all(DATA_DIR).unwrap();
    let history_path = Path::new(DATA_DIR).join(HISTORY_FILE);
    let history_file = fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(&history_path)
        .unwrap();
    let mut input = String::new();
    let mut history = load_history(&history_path);
    let mut graceful_exit = 0;
    let default_panic = std::panic::take_hook();
    {   // extra scope to drop stdout before exiting
    let stdout = Arc::new(Mutex::new(std::io::stdout().into_raw_mode().unwrap()));
    let cls_stdout = Arc::clone(&stdout);
    std::panic::set_hook(Box::new(move |info| {
        let mut stdout = cls_stdout.lock().unwrap();
        stdout.suspend_raw_mode().unwrap();
        stdout.write(b"\n").unwrap();
        default_panic(info);
    }));
    let mut history_index = 0;
    let mut command = String::new();
    loop {
            history = load_history(&history_path);
            print!(
                "\r{} {} ",
                style(AnsiStyle::BoldHighIntensityText, AnsiColor::Cyan, "dfsc"),
                style(AnsiStyle::BoldHighIntensityText, AnsiColor::Yellow, ">")
            );
            stdout.lock().unwrap().flush().unwrap();

            // Capture input one key at a time
            loop {
                if let Event::Key(KeyEvent { code, modifiers, kind, state }) = event::read()? {
                    match code {
                        KeyCode::Char(c) if modifiers.contains(event::KeyModifiers::CONTROL) && (c == 'c' || c == 'd' || c == 'z') => {
                            stdout.lock().unwrap().suspend_raw_mode().unwrap();
                            graceful_exit = 1;
                            break;
                        }
                        KeyCode::Char(c) => {
                            if c.is_ascii() {
                                input.push(c);
                                print!("{}", c);
                                stdout.lock().unwrap().flush().unwrap();
                            }
                        }
                        KeyCode::Backspace => {
                            if !input.is_empty() {
                                input.pop();
                                print!("\x08 \x08"); // \x08 is the backspace character, two times because we want to move the cursor back twice
                                stdout.lock().unwrap().flush().unwrap();
                            }
                        }
                        KeyCode::Delete => { // Delete is the same as backspace  but in opposite direction
                            if !input.is_empty() {
                                input.remove(0);
                                print!("\x7f");
                                stdout.lock().unwrap().flush().unwrap();
                            }
                        }
                        KeyCode::Left => {
                            if !input.is_empty() {
                                print!("\x1b[D");
                                stdout.lock().unwrap().flush().unwrap();
                            }
                        }
                        KeyCode::Right => {
                            if !input.is_empty() {
                                // print!("\x08");
                                print!("\x1b[C");
                                stdout.lock().unwrap().flush().unwrap();
                            }
                        }
                        KeyCode::Up => { // scroll through history
                            if history_index > 0 {
                                history_index -= 1;
                                if let Some(previous_command) = history.get(history_index) {
                                    // Clear the current input
                                    for _ in 0..input.len() {
                                        print!("\x08 \x08");
                                    }
                                    // Replace with the previous command
                                    input = previous_command.clone();
                                    print!("{}", input);
                                }
                            } else {
                                history_index = history.len();
                            }
                            stdout.lock().unwrap().flush().unwrap();
                        }
                        KeyCode::Down => {
                            if history_index < history.len() {
                                history_index += 1;
                                // Some allows us to check if the value is not null
                                if let Some(next_command) = history.get(history_index) {
                                    // Clear the current input
                                    for _ in 0..input.len() {
                                        print!("\x08 \x08");
                                    }
                                    // Replace with the next command
                                    input = next_command.clone();
                                    print!("{}", input);
                                }
                            } else {
                                history_index = 0;
                            }
                            stdout.lock().unwrap().flush().unwrap();
                        }
                        KeyCode::Enter => {
                            command = input.trim().to_string();
                            if !command.is_empty() {
                                add_to_history(&history_path, &command)?;
                            }
                            input.clear();
                            break; // Exit the input loop and print the prompt again
                        }
                        _ => {} // ignore other keys
                    }
                }
            }
            rprintln!("");
            if graceful_exit == 1 {
                break;
            }
            if command == "exit" {
                stdout.lock().unwrap().flush().unwrap();
                stdout.lock().unwrap().suspend_raw_mode().unwrap();
                graceful_exit = 0;
                break;
            } else if command != "" {
                rprintln!("command: {}", command);
                parse_command(&command).await?;
            }

            command.clear();
        }
    }
    
    exit(graceful_exit);
    Ok(())
}


fn exit(graceful: i32) {
    rprintln!("");
    if graceful == 0 {
        println!("{}", style(AnsiStyle::BoldHighIntensityText, AnsiColor::Green, "Until Next Time 👋 👼🏼"));
    } else {
        println!("{}", style(AnsiStyle::BoldHighIntensityText, AnsiColor::Red, "(╯°□°)╯︵ ┻━┻ Exiting disgracefully because you pressed ctrl+c or ctrl+z. Et tu, Brute? 👹"));
    }
    std::process::exit(graceful);
}


async fn parse_command(command: &str) -> Result<(), Box<dyn std::error::Error>> {
    let args = command.split_whitespace().collect::<Vec<&str>>();
    let command = args[0];
    let args = &args[1..];
    match command {
        "exit" => exit(0),
        "ls" => {
            // let mut client = NameNodeClient::connect("http://localhost:50051").await?;
            // let request = tonic::Request::new(ListDirectoryRequest {
            //     path: "/".to_string(),
            // });
            // let response = client.list_directory(request).await?;
            // println!("Response: {:?}", response);
            rprintln!("ls");
        },
        "put" => {
            let filename = args[1].to_string();
            let data = args[2].to_string();
            rprintln!("put {}", filename);
            let mut client = NameNodeClient::connect("http://localhost:50051").await?;
            let request = tonic::Request::new(WriteFileRequest {
                filename: "/".to_string(),
                data: vec![],
                nodes_left: vec![],
            });
            let response = client.write_file(request).await?;
            rprintln!("Response: {:?}", response);
        },
        _ => rprintln!("Unknown command: {}", command),
    }
    Ok(())
}


// add_to_history adds a command to the history file, also checks if the command is already in the history, if it is, it removes the old one, and adds the new one
fn add_to_history(history_path: &PathBuf, command: &str) -> io::Result<()> {

    let mut history = load_history(history_path);
    if let Some(position) = history.iter().position(|x| x == command) {
        history.remove(position);
    }
    history.push(command.to_string());
    
    let mut history_file = fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open(history_path)?;
    
    for cmd in history {
        writeln!(history_file, "{}", cmd)?;
    }
    
    Ok(())
}

fn load_history(history_path: &PathBuf) -> Vec<String> {
    if !history_path.exists() {
        return Vec::new();
    }

    let file = OpenOptions::new().read(true).open(history_path).unwrap();
    let reader = BufReader::new(file);

    reader.lines()
        .filter_map(|line| line.ok())
        .collect::<Vec<String>>()
}