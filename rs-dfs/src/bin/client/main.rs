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

const DATA_DIR: &str = ".data";
const HISTORY_FILE: &str = ".history";
const CURSOR_OFFSET: usize = 6;

fn main() -> io::Result<()> {
    println!("{}", style(AnsiStyle::BoldHighIntensityText, AnsiColor::Green, "HDFS starting...\n"));
    fs::create_dir_all(DATA_DIR).unwrap();
    let history_path = Path::new(DATA_DIR).join(HISTORY_FILE);
    let history_file = fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(&history_path)
        .unwrap();
    let mut input = String::new();
    let mut history = load_history(&history_path);
    let mut stdout = io::stdout().into_raw_mode()?;
    let mut history_index = 0;
    loop {
        history = load_history(&history_path);
        // Print the prompt
        print!(
            "{} {} ",
            style(AnsiStyle::BoldHighIntensityText, AnsiColor::Cyan, "hdfsc"),
            style(AnsiStyle::BoldHighIntensityText, AnsiColor::Yellow, ">")
        );
        stdout.flush()?;

        // Capture input one key at a time
        loop {
            if let Event::Key(KeyEvent { code, modifiers, kind, state }) = event::read()? {
                match code {
                    KeyCode::Char('c') if modifiers.contains(event::KeyModifiers::CONTROL) => {
                        println!("\rCtrl+C pressed. Exiting...\r");
                        return Ok(());
                    }
                    KeyCode::Char('d') if modifiers.contains(event::KeyModifiers::CONTROL) => {
                        println!("\rCtrl+D pressed. Exiting...\r");
                        return Ok(());
                    }
                    KeyCode::Char(c) => {
                        if c.is_ascii() {
                            input.push(c);
                            print!("{}", c);
                            stdout.flush()?;
                        }
                    }
                    KeyCode::Backspace => {
                        if !input.is_empty() {
                            input.pop();
                            print!("\x08 \x08"); // \x08 is the backspace character, two times because we want to move the cursor back twice
                            stdout.flush()?;
                        }
                    }
                    KeyCode::Delete => { // Delete is the same as backspace  but in opposite direction
                        if !input.is_empty() {
                            input.remove(0);
                            print!("\x7f");
                            stdout.flush()?;
                        }
                    }
                    KeyCode::Left => {
                        if !input.is_empty() {
                            print!("\x1b[D");
                            stdout.flush()?;
                        }
                    }
                    KeyCode::Right => {
                        if !input.is_empty() {
                            // print!("\x08");
                            print!("\x1b[C");
                            stdout.flush()?;
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
                        stdout.flush()?;
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
                        stdout.flush()?;
                    }
                    KeyCode::Enter => {
                        println!();
                        let command = input.trim();
                        if !command.is_empty() {
                            add_to_history(&history_path, command)?;
                        }
                        if command == "exit" {
                            println!();
                            return Ok(());
                        }
                        println!("\rYou entered: {}\r", command);
                        input.clear();
                        break; // Exit the input loop and print the prompt again
                    }
                    _ => {} // ignore other keys
                }
            }
        }
    }
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