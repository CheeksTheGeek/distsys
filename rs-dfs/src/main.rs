mod ansi;
use ansi::{AnsiStyle, AnsiColor, ansi};

// Usage example:
fn main() {
    println!(
        "{}Hello, World!{}",
        ansi(AnsiStyle::BoldText, AnsiColor::Red),
        ansi(AnsiStyle::Reset, AnsiColor::Default)
    );
}