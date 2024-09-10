#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AnsiColor {
    Black = 0,
    Red = 1,
    Green = 2,
    Yellow = 3,
    Blue = 4,
    Magenta = 5,
    Cyan = 6,
    White = 7,
    Default = 9,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AnsiStyle {
    Reset,
    RegText,
    BoldText,
    UnderlineText,
    HighIntensityText,
    BoldHighIntensityText,
    UnderlineHighIntensityText,
}

impl AnsiStyle {
    pub fn to_code(&self) -> &'static str {
        match self {
            AnsiStyle::Reset => "\x1b[0m",
            AnsiStyle::RegText => "\x1b[0;3",
            AnsiStyle::BoldText => "\x1b[1;3",
            AnsiStyle::UnderlineText => "\x1b[4;3",
            AnsiStyle::HighIntensityText => "\x1b[0;9",
            AnsiStyle::BoldHighIntensityText => "\x1b[1;9",
            AnsiStyle::UnderlineHighIntensityText => "\x1b[4;9",
        }
    }
}

impl AnsiColor {
    pub fn to_code(&self) -> String {
        (*self as u8).to_string()
    }
}

pub fn ansi(style: AnsiStyle, color: AnsiColor) -> String {
    if style == AnsiStyle::Reset {
        style.to_code().to_string()
    } else {
        format!("\x1b[{}{}m", style.to_code(), color.to_code())
    }
}

pub fn style(style: AnsiStyle, color: AnsiColor, text: &str) -> String {
    return format!("{}{}{}", ansi(style, color), text, ansi(AnsiStyle::Reset, AnsiColor::Default));
}
