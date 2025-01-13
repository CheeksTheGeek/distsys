module;
export module ansi;
import <ostream>;
import <map>;

export enum class AnsiColor {
    Black = 0,
    Red = 1,
    Green = 2,
    Yellow = 3,
    Blue = 4,
    Magenta = 5,
    Cyan = 6,
    White = 7,
    Default = 9
};

export enum class AnsiStyle {
    Reset,
    RegText,
    BoldText,
    UnderlineText,
    HighIntensityText,
    BoldHighIntensityText,
    UnderlineHighIntensityText,
    HighIntensityBoldText
};

// Updated the map to hold valid strings for colors and styles
std::map<AnsiStyle, std::string> ansi_styles = {
    {AnsiStyle::Reset, "\x1b[0m"},
    {AnsiStyle::RegText, "\x1b[0;3"},
    {AnsiStyle::BoldText, "\x1b[1;3"},
    {AnsiStyle::UnderlineText, "\x1b[4;3"},
    {AnsiStyle::HighIntensityText, "\x1b[0;9"},
    {AnsiStyle::BoldHighIntensityText, "\x1b[1;9"},
    {AnsiStyle::UnderlineHighIntensityText, "\x1b[4;9"},
    {AnsiStyle::HighIntensityBoldText, "\x1b[1;9"}
};

export {
    std::string Ansi(AnsiStyle style, AnsiColor color = AnsiColor::Default) {
        if (style == AnsiStyle::Reset) {
            return ansi_styles[AnsiStyle::Reset];
        }
        return ansi_styles[style] + std::to_string(static_cast<int>(color)) + "m";
    }
}