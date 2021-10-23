#pragma once

#include <iostream>

namespace MLDB {
namespace ansi {

extern const bool enable_ansi;

#define ANSI_CODE(name, sequence) \
static constexpr const char ansi_color_##name [] = "\u001b" sequence "m\0"; \
inline std::ostream & name(std::ostream & stream) { if (enable_ansi) stream << ansi_color_##name;  return stream; } \
inline const char * ansi_str_##name() { return enable_ansi ? ansi_color_##name : ""; }

ANSI_CODE(reset, "[0");
ANSI_CODE(bold, "[1");
ANSI_CODE(underline, "[4");
ANSI_CODE(reversed, "[7");

#define ANSI_COLOR(name, sequence) \
ANSI_CODE(name, "[3" sequence) \
ANSI_CODE(bright_##name, "[3" sequence ";1") \
ANSI_CODE(bg_##name, "[4" sequence) \
ANSI_CODE(bg_bright_##name, "[4" sequence ";1")

ANSI_COLOR(black,   "0");
ANSI_COLOR(red,     "1");
ANSI_COLOR(green,   "2");
ANSI_COLOR(yellow,  "3");
ANSI_COLOR(blue,    "4");
ANSI_COLOR(magenta, "5");
ANSI_COLOR(cyan,    "6");
ANSI_COLOR(white,   "7");

} // namespace ansi
} // namespace MLDB