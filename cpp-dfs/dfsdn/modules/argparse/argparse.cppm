module;

#ifndef ARGPARSE_MODULE_USE_STD_MODULE
#include "argparse.hpp"
#endif 

export module argparse;

#ifdef ARGPARSE_MODULE_USE_STD_MODULE
import std;
import std.compat;

extern "C++" {
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Winclude-angled-in-module-purview"
#include <argparse/argparse.hpp>
#pragma clang diagnostic pop
}
#endif


export namespace argparse {
    using argparse::nargs_pattern;
    using argparse::default_arguments;
    using argparse::operator&;
    using argparse::Argument;
    using argparse::ArgumentParser;
}