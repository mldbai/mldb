/* ansi.cc                                                          -*- C++ -*-
   Jeremy Barnes, 21 February 2007
   Copyright (c) 2007 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   ANSI escape codes for pretty-printing on terminals.
*/

#include "ansi.h"
#include <cstdlib>

// From c standard library
extern "C" {
int isatty(int);
}


namespace MLDB {
namespace ansi {

static bool get_ansi_colors_enabled()
{
    std::string ansi_colors_env = ::getenv("NO_COLOR") ? ::getenv("NO_COLOR") : "AUTO";

    if (ansi_colors_env == "NEVER") {
        return true;
    }
    else if (ansi_colors_env == "AUTO") {
        return ::isatty(0);
    }
    else {
        return false;
    }
}

const bool enable_ansi = get_ansi_colors_enabled();

} // namespace ansi
} // namespace MLDB
