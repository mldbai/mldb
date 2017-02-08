// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** print_utils.h                                 -*- C++ -*-
    Rémi Attab, 14 May 2013
    Copyright (c) 2013 mldb.ai inc.  All rights reserved.

    Useful print out utilities.

*/

#pragma once

#include <sys/types.h>
#include <unistd.h>

#include "mldb/arch/format.h"


namespace MLDB {

/******************************************************************************/
/* PRINT UTILITIES                                                            */
/******************************************************************************/

inline std::string
printElapsed(double elapsed)
{
    static const std::string scaleIndicators = "smunpf?";

    size_t i = 0;
    while (elapsed < 1.0 && i < (scaleIndicators.size() - 1)) {
        elapsed *= 1000.0;
        i++;
    }

    return MLDB::format("%6.2f%c", elapsed, scaleIndicators[i]);
}

inline std::string
printValue(double value)
{
    static const std::string scaleIndicators = " kmgth?";

    size_t i = 0;
    while (value >= 1000.0 && i < (scaleIndicators.size() - 1)) {
        value /= 1000.0;
        i++;
    }

    return MLDB::format("%6.2f%c", value, scaleIndicators[i]);
}

inline std::string
printPct(double value)
{
    return MLDB::format("%6.2f%%", value * 100.0);
}


/******************************************************************************/
/* RANDOM STRING                                                              */
/******************************************************************************/

/** Generates an easily printable random string of a given size. */
inline std::string
randomString(size_t size)
{
    const std::string chars =
        "0123456789"
        "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    /* provide some randomness based on time and pid */
    static struct AtInit {
        AtInit()
        {
            ::srandom((::getpid() << 16 | 0xffff) ^ ::time(NULL));
        }
    } atInit;

    std::string s;
    for (size_t i = 0; i < size; ++i)
        s += chars[random() % chars.size()];
    return s;
}


} // namespace MLDB
