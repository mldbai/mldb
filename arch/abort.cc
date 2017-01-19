// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** abort.cc                                 -*- C++ -*-
    Rémi Attab, 13 Nov 2012
    Copyright (c) 2012 mldb.ai inc.  All rights reserved.

    Utilities related to the abort() function

*/

#include "abort.h"

#include <cstdlib>
#include <iostream>

using namespace std;


namespace MLDB {

namespace {


/******************************************************************************/
/* COMPILE SETTING                                                            */
/******************************************************************************/

#ifndef MLDB_ABORT
#   define MLDB_ABORT 0
#else
#   undef MLDB_ABORT
#   define MLDB_ABORT 1
#endif

enum { COMPILE_STATE = MLDB_ABORT };


/******************************************************************************/
/* ABORT STATE                                                                */
/******************************************************************************/

struct AbortState {

    AbortState() :
        state(COMPILE_STATE)
    {
        state = state || getenv("MLDB_ABORT") != NULL;
    }

    bool state;
} staticState;

}; // namespace anonymous


/******************************************************************************/
/* INTERFACE                                                                  */
/******************************************************************************/

void do_abort()
{
    if (staticState.state) abort();
}

bool get_abort_state()
{
    return staticState.state;
}

void set_abort_state(bool b)
{
    staticState.state = b;
}



} // namepsace MLDB
