// 

/** abort.h                                 -*- C++ -*-
    Rémi Attab, 13 Nov 2012
    Copyright (c) 2012 mldb.ai inc.  All rights reserved.

    Utilities related to the abort() function.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    These functions are meant to be used as debugging helpers so that the
    program can be stopped as soon as an error is detected. This is mainly
    useful if we can't use "catch throw" in gdb because it would yield too many
    false positives. Instead we just sprinkle several calls to do_abort() and
    let gdb break on SIGABRT.

*/

#pragma once

namespace MLDB {

/** Calls abort() if one of the following criterias are met:

    - The environment variable MLDB_ABORT is set.
    - The macro MLDB_ABORT is defined.
    - set_abort_state(true) is called.

    Note that the value passed to set_abort_state() will override all other
    settings. Also note that the environmnent variable is only read once at
    startup in a static constructor.

 */
void do_abort();

/** If false, do_abort() will not call abort(). If true, the oposite happens. */
bool get_abort_state();

/** Overides the current behaviour of the do_abort() function. */
void set_abort_state(bool b);


} // MLDB
