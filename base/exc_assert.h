/* exc_assert.h                                                    -*- C++ -*-
   Jeremy Barnes, 15 July 2010
   Copyright (c) 2010 mldb.ai inc.  All rights reserved.
   Copyright (c) 2010 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Simple functionality to include asserts that throw exceptions rather than
   abort the program.
*/

#pragma once

#include "mldb/arch/exception.h"
#include "mldb/base/exc_check.h"

/// Simple forwarders with the right exception type.
/// These will eventually be disable-able using a compiler switch.
#define ExcAssert(condition)                    \
    ExcCheckImpl(condition, "Assert failure", ::MLDB::AssertionFailure)

#define ExcAssertOp(op, value1, value2)         \
    ExcCheckOpImpl(op, value1, value2, "Assert failure", ::MLDB::AssertionFailure)

#define ExcAssertErrno(condition)               \
    ExcCheckErrnoImpl(condition, "Assert failure", ::MLDB::AssertionFailure)

/// see ExcCheckOpImpl for more details
#define ExcAssertEqual(value1, value2)          \
    ExcAssertOp(==, value1, value2)

#define ExcAssertNotEqual(value1, value2)       \
    ExcAssertOp(!=, value1, value2)

#define ExcAssertLessEqual(value1, value2)      \
    ExcAssertOp(<=, value1, value2)

#define ExcAssertLess(value1, value2)           \
    ExcAssertOp(<, value1, value2)

#define ExcAssertGreaterEqual(value1, value2)   \
    ExcAssertOp(>=, value1, value2)

#define ExcAssertGreater(value1, value2)        \
    ExcAssertOp(>, value1, value2)


