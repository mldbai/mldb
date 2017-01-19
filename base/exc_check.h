/* exc_check.h                                                    -*- C++ -*-
   Rémi Attab, 24 Febuary 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Quick and easy way to throw an exception on a failed condition.

   Note that when a check fails, the macro will issue a call to ML::do_abort().
   This makes the macros more debugger friendly in certain situations.

*/


#pragma once

#include "mldb/arch/abort.h"
#include "mldb/arch/exception.h"
#include "mldb/arch/format.h"
#include <sstream>
#include <errno.h>

/// Throws a formatted exception if the condition is false.
#define ExcCheckImpl(condition, message, exc_type)                      \
    do {                                                                \
        if (!(condition)) {                                             \
            std::string msg__ =                                         \
                MLDB::format("%s: %s", message, #condition);              \
            MLDB::do_abort();                                             \
            throw exc_type(msg__.c_str(), __PRETTY_FUNCTION__,          \
                    __FILE__, __LINE__);                                \
        }                                                               \
    } while (0)

/// Check that the two values meet the operand.  
/// They must not have any side effects as they may be evaluated more than once.
#define ExcCheckOpImpl(op, value1, value2, message, exc_type)           \
    do {                                                                \
        if (!((value1) op (value2))) {                                  \
            std::ostringstream stream1__, stream2__;                    \
            stream1__ << value1;  stream2__ << value2;                  \
            std::string v1__ = stream1__.str();                         \
            std::string v2__ = stream2__.str();                         \
            std::string msg__ = MLDB::format(                             \
                    "%s: !(%s " #op " %s) [!(%s " #op " %s)]",          \
                    message, #value1, #value2, v1__.c_str(),            \
                    v2__.c_str());                                      \
            MLDB::do_abort();                                             \
            throw exc_type(msg__.c_str(), __PRETTY_FUNCTION__,          \
                                        __FILE__, __LINE__);            \
        }                                                               \
    } while (0)

/// Throws a formatted exception if the condition is false.
#define ExcCheckErrnoImpl(condition, message, exc_type)                 \
    do {                                                                \
        if (!(condition)) {                                             \
            std::string msg__ = MLDB::format("%s: %s(%d)",                \
                    message, strerror(errno), errno);                   \
            MLDB::do_abort();                                             \
            throw exc_type(msg__.c_str(), __PRETTY_FUNCTION__,          \
                    __FILE__, __LINE__);                                \
        }                                                               \
    } while (0)


/// Simple forwarders with the right exception type.
#define ExcCheck(condition, message)                    \
        ExcCheckImpl(condition, message, ::MLDB::AssertionFailure)

#define ExcCheckOp(op, value1, value2, message)                         \
        ExcCheckOpImpl(op, value1, value2, message, ::MLDB::AssertionFailure)

#define ExcCheckErrno(condition, message)       \
        ExcCheckErrnoImpl(condition, message, ::MLDB::AssertionFailure)


/// see ExcCheckOpImpl for more details
#define ExcCheckEqual(value1, value2, message)  \
    ExcCheckOp(==, value1, value2, message)

#define ExcCheckNotEqual(value1, value2, message)       \
    ExcCheckOp(!=, value1, value2, message)

#define ExcCheckLessEqual(value1, value2, message)      \
    ExcCheckOp(<=, value1, value2, message)

#define ExcCheckLess(value1, value2, message)   \
    ExcCheckOp(<, value1, value2, message)

#define ExcCheckGreaterEqual(value1, value2, message)   \
    ExcCheckOp(>=, value1, value2, message)

#define ExcCheckGreater(value1, value2, message)        \
    ExcCheckOp(>, value1, value2, message)

