 /** cancellation_exception.h                                               -*- C++ -*-
    Guy Dumais, 5 October 2016
    Copyright (c) 2016 Datacratic Inc.  All rights.

    This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
    
    Exception class to use to return procedure cancellation.
*/

#pragma once

#include "mldb/arch/exception.h"
#include "mldb/types/any.h"
#include "mldb/types/string.h"

namespace MLDB {

struct CancellationException: public Exception {
    CancellationException(const Utf8String & message)
        : Exception(message.rawData()), message(message)
    {
    }

    ~CancellationException() throw ()
    {
    }

    Utf8String message;
};

} // namespace MLDB
