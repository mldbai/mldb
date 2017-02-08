 /** cancellation_exception.h                                               -*- C++ -*-
    Guy Dumais, 5 October 2016
    Copyright (c) 2016 mldb.ai inc.  All rights.

    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
    
    Exception class to use to return procedure cancellation.
*/

#pragma once

#include <string>
#include "mldb/arch/exception.h"

namespace MLDB {

struct CancellationException: public SilentException {

CancellationException(const std::string & message);

~CancellationException() throw();

virtual const char * what() const throw();

private:
std::string message;
};

} // namespace MLDB
