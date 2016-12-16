/* cancellation_exception.cc                                                -*- C++ -*-
   Guy Dumais, 13 October 2016

   Copyright (c) 2016 Datacratic Inc.  All rights reserved.

   This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
*/

#include "cancellation_exception.h"

namespace MLDB {
    
CancellationException::CancellationException(const std::string & message)
    : message(message)
{
}

CancellationException::~CancellationException() throw()
{
}

const char * CancellationException::what() const throw()
{
    return message.c_str();
}

} // namespace MLDB
