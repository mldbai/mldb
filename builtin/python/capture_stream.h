/** capture_stream.h                                               -*- C++ -*-
    Jeremy Barnes, 17 August 2018
    Copyright (c) 2018 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2018 mldb.ai inc. All rights reserved.

    Functionality to capture Python streams.
*/

#pragma once

#include <functional>
#include <string>
#include <memory>
#include "python_interpreter.h"

namespace MLDB {

typedef std::function<void(const EnterThreadToken &, std::string)> stdstream_write_type;

/** Capture the output of the standard stream of the given name,
    and cause the given lambda to be called whenever it is
    written to.

    \param token      Token that proves that we are in the context of a thread
                      and have acquired the GIL
    \param write      Function to be called when something is written to the
                      stream
    \param streamName Stream to be captured.  Normally this will be 'stdout'
                      or 'stderr'.
*/
std::shared_ptr<const void>
setStdStream(const EnterThreadToken & token,
             stdstream_write_type write,
             const std::string & streamName);

} // namespace MLDB
