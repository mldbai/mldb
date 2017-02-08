// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* connectfd.h                                                     -*- C++ -*-
   Jeremy Barnes, 5 August 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

*/

#pragma once

#include <string>

namespace MLDB {

/** Utility function to return a socket connected to the given port on
    localhost.
*/
int connectLocalhost(int port);

/** Utility function to return a socket connected to the given port on
    the given host.
*/
int connectHost(const std::string & hostname, int port);

/** Utility function to return a datagram socket connected to the given
    port on the given host.
*/
int connectHostDgram(const std::string & hostname, int port);





} // namespace MLDB
