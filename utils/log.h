/* log.h                                                           -*- C++ -*-
   Guy Dumais, 29 January 2016

   This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.

   Logging interface.
*/

#pragma once

#include "mldb/ext/spdlog/include/spdlog/spdlog.h"

namespace spdlog {
    class logger;
}

namespace Datacratic {

namespace MLDB {

std::shared_ptr<spdlog::logger> getQueryLog();
std::shared_ptr<spdlog::logger> getMldbLog();
std::shared_ptr<spdlog::logger> getServerLog();

} // MLDB

} // Datacratic
