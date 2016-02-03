/* log.h                                                           -*- C++ -*-
   Guy Dumais, 29 January 2016

   This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.

   Logging interface.
*/

#include "log.h"

namespace Datacratic {

namespace MLDB {

static std::shared_ptr<spdlog::logger> getQueryLog() {
    static std::shared_ptr<spdlog::logger> queryLog = spdlog::stderr_logger_mt("query-log");
    return queryLog;
}

static std::shared_ptr<spdlog::logger> getMldbLog() {
    static std::shared_ptr<spdlog::logger> mldbLog = spdlog::stderr_logger_mt("mldb");
    return mldbLog;
}

static std::shared_ptr<spdlog::logger> getServerLog() {
    static std::shared_ptr<spdlog::logger> serverLog = spdlog::stderr_logger_mt("server-log");
    return serverLog;
}
 
void dummy() {
    (void)getQueryLog();
    (void)getMldbLog();
    (void)getServerLog();
}

} // MLDB

} // Datacratic
