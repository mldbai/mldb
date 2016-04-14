/* log.h                                                           -*- C++ -*-
   Guy Dumais, 29 January 2016

   This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.

   Logging interface.
*/

#include "log.h"
#include "mldb/utils/config.h"
#include "mldb/arch/exception.h"

namespace {
    spdlog::level::level_enum stringToLevel(const std::string & level) {
        if (level == "debug")
            return spdlog::level::debug;
        else if (level == "info")
            return spdlog::level::info;
        else if (level == "warning")
            return spdlog::level::warn;
        else if (level == "error")
            return spdlog::level::err;
        else 
            throw ML::Exception("Unknown level '" + level 
                                + "' expected one of \"debug\", \"info\", \"warning\" or \"error\"");
    }
}

namespace Datacratic {

namespace MLDB {

static constexpr char const * timestampFormat = "%Y-%m-%dT%T.%e%z";

std::shared_ptr<spdlog::logger> getConfiguredLogger(const std::string & name, const std::string & format) {
    std::shared_ptr<spdlog::logger> logger = spdlog::stdout_logger_mt(name);
    logger->set_pattern(format);
    return logger;
}

std::shared_ptr<spdlog::logger> getQueryLog() {
    static std::shared_ptr<spdlog::logger> queryLog = 
        getConfiguredLogger("query-log", std::string("Q [") + timestampFormat + "] %l %v");
    return queryLog;
}

std::shared_ptr<spdlog::logger> getMldbLog(const std::string & loggerName) {
    auto logger = spdlog::get(loggerName);
    if (!logger) {
        auto config = Config::get();
        std::string level = config ? config->getString("logging." + loggerName + ".level", "info") : "info";
        std::string className = loggerName.substr(loggerName.find_last_of(':') + 1);
        logger = getConfiguredLogger(loggerName, className + std::string(" [") + timestampFormat + "] %l %v");
        logger->set_level(stringToLevel(level));
    }
    return logger;
}


std::shared_ptr<spdlog::logger> getServerLog() {
    static std::shared_ptr<spdlog::logger> serverLog = 
        getConfiguredLogger("server-log", std::string("A [") + timestampFormat + "] %v");
    return serverLog;
}

// force gcc to export these types
void dummy() {
    (void)getQueryLog();
    (void)getMldbLog("dummy");
    (void)getServerLog();
}

} // MLDB

} // Datacratic
