/* log_fwd.h                                                           -*- C++ -*-
   Guy Dumais, 29 January 2016

   This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

   Logging interface.
*/

#pragma once

#include "mldb/arch/demangle.h"
#include <memory>

namespace spdlog {
    class logger;
    namespace details {
        class line_logger;
    }
}

namespace MLDB {

std::shared_ptr<spdlog::logger> getQueryLog();
std::shared_ptr<spdlog::logger> getMldbLog(const std::string & loggerName);
std::shared_ptr<spdlog::logger> getServerLog();

template <typename Class>
std::string
getLoggerNameFromClass() {
    return demangle(typeid(Class));
}

template <typename Class>
std::shared_ptr<spdlog::logger>
getMldbLog() {
    return getMldbLog(getLoggerNameFromClass<Class>());
}

/** Trick to create a void returning function with proper precedence.

    The operator & has a lower precedence than the stream operator.
    So the stream parameters are first pass to the logger and then the
    logger is passed to the dummy void returning function.
    Note that the void returning function is required so that both sides
    of the ternary operator have the same type.
*/
struct LogDummy {
    void operator&(const spdlog::details::line_logger & line_logger) {}
};

#define TRACE_MSG(logger)                                               \
    !logger->should_log(spdlog::level::trace) ? (void) 0 : MLDB::LogDummy() & logger->trace()

#define DEBUG_MSG(logger)                                               \
    !logger->should_log(spdlog::level::debug) ? (void) 0 : MLDB::LogDummy() & logger->debug()

#define INFO_MSG(logger)                                                \
    !logger->should_log(spdlog::level::info) ? (void) 0 : MLDB::LogDummy() & logger->info()

#define WARNING_MSG(logger)                                             \
    !logger->should_log(spdlog::level::warn) ? (void) 0 : MLDB::LogDummy() & logger->warn()

#define ERROR_MSG(logger)                                               \
    !logger->should_log(spdlog::level::err) ? (void) 0 :  MLDB::LogDummy() & logger->error()

} // MLDB
