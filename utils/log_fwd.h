/* log.h                                                           -*- C++ -*-
   Guy Dumais, 29 January 2016

   This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.

   Logging interface.
*/

#pragma once

#include "mldb/arch/demangle.h"
#include <memory>

namespace spdlog {
    class logger;
}

namespace Datacratic {

namespace MLDB {

std::shared_ptr<spdlog::logger> getQueryLog();
std::shared_ptr<spdlog::logger> getMldbLog(const std::string & loggerName);
std::shared_ptr<spdlog::logger> getServerLog();

template <typename Class> 
std::string 
getLoggerNameFromClass() {
    std::string demangledName = ML::demangle(typeid(Class));
    return demangledName.substr(demangledName.find_last_of(':') + 1);
}
    
template <typename Class> 
std::shared_ptr<spdlog::logger> 
getMldbLog() {
    return getMldbLog(getLoggerNameFromClass<Class>());
}

} // MLDB

} // Datacratic
