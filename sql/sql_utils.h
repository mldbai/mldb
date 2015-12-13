/** sql_utils.h                                                    -*- C++ -*-
    Jeremy Barnes, 30 November 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    Utilities for SQL.
*/

#pragma once

#include "mldb/types/string.h"

namespace Datacratic {
namespace MLDB {

Utf8String escapeSql(const Utf8String & str);
std::string escapeSql(const std::string & str);
std::string escapeSql(const char * str);


} // namespace MLDB
} // namespace Datacratic
    
