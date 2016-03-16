/** sql_utils.h                                                    -*- C++ -*-
    Jeremy Barnes, 30 November 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Utilities for SQL.
*/

#pragma once

#include "mldb/types/string.h"

namespace Datacratic {
namespace MLDB {

Utf8String escapeSql(const Utf8String & str);
std::string escapeSql(const std::string & str);
std::string escapeSql(const char * str);

bool matchSqlFilter(const Utf8String& valueString, const Utf8String& filterString);

} // namespace MLDB
} // namespace Datacratic
    
