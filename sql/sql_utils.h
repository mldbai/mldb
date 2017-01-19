/** sql_utils.h                                                    -*- C++ -*-
    Jeremy Barnes, 30 November 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Utilities for SQL.
*/

#pragma once

#include "mldb/types/string.h"
#include "regex_helper.h"

namespace MLDB {

struct Path;

Utf8String escapeSql(const Utf8String & str);
std::string escapeSql(const std::string & str);
std::string escapeSql(const char * str);

/** For when we have a variable reference like "x.y" in table x, when
    passing the expression through the table we need to remove it from
    the passed version.  This function does that, removing the table
    name in alias from the beginning of the given variable name.
*/
Path removeTableName(const Utf8String & alias,
                       const Path & variableName);

} // namespace MLDB

    
