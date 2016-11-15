/** sql_utils.h                                                    -*- C++ -*-
    Jeremy Barnes, 30 November 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Utilities for SQL.
*/

#pragma once

#include "mldb/types/string.h"
#include "regex_helper.h"

namespace MLDB {

struct Path;

/** These three escape the given string to an SQL string
    name, by replacing any single quotes with their doubled versions.
*/
Utf8String escapeSql(const Utf8String & str);
std::string escapeSql(const std::string & str);
std::string escapeSql(const char * str);

/** These three escape the given string to an SQL variable (or table)
    name, by replacing any double quotes with their doubled versions.
*/
Utf8String escapeSqlVar(const Utf8String & str);
std::string escapeSqlVar(const std::string & str);
std::string escapeSqlVar(const char * str);

/** For when we have a variable reference like "x.y" in table x, when
    passing the expression through the table we need to remove it from
    the passed version.  This function does that, removing the table
    name in alias from the beginning of the given variable name.
*/
Path removeTableName(const Utf8String & alias,
                       const Path & variableName);

} // namespace MLDB

    
