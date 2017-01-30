/** sql_utils.cc
    Jeremy Barnes, 30 November 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "sql_utils.h"
#include "path.h"
#include "http/http_exception.h"
#include <iostream>


using namespace std;


namespace MLDB {

Utf8String escapeSql(const Utf8String & str)
{
    return Utf8String(escapeSql(str.rawString()), false /* check */);
}

std::string escapeSql(const std::string & str)
{
    std::string result;
    result.reserve(str.length() + 16);
    result += '\'';
    for (auto & c: str) {
        if (!c)
            throw HttpReturnException(400, "SQL string contains embedded nul");
        if (c == '\'') {
            result += '\'';
        }
        result += c;
    }
    result += '\'';
    return result;
}

std::string escapeSql(const char * str)
{
    return escapeSql(std::string(str));
}

//In a single-dataset context
//If we know the alias of the dataset we are working on, this will remove it from the variable's name
//It will also verify that the variable identifier does not explicitly specify an dataset that is not of this context.
//Aka "unknown".identifier
Path
removeTableName(const Utf8String & alias, const Path & columnName)
{    
    if (!alias.empty() && columnName.size() > 1 && columnName.startsWith(alias)) {
        return columnName.removePrefix();
    }
    return columnName;
}

} // namespace MLDB

    
