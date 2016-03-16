/** sql_utils.cc
    Jeremy Barnes, 30 November 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
*/

#include "sql_utils.h"
#include "http/http_exception.h"

namespace Datacratic {
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
Utf8String
removeTableName(const Utf8String & alias, const Utf8String & variableName)
{    
    Utf8String shortVariableName = variableName;

    if (!alias.empty() && !variableName.empty()) {

        Utf8String aliasWithDot = alias + ".";

        if (!shortVariableName.removePrefix(aliasWithDot)) {

            //check if there is a quoted prefix
            auto it1 = shortVariableName.begin();
            if (*it1 == '\"')
            {
                ++it1;
                auto end1 = shortVariableName.end();
                auto it2 = alias.begin(), end2 = alias.end();

                while (it1 != end1 && it2 != end2 && *it1 == *it2) {
                    ++it1;
                    ++it2;
                }

                if (it2 == end2 && it1 != end1 && *(it1) == '\"') {

                    shortVariableName.erase(shortVariableName.begin(), it1);
                    it1 = shortVariableName.begin();
                    end1 = shortVariableName.end();

                    //if the context's dataset name is specified we requite a .identifier to follow
                    if (it1 == end1 || *(++it1) != '.') 
                        throw HttpReturnException(400, "Expected a dot '.' after table name " + alias);
                    else
                        shortVariableName.erase(shortVariableName.begin(), ++it1);
                }
                else {

                    //no match, but return an error on an unknown explicit table name.
                    while (it1 != end1) {
                        if (*it1 == '\"') {
                            if (++it1 != end1)
                            {
                                Utf8String datasetName(shortVariableName.begin(), it1);
                                throw HttpReturnException(400, "Unknown dataset '" + datasetName + "'");
                            }  
                        }
                        else {
                            ++it1;
                        }
                    }

                }
            }  
        }
    }   

    return std::move(shortVariableName); 
}


} // namespace MLDB
} // namespace Datacratic
    
