/** sql_expression_extractors.h                                         -*- C++ -*-
    Guy Dumais, 18 December 2015

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Helper functions to extra an expression from a SQL parse tree.
*/

#pragma once

namespace Datacratic {
namespace MLDB {

/**  Iterates over the select sub expressions of type ComputedColumn to match a given named.
 *   Returns the matched expression or nullptr if there are no matches.
*/
inline std::shared_ptr<ComputedColumn>
extractNamedSubSelect (const Utf8String & name, const SelectExpression & select) 
{
    for (const auto & clause : select.clauses) {
        auto computedVariable = std::dynamic_pointer_cast<ComputedColumn>(clause);
        if (computedVariable
            && computedVariable->alias.size() == 1
            && computedVariable->alias[0] == name)
            return computedVariable;
    }
    return nullptr;
}
    
} // namespace MLDB
} // namespace Datacratic
