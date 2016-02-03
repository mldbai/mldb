/** builtin_functions.h                                             -*- C++ -*-
    Francois Maillet, 21 janvier 2016
    This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
*/

#pragma once

#include "sql/expression_value.h"
#include "mldb/ext/jsoncpp/value.h"

namespace Datacratic {
namespace MLDB {
namespace Builtins {

void
unpackJson(RowValue & row,
           const std::string & id,
           const Json::Value & val,
           const Date & ts);


} // namespace Builtins
} // namespace MLDB
} // namespace Datacratic
