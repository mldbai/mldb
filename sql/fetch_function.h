/**                                                                 -*- C++ -*-
 * fetch_function.h
 * Mich, 2016-10-03
 * This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
 **/

#include <string>
#include <vector>
#include <tuple>
#include "path.h"
#include "expression_value.h"

namespace MLDB {

typedef class std::vector<std::tuple<MLDB::PathElement, MLDB::ExpressionValue> > StructValue;

struct FetchFunction {
    static StructValue fetch(const std::string & uri);
};

} // namespace MLDB
