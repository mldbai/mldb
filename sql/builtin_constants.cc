/** builtin_constants.cc
    Jeremy Barnes, 3 October 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Builtin constants for SQL.
*/

#include "mldb/sql/builtin_functions.h"
#include <cmath>

namespace MLDB {
namespace Builtins {

static RegisterBuiltinConstant registerPi("pi", M_PI);
static RegisterBuiltinConstant registerE("e", M_E);

} // namespace Builtins
} // namespace MLDB
