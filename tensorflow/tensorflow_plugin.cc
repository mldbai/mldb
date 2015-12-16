/** tensorflow_plugin.cc
    Jeremy Barnes, 24 November 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

*/

#include "mldb/core/mldb_entity.h"

namespace Datacratic {
namespace MLDB {

const Package & tensorflowPackage()
{
    static const Package result("tensorflow");
    return result;
}

} // namespace MLDB
} // namespace Datacratic
