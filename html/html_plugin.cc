/** html_plugin.cc
    Jeremy Barnes, 24 November 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

*/

#include "mldb/core/mldb_entity.h"

namespace MLDB {

const Package & htmlPackage()
{
    static const Package result("html");
    return result;
}

} // namespace MLDB
