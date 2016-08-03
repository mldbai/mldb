/**
 * mongo_package.cc
 * Jeremy Barnes, 23 February 2015
 * Mich, 2016-08-02
 * This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
 **/

#include "mongo_package.h"

namespace Datacratic {
namespace MLDB {

const Package & mongodbPackage()
{
    static const Package result("mongodb");
    return result;
}

} // namespace MLDB
} // namespace Datacratic
