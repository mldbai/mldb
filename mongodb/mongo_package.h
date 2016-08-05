/**
 * mongo_package.h
 * Jeremy Barnes, 23 February 2015
 * Mich, 2016-08-02
 * This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
 **/

#pragma once
#include "mldb/rest/poly_entity.h"

namespace Datacratic {
namespace MLDB {

const Package & mongodbPackage();

const static std::string mongoScheme =
    "MongoDB connection scheme. "
    "mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database]]";

} // namespace MLDB
} // namespace Datacratic
