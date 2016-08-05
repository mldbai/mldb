/**                                                                 -*- C++ -*-
 * mongo_common.h
 * Jeremy Barnes, 23 February 2015
 * Mich, 2016-08-02
 * This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
 **/

#pragma once
#include "bsoncxx/builder/stream/document.hpp"

namespace Datacratic {

struct Date;

namespace MLDB {

struct CellValue;
struct PathElement;
struct ExpressionValue;
typedef std::vector<std::tuple<PathElement, ExpressionValue>> StructValue;

namespace Mongo {

const Package & mongodbPackage();

const static std::string mongoScheme =
    "MongoDB connection scheme. "
    "mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database]]";

CellValue bsonToCell(const bsoncxx::types::value & val);
StructValue extract(const Date & ts, const bsoncxx::document::view & doc);
StructValue extract(const Date & ts, const bsoncxx::array::view & arr);

} // namespace Mongo
} // namespace MLDB
} // namespace Datacratic
