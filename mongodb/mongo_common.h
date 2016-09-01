/**                                                                 -*- C++ -*-
 * mongo_common.h
 * Jeremy Barnes, 23 February 2015
 * Mich, 2016-08-02
 * This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
 **/

#pragma once
#include "bsoncxx/builder/stream/document.hpp"

#include "sql/sql_expression.h"
#include "server/dataset_context.h"

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
void validateConnectionScheme(const std::string & connectionScheme);
void validateCollection(const std::string & collection);

struct MongoRowScope : SqlRowScope {
    MongoRowScope(const ExpressionValue & expr)
        : expr(expr) {}
    const ExpressionValue & expr;
};

struct MongoScope : SqlExpressionMldbScope {

    MongoScope(MldbServer * server) : SqlExpressionMldbScope(server){}

    ColumnGetter doGetColumn(const Utf8String & tableName,
                             const ColumnName & columnName) override;

    GetAllColumnsOutput
    doGetAllColumns(const Utf8String & tableName,
                    const ColumnFilter & keep) override;

    BoundFunction
    doGetFunction(const Utf8String & tableName,
                  const Utf8String & functionName,
                  const std::vector<BoundSqlExpression> & args,
                  SqlBindingScope & argScope) override;
};



} // namespace Mongo
} // namespace MLDB
} // namespace Datacratic
