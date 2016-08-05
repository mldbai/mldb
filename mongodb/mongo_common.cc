/**                                                                 -*- C++ -*-
 * mongo_common.cc
 * Jeremy Barnes, 23 February 2015
 * Mich, 2016-08-02
 * This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
 **/
#include "bsoncxx/types/value.hpp"

#include "mldb/core/dataset.h"
#include "mldb/rest/rest_request_router.h"
#include "mldb/sql/expression_value.h"
#include "mldb/types/date.h"

#include "mongo_common.h"

using namespace std;

namespace Datacratic {
namespace MLDB {
namespace Mongo {

const Package & mongodbPackage()
{
    static const Package result("mongodb");
    return result;
}

CellValue bsonToCell(const bsoncxx::types::value & val)
{
    switch (val.type()) {
    case bsoncxx::type::k_undefined:
    case bsoncxx::type::k_null:
        return CellValue{};
    case bsoncxx::type::k_double:
        return CellValue(val.get_double().value);
    case bsoncxx::type::k_utf8:
        return CellValue(val.get_utf8().value.to_string());
    case bsoncxx::type::k_binary: {
        auto bin = val.get_binary();
        return CellValue::blob((const char *)bin.bytes, bin.size);
    }
    case bsoncxx::type::k_oid:
        return CellValue(val.get_oid().value.to_string());
    case bsoncxx::type::k_bool:
        return CellValue(val.get_bool().value);
    case bsoncxx::type::k_date:
        return CellValue(val.get_date().value);
    case bsoncxx::type::k_timestamp:
        return CellValue(val.get_timestamp().timestamp);
    case bsoncxx::type::k_int32:
        return CellValue(val.get_int32().value);
    case bsoncxx::type::k_int64:
        return CellValue(val.get_int64().value);
    case bsoncxx::type::k_symbol:
        return CellValue(val.get_symbol().symbol.to_string());

    case bsoncxx::type::k_regex:
        throw HttpReturnException(500, "BSON regex conversion not done");

    case bsoncxx::type::k_array:
    case bsoncxx::type::k_document:
        ExcAssert(false);

    case bsoncxx::type::k_dbpointer:
    case bsoncxx::type::k_code:
    case bsoncxx::type::k_codewscope:
    case bsoncxx::type::k_maxkey:
    case bsoncxx::type::k_minkey:
        throw HttpReturnException(500, "BSON internal conversions not accepted");
    }

    throw HttpReturnException(500, "Unknown bson expression type");
}

StructValue extract(const Date & ts, const bsoncxx::document::view & doc)
{
    StructValue row;
    for (auto & el: doc) {
        if (el.type() == bsoncxx::type::k_document) {
            row.emplace_back(
                std::move(el.key().to_string()),
                extract(ts, el.get_document().view()));
        }
        else if (el.type() == bsoncxx::type::k_array) {
            row.emplace_back(
                std::move(el.key().to_string()),
                extract(ts, el.get_array()));
        }
        else {
            row.emplace_back(
                std::move(el.key().to_string()),
                ExpressionValue(bsonToCell(el.get_value()), ts));
        }
    }
    return row;
}

StructValue extract(const Date & ts, const bsoncxx::array::view & arr)
{
    int i = 0;
    StructValue row;
    for (auto it = arr.begin(); it != arr.end(); ++it, ++i) {
        bsoncxx::array::element el = *it;
        if (el.type() == bsoncxx::type::k_document) {
            row.emplace_back(i, extract(ts, el.get_document().view()));
        }
        else if (el.type() == bsoncxx::type::k_array) {
            row.emplace_back(i, extract(ts, el.get_array()));
        }
        else {
            row.emplace_back(
                i, ExpressionValue(bsonToCell(el.get_value()), ts));
        }
    }
    return row;
}



} // namespace Mongo
} // namespace MLDB
} // namespace Datacratic
