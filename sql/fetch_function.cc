/**                                                                 -*- C++ -*-
 * fetch_function.cc
 * Mich, 2016-10-03
 * This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
 **/

#include "fetch_function.h"

#include "builtin_functions.h"
#include "sql_expression.h"
#include "expression_value.h"
#include "cell_value.h"
#include "expression_value.h"
#include "mldb/compiler/compiler.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/vector_description.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/vfs/fs_utils.h"



using namespace std;

namespace MLDB {

StructValue
FetchFunction::
fetch(const string & url)
{
    StructValue result;
    auto content = ExpressionValue::null(Date::notADate());
    auto error = ExpressionValue::null(Date::notADate());
    try {
        filter_istream stream(url, { { "mapped", "true" } });

        FsObjectInfo info = stream.info();

        const char * mappedAddr;
        size_t mappedSize;
        std::tie(mappedAddr, mappedSize) = stream.mapped();

        CellValue blob;
        if (mappedAddr) {
            blob = CellValue::blob(mappedAddr, mappedSize);
        }
        else {
            std::ostringstream streamo;
            streamo << stream.rdbuf();
            blob = CellValue::blob(streamo.str());
        }
        content = ExpressionValue(std::move(blob),
                                  info.lastModified);
    }
    JML_CATCH_ALL {
        error = ExpressionValue(ML::getExceptionString(),
                                Date::now());
    }
    result.emplace_back("content", content);
    result.emplace_back("error", error);
    return result;
}

namespace Builtins {

BoundFunction fetcher(const std::vector<BoundSqlExpression> & args)
{
    MLDB::checkArgsSize(args.size(), 1);

    vector<KnownColumn> columnsInfo;
    columnsInfo.emplace_back(Path("content"), make_shared<BlobValueInfo>(),
                             ColumnSparsity::COLUMN_IS_DENSE);
    columnsInfo.emplace_back(Path("error"), make_shared<StringValueInfo>(),
                             ColumnSparsity::COLUMN_IS_DENSE);
    auto outputInfo
        = std::make_shared<RowValueInfo>(columnsInfo);
    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                return FetchFunction::fetch(args[0].toString());
            },
            outputInfo
        };
}
static RegisterBuiltin registerFetcherFunction(fetcher, "fetch");

} // namespace Builtins

} // namespace MLDB
