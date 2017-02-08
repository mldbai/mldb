/** tensorflow_additional_builtin.cc
    Mathieu Marquis Bolduc, January 18, 2017
    Copyright (c) 2017 MLDB.ai Inc.  All rights reserved.

*/

#include "mldb/sql/eval_sql.h"
#include "mldb/core/mldb_entity.h"
#include "mldb/core/function.h"
#include "mldb/core/plugin.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/vector_description.h"
#include "mldb/types/enum_description.h"
#include "mldb/types/map_description.h"
#include "mldb/sql/builtin_functions.h"
#include "mldb/types/any_impl.h"

namespace MLDB {

BoundFunction encodePngs(const std::vector<BoundSqlExpression> & args)
{
    checkArgsSize(args.size(), 1);

    if (!args[0].info->couldBeRow())
        throw HttpReturnException(400, "requires a row as input");

    Utf8String query = "tf_EncodePng($1)";

    auto outputInfo
        = std::make_shared<UnknownRowValueInfo>();
    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                SqlBindingScope emptyBindingScope;
                SqlRowScope emptyrowScope;
                ExcAssertEqual(args.size(), 1);
                StructValue encodedValues;

                std::function<bool (const PathElement & columnName,
                                    const ExpressionValue & val)>
                onColumn = [&] (const PathElement & columnName,
                                const ExpressionValue & val)
                {
                    if (!val.isRow())
                        throw HttpReturnException(400, "requires a row as input, got " + val.getTypeAsString());

                    encodedValues.emplace_back(columnName, evalSql(emptyBindingScope, query, emptyrowScope, val));
                    return true;
                };

                args[0].forEachColumn(onColumn);

                return encodedValues;
            },
            outputInfo
        };
}
static Builtins::RegisterBuiltin registerencodePngsFunction(encodePngs, "tf_encodePngs");

} // namespace MLDB