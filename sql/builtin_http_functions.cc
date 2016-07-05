/** builtin_http_functions.cc
    Francois Maillet, 4 juillet 2016
    This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.

    Builtin http parsing functions for SQL.
*/

#include "mldb/sql/builtin_functions.h"
#include "mldb/types/url.h"
#include "mldb/types/basic_value_descriptions.h"

using namespace std;


namespace Datacratic {
namespace MLDB {
namespace Builtins {

/*****************************************************************************/
/* HTTP FUNCTIONS                                                            */
/*****************************************************************************/

BoundFunction extract_domain(const std::vector<BoundSqlExpression> & args)
{
    // Return an expression but with the timestamp modified to something else

    if (args.size() != 1 && args.size() != 2)
        throw HttpReturnException(400, "extract_domain function takes 1 or "
                "2 arguments");

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                bool check[] = {false};
                auto assertArg = [&] (size_t field, const string & name)
                    {
                        if (check[field])
                            throw HttpReturnException(400, "Argument " + name + " is specified more than once");
                        check[field] = true;
                    };

                bool removeSubdomain = false;
                if(args.size() == 2) {
                    const ExpressionValue::Structured & argRow =
                        args.at(1).getStructured();

                    for (auto& arg : argRow) {
                        const ColumnName& columnName = std::get<0>(arg);
                        if (columnName == ColumnName("removeSubdomain")) {
                            assertArg(0, "removeSubdomain");
                            removeSubdomain = std::get<1>(arg).asBool();
                        }
                        else {
                            throw HttpReturnException(400, "Unknown argument "
                                    "in extract_domain", "argument", columnName);
                        }
                    }
                }

                if(args[0].getAtom().empty())
                    return ExpressionValue();

                Url url(args[0].getAtom().toUtf8String());

                string return_host = url.host();
                if(removeSubdomain && !url.hostIsIpAddress()) {
                    size_t last_dot = return_host.rfind('.');
                    size_t second_last_dot = return_host.rfind('.', last_dot-1);
                    if(second_last_dot != std::string::npos)
                        return_host = return_host.substr(second_last_dot+1);
                }

                ExpressionValue result(std::move(return_host),
                                       args[0].getEffectiveTimestamp());

                return result;
            },
            std::make_shared<Utf8StringValueInfo>()
            };
}

static RegisterBuiltin registerExtractDomain(extract_domain, "extract_domain");




} // namespace Builtins
} // namespace MLDB
} // namespace Datacratic
