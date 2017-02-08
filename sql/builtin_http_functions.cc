/** builtin_http_functions.cc
    Francois Maillet, 4 juillet 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Builtin http parsing functions for SQL.
*/

#include "mldb/sql/builtin_functions.h"
#include "mldb/types/url.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/types/structure_description.h"

using namespace std;



namespace MLDB {
namespace Builtins {

/*****************************************************************************/
/* HTTP FUNCTIONS                                                            */
/*****************************************************************************/

struct ExtractDomainOptions {
    bool removeSubdomain = false;
};

DECLARE_STRUCTURE_DESCRIPTION(ExtractDomainOptions);
DEFINE_STRUCTURE_DESCRIPTION(ExtractDomainOptions);

ExtractDomainOptionsDescription::
ExtractDomainOptionsDescription()
{
    addAuto("removeSubdomain", &ExtractDomainOptions::removeSubdomain,
            "Flag to specify whether or not the subdomain is kept.");
}

BoundFunction extract_domain(const std::vector<BoundSqlExpression> & args)
{
    // Return an expression but with the timestamp modified to something else

    if (args.size() != 1 && args.size() != 2)
        throw HttpReturnException(400, "extract_domain function takes 1 or "
                "2 arguments");

    return {[=] (const std::vector<ExpressionValue> & args,
                 const SqlRowScope & scope) -> ExpressionValue
            {
                ExtractDomainOptions options;
                if (args.size() == 2) {
                    options
                        = jsonDecode<ExtractDomainOptions>(args[1].extractJson());
                }

                auto & val = args[0];
                if(val.empty())
                    return ExpressionValue::null(val.getEffectiveTimestamp());

                Url url(val.getAtom().toUtf8String());

                string return_host = url.host();
                if(options.removeSubdomain && !url.hostIsIpAddress()) {
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

