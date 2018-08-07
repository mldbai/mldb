/** mldb_entity.cc
    Jeremy Barnes, 5 August 2018
    Copyright (c) 2018 mldb.ai inc.  All rights reserved.
*/

#include "mldb_entity.h"
#include "mldb_engine.h"
#include "mldb/rest/rest_entity.h"
#include "mldb/rest/rest_connection.h"

namespace MLDB {

MldbEngine *
MldbEntity::
getOwner(RestDirectory * peer)
{
    // TODO: less expensive way of doing this...
    MldbEngine * result = dynamic_cast<MldbEngine *>(peer);
    if (!result) {
        throw MLDB::Exception("Attempt to use RestDirectory that's not "
                              " an MLDB engine");
    }
    return result;
}

/*****************************************************************************/
/* UTILITY FUNCTIONS                                                         */
/*****************************************************************************/

/** Create a request handler that redirects to the given place for internal
    documentation.
*/
TypeCustomRouteHandler
makeInternalDocRedirect(const Package & package, const Utf8String & relativePath)
{
    return [=] (RestDirectory * server,
                RestConnection & connection,
                const RestRequest & req,
                const RestRequestParsingContext & cxt)
        {
            Utf8String basePath = MldbEntity::getOwner(server)
                ->getPackageDocumentationPath(package);
            connection.sendRedirect(301, (basePath + relativePath).rawString());
            return MR_YES;
        };
}


const Package & builtinPackage()
{
    static const Package result("builtin");
    return result;
}


} // namespace MLDB
