/** mldb_entity.cc
    Jeremy Barnes, 5 August 2018
    Copyright (c) 2018 mldb.ai inc.  All rights reserved.
*/

#include "mldb_entity.h"
#include "mldb_engine.h"
#include "mldb/rest/rest_entity.h"

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

} // namespace MLDB
