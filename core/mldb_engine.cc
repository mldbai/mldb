/** mldb_engine.cc
    Jeremy Barnes, 5 August 2018
    Copyright (c) 2018 mldb.ai inc.  All rights reserved.
*/

#include "mldb_engine.h"
#include "mldb/types/string.h"

namespace MLDB {

static const Utf8String & getEmptyUtf8Impl()
{
    static const Utf8String result;
    return result;
}

const Utf8String & EMPTY_UTF8 = getEmptyUtf8Impl();

/*****************************************************************************/
/* MLDB ENGINE                                                               */
/*****************************************************************************/

MldbEngine::
~MldbEngine()
{
}

} // namespace MLDB
