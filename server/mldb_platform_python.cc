/** mldb_platform_python.cc
    Jeremy Barnes, 12 December 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Python platform support for MLDB.
*/

#include "mldb_server.h"
#include "mldb/builtin/python/python_plugin_context.h"

using namespace std;
using namespace MLDB;

namespace MLDB {

std::shared_ptr<MldbPythonContext>
getPythonEnvironment()
{
    // If this function is called, we are certainly within a Python
    // module and all of our MLDB runtime libraries are loaded.  It's
    // the right time to tell the MLDB Python initialization code that
    // we're in a module when it runs a little later on.
    PythonInterpreter::initializeFromModuleInit();

    // Currently we use the same MldbServer class as the standalone mldb_runner;
    // eventually we will set it up so that we use Python facilities for things
    // like performing HTTP requests
    auto server = std::make_shared<MldbServer>();
    server->init();
    server->router.addAutodocRoute("/autodoc", "/v1/help", "autodoc");
    server->threadPool->ensureThreads(4 /*numThreads*/);

    server->start();

    // We need a PythonContext
    std::shared_ptr<PythonContext> context
        = std::make_shared<PythonContext>("MLDB Python Environment",
                                          server.get());

    // Which we wrap for our result
    std::shared_ptr<MldbPythonContext> result
        = std::make_shared<MldbPythonContext>(context);

    // This lambda captures all of the shared pointers whose lifecycle is
    // controlled by the MldbPythonContext; they will all be destroyed in
    // the right order when the returned shared_ptr goes out of scope.
    auto capture = [server, context, result] (MldbPythonContext *)
        {
        };

    return std::shared_ptr<MldbPythonContext>(result.get(), capture);
}

} // namespace MLDB


