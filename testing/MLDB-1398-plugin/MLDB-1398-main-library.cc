/* MLDB-1398-main-library.cc
   Jeremy Barnes, 22 February 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   Main library for MLDB-1398 test.
*/

#include "mldb/core/plugin.h"
#include "mldb/types/basic_value_descriptions.h"
#include <iostream>

using namespace std;

using namespace MLDB;

// From the dependent plugin library
std::string getStatusMessage();

struct PluginHandler: public Plugin {
    PluginHandler(MldbServer * server)
        : Plugin(server)
    {
    }

    virtual Any getStatus() const
    {
        return getStatusMessage();
    }
};

MLDB::Plugin *
mldbPluginEnterV100(MLDB::MldbServer * server)
{
    //commenting until MLDBFB-403 is fixed
    //cerr << "entering plugins library" << endl;  
    return new PluginHandler(server);
}
