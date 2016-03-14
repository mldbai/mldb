/* MLDB-1398-main-library.cc
   Jeremy Barnes, 22 February 2015
   Copyright (c) 2015 Datacratic Inc.  All rights reserved.

   Main library for MLDB-1398 test.
*/

#include "mldb/core/plugin.h"
#include "mldb/types/basic_value_descriptions.h"
#include <iostream>

using namespace std;
using namespace Datacratic;
using namespace Datacratic::MLDB;

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

Datacratic::MLDB::Plugin *
mldbPluginEnterV100(Datacratic::MLDB::MldbServer * server)
{
    cerr << "entering plugins library" << endl;
    return new PluginHandler(server);
}
