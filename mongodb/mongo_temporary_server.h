/**                                                                 -*- C++ -*-
 * mongo_temporary_server.h
 * Sunil Rottoo, 2 September 2014
 * This file is part of MLDB. Copyright 2014 mldb.ai inc. All rights reserved.
 *
 * A temporary server for testing of mongo-based services.  Starts one up in a
 * temporary directory and gives the uri to connect to.
 *
 * NOTE: This is not a self-contained util. mongod need to be installed prior
 *       to using mongo_temporary_server.
 **/

#pragma once

#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <sys/prctl.h>
#include <signal.h>
#include <boost/filesystem.hpp>

#include "mldb/jml/utils/environment.h"
#include "mldb/jml/utils/file_functions.h"
#include "mldb/io/message_loop.h"
#include "mldb/utils/runner.h"


namespace Mongo {

struct MongoTemporaryServer : boost::noncopyable {
    MongoTemporaryServer(const std::string & uniquePath = "",
                         const int portNum = 28356);
    ~MongoTemporaryServer();
    
    void testConnection();
    void start();
    void suspend();
    void resume();
    void shutdown();

    int getPortNum()
        const
    {
        return portNum;
    }

    const std::string & unixSocketPath()
        const
    {
        return socketPath_;
    }

private:
    enum State { Inactive, Stopped, Suspended, Running };
    State state;
    std::string uniquePath_;
    std::string socketPrefix_;
    std::string socketPath_;
    std::string logfile_;
    MLDB::MessageLoop loop_;
    MLDB::Runner runner_;
    int portNum;
};

} // namespace Mongo


/* Make MongoTemporaryServer available in MLDB ns */

namespace MLDB {

using Mongo::MongoTemporaryServer;

} // namespace MLDB




