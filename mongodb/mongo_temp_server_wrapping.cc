/**                                                                 -*- C++ -*-
 * mongo_temp_server_wrapping.cc
 * Mich, 2015-07-02
 * This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
 **/

#include <boost/python.hpp>
#include "mongo_temporary_server.h"

using namespace std;
using namespace boost::python;
using namespace MLDB;

using namespace Mongo;

struct MongoTemporaryServerPtr {

    shared_ptr<MongoTemporaryServer> mongoTmpServer;

    MongoTemporaryServerPtr(const std::string & uniquePath = "",
                            const int portNum = 28356) :
        mongoTmpServer(new MongoTemporaryServer(uniquePath, portNum))
    {
    }

    void testConnection() {
        mongoTmpServer->testConnection();
    }

    int getPortNum() {
        return mongoTmpServer->getPortNum();
    }
};

BOOST_PYTHON_MODULE(python_mongo_temp_server_wrapping) {
    class_<MongoTemporaryServerPtr>("MongoTemporaryServerPtr",
                                    init<std::string, int>())
        .def("test_connection", &MongoTemporaryServerPtr::testConnection)
        .def("get_port_num", &MongoTemporaryServerPtr::getPortNum);
}

