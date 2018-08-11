/** fd_exhauster.h                                                 -*- C++ -*-
    Jeremy Barnes, 16 May 2011
    Copyright (c) 2011 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Class to exhaust file descriptors for testing purposes.
*/

#pragma once

#include <boost/test/unit_test.hpp>
#include <sys/types.h>
#include <sys/socket.h>

namespace MLDB {

// Create sockets until no FDs left to exhaust FDs
struct FDExhauster {
    std::vector<int> sockets;

    FDExhauster()
    {
        int sock;
        do {
            sock = socket(AF_INET, SOCK_STREAM, 0);
            if (sock != -1)
                sockets.push_back(sock);
        } while (sockets.size() < 65536 && sock != -1);

        // If this fails, we can't extinguish the sockets
        BOOST_REQUIRE_EQUAL(sock, -1);
    }

    ~FDExhauster()
    {
        for (unsigned i = 0;  i < sockets.size();  ++i)
            close(sockets[i]);
    }
};


} // namespace MLDB
