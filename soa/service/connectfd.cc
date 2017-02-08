// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* connectfd.h                                                     -*- C++ -*-
   Jeremy Barnes, 5 August 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

*/

#include "connectfd.h"

#include <netdb.h>
#include <string.h>
#include "mldb/soa/service/connectfd.h"
#include "mldb/arch/exception.h"
#include <unistd.h>


using namespace std;


namespace MLDB {

int connectHostDgram(const std::string & hostname, int port)
{
    struct addrinfo hints;
    struct addrinfo *result, *rp;
    int sfd, s;

    /* Obtain address(es) matching host/port */

    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_UNSPEC;     /* Allow IPv4 or IPv6 */
    hints.ai_socktype = SOCK_DGRAM;  /* Datagram socket */
    hints.ai_flags = 0;
    hints.ai_protocol = 0;           /* Any protocol */

    s = getaddrinfo(hostname.c_str(), std::to_string(port).c_str(), &hints, &result);
    if (s != 0) {
        throw MLDB::Exception("getaddrinfo: %s\n", gai_strerror(s));
    }

    /* getaddrinfo() returns a list of address structures.
       Try each address until we successfully connect(2).
       If socket(2) (or connect(2)) fails, we (close the socket
       and) try the next address. */

    for (rp = result; rp != NULL; rp = rp->ai_next) {
        sfd = socket(rp->ai_family, rp->ai_socktype,
                     rp->ai_protocol);
        if (sfd == -1)
            continue;

        if (connect(sfd, rp->ai_addr, rp->ai_addrlen) != -1)
            break;                  /* Success */

        close(sfd);
    }

    if (rp == NULL) {               /* No address succeeded */
        fprintf(stderr, "Could not connect\n");
        exit(EXIT_FAILURE);
    }
           
    freeaddrinfo(result);           /* No longer needed */

    return sfd;
}

int connectLocalhost(int port)
{
    return connectHost("localhost", port);
}

int connectHost(const std::string & hostname, int port)
{
    struct addrinfo hints;
    struct addrinfo *result, *rp;
    int sfd, s;

    /* Obtain address(es) matching host/port */

    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_UNSPEC;     /* Allow IPv4 or IPv6 */
    hints.ai_socktype = SOCK_STREAM; /* Datagram socket */
    hints.ai_flags = 0;
    hints.ai_protocol = 0;           /* Any protocol */

    s = getaddrinfo(hostname.c_str(), std::to_string(port).c_str(), &hints, &result);
    if (s != 0) {
        throw MLDB::Exception("getaddrinfo: %s\n", gai_strerror(s));
    }

    /* getaddrinfo() returns a list of address structures.
       Try each address until we successfully connect(2).
       If socket(2) (or connect(2)) fails, we (close the socket
       and) try the next address. */

    for (rp = result; rp != NULL; rp = rp->ai_next) {
        sfd = socket(rp->ai_family, rp->ai_socktype,
                     rp->ai_protocol);
        if (sfd == -1)
            continue;

        if (connect(sfd, rp->ai_addr, rp->ai_addrlen) != -1)
            break;                  /* Success */

        close(sfd);
    }

    if (rp == NULL) {               /* No address succeeded */
        fprintf(stderr, "Could not connect\n");
        exit(EXIT_FAILURE);
    }
           
    freeaddrinfo(result);           /* No longer needed */

    return sfd;
}

} // namespace MLDB
