/* statsd_connector.cc
   Nicolas Kruchten
   Copyright (c) 2011 mldb.ai inc.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

*/

#include "mldb/soa/service/statsd_connector.h"
#include "mldb/arch/exception.h"
#include "mldb/arch/format.h"
#include <iostream>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <string.h>
#include "mldb/soa/service/connectfd.h"
#include <unistd.h>


using namespace std;

namespace MLDB {


/*****************************************************************************/
/* STATSD CONNECTOR                                                          */
/*****************************************************************************/

StatsdConnector::
StatsdConnector()
    : fd(-1)
{
}

StatsdConnector::
StatsdConnector(const string& statsdAddr)
{
    open(statsdAddr);
}

StatsdConnector::
~StatsdConnector()
{
    ::close(fd);
}

void
StatsdConnector::
open(const string& statsdAddr)
{
    auto lastColon = statsdAddr.rfind(':');
    if (lastColon == string::npos)
        throw MLDB::Exception("Need a host and portname to connect to statsd: passed '%s'",
                            statsdAddr.c_str());

    int newFd = connectHostDgram(string(statsdAddr, 0, lastColon),
                                 std::stoi(string(statsdAddr, lastColon + 1)));
    
    ::close(fd);
    fd = newFd;
}
    
void
StatsdConnector::
incrementCounter(const char* counterName, float sampleRate, int value)
{
    if (sampleRate < 1.0 && ((random() % 10000) / 10000.0) >= sampleRate)
        return;

    char msgBuf[1024];
    int res = snprintf(msgBuf, 1024, "%s:%d|c|@%.2f", counterName, value,
                       sampleRate);
    if (res >= 1024) {
        cerr << "invalid statsd counter name: " << counterName << endl;
        return;
    }
    
    int sendRes = ::send(fd, msgBuf, res, MSG_DONTWAIT);
    if (sendRes == -1) {
        cerr << "statsd message failure: " << strerror(errno)
             << endl;
    }
}

void
StatsdConnector::
recordGauge(const char* counterName, float sampleRate, float value)
{
    if (sampleRate < 1.0 && ((random() % 10000) / 10000.0) >= sampleRate)
        return;

    char msgBuf[1024];
    int res = snprintf(msgBuf, 1024, "%s:%f|ms", counterName, value);

    if (res >= 1024) {
        cerr << "invalid statsd counter name: " << counterName << endl;
        return;
    }
    
    int sendRes = ::send(fd, msgBuf, res, MSG_DONTWAIT);
    if (sendRes == -1) {
        cerr << "statsd message failure: " << strerror(errno)
             << endl;
    }
}

} // namespace MLDB
