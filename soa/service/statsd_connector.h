// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* statsd_connector.h                                              -*- C++ -*-
   Send messages to statsd.
*/

#pragma once

#include <string>

namespace MLDB {


/*****************************************************************************/
/* STATSD CONNECTOR                                                          */
/*****************************************************************************/

/** Class that sends UDP packets to statsd for monitoring purposes.
*/

class StatsdConnector {
    int fd;
    std::string hostname;

public:
    StatsdConnector();
    StatsdConnector(const std::string & statsdAddr);
    ~StatsdConnector();

    void open(const std::string & statsdAddr);

    void incrementCounter(const char* counterName, float sampleRate, int value=1 );
    void recordGauge(const char* counterName, float sampleRate, float gauge );
};


} // namespace MLDB
