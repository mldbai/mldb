// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* proc_stats.h                                                   -*- C++ -*-
   Rémi Attab, 19 January 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

   Gathers process and system stats from the proc files.
*/


#ifndef __logger__process_stats_h__
#define __logger__process_stats_h__

#include "mldb/ext/jsoncpp/json.h"
#include <string>
#include <functional>


namespace MLDB {

/*
Reccords statistics related to a process and the system.
The stats should preferably be formatted and dumped via the logToCallback()
method.
*/
struct ProcessStats {
    ProcessStats (bool doLoadAverage = false) :
        doLoadAverage(doLoadAverage)
    {
        sample();
    }

    void sample () {
        sampleLoadAverage();
        sampleStatm();
        sampleRUsage();
    }


    typedef std::function<void(std::string, double)> LogCallback;
    static void logToCallback (
            LogCallback cb,
            const ProcessStats& lastStats,
            const ProcessStats& curStats,
            const std::string& prefix = "");
    static Json::Value toJson (
            const ProcessStats& lastStats,
            const ProcessStats& curStats,
            const std::string& prefix = "");

    uint64_t majorFaults;
    uint64_t minorFaults;
    uint64_t totalFaults() const { return majorFaults + minorFaults; }

    double userTime;
    double systemTime;
    double totalTime() const { return userTime + systemTime; } 

    uint64_t virtualMem;
    uint64_t residentMem;
    uint64_t sharedMem;

    bool doLoadAverage;
    float loadAverage1;
    float loadAverage5;
    float loadAverage15;

    uint64_t voluntaryContextSwitches;
    uint64_t involuntaryContextSwitches;
    uint64_t totalContextSwitches() const {
        return voluntaryContextSwitches + involuntaryContextSwitches;
    }

private:
    void sampleLoadAverage ();
    void sampleStat ();
    void sampleStatm ();
    void sampleRUsage ();
};



} // namespace MLDB

#endif // __logger__process_stats_h__


