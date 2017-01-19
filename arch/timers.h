/* timers.h                                                        -*- C++ -*-
   Jeremy Barnes, 1 April 2009
   Copyright (c) 2009 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Different types of timers.
*/

#pragma once

#include <sys/time.h>
#include "tick_counter.h"
#include "format.h"
#include <string>
#include <cerrno>
#include <string.h>
#include "exception.h"
#include <sys/select.h>
#include <atomic>


namespace MLDB {

inline double cpu_time()
{
    clock_t clk = clock();
    return clk / 1000000.0;  // clocks per second
}

inline double wall_time()
{
    struct timeval tv;
    struct timezone tz;
    int res = gettimeofday(&tv, &tz);
    if (res != 0)
        throw Exception("gettimeofday() returned "
                        + std::string(strerror(errno)));

    return tv.tv_sec + (tv.tv_usec / 1000000.0);
}
 
struct Timer {
    double wall_, cpu_;
    unsigned long long ticks_;
    bool enabled;

    explicit Timer(bool enable = true)
        : enabled(enable)
    {
        restart();
    }
    
    void restart()
    {
        if (!enabled)
            return;
        wall_ = wall_time();
        cpu_ = cpu_time();
        ticks_ = ticks();
    }

    std::string elapsed() const
    {
        if (!enabled)
            return "disabled";
        return format("elapsed: [%.2fs cpu, %.4f mticks, %.2fs wall, %.2f cores]",
                      elapsed_cpu(),
                      elapsed_ticks() / 1000000.0,
                      elapsed_wall(),
                      elapsed_cpu() / elapsed_wall());
    }

    double elapsed_cpu() const { return cpu_time() - cpu_; }
    double elapsed_ticks() const { return ticks() - ticks_; }
    double elapsed_wall() const { return wall_time() - wall_; }
};

inline int64_t timeDiff(const timeval & tv1, const timeval & tv2)
{
    return 1000000 * ((int64_t)tv2.tv_sec - (int64_t)tv1.tv_sec)
        + (int64_t)tv2.tv_usec - (int64_t)tv1.tv_usec;
}

struct Duty_Cycle_Timer {

    struct Stats {
        Stats()
            : usAsleep(0), usAwake(0), numWakeups(0)
        {
        }

        Stats(const Stats & other)
            : usAsleep(other.usAsleep.load()),
              usAwake(other.usAwake.load()),
              numWakeups(other.numWakeups.load())
        {
        }

        std::atomic<uint64_t> usAsleep, usAwake, numWakeups;

        double duty_cycle() const
        {
            return (double)usAwake / (double)(usAsleep + usAwake);
        }
    };

    enum Timer_Source {
        TS_TSC,   ///< Get from the timestamp counter (fast)
        TS_RTC    ///< Get from the real time clock (accurate)
    };
    
    Duty_Cycle_Timer(Timer_Source source = TS_TSC)
        : source(source)
    {
        clear();
        afterSleep = getTime();
    }

    void clear()
    {
        afterSleep = getTime();
        current.usAsleep = current.usAwake = current.numWakeups = 0;
    }

    void notifyBeforeSleep()
    {
        beforeSleep = getTime();
        uint64_t useconds = (beforeSleep - afterSleep) * 1000000;
#if 0
        using namespace std;
        cerr << "sleeping at " << beforeSleep << " after "
             << (beforeSleep - afterSleep)
             << " (" << useconds << "us)" << endl;
#endif
        current.usAwake += useconds;
    }

    void notifyAfterSleep()
    {
        afterSleep = getTime();
        uint64_t useconds = (afterSleep - beforeSleep) * 1000000;
#if 0
        using namespace std;
        cerr << "awake at " << beforeSleep << " after "
             << (afterSleep - beforeSleep)
             << " (" << useconds << "us)" << endl;
        cerr << "seconds_per_tick = " << seconds_per_tick << endl;
#endif
        current.usAsleep += useconds;
        current.numWakeups += 1;
    }

    double getTime()
    {
        if (source == TS_TSC)
            return ticks() * seconds_per_tick;
        else return wall_time();
    }

    Stats stats() const
    {
        return current;
    }
    
    Stats current;
    Timer_Source source;

    double beforeSleep, afterSleep;
};

} // namespace MLDB
