#pragma once

#include <memory>
#include <chrono>

namespace MLDB {

enum TimerTypes {
    TIMER_REALTIME = 1,
    TIMER_MONOTONIC = 2
};

enum TimerOptions {
    TIMER_CLOSE_ON_EXEC,
    TIMER_NON_BLOCKING
};

struct TimerFD {
    TimerFD();
    TimerFD(TimerTypes type, int options = 0);

    ~TimerFD();

    void init(TimerTypes type, int options = 0);
    bool initialized() const;

    int fd() const;

    // Returns the number of timers that have elapsed since the last read
    uint64_t read() const;

    void setTimeout(std::chrono::nanoseconds durationFromNow);

private:
    struct Itl;
    std::unique_ptr<Itl> itl_;
};

} // namespace MLDB