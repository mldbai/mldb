#include "timerfd.h"
#include "mldb/base/exc_assert.h"
#include <unistd.h>
#include <cstring>

using namespace std;

#if defined(__linux__)
#include <sys/timerfd.h>

namespace MLDB {

struct TimerFD::Itl {
    Itl(TimerTypes type, int options)
    {
	auto clock = (type == TIMER_REALTIME ? CLOCK_REALTIME : CLOCK_MONOTONIC);
        auto flags = ((options & TIMER_CLOSE_ON_EXEC) ? TFD_CLOEXEC : 0)
	    | ((options & TIMER_NON_BLOCKING) ? TFD_NONBLOCK : 0);

        fd_ = timerfd_create(clock, flags);
        if (fd_ == -1) {
            throw MLDB::Exception(errno, "timerfd_create()");
        }
    }

    ~Itl()
    {
	::close(fd_);
    }

    int fd() const { return fd_; }

    uint64_t read() const
    {
	uint64_t misses;
	ssize_t len = ::read(fd_, &misses, sizeof(misses));
	if (len == -1) {
	    if (errno != EAGAIN) {
		throw MLDB::Exception(errno, "read timerd");
	    }
	}
	return misses;
    }

    void setTimeout(std::chrono::nanoseconds durationFromNow)
    {
	ExcAssert(fd_ != -1);
	auto seconds = durationFromNow.count() / 1000000000ULL;
        auto nanoseconds = durationFromNow.count() % 1000000000ULL;
	
	struct itimerspec timespec;
	memset(&timespec, 0, sizeof(timespec));
	timespec.it_value.tv_sec = seconds;
	timespec.it_value.tv_nsec = nanoseconds;

	int res = ::timerfd_settime(fd_, 0, &timespec, nullptr);
	if (res == -1) {
	    throw MLDB::Exception(errno, "timerfd_settime");
	}
    }

    int fd_ = -1;
};

} // namespace MLDB

#elif defined(__APPLE__)
#  include <sys/types.h>
#  include <sys/event.h>
#  include <sys/time.h>
#  include <sys/ioctl.h>

namespace MLDB {



struct TimerFD::Itl {
    Itl(TimerTypes type, int options)
    {
        fd_ = kqueue();
        cerr << "timerfd init: fd = " << fd_ << endl;

        if (fd_ == -1) {
            throw MLDB::Exception(errno, "kqueue for timerfd");
        }
        int res = ioctl(fd_, (options & TIMER_CLOSE_ON_EXEC) ? FIOCLEX : FIONCLEX, 0);
        if (res == -1)
            throw MLDB::Exception(errno, "timerfd O_CLOEXEC");

        if (options & TIMER_NON_BLOCKING) {
            int res = ioctl(fd_, FIONBIO, 0);
            if (res == -1)
                throw MLDB::Exception(errno, "timerfd O_NONBLOCK");
        }
    }

    ~Itl()
    {
        ::close(fd_);
    }

    int fd() const { return fd_; }

    uint64_t read() const
    {
        throw MLDB::Exception("unimplemented read");
    }

    void setTimeout(std::chrono::nanoseconds durationFromNow)
    {
        auto nanoseconds = durationFromNow.count();

        struct kevent64_s event;
        EV_SET64(&event, reinterpret_cast<uint64_t>(this), EVFILT_TIMER, EV_ADD | EV_ONESHOT,
                0 /* fflags */, nanoseconds, 0, 0, 0);

        int res;
        do {
            res = kevent64(fd_, &event, 1, nullptr, 0, 0 /* flags */, nullptr);
            if (res == -1 && errno != EINTR && errno != EAGAIN) {
                throw MLDB::Exception(errno, "timerfd kevent64 add");
            }
        }
        while (res == -1);
    }

    int fd_ = -1;
};

} // namespace MLDB

#endif

namespace MLDB {

TimerFD::TimerFD()
{
}

TimerFD::TimerFD(TimerTypes type, int options)
{
    this->init(type, options);
}

TimerFD::~TimerFD()
{
}

void TimerFD::init(TimerTypes type, int options)
{
    ExcAssert(!initialized());
    itl_.reset(new Itl(type, options));
}

bool TimerFD::initialized() const
{
    return itl_.get();
}

int TimerFD::fd() const
{
    ExcAssert(initialized());
    return itl_->fd();
}

// Returns the number of timers that have elapsed since the last read
uint64_t TimerFD::read() const
{
    ExcAssert(initialized());
    return itl_->read();
}

void TimerFD::setTimeout(std::chrono::nanoseconds durationFromNow)
{
    ExcAssert(initialized());
    itl_->setTimeout(durationFromNow);
}

} // namespace MLDB
