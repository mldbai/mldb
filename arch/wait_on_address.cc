/* futex.cc                                                         -*- C++ -*-
   Jeremy Barnes, 25 January 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.
   This file is part of MLDB. Copyright 2021 mldb.ai inc. All rights reserved.
   Some functions are copied from the Darwin API specifications, copyright Apple (APSL/fair use)
   Some functions are copied from libdispatch, copyright Apple (Apache 2.0)

   Basic futex function wrappers.
*/

#include "wait_on_address.h"
#include <unistd.h>
#include <limits.h>
#include <time.h>
#include <errno.h>
#include <stdexcept>
#include <atomic>
#include "mldb/compiler/compiler.h"
#include "mldb/base/exc_assert.h"
#include "exception.h"
#include <iostream>

#define MLDB_FALLTHROUGH [[fallthrough]]

#ifdef __linux__
#include <linux/futex.h>
#include <sys/syscall.h>
#endif /* __linux__ */

#ifdef __APPLE__

extern "C" {
extern int __ulock_wait(uint32_t operation, void *addr, uint64_t value,
    uint32_t timeout);             /* timeout is specified in microseconds */
extern int __ulock_wait2(uint32_t operation, void *addr, uint64_t value,
    uint64_t timeout, uint64_t value2);
extern int __ulock_wake(uint32_t operation, void *addr, uint64_t wake_value);
} // extern "C"

// https://opensource.apple.com/source/xnu/xnu-7195.50.7.100.1/bsd/sys/ulock.h.auto.html
// These are API specifications, so incorporated under fair use as the header files are
// not exported as OS definitions.

/*
 * operation bits [7, 0] contain the operation code.
 *
 * NOTE: make sure to add logic for handling any new
 *       types to kdp_ulock_find_owner()
 */
#define UL_COMPARE_AND_WAIT             1
#define UL_UNFAIR_LOCK                  2
#define UL_COMPARE_AND_WAIT_SHARED      3
#define UL_UNFAIR_LOCK64_SHARED         4
#define UL_COMPARE_AND_WAIT64           5
#define UL_COMPARE_AND_WAIT64_SHARED    6

/*
 * operation bits [15, 8] contain the flags for __ulock_wake
 */
#define ULF_WAKE_ALL                    0x00000100
#define ULF_WAKE_THREAD                 0x00000200
#define ULF_WAKE_ALLOW_NON_OWNER        0x00000400

/*
 * operation bits [23, 16] contain the flags for __ulock_wait
 *
 * @const ULF_WAIT_WORKQ_DATA_CONTENTION
 * The waiter is contending on this lock for synchronization around global data.
 * This causes the workqueue subsystem to not create new threads to offset for
 * waiters on this lock.
 *
 * @const ULF_WAIT_CANCEL_POINT
 * This wait is a cancelation point
 *
 * @const ULF_WAIT_ADAPTIVE_SPIN
 * Use adaptive spinning when the thread that currently holds the unfair lock
 * is on core.
 */
#define ULF_WAIT_WORKQ_DATA_CONTENTION  0x00010000
#define ULF_WAIT_CANCEL_POINT           0x00020000
#define ULF_WAIT_ADAPTIVE_SPIN          0x00040000

/*
 * operation bits [31, 24] contain the generic flags
 */
#define ULF_NO_ERRNO                    0x01000000

/*
 * masks
 */
#define UL_OPCODE_MASK          0x000000FF
#define UL_FLAGS_MASK           0xFFFFFF00
#define ULF_GENERIC_MASK        0xFFFF0000

#define ULF_WAIT_MASK           (ULF_NO_ERRNO | \
	                         ULF_WAIT_WORKQ_DATA_CONTENTION | \
	                         ULF_WAIT_CANCEL_POINT | ULF_WAIT_ADAPTIVE_SPIN)

#define ULF_WAKE_MASK           (ULF_NO_ERRNO | \
	                         ULF_WAKE_ALL | \
	                         ULF_WAKE_THREAD | \
	                         ULF_WAKE_ALLOW_NON_OWNER)

namespace MLDB {

// returns 0, ETIMEDOUT, ENOTEMPTY, EFAULT, EINTR
static int
_dlock_wait(uint32_t *uaddr, uint32_t val, uint32_t timeout, uint32_t flags)
{
	for (;;) {
		int rc = __ulock_wait(flags | ULF_NO_ERRNO, uaddr, val, timeout);
		if (rc > 0) {
			return ENOTEMPTY;
		}
		switch (-rc) {
		case 0:
			return 0;
		case EINTR:
			/*
			 * if we have a timeout, we need to return for the caller to
			 * recompute the new deadline, else just go back to wait.
			 */
			if (timeout == 0) {
				continue;
			}
			MLDB_FALLTHROUGH;
		case ETIMEDOUT:
		case EFAULT:
			return -rc;
		default:
			throw std::runtime_error("ulock_wait() failed");
		}
	}
}

static void
_dlock_wake(uint32_t *uaddr, uint32_t flags)
{
	int rc = __ulock_wake(flags | ULF_NO_ERRNO, uaddr, 0);
	if (rc == 0 || rc == -ENOENT) return;
	throw std::runtime_error("ulock_wake() failed");
}

static int
ulock_wait(uint32_t *uaddr, uint32_t val, uint32_t timeout,
		uint32_t flags)
{
	return _dlock_wait(uaddr, val, timeout, flags | UL_COMPARE_AND_WAIT);
}

static void
ulock_wake(uint32_t *uaddr, uint32_t flags)
{
	return _dlock_wake(uaddr, flags | UL_COMPARE_AND_WAIT);
}

static int
unfair_lock_wait(uint32_t *uaddr, uint32_t val, uint32_t timeout,
		WaitOnAddressLockOptions flags)
{
	return _dlock_wait(uaddr, val, timeout, flags | UL_UNFAIR_LOCK);
}

static void
unfair_lock_wake(uint32_t *uaddr, uint32_t flags)
{
	return _dlock_wake(uaddr, flags | UL_UNFAIR_LOCK);
}

} // namespace MLDB
#endif /* __APPLE__ */


#ifdef __linux__
#include <sys/time.h>
#ifdef __ANDROID__
#include <sys/syscall.h>
#else
#include <syscall.h>
#endif /* __ANDROID__ */

namespace MLDB {

MLDB_ALWAYS_INLINE
static int
futex(uint32_t *uaddr, int op, uint32_t val,
		const struct timespec *timeout, uint32_t *uaddr2, uint32_t val3,
		int opflags)
{
	return (int)syscall(SYS_futex, uaddr, op | opflags, val, timeout, uaddr2, val3);
}

// returns 0, ETIMEDOUT, EFAULT, EINTR, EWOULDBLOCK
MLDB_ALWAYS_INLINE
static int
_futex_blocking_op(uint32_t *uaddr, int futex_op, uint32_t val,
		const struct timespec *timeout, int flags)
{
	for (;;) {
		int rc = futex(uaddr, futex_op, val, timeout, NULL, 0, flags);
		if (!rc) {
			return 0;
		}
		switch (errno) {
		case EINTR:
			/*
			 * if we have a timeout, we need to return for the caller to
			 * recompute the new deadline, else just go back to wait.
			 */
			if (timeout == 0) {
				continue;
			}
			MLDB_FALLTHROUGH;
		case ETIMEDOUT:
		case EFAULT:
		case EWOULDBLOCK:
			return errno;
		default:
		  throw Exception(errno, "_futex_op() failed");
		}
	}
}

static int
futex_wait(uint32_t *uaddr, uint32_t val,
		const struct timespec *timeout, int opflags)
{
	return _futex_blocking_op(uaddr, FUTEX_WAIT, val, timeout, opflags);
}

static void
futex_wake(uint32_t *uaddr, int wake, int opflags)
{
	int rc = futex(uaddr, FUTEX_WAKE, (uint32_t)wake, NULL, NULL, 0,
			opflags);
	if (rc >= 0 || errno == ENOENT) return;
	throw Exception(errno, "_dlock_wake() failed");
}

} // namespace MLDB

#endif

namespace MLDB {

int
wait_on_address(std::atomic<uint32_t> & i, uint32_t value,
		        double timeout, WaitOnAddressLockOptions flags)
{
	uint32_t *address = (uint32_t *)(&i);
	ExcAssertGreaterEqual(timeout, 0);
	constexpr double maxTimeout = 18446744073.709551615; //std::numeric_limits<uint64_t>::max() / 1000000000.0;
	auto nsecs = std::numeric_limits<uint64_t>::max();
	if (timeout < maxTimeout) {
		nsecs = timeout * 1000000000.0;
	}
	if (nsecs == 0) {
		return ETIMEDOUT;
	}
#if __APPLE__
	uint64_t usecs = 0;
	int rc;
	if (timeout >= maxTimeout) {
		return ulock_wait(address, value, 0, flags);
	}
	do {
		usecs = nsecs / 1000;
		if (usecs > UINT32_MAX) usecs = UINT32_MAX;
		rc = ulock_wait(address, value, (uint32_t)usecs, flags);
	} while (0);  // usecs == UINT32_MAX && rc == ETIMEDOUT && (Usecs = timeout(timeout)) != 0);
	return rc;
#elif __linux__
	(void)flags;
	if (timeout < maxTimeout) {
	    struct timespec ttimeout = {0, 0};
	    ttimeout.tv_sec = nsecs / 1000000000;
	    ttimeout.tv_nsec = (nsecs - ttimeout.tv_sec * 1000000000);
	    return futex_wait(address, value, &ttimeout, FUTEX_PRIVATE_FLAG);
	}
	return futex_wait(address, value, NULL, FUTEX_PRIVATE_FLAG);
#elif defined(_WIN32)
	return WaitOnAddress(address, &value, sizeof(value), INFINITE) == TRUE;
#else
#error _wait_on_address unimplemented for this platform
#endif
}

void
wake_by_address(std::atomic<uint32_t> & address)
{
#if __APPLE__
	ulock_wake((uint32_t *)&address, ULF_WAKE_ALL);
#elif __linux__
	futex_wake((uint32_t *)&address, INT_MAX, FUTEX_PRIVATE_FLAG);
#elif defined(_WIN32)
	WakeByAddressAll((uint32_t *)&address);
#else
	(void)address;
#endif
}

} // namespace MLDB
