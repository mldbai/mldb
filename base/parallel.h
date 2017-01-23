/** parallel.h                                                     -*- C++ -*-
    Jeremy Barnes, 5 February 2016
    Copyright (c) 2016 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include <functional>

namespace MLDB {

/** Run a set of jobs in multiple threads.

    This will count from first to last, submitting a job to doWork for
    each of the values, across multiple threads.

    A maximum of occupancyLimit jobs will be run in parallel at once.  This
    is useful for limiting lock contention in a downstream reduction job.

    The doWork() function is permitted to throw an exception.  In the case
    that an exception is thrown, the following behaviour will happen:

    1.  The exception will be captured.
    2.  The function will wait for all currently executing doWork() calls
        to finish.  If any of these throw a second exception, that exception
        will be will be ignored.
    3.  Any doWork() calls that haven't yet started will be cancelled and
        will not take place.
    4.  This function will rethrow the captured exception, in the context
        of the thread that called this function.

    Different behaviour can be obtained by using a try block inside the
    doWork function, or by using another mechanism apart from exceptions
    to signal errors.
*/
void parallelMap(size_t first, size_t last,
                 const std::function<void (size_t)> & doWork,
                 int occupancyLimit = -1);

/** Same as parallelMap(), but takes a lambda which will short-circuit the
    work if it returns false.  Returns false if and only if a doWork()
    call returned false.
*/
bool parallelMapHaltable(size_t first, size_t last,
                         const std::function<bool (size_t)> & doWork,
                         int occupancyLimit = -1);

/** Same as parallelMap, except that each doWork() call will be passed
    a chunk of work of chunkSize.  This is useful to reduce the amount
    of calling overhead on a very fine-grained job.
*/
void parallelMapChunked(size_t first, size_t last, size_t chunkSize,
                        const std::function<void (size_t, size_t)> & doWork,
                        int occupancyLimit = -1);

} // namespace MLDB
