// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* semaphore.h                                                     -*- C++ -*-
   Jeremy Barnes, 7 September 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   Implementation of a semaphore; nominally on top of the futex.
*/

#pragma once

#include "mldb/arch/exception.h"
#include <iostream>

#include "mldb/arch/futex.h"
#include <semaphore.h>

namespace ML {

struct Semaphore {
    sem_t val;

    Semaphore(int initialVal = 1)
    {
        if (sem_init(&val, 0, initialVal))
            throw MLDB::Exception(errno, "sem_init");
    }

    ~Semaphore()
    {
        if (sem_destroy(&val))
            throw MLDB::Exception(errno, "sem_destroy");
    }

    void acquire()
    {
        int res;
        while ((res = sem_wait(&val)) && errno == EINTR) ;
        if (res)
            throw MLDB::Exception(errno, "sem_wait");
    }

    int acquire(double secondsToWait)
    {
        int res;
        time_t seconds = (time_t) secondsToWait;
        long int nanoseconds = (long int)((secondsToWait - seconds) * 1000000000.0);
        struct timespec ts = { seconds, nanoseconds };
        while ((res = sem_timedwait(&val, &ts)) && errno == EINTR) ;
        if (res && errno != ETIMEDOUT)
            throw MLDB::Exception(errno, "sem_timedwait");
        return res;
    }

    int tryacquire()
    {
        int res;
        while ((res = sem_trywait(&val)) && errno == EINTR) ;

        if (res && (errno == EAGAIN))
            return -1;
        if (res)
            throw MLDB::Exception(errno, "sem_trywait");

        return 0;
    }

    void release()
    {
        int res;
        while ((res = sem_post(&val)) && errno == EINTR) ;
        if (res)
            throw MLDB::Exception(errno, "sem_post");
    }
};


struct Release_Sem {
    Release_Sem(Semaphore & sem)
        : sem(&sem)
    {
    }

    void operator () ()
    {
        sem->release();
    }

    Semaphore * sem;
};


} // namespace ML
