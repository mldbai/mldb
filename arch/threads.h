// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* threads.h                                                       -*- C++ -*-
   Jeremy Barnes, 30 January 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.

   Catch-all include for architecture dependent threading constructions.
*/

#pragma once

#include <unistd.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <mutex>

typedef std::recursive_mutex Lock;
typedef std::unique_lock<Lock> Guard;

// c++14
//typedef std::shared_lock<Lock> Read_Guard;
//typedef std::unique_lock<Lock> Read_Guard;
//typedef std::unique_lock<Lock> Write_Guard;

inline pid_t gettid()
{
    return (pid_t) syscall(SYS_gettid);
}
