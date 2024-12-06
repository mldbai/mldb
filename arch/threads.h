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

