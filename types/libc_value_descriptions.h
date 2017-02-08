/** libc_value_descriptions.h                                      -*- C++ -*-
    Wolfgang Sourdeau, 7 July 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Value descriptions for types defined by the libc
*/

#pragma once

#include <sys/resource.h>
#include <sys/time.h>

#include "mldb/types/value_description.h"


/* value descriptions for "timeval" and "rusage" */

DECLARE_STRUCTURE_DESCRIPTION(timeval);
DECLARE_STRUCTURE_DESCRIPTION(rusage);
