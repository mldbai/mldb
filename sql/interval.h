// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** interval.h
    Mathieu Marquis Bolduc, October 14th 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.
*/

#pragma once

#include <stdint.h>

namespace ML {
	struct Parse_Context;
}

namespace MLDB {

void expect_interval(ML::Parse_Context & context, uint32_t& months, uint32_t& days, double& seconds);

}

