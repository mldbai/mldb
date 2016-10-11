/** interval.h
    Mathieu Marquis Bolduc, October 14th 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
*/

#pragma once

#include <stdint.h>

namespace MLDB {

struct ParseContext;
void expect_interval(ParseContext & context, uint32_t& months, uint32_t& days, double& seconds);

} // namespace MLDB


