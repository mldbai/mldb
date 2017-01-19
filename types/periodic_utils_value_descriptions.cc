// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** periodic_utils_value_descriptions.cc
    Jeremy Barnes, 26 August 2013
    Copyright (c) 2013 mldb.ai inc.  All rights reserved.

*/

#include "periodic_utils.h"
#include "value_description.h"

namespace MLDB {

struct TimePeriodDescription: public ValueDescriptionT<TimePeriod> {
    virtual void parseJsonTyped(TimePeriod * val,
                                JsonParsingContext & context) const
    {
        val->parse(context.expectStringAscii());
    }

    virtual void printJsonTyped(const TimePeriod * val,
                                JsonPrintingContext & context) const
    {
        context.writeString(val->toString());
    }
};

DEFINE_VALUE_DESCRIPTION(TimePeriod, TimePeriodDescription);


} // namespace MLDB
