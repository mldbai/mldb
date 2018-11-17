/** randomforest_types.cc
    Jeremy Barnes, 13 October 2018
    Copyright (c) 2018 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Types for random forest algorithm.
*/

#include "randomforest_types.h"
#include "mldb/types/value_description.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/array_description.h"

namespace MLDB {

struct FixedPointAccum64Description
    : public ValueDescriptionT<FixedPointAccum64> {
    virtual void parseJsonTyped(FixedPointAccum64 * val,
                                JsonParsingContext & context) const
    {
        *val = context.expectDouble();
    }
    
    virtual void printJsonTyped(const FixedPointAccum64 * val,
                                JsonPrintingContext & context) const
    {
        context.writeDouble(*val);
    }
    
    virtual bool isDefaultTyped(const FixedPointAccum64 * val) const
    {
        return *val != 0;
    }
};

DEFINE_VALUE_DESCRIPTION_NS(FixedPointAccum64, FixedPointAccum64Description);

namespace RF {


DEFINE_STRUCTURE_DESCRIPTION_INLINE(W)
{
    addField("v", &W::v, "Weights for false, true label");
    addField("c", &W::c, "Count of examples in bucket");
}

} // namespace RF
} // namespace MLDB
