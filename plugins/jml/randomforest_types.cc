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
#include "mldb/types/enum_description.h"

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
        context.writeString(MLDB::format("%.18f (%016llx)", val->operator float(), (long long)val->hl));
        //context.writeDouble(*val);
    }
    
    virtual bool isDefaultTyped(const FixedPointAccum64 * val) const
    {
        return *val != 0;
    }
};

DEFINE_VALUE_DESCRIPTION_NS(FixedPointAccum64, FixedPointAccum64Description);

struct FixedPointAccum32Description
    : public ValueDescriptionT<FixedPointAccum32> {
    virtual void parseJsonTyped(FixedPointAccum32 * val,
                                JsonParsingContext & context) const
    {
        *val = context.expectDouble();
    }
    
    virtual void printJsonTyped(const FixedPointAccum32 * val,
                                JsonPrintingContext & context) const
    {
        context.writeDouble(*val);
    }
    
    virtual bool isDefaultTyped(const FixedPointAccum32 * val) const
    {
        return *val != 0;
    }
};

DEFINE_VALUE_DESCRIPTION_NS(FixedPointAccum32, FixedPointAccum32Description);

namespace RF {

DEFINE_ENUM_DESCRIPTION_INLINE(WeightFormat)
{
    addValue("WF_INT_MULTIPLE", WF_INT_MULTIPLE);
    addValue("WF_TABLE",        WF_TABLE);
    addValue("WF_FLOAT",        WF_FLOAT);
}

REGISTER_VALUE_DESCRIPTION(WeightFormat);

DEFINE_STRUCTURE_DESCRIPTION_INLINE(W64)
{
    addField("v", &W64::v, "Weights for false, true label");
    addField("c", &W64::c, "Count of examples in bucket");
}

REGISTER_VALUE_DESCRIPTION(W64);

DEFINE_STRUCTURE_DESCRIPTION_INLINE(W32)
{
    addField("v", &W32::v, "Weights for false, true label");
    addField("c", &W32::c, "Count of examples in bucket");
}

REGISTER_VALUE_DESCRIPTION(W32);

REGISTER_VALUE_DESCRIPTION_ALIAS(W32);
REGISTER_VALUE_DESCRIPTION_ALIAS(W64);
REGISTER_VALUE_DESCRIPTION_ALIAS(W);

std::ostream & operator << (std::ostream & stream, PartitionIndex idx)
{
    return stream << idx.path();
}

struct PartitionIndexDescription
    : public ValueDescriptionT<PartitionIndex> {
    
    virtual void parseJsonTyped(PartitionIndex * val,
                                JsonParsingContext & context) const
    {
        val->index = context.expectInt();
    }
    
    virtual void printJsonTyped(const PartitionIndex * val,
                                JsonPrintingContext & context) const
    {
        context.writeString(val->path());
    }

    virtual bool isDefaultTyped(const PartitionIndex * val) const
    {
        return *val == PartitionIndex::none();
    }
};

DEFINE_VALUE_DESCRIPTION_NS(PartitionIndex,
                            PartitionIndexDescription);


DEFINE_STRUCTURE_DESCRIPTION_INLINE(PartitionSplit)
{
    addField("score", &PartitionSplit::score, "");
    addField("feature", &PartitionSplit::feature, "");
    addField("value", &PartitionSplit::value, "");
    addField("left", &PartitionSplit::left, "");
    addField("right", &PartitionSplit::right, "");
    addField("index", &PartitionSplit::index, "");
}

DEFINE_STRUCTURE_DESCRIPTION_INLINE(RowPartitionInfo)
{
    addField("partition", &RowPartitionInfo::partition_, "");
}

REGISTER_VALUE_DESCRIPTION(RowPartitionInfo);

} // namespace RF
} // namespace MLDB
