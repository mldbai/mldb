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

    // Really, this contains a 64 bit integer
    virtual std::shared_ptr<const ValueDescription> containedPtr() const override
    {
        return getDefaultDescriptionSharedT<int64_t>();
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

    // Really, this contains a 32 bit integer
    virtual std::shared_ptr<const ValueDescription> containedPtr() const override
    {
        return getDefaultDescriptionSharedT<int32_t>();
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

DEFINE_STRUCTURE_DESCRIPTION_INLINE(WIndexed)
{
    addParent<W>();
    addField("index", &WIndexed::index, "Extra working metadata used on GPU kernels");
}

REGISTER_VALUE_DESCRIPTION_ALIAS(W32);
REGISTER_VALUE_DESCRIPTION_ALIAS(W64);
REGISTER_VALUE_DESCRIPTION_ALIAS(W);
REGISTER_VALUE_DESCRIPTION_ALIAS(WIndexed);
REGISTER_VALUE_DESCRIPTION_ALIAS(WeightFormat);

// Verify that sizes of structures are matched by GPU versions
static_assert(sizeof(W32) == 12, "GPU kernels expect that W32 should be a 12 byte structure");
static_assert(alignof(W32) == 4, "GPU kernels expect that W32 should be 4 byte aligned");
static_assert(sizeof(W64) == 24, "GPU kernels expect that W64 should be a 24 byte structure");
static_assert(alignof(W64) == 8, "GPU kernels expect that W64 should be 8 byte aligned");
static_assert(sizeof(PartitionIndex) == 4, "GPU kernels expect that PartitionIndex should be a 4 byte structure");
static_assert(alignof(PartitionIndex) == 4, "GPU kernels expect that PartitionIndex should be 4 byte aligned");
static_assert(sizeof(WIndexed) == 16, "GPU kernels expect that WIndexed is a 16 byte structure");
static_assert(sizeof(PartitionSplit) == 32, "GPU kernels expect that PartitionSplit is a 32 byte structure");
static_assert(sizeof(IndexedPartitionSplit) == 36, "GPU kernels expect that PartitionSplitIndexed is a 36 byte structure");
static_assert(offsetof(PartitionSplit, score) == 0);
static_assert(offsetof(PartitionSplit, feature) == 4);
static_assert(offsetof(PartitionSplit, value) == 6);
static_assert(offsetof(PartitionSplit, left) == 8);
static_assert(offsetof(PartitionSplit, right) == 20);
static_assert(offsetof(IndexedPartitionSplit, index) == 32);

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

    // Really, this contains a 16 bit integer
    virtual std::shared_ptr<const ValueDescription> containedPtr() const override
    {
        return getDefaultDescriptionSharedT<int16_t>();
    }
};

DEFINE_VALUE_DESCRIPTION_NS(PartitionIndex,
                            PartitionIndexDescription);
REGISTER_VALUE_DESCRIPTION(PartitionIndex);
REGISTER_VALUE_DESCRIPTION_ALIAS(PartitionIndex);

DEFINE_STRUCTURE_DESCRIPTION_INLINE(PartitionSplit)
{
    addField("score", &PartitionSplit::score, "");
    addField("feature", &PartitionSplit::feature, "");
    addField("value", &PartitionSplit::value, "");
    addField("left", &PartitionSplit::left, "");
    addField("right", &PartitionSplit::right, "");
}

REGISTER_VALUE_DESCRIPTION(PartitionSplit);
REGISTER_VALUE_DESCRIPTION_ALIAS(PartitionSplit);

DEFINE_STRUCTURE_DESCRIPTION_INLINE(IndexedPartitionSplit)
{
    addParent<PartitionSplit>();
    addField("index", &IndexedPartitionSplit::index, "");
}

REGISTER_VALUE_DESCRIPTION(IndexedPartitionSplit);
REGISTER_VALUE_DESCRIPTION_ALIAS(IndexedPartitionSplit);

DEFINE_STRUCTURE_DESCRIPTION_INLINE(RowPartitionInfo)
{
    addField("partition", &RowPartitionInfo::partition_, "");
}

REGISTER_VALUE_DESCRIPTION(RowPartitionInfo);
REGISTER_VALUE_DESCRIPTION_ALIAS(RowPartitionInfo);

DEFINE_STRUCTURE_DESCRIPTION_INLINE(PartitionInfo)
{
    addField("left", &PartitionInfo::left, "Position of left buckets");
    addField("right", &PartitionInfo::right, "Position of right buckets");
}

REGISTER_VALUE_DESCRIPTION(PartitionInfo);
REGISTER_VALUE_DESCRIPTION_ALIAS(PartitionInfo);

DEFINE_STRUCTURE_DESCRIPTION_INLINE(UpdateWorkEntry)
{
    addField("row", &UpdateWorkEntry::row, "Row number to update");
    addField("partition", &UpdateWorkEntry::partition, "Partition number of row");
    addField("smallSideIndex", &UpdateWorkEntry::smallSideIndex, "Linear index of small side bucket to update");
    addField("decodedRow", &UpdateWorkEntry::decodedRow, "Decoded version of row");
}

REGISTER_VALUE_DESCRIPTION(UpdateWorkEntry);
REGISTER_VALUE_DESCRIPTION_ALIAS(UpdateWorkEntry);

} // namespace RF
} // namespace MLDB
