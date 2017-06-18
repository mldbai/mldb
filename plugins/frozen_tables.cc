/** frozen_tables.cc
    Jeremy Barnes, 12 June 2017
    Copyright (c) 2017 Element AI Inc.  All rights reserved.
    This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
*/

#include "frozen_tables.h"
#include "mldb/http/http_exception.h"

namespace MLDB {
    

/*****************************************************************************/
/* MUTABLE CELL VALUE TABLE                                                  */
/*****************************************************************************/

FrozenCellValueTable
MutableCellValueTable::
freeze(MappedSerializer & serializer)
{
    MutableIntegerTable offsets;
    size_t totalOffset = 0;
        
    for (size_t i = 0;  i < values.size();  ++i) {
        totalOffset += values[i].serializedBytes(true /* exact length */);
        offsets.add(totalOffset);
    }

    FrozenIntegerTable frozenOffsets
        = offsets.freeze(serializer);
    MutableMemoryRegion region
        = serializer.allocateWritable(totalOffset, 8);

    char * c = region.data();

    size_t currentOffset = 0;

    for (size_t i = 0;  i < values.size();  ++i) {
        size_t length = frozenOffsets.get(i) - currentOffset;
        c = values[i].serialize(c, length, true /* exact length */);
        currentOffset += length;
        ExcAssertEqual(c - region.data(), currentOffset);
    }

    ExcAssertEqual(c - region.data(), totalOffset);
    ExcAssertEqual(currentOffset, totalOffset);

    FrozenCellValueTable result;
    result.offsets = std::move(frozenOffsets);
    result.cells = region.freeze();
    return result;
}


/*****************************************************************************/
/* MUTABLE CELL VALUE SET                                                    */
/*****************************************************************************/

std::pair<FrozenCellValueSet, std::vector<uint32_t> >
MutableCellValueSet::
freeze(MappedSerializer & serializer)
{
    // For now, we don't remap.  Later we will...
    std::vector<uint32_t> remapping;
    remapping.reserve(indexes.size());

    MutableIntegerTable offsets;
    size_t totalOffset = 0;
        
    for (size_t i = 0;  i < others.size();  ++i) {
        totalOffset += others[i].serializedBytes(true /* exact length */);
        offsets.add(totalOffset);
        remapping.push_back(i);
    }

    FrozenIntegerTable frozenOffsets
        = offsets.freeze(serializer);
    MutableMemoryRegion region
        = serializer.allocateWritable(totalOffset, 8);

    char * c = region.data();

    size_t currentOffset = 0;

    for (size_t i = 0;  i < others.size();  ++i) {
        size_t length = frozenOffsets.get(i) - currentOffset;
        c = others[i].serialize(c, length, true /* exact length */);
        currentOffset += length;
        ExcAssertEqual(c - region.data(), currentOffset);
    }

    ExcAssertEqual(c - region.data(), totalOffset);
    ExcAssertEqual(currentOffset, totalOffset);

    FrozenCellValueSet result;
    result.offsets = std::move(frozenOffsets);
    result.cells = region.freeze();
    return std::make_pair(std::move(result), std::move(remapping));
}

void
MutableCellValueSet::
add(CellValue val)
{
    indexes.emplace_back(OTHER, others.size());
    others.emplace_back(std::move(val));
    return;

    switch (val.cellType()) {
    case CellValue::EMPTY:
        throw HttpReturnException(500, "Can't add null value to CellValueSet");
    case CellValue::INTEGER:
        indexes.emplace_back(INT, intValues.add(val.toInt()));
        return;
    case CellValue::FLOAT:
        indexes.emplace_back(DOUBLE, doubleValues.add(val.toDouble()));
        doubleValues.add(val.toInt());
        return;
    case CellValue::ASCII_STRING:
    case CellValue::UTF8_STRING:
        indexes.emplace_back(STRING, stringValues.add(val.toUtf8String().stealRawString()));
        return;
    case CellValue::BLOB:
        indexes.emplace_back(BLOB,
                             blobValues.add((const char *)val.blobData(),
                                            val.blobLength()));
        return;
    case CellValue::PATH:
        indexes.emplace_back(PATH, pathValues.add(val.coerceToPath()));
        return;
    case CellValue::TIMESTAMP:
    case CellValue::TIMEINTERVAL:
        indexes.emplace_back(OTHER, others.size());
        others.emplace_back(std::move(val));
        return;
    default:
        break;
    }

    throw HttpReturnException
        (500, "Couldn't add unknown cell to MutableCellValueSet");
}

} // namespace MLDB
