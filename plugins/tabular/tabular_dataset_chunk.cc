/** tabular_dataset_chunk.cc
    Jeremy Barnes, 23 October 2016
    Copyright (c) 2016 mldb.ai Inc.  All rights reserved.

    Chunk of tabular dataset.
*/

#include "tabular_dataset_chunk.h"
#include "mldb/sql/expression_value.h"

namespace MLDB {

/*****************************************************************************/
/* TABULAR DATASET CHUNK                                                     */
/*****************************************************************************/

size_t
TabularDatasetChunk::
memusage() const
{
    using namespace std;
    size_t result = sizeof(*this);
    size_t before = result;
    for (auto & c: columns)
        result += c->memusage();
        
    //cerr << columns.size() << " columns took " << result - before << endl;
    before = result;
        
    for (auto & c: sparseColumns)
        result += c.first.memusage() + c.second->memusage();

    //cerr << sparseColumns.size() << " sparse columns took "
    //     << result - before << endl;
    before = result;

    result += rowNames->memusage();

    //cerr << rowNames->size() << " row names took "
    //     << result - before << endl;
    before = result;

    result += timestamps->memusage();

    //cerr << "timestamps took " << result - before << endl;

    //cerr << "total memory is " << result << endl;
    return result;
}

const FrozenColumn *
TabularDatasetChunk::
maybeGetColumn(size_t columnIndex, const PathElement & columnName) const
{
    if (columnIndex < columns.size()) {
        return columns[columnIndex].get();
    }
    else {
        auto it = sparseColumns.find(columnName);
        if (it == sparseColumns.end())
            return nullptr;
        return it->second.get();
    }
}

/// Return an owned version of the rowname
RowPath
TabularDatasetChunk::
getRowPath(size_t index) const
{
    return rowNames->get(index).coerceToPath();
}

/// Return a reference to the rowName, stored in storage if it's a temp
const RowPath &
TabularDatasetChunk::
getRowPath(size_t index, RowPath & storage) const
{
    return storage = getRowPath(index);
}

const FrozenColumn *
TabularDatasetChunk::
maybeGetColumn(size_t columnIndex, const Path & columnName) const
{
    if (columnIndex < columns.size()) {
        return columns.at(columnIndex).get();
    }
    else {
        auto it = sparseColumns.find(columnName);
        if (it == sparseColumns.end())
            return nullptr;
        return it->second.get();
    }
}

/// Get the row with the given index
std::vector<std::tuple<ColumnPath, CellValue, Date> >
TabularDatasetChunk::
getRow(size_t index, const std::vector<ColumnPath> & fixedColumnNames) const
{
    ExcAssertLess(index, rowCount());
    std::vector<std::tuple<ColumnPath, CellValue, Date> > result;
    result.reserve(columns.size());
    Date ts = timestamps->get(index).mustCoerceToTimestamp();
    for (size_t i = 0;  i < columns.size();  ++i) {
        CellValue val = columns[i]->get(index);
        if (val.empty())
            continue;
        result.emplace_back(fixedColumnNames[i], std::move(val), ts);
    }

    for (auto & c: sparseColumns) {
        CellValue val = c.second->get(index);
        if (val.empty())
            continue;
        result.emplace_back(c.first, std::move(val), ts);

    }
    return result;
}

/// Get the row with the given index
ExpressionValue
TabularDatasetChunk::
getRowExpr(size_t index, const std::vector<ColumnPath> & fixedColumnNames) const
{
    ExcAssertLess(index, rowCount());
    std::vector<std::tuple<ColumnPath, CellValue, Date> > result;
    result.reserve(columns.size());
    Date ts = timestamps->get(index).mustCoerceToTimestamp();
    for (size_t i = 0;  i < columns.size();  ++i) {
        CellValue val = columns[i]->get(index);
        if (val.empty())
            continue;
        result.emplace_back(fixedColumnNames[i], std::move(val), ts);
    }

    for (auto & c: sparseColumns) {
        CellValue val = c.second->get(index);
        if (val.empty())
            continue;
        result.emplace_back(c.first, std::move(val), ts);

    }
    return std::move(result);
}

void
TabularDatasetChunk::
addToColumn(int columnIndex,
            const ColumnPath & colName,
            std::vector<std::tuple<RowPath, CellValue, Date> > & rows,
            bool dense) const
{
    const FrozenColumn * col = nullptr;
    if (columnIndex < columns.size())
        col = columns[columnIndex].get();
    else {
        auto it = sparseColumns.find(colName);
        if (it == sparseColumns.end()) {
            if (dense) {
                for (unsigned i = 0;  i < rowCount();  ++i) {
                    rows.emplace_back(getRowPath(i),
                                      CellValue(),
                                      timestamps->get(i).mustCoerceToTimestamp());
                }
            }
            return;
        }
        col = it->second.get();
    }

    for (unsigned i = 0;  i < rowCount();  ++i) {
        CellValue val = col->get(i);
        if (dense || !val.empty()) {
            rows.emplace_back(getRowPath(i),
                              std::move(val),
                              timestamps->get(i).mustCoerceToTimestamp());
        }
    }
}

void
TabularDatasetChunk::
serialize(StructuredSerializer & serializer) const
{
    auto serializeAs = [&] (PathElement name,
                            const FrozenColumn & col)
        {
            col.serialize(*serializer.newStructure(name));
        };
    
    for (size_t i = 0;  i < columns.size();  ++i) {
        serializeAs(i, *columns[i]);
    }

    if (!sparseColumns.empty()) {
        auto sparseSerializer = serializer.newStructure("sp");
        for (auto & c: sparseColumns) {
            c.second->serialize(*sparseSerializer->newStructure(c.first.toUtf8String()));
        }
    }
    serializeAs("rn", *rowNames);
    serializeAs("ts", *timestamps);
}

TabularDatasetChunk::
TabularDatasetChunk(StructuredReconstituter & reconstituter)
{
    auto dir = reconstituter.getDirectory();

    // How many columns are there?
    ssize_t maxIndex = -1;
    for (auto & entry: dir) {
        using namespace std;
        if (entry.name.isIndex())
            maxIndex = std::max(maxIndex, entry.name.toIndex());
    }

    columns.resize(maxIndex + 1);

    // Now do each one
    for (auto & entry: dir) {
        if (entry.name.isIndex()) {
            ExcAssert(!columns.at(entry.name.toIndex()));
            columns.at(entry.name.toIndex())
                .reset(FrozenColumnFormat::thaw(*entry.getStructure()));
        }
        else if (entry.name == PathElement("sp")) {
            // reconstitute the sparse columns
            auto sparseDirectory = entry.getStructure();

            for (auto & e: sparseDirectory->getDirectory()) {
                Path p = Path::parse(e.name.toUtf8String());
                ExcAssert(!sparseColumns.count(p));

                std::shared_ptr<FrozenColumn> column
                    (FrozenColumnFormat::thaw(*e.getStructure()));
                sparseColumns.emplace(std::move(p), std::move(column));
            }
        }
        else if (entry.name == PathElement("rn")) {
            ExcAssert(!rowNames);
            rowNames.reset(FrozenColumnFormat::thaw(*entry.getStructure()));
        }
        else if (entry.name == PathElement("ts")) {
            ExcAssert(!timestamps);
            timestamps.reset(FrozenColumnFormat::thaw(*entry.getStructure()));
        }
    }

    // Verify integrity; this is user provided data
    ExcAssert(rowNames);
    size_t len = rowNames->size();
    ExcAssert(timestamps);
    ExcAssertEqual(len, timestamps->size());
    for (size_t i = 0;  i < columns.size();  ++i) {
        ExcAssert(columns[i]);
        ExcAssertEqual(len, columns[i]->size());
    }
    for (auto & s: sparseColumns) {
        ExcAssertEqual(s.second->size(), len);
    }
}



/*****************************************************************************/
/* MUTABLE TABULAR DATASET CHUNK                                             */
/*****************************************************************************/

MutableTabularDatasetChunk::
MutableTabularDatasetChunk(size_t numColumns, size_t maxSize)
    : maxSize(maxSize), rowCount_(0),
      columns(numColumns), isFrozen(false),
      addFailureNotified(false)
{
    timestamps.reserve(maxSize);
    rowNames.reserve(maxSize);
    for (unsigned i = 0;  i < numColumns;  ++i)
        columns[i].reserve(maxSize);
}

TabularDatasetChunk
MutableTabularDatasetChunk::
freeze(MappedSerializer & serializer,
       const ColumnFreezeParameters & params)
{
    std::unique_lock<std::mutex> guard(mutex);

    ExcAssert(!isFrozen);

    TabularDatasetChunk result;
    result.columns.resize(columns.size());
    result.sparseColumns.reserve(sparseColumns.size());

    for (unsigned i = 0;  i < columns.size();  ++i)
        result.columns[i] = columns[i].freeze(serializer, params);
    for (auto & c: sparseColumns)
        result.sparseColumns.emplace(c.first, c.second.freeze(serializer, params));

    result.timestamps = timestamps.freeze(serializer, params);
    result.rowNames = rowNames.freeze(serializer, params);

    isFrozen = true;

    return result;
}

int
MutableTabularDatasetChunk::
add(RowPath & rowName,
    Date ts,
    CellValue * vals,
    size_t numVals,
    std::vector<std::pair<ColumnPath, CellValue> > & extra)
{
    std::unique_lock<std::mutex> guard(mutex);
    if (isFrozen)
        return ADD_AWAIT_ROTATION;
    size_t numRows = rowCount_;

    if (numRows == maxSize) {
        if (addFailureNotified)
            return ADD_AWAIT_ROTATION;
        else {
            addFailureNotified = true;
            return ADD_PERFORM_ROTATION;
        }
    }

    ExcAssertEqual(columns.size(), numVals);
    ssize_t index = rowName.toIndex();
    if (index != -1)
        rowNames.add(numRows, index);
    else
        rowNames.add(numRows, std::move(rowName));

    timestamps.add(numRows, ts.secondsSinceEpoch());

    for (unsigned i = 0;  i < columns.size();  ++i) {
        columns[i].add(numRows, std::move(vals[i]));
    }

    for (auto & e: extra) {
        auto it = sparseColumns.emplace(std::move(e.first), TabularDatasetColumn()).first;
        it->second.add(numRows, std::move(e.second));
    }

    ++rowCount_;

    return ADD_SUCCEEDED;
}

} // namespace MLDB
