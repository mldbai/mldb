// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** tabular_dataset.cc                                             -*- C++ -*-
    Jeremy Barnes, 26 November 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

*/

#include "tabular_dataset.h"
#include "mldb/arch/timers.h"
#include "mldb/types/basic_value_descriptions.h"

using namespace std;

namespace Datacratic {
namespace MLDB {

/*****************************************************************************/
/* TABULAR DATA STORE                                                        */
/*****************************************************************************/

/** Data store that can record tabular data and act as a matrix and
    column view to the underlying data.
*/

struct TabularDataset::TabularDataStore: public ColumnIndex, public MatrixView {

    struct TabularDataStoreRowStream : public RowStream {

        TabularDataStoreRowStream(TabularDataStore * store) : store(store)
        {}

        virtual std::shared_ptr<RowStream> clone() const{
            auto ptr = std::make_shared<TabularDataStoreRowStream>(store);
            return ptr;
        }

        virtual void initAt(size_t start){
            size_t sum = 0;
            chunkiter = store->chunks.begin();
            while (chunkiter != store->chunks.end() && start > sum + chunkiter->rowNames.size())  {
                sum += chunkiter->rowNames.size();
                ++chunkiter;
            }

            if (chunkiter != store->chunks.end()) {
                rowiter = chunkiter->rowNames.begin() + (start - sum);
            }
        }

        virtual RowName next() {
            RowName row = *rowiter;
            rowiter++;
            if (rowiter == chunkiter->rowNames.end())
            {
                ++chunkiter;
                if (chunkiter != store->chunks.end())
                {
                    rowiter = chunkiter->rowNames.begin();
                    ExcAssert(rowiter != chunkiter->rowNames.end());
                }
            }
            return row;
        }

        TabularDataStore* store;
        std::vector<TabularDatasetChunk>::const_iterator chunkiter;
        std::vector<RowName>::const_iterator rowiter;

    };

    int64_t rowCount;

    std::vector<ColumnName> columnNames;
    std::vector<ColumnHash> columnHashes;
    ML::Lightweight_Hash<ColumnHash, int> columnIndex;
    
    std::vector<TabularDatasetChunk> chunks;

    /// Index from rowHash to (chunk, indexInChunk) when line number not used for rowName
    ML::Lightweight_Hash<RowHash, std::pair<int, int> > rowIndex;
    std::string filename;
    Date earliestTs, latestTs;

    // Return the value of the column for all rows
    virtual MatrixColumn getColumn(const ColumnName & column) const
    {
        auto it = columnIndex.find(column);
        if (it == columnIndex.end()) {
            throw HttpReturnException(400, "Tabular dataset contains no column with given hash",
                                      "columnHash", column,
                                      "knownColumns", columnNames);
        }

        MatrixColumn result;
        result.columnHash = result.columnName = column;

        for (unsigned i = 0;  i < chunks.size();  ++i) {
            chunks[i].addToColumn(it->second, result.rows);
        }
        
        return result;
    }

    virtual uint64_t getColumnRowCount(const ColumnName & column) const
    {
        return rowCount;
    }

    virtual bool knownColumn(const ColumnName & column) const
    {
        return columnIndex.count(column);
    }

    virtual std::vector<ColumnName> getColumnNames() const
    {
        return columnNames;
    }

    // TODO: we know more than this...
    virtual KnownColumn getKnownColumnInfo(const ColumnName & columnName) const
    {
        auto it = columnIndex.find(columnName);
        if (it == columnIndex.end()) {
            throw HttpReturnException(400, "Tabular dataset contains no column with given hash",
                                      "columnName", columnName,
                                      "knownColumns", columnNames);
        }

        ColumnTypes types;
        for (auto & c: chunks) {
            types.update(c.columns[it->second].columnTypes);
        }

#if 0
        using namespace std;
        cerr << "knownColumnInfo for " << columnName << " is "
             << jsonEncodeStr(types.getExpressionValueInfo()) << endl;
        cerr << "hasNulls = " << types.hasNulls << endl;
        cerr << "hasIntegers = " << types.hasIntegers << endl;
        cerr << "minNegativeInteger = " << types.minNegativeInteger;
        cerr << "maxPositiveInteger = " << types.maxPositiveInteger;
        cerr << "hasReals = " << types.hasReals << endl;
        cerr << "hasStrings = " << types.hasStrings << endl;
        cerr << "hasOther = " << types.hasOther << endl;
#endif

        return KnownColumn(columnName, types.getExpressionValueInfo(),
                           COLUMN_IS_DENSE);
    }

    virtual std::vector<RowName>
    getRowNames(ssize_t start = 0, ssize_t limit = -1) const
    {
        ExcAssertEqual(start, 0);
        ExcAssertEqual(limit, -1);

        std::vector<RowName> result;
        result.reserve(rowCount);

        for (auto & c: chunks) {
            result.insert(result.end(), c.rowNames.begin(), c.rowNames.end());
        }

        std::sort(result.begin(), result.end(),
                  [] (const RowName & n1,
                      const RowName & n2)
                  {
                      return n1.hash() < n2.hash();
                  });

        return result;
    }

    virtual std::vector<RowHash>
    getRowHashes(ssize_t start = 0, ssize_t limit = -1) const
    {
        ExcAssertEqual(start, 0);
        ExcAssertEqual(limit, -1);

        std::vector<RowHash> result;

        for (auto & i: rowIndex) {
            result.emplace_back(i.first);
        }

        return result;
    }

    std::pair<int, int> tryLookupRow(const RowName & rowName) const
    {
        auto it = rowIndex.find(rowName);
        if (it == rowIndex.end())
            return { -1, -1 };
        return it->second;
    }
    
    std::pair<int, int> lookupRow(const RowName & rowName) const
    {
        auto result = tryLookupRow(rowName);
        if (result.first == -1)
            throw HttpReturnException(400, "Row not found in CSV dataset");
        return result;
    }

    virtual bool knownRow(const RowName & rowName) const
    {
        int chunkIndex;
        int rowIndex;

        std::tie(chunkIndex, rowIndex) = tryLookupRow(rowName);
        return chunkIndex > 0;
    }

    virtual MatrixNamedRow getRow(const RowName & rowName) const
    {
        //cerr << "getting row " << rowName << endl;

        MatrixNamedRow result;
        result.rowHash = rowName;
        result.rowName = rowName;

        auto it = rowIndex.find(rowName);
        if (it == rowIndex.end()) {
            throw HttpReturnException(400, "Row not found in CSV dataset");
        }

        //cerr << "row is in chunk " << it->second.first << " offset "
        //     << it->second.second << endl;

        Date ts = chunks.at(it->second.first).timestamps[it->second.second].toTimestamp();

        for (unsigned i = 0;  i < columnNames.size();  ++i) {
            result.columns.emplace_back(columnNames[i], chunks.at(it->second.first).columns.at(i)[it->second.second], ts);
        }

        return result;
    }

    virtual RowName getRowName(const RowHash & rowHash) const
    {
        auto it = rowIndex.find(rowHash);
        if (it == rowIndex.end()) {
            throw HttpReturnException(400, "Row not found in CSV dataset");
        }

        return RowName(chunks.at(it->second.first).rowNames[it->second.second].toUtf8String());
    }

    virtual ColumnName getColumnName(ColumnHash column) const
    {
        auto it = columnIndex.find(column);
        if (it == columnIndex.end())
            throw HttpReturnException(400, "CSV dataset contains no column with given hash",
                                      "columnHash", column,
                                      "knownColumns", columnNames,
                                      "knownColumnHashes", columnHashes);
        return columnNames[it->second];
    }

    virtual const ColumnStats &
    getColumnStats(const ColumnName & column, ColumnStats & stats) const
    {
        auto it = columnIndex.find(column);
        if (it == columnIndex.end()) {
            throw HttpReturnException(400, "CSV dataset contains no column with given hash",
                                      "columnHash", column,
                                      "knownColumns", columnNames,
                                      "knownColumnHashes", columnHashes);
        }

        stats = ColumnStats();

        bool isNumeric = true;

        for (auto & c: chunks) {

            auto onValue = [&] (const CellValue & value,
                                size_t rowCount)
                {
                    if (!value.isNumber())
                        isNumeric = false;
                    stats.values[value].rowCount_ += 1;
                    return true;
                };
                                
            c.columns[it->second].forEachDistinctValue(onValue);
        }

        stats.isNumeric_ = isNumeric && !chunks.empty();
        stats.rowCount_ = rowCount;
        return stats;
    }

    virtual size_t getRowCount() const
    {
        return rowCount;
    }

    virtual size_t getColumnCount() const
    {
        return columnNames.size();
    }

    virtual std::pair<Date, Date> getTimestampRange() const
    {
        return { earliestTs, latestTs };
    }

    GenerateRowsWhereFunction
    generateRowsWhere(const SqlBindingScope & context,
                      const SqlExpression & where,
                      ssize_t offset,
                      ssize_t limit) const
    {
        GenerateRowsWhereFunction result;
        return result;
    }

    void finalize( std::vector<TabularDatasetChunk>& inputChunks, uint64_t totalRows)
    {
        rowCount = totalRows;

        chunks = std::move(inputChunks);

        ML::Timer rowIndexTimer;
        //cerr << "creating row index" << endl;
        rowIndex.reserve(4 * totalRows / 3);
        //cerr << "rowIndex capacity is " << rowIndex.capacity() << endl;
        for (unsigned i = 0;  i < chunks.size();  ++i) {
            for (unsigned j = 0;  j < chunks[i].rowNames.size();  ++j) {
                if (!rowIndex.insert({ chunks[i].rowNames[j], { i, j } }).second)
                    throw HttpReturnException(400, "Duplicate row name in CSV dataset",
                                              "rowName", chunks[i].rowNames[j]);
            }
        }
        //cerr << "done creating row index" << endl;
        //cerr << "row index took " << rowIndexTimer.elapsed() << endl;

    }

    void initialize(vector<ColumnName>& columnNames, ML::Lightweight_Hash<ColumnHash, int>& columnIndex)
    {
        this->columnNames = std::move(columnNames);
        this->columnIndex = std::move(columnIndex);

        for (const auto& c : this->columnNames) {
                ColumnHash ch(c);
                columnHashes.push_back(ch);
            }
    }
};

TabularDataset::
TabularDataset(MldbServer * owner,
               PolyConfig config,
               const std::function<bool (const Json::Value &)> & onProgress)
: Dataset(owner)
{
    itl = make_shared<TabularDataStore>();
}

void 
TabularDataset::
initialize(vector<ColumnName>& columnNames, ML::Lightweight_Hash<ColumnHash, int>& columnIndex)
{
	itl->initialize(columnNames, columnIndex);
}

TabularDatasetChunk* 
TabularDataset::
createNewChunk(size_t rowsPerChunk)
{
    return new TabularDatasetChunk(itl->columnNames.size(), rowsPerChunk);
}

void
TabularDataset::
finalize( std::vector<TabularDatasetChunk>& inputChunks, uint64_t totalRows)
{
    itl->finalize(inputChunks, totalRows);
}

TabularDataset::
~TabularDataset()
{
}

Any
TabularDataset::
getStatus() const
{
    Json::Value status;
    status["rowCount"] = itl->rowCount;
    return status;
}

std::pair<Date, Date>
TabularDataset::
getTimestampRange() const
{
    return itl->getTimestampRange();
}

std::shared_ptr<MatrixView>
TabularDataset::
getMatrixView() const
{
    return itl;
}

std::shared_ptr<ColumnIndex>
TabularDataset::
getColumnIndex() const
{
    return itl;
}

std::shared_ptr<RowStream> 
TabularDataset::
getRowStream() const 
{ 
    return std::make_shared<TabularDataStore::TabularDataStoreRowStream>(itl.get()); 
} 

GenerateRowsWhereFunction
TabularDataset::
generateRowsWhere(const SqlBindingScope & context,
                  const SqlExpression & where,
                  ssize_t offset,
                  ssize_t limit) const
{
    GenerateRowsWhereFunction fn
        = itl->generateRowsWhere(context, where, offset, limit);
    if (!fn)
        fn = Dataset::generateRowsWhere(context, where, offset, limit);
    return fn;
}

KnownColumn
TabularDataset::
getKnownColumnInfo(const ColumnName & columnName) const
{
    return itl->getKnownColumnInfo(columnName);
}

namespace {

RegisterDatasetType<TabularDataset, PersistentDatasetConfig>
regCsv(builtinPackage(),
       "tabular",
       "NJK FILL ME",
       "datasets/TabularDataset.md.html");

} // file scope*/

} // MLDB
} // Datacratic