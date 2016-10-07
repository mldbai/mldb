// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** merged_dataset.cc                                              -*- C++ -*-
    Jeremy Barnes, 28 February 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

*/

#include "merged_dataset.h"
#include "mldb/sql/sql_expression.h"
#include "id_hash.h"
#include "merge_hash_entries.h"
#include "mldb/types/any_impl.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/vector_description.h"
#include "mldb/http/http_exception.h"
#include <thread>


using namespace std;



namespace MLDB {


/*****************************************************************************/
/* MERGED DATASET CONFIG                                                     */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(MergedDatasetConfig);

MergedDatasetConfigDescription::
MergedDatasetConfigDescription()
{
    nullAccepted = true;

    addField("datasets", &MergedDatasetConfig::datasets,
             "Datasets to merge together");
}


/*****************************************************************************/
/* MERGED INTERNAL REPRESENTATION                                            */
/*****************************************************************************/


struct MergedDataset::Itl
    : public MatrixView, public ColumnIndex {

    IdHashes rowIndex;
    IdHashes columnIndex;

    /// Datasets that it was constructed with
    std::vector<std::shared_ptr<Dataset> > datasetsIn;

    /// Datasets that were actually merged.  There will be a maximum of 31
    /// of them, as any more will be sub-merged
    std::vector<std::shared_ptr<Dataset> > datasets;

    /// Matrix view.  Length is the same as that of datasets.
    std::vector<std::shared_ptr<MatrixView> > matrices;

    Itl(MldbServer * server, std::vector<std::shared_ptr<Dataset> > datasets)
    {
        // 1.  Sort them so that the biggest ones are at the start

        //for (auto & d: datasets) {
        //    cerr << "dataset has " << d->getMatrixView()->getRowHashes().size()
        //         << " rows and " << d->getMatrixView()->getColumnNames().size()
        //         << " columns" << endl;
        //}

        if (datasets.empty())
            throw ML::Exception("Attempt to merge no datasets together");


        std::sort(datasets.begin(), datasets.end(),
                  [] (std::shared_ptr<Dataset> p1,
                      std::shared_ptr<Dataset> p2)
                  {
                    return p1->getRowCount() > p2->getRowCount();
                  });

        std::vector<std::shared_ptr<Dataset> > toMerge;

        // Now work out how our tree is laid out.  We aim to have the biggest
        // entries at the start so they don't need a sub-tree.
        int numSubMerges = (datasets.size() - 1) / 31;

        // The first 31 - numSubMerges get added directly
        int numDirect = std::max(0, 31 - numSubMerges);
        numDirect = std::min<int>(numDirect, datasets.size());

        for (unsigned i = 0;  i < numDirect;  ++i) {
            toMerge.push_back(datasets[i]);
        }

        // The rest get broken down into groups of 31, pre-merged, and then added
        for (int current = numDirect;  current < datasets.size();  ) {
            //cerr << "current = " << current << " toMerge.size() = "
            //     << toMerge.size() << endl;
            vector<std::shared_ptr<Dataset> > subToMerge;
            for (; current < datasets.size() && subToMerge.size() < 31;  ++current) {
                //cerr << "adding " << current << endl;
                subToMerge.push_back(datasets[current]);
            }

            //cerr << "merging " << subToMerge.size() << " files" << endl;

            toMerge.push_back(std::make_shared<MergedDataset>(server, subToMerge));
            //cerr << "done merging " << subToMerge.size() << " files" << endl;
        }

        ExcAssertLessEqual(toMerge.size(), 32);
    
        auto getRowHashes = [&] (int datasetIndex)
            {
                auto dataset = toMerge[datasetIndex];
                MergeHashEntries result;
                vector<RowHash> rows = dataset->getMatrixView()->getRowHashes();
                std::sort(rows.begin(), rows.end());
                ExcAssert(std::unique(rows.begin(), rows.end()) == rows.end());

                result.reserve(rows.size());
                for (auto r: rows)
                    result.add(r.hash(), 1ULL << datasetIndex);
                return result;
            };

        auto getColumnHashes = [&] (int datasetIndex)
            {
                auto dataset = toMerge[datasetIndex];
                MergeHashEntries result;
                vector<ColumnPath> cols = dataset->getMatrixView()->getColumnNames();
                std::sort(cols.begin(), cols.end());
                ExcAssert(std::unique(cols.begin(), cols.end()) == cols.end());
                result.reserve(cols.size());
                for (auto c: cols)
                    result.add(c.hash(), 1ULL << datasetIndex);
                return result;
            };

        auto initRowBucket = [&] (int i, MergeHashEntryBucket & b)
            {
                auto & b2 = rowIndex.buckets[i];

                b2.reserve(b.size());

                for (auto & e: b) {
                    uint64_t h = e.hash;
                    uint32_t bm = e.bitmap;
                    b2.insert(make_pair(h, bm));
                }
            };

        auto initColumnBucket = [&] (int i, MergeHashEntryBucket & b)
            {
                auto & b2 = columnIndex.buckets[i];

                b2.reserve(b.size());

                for (auto & e: b) {
                    uint64_t h = e.hash;
                    uint32_t bm = e.bitmap;
                    b2.insert(make_pair(h, bm));
                }
            };

        std::thread mergeColumns([&] () { extractAndMerge(toMerge.size(), getColumnHashes, initColumnBucket); });
        std::thread mergeRows([&] () { extractAndMerge(toMerge.size(), getRowHashes, initRowBucket); });

        mergeColumns.join();
        mergeRows.join();

        this->datasetsIn = std::move(datasets);
        this->datasets = std::move(toMerge);
        for (auto & d: this->datasets) {
            matrices.emplace_back(d->getMatrixView());
        }

        cerr << "merged dataset has " << this->getRowHashes().size()
             << " rows and " << this->getColumnNames().size()
             << " columns" << endl;
    }

    struct MergedRowStream : public RowStream {

        MergedRowStream(const MergedDataset::Itl* source) : source(source)
        {            
        }

        virtual std::shared_ptr<RowStream> clone() const
        {
            return make_shared<MergedRowStream>(source);
        }

        /* set where the stream should start*/
        virtual void initAt(size_t start)
        {
            it = source->rowIndex.begin();
            for (size_t i = 0; i < start; ++i)
                ++it;
        }

        virtual RowPath next()
        {
            uint64_t hash = (*it).first;
            ++it;

            return source->getRowPath(RowHash(hash));
        }

        virtual const RowPath & rowName(RowPath & storage) const
        {
            uint64_t hash = (*it).first;
            return storage = source->getRowPath(RowHash(hash));
        }

        const MergedDataset::Itl* source;
        IdHashes::const_iterator it;

    };
       
    virtual std::vector<RowPath>
    getRowPaths(ssize_t start = 0, ssize_t limit = -1) const
    {
        auto hashes = getRowHashes(start, limit);
        
        std::vector<RowPath> result;

        for (auto & h: getRowHashes(start, limit))
            result.emplace_back(getRowPath(h));

        return result;
    }

    virtual std::vector<RowHash>
    getRowHashes(ssize_t start = 0, ssize_t limit = -1) const
    {
        std::vector<RowHash> result;

        size_t index = 0;
        auto onRow = [&] (uint64_t hash, uint32_t bitmap)
            {
                if (index < start) {
                    ++index;
                    return true;
                }

                if (limit != -1 && result.size() >= index)
                    return false;

                RowHash rowHash(hash);
                result.push_back(rowHash);

                return true;
            };

        rowIndex.forEach(onRow);

        return result;
    }

    virtual bool knownRow(const RowPath & rowName) const
    {
        uint32_t bitmap = getRowBitmap(rowName);
        return bitmap != 0;
    }

    virtual bool knownRowHash(const RowHash & rowHash) const
    {
        uint32_t bitmap = getRowBitmap(rowHash);
        return bitmap != 0;
    }

    virtual RowPath getRowPath(const RowHash & rowHash) const
    {
        uint32_t bitmap = getRowBitmap(rowHash);
        if (!bitmap)
            throw ML::Exception("Row not known");

        int bit = ML::lowest_bit(bitmap, -1);
        return datasets[bit]->getMatrixView()->getRowPath(rowHash);
    }

    virtual MatrixNamedRow getRow(const RowPath & rowName) const
    {
        uint32_t bitmap = getRowBitmap(rowName);
        if (!bitmap)
            throw ML::Exception("Row not known");

        int bit = ML::lowest_bit(bitmap, -1);
        MatrixNamedRow result = datasets[bit]->getMatrixView()->getRow(rowName);
        bitmap = bitmap & ~(1 << bit);

        while (bitmap) {
            int bit = ML::lowest_bit(bitmap, -1);
            auto row = datasets[bit]->getMatrixView()->getRow(rowName);
            //ExcAssertEqual(result.rowName, row.rowName);

            result.columns.insert(result.columns.end(),
                                  std::make_move_iterator(row.columns.begin()),
                                  std::make_move_iterator(row.columns.end()));
            
            bitmap = bitmap & ~(1 << bit);
        }

        return result;
    }

    virtual bool knownColumn(const ColumnPath & column) const
    {
        return getColumnBitmap(column) != 0;
    }

    virtual ColumnPath getColumnName(ColumnHash columnHash) const
    {
        uint32_t bitmap = getColumnBitmap(columnHash);

        if (bitmap == 0)
            throw ML::Exception("Column not found in merged dataset");

        int bit = ML::lowest_bit(bitmap, -1);

        return datasets[bit]->getMatrixView()->getColumnName(columnHash);
    }

    /** Return a list of all columns. */
    virtual std::vector<ColumnPath> getColumnNames() const
    {
        std::vector<ColumnPath> result;

        auto onColumn = [&] (uint64_t hash, uint32_t bitmap)
            {
                ColumnHash columnHash(hash);
                int bit = ML::lowest_bit(bitmap, -1);
                ExcAssertNotEqual(bit, -1);
                result.push_back(datasets[bit]->getMatrixView()->getColumnName(columnHash));
                
                return true;
            };
        
        columnIndex.forEach(onColumn);
        
        return result;
    }

#if 0  // default version in dataset.cc is correct but less efficient
    virtual const ColumnStats &
    getColumnStats(const ColumnPath & columnName, ColumnStats & toStoreResult) const
    {
        uint32_t bitmap = getColumnBitmap(columnName);
        if (!bitmap)
            throw ML::Exception("Column not known");

        int bit = ML::lowest_bit(bitmap, -1);
        bitmap = bitmap & ~(1 << bit);

        if (!bitmap) {
            // only one has this column; we can just call it
            return datasets[bit]->getColumnIndex()
                ->getColumnStats(columnName, toStoreResult);
        }

        auto column = getColumn(columnName);

        // TODO: fill in the stats...

        throw ML::Exception("MergedDataset::getColumnStats() not finished");
    }
#endif

    /** Return the value of the column for all rows and timestamps. */
    virtual MatrixColumn getColumn(const ColumnPath & columnHash) const
    {
        uint32_t bitmap = getColumnBitmap(columnHash);
        if (!bitmap)
            throw ML::Exception("Column not known");

        int bit = ML::lowest_bit(bitmap, -1);
        MatrixColumn result = datasets[bit]->getColumnIndex()->getColumn(columnHash);
        bitmap = bitmap & ~(1 << bit);

        while (bitmap) {
            int bit = ML::lowest_bit(bitmap, -1);
            auto column = datasets[bit]->getColumnIndex()->getColumn(columnHash);
            ExcAssertEqual(result.columnName, column.columnName);

            result.rows.insert(result.rows.end(),
                                  std::make_move_iterator(column.rows.begin()),
                                  std::make_move_iterator(column.rows.end()));
            
            bitmap = bitmap & ~(1 << bit);
        }

        return result;
    }

    /** Return the value of the column for all rows and timestamps. */
    virtual std::vector<std::tuple<RowPath, CellValue> >
    getColumnValues(const ColumnPath & columnName,
                    const std::function<bool (const CellValue &)> & filter) const
    {
        std::vector<std::tuple<RowPath, CellValue> > result;
        uint32_t bitmap = getColumnBitmap(columnName);
        if (bitmap)
        {
            int bit = ML::lowest_bit(bitmap, -1);
            result = datasets[bit]->getColumnIndex()->getColumnValues(columnName, filter);

            bitmap = bitmap & ~(1 << bit);
            bool sorted = std::is_sorted(result.begin(), result.end());  // true

            while (bitmap) {
                int bit = ML::lowest_bit(bitmap, -1);
                auto column = datasets[bit]->getColumnIndex()->getColumnValues(columnName, filter);
                if (!column.empty())
                {
                    if (!result.empty())
                        sorted = false;

                    result.insert(result.end(),
                                  std::make_move_iterator(column.begin()),
                                  std::make_move_iterator(column.end()));

                }
             
                bitmap = bitmap & ~(1 << bit);
            }

            if (!sorted) {
                std::sort(result.begin(), result.end());
                result.erase(std::unique(result.begin(), result.end()),
                             result.end());
            }
        }

        return result;
    }

    virtual size_t getRowCount() const
    {
        return rowIndex.size();
    }

    virtual size_t getColumnCount() const
    {
        return columnIndex.size();
    }

    uint32_t getRowBitmap(RowHash rowHash) const
    {
        return rowIndex.getDefault(rowHash, 0);
    }

    uint32_t getColumnBitmap(ColumnHash columnHash) const
    {
        return columnIndex.getDefault(columnHash, 0);
    }

    std::pair<Date, Date> getTimestampRange() const
    {
        std::pair<Date, Date> result(Date::notADate(), Date::notADate());
        bool first = true;

        for (auto & d: datasets) {
            std::pair<Date, Date> dsRange = d->getTimestampRange();
            if (!dsRange.first.isADate()
                || !dsRange.second.isADate())
                continue;
            if (first) {
                result = dsRange;
                first = false;
            }
            else {
                result.first.setMin(dsRange.first);
                result.second.setMax(dsRange.second);
            }
        }

        return result;
    }
};


/*****************************************************************************/
/* MERGED DATASET                                                            */
/*****************************************************************************/

MergedDataset::
MergedDataset(MldbServer * owner,
              PolyConfig config,
              const std::function<bool (const Json::Value &)> & onProgress)
    : Dataset(owner)
{
    auto mergeConfig = config.params.convert<MergedDatasetConfig>();
    
    std::vector<std::shared_ptr<Dataset> > datasets;

    for (auto & d: mergeConfig.datasets) {
        datasets.emplace_back(obtainDataset(owner, d, onProgress));
    }

    itl.reset(new Itl(server, datasets));
}

MergedDataset::
MergedDataset(MldbServer * owner,
              std::vector<std::shared_ptr<Dataset> > datasetsToMerge)
    : Dataset(owner)
{
    itl.reset(new Itl(server, datasetsToMerge));
}

MergedDataset::
~MergedDataset()
{
}

Any
MergedDataset::
getStatus() const
{
    std::vector<Any> result;
    for (auto & d: itl->datasets)
        result.emplace_back(d->getStatus());
    return result;
}

std::pair<Date, Date>
MergedDataset::
getTimestampRange() const
{
    return itl->getTimestampRange();
}

std::shared_ptr<MatrixView>
MergedDataset::
getMatrixView() const
{
    return itl;
}

std::shared_ptr<ColumnIndex>
MergedDataset::
getColumnIndex() const
{
    return itl;
}

std::shared_ptr<RowStream> 
MergedDataset::
getRowStream() const
{
    return make_shared<MergedDataset::Itl::MergedRowStream>(itl.get());
}

static RegisterDatasetType<MergedDataset, MergedDatasetConfig> 
regMerged(builtinPackage(),
          "merged",
          "Merges together several datasets into one virtual dataset",
          "datasets/MergedDataset.md.html");

extern std::shared_ptr<Dataset> (*createMergedDatasetFn) (MldbServer *, std::vector<std::shared_ptr<Dataset> > datasets);

std::shared_ptr<Dataset> createMergedDataset(MldbServer * server, std::vector<std::shared_ptr<Dataset> > datasets)
{
    return std::make_shared<MergedDataset>(server, datasets);
}

namespace {
struct AtInit {
    AtInit()
    {
        createMergedDatasetFn = createMergedDataset;
    }
} atInit;
}

} // namespace MLDB

