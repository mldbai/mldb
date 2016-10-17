/**                                                                 -*- C++ -*-
 * union_dataset.cc
 * Mich, 2016-09-14
 * This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
 **/
#include "union_dataset.h"

#include <thread>
#include <math.h>

#include "mldb/builtin/id_hash.h"
#include "mldb/builtin/merge_hash_entries.h"
#include "mldb/types/any_impl.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/vector_description.h"

using namespace std;


namespace MLDB {


/*****************************************************************************/
/* UNION DATASET CONFIG                                                      */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(UnionDatasetConfig);

UnionDatasetConfigDescription::
UnionDatasetConfigDescription()
{
    nullAccepted = true;

    addField("datasets", &UnionDatasetConfig::datasets,
             "Datasets to unify together");
}

static RegisterDatasetType<UnionDataset, UnionDatasetConfig>
regUnion(builtinPackage(),
          "union",
          "Unify together several datasets",
          "datasets/UnionDataset.md.html");

std::shared_ptr<Dataset> createUnionDataset(
    MldbServer * server, vector<std::shared_ptr<Dataset> > datasets)
{
    return std::make_shared<UnionDataset>(server, datasets);
}

struct UnionDataset::Itl
    : public MatrixView, public ColumnIndex {

    Lightweight_Hash<RowHash, pair<int, RowHash> > rowIndex;

    // Datasets that it was constructed with
    vector<std::shared_ptr<Dataset> > datasets;

    Itl(MldbServer * server, vector<std::shared_ptr<Dataset> > datasets) {
        if (datasets.empty()) {
            throw MLDB::Exception("Attempt to unify no datasets together");
        }
        this->datasets = datasets;
        int indexWidth = getIndexBinaryWidth();
        if (indexWidth > 31) {
            throw MLDB::Exception("Too many datasets in the union");
        }
        for (int i = 0; i < datasets.size(); ++i) {
            for (const auto & rowPath: datasets[i]->getMatrixView()->getRowPaths()) {
                rowIndex[RowHash(PathElement(i) + rowPath)] =
                    make_pair(i, RowHash(rowPath));
            }
        }
    }

    int getIndexBinaryWidth() const {
        return ceil(log(datasets.size()) / log(2));
    }

    int getIdxFromRowPath(const RowPath & rowPath) const {
        // Returns idx > -1 if the index is valid, -1 otherwise
        if (rowPath.size() < 2) {
            return -1;
        }
        int idx = static_cast<int>(rowPath.at(0).toIndex());
        if (idx > datasets.size()) {
            return -1;
        }
        ExcAssert(idx == -1 || idx <= datasets.size());
        return idx;
    }

    struct UnionRowStream : public RowStream {

        UnionRowStream(const UnionDataset::Itl* source) : source(source)
        {
            cerr << "UNIMPLEMENTED " << __FILE__ << ":" << __LINE__ << endl;
            //throw MLDB::Exception("Unimplemented %s : %d", __FILE__, __LINE__);
        }

        virtual std::shared_ptr<RowStream> clone() const
        {
            return make_shared<UnionRowStream>(source);
        }

        /* set where the stream should start*/
        virtual void initAt(size_t start)
        {
            cerr << "UNIMPLEMENTED " << __FILE__ << ":" << __LINE__ << endl;
            //throw MLDB::Exception("Unimplemented %s : %d", __FILE__, __LINE__);
        }

        virtual RowPath next()
        {
            cerr << "UNIMPLEMENTED " << __FILE__ << ":" << __LINE__ << endl;
            throw MLDB::Exception("Unimplemented %s : %d", __FILE__, __LINE__);
            uint64_t hash = (*it).first;
            ++it;

            return source->getRowPath(RowHash(hash));
        }

        virtual const RowPath & rowName(RowPath & storage) const override
        {
            cerr << "UNIMPLEMENTED " << __FILE__ << ":" << __LINE__ << endl;
            throw MLDB::Exception("Unimplemented %s : %d", __FILE__, __LINE__);
            uint64_t hash = (*it).first;
            return storage = source->getRowPath(RowHash(hash));
        }

        const UnionDataset::Itl* source;
        IdHashes::const_iterator it;

    };

    virtual vector<Path>
    getRowPaths(ssize_t start = 0, ssize_t limit = -1) const
    {
        // Row names are idx.rowPath where idx is the index of the dataset
        // in the union and rowPath is the original rowPath.
        vector<RowPath> result;
        for (int i = 0; i < datasets.size(); ++i) {
            const auto & d = datasets[i];
            for (const auto & name: d->getMatrixView()->getRowPaths()) {
                result.emplace_back(PathElement(i) + name);
            }

        }
        return result;
    }

    virtual vector<RowHash>
    getRowHashes(ssize_t start = 0, ssize_t limit = -1) const
    {
        std::vector<RowHash> result;
        for (const auto & it: rowIndex) {
            result.emplace_back(it.first);
        }
        return result;
    }

    virtual bool knownRow(const Path & rowPath) const
    {
        int idx = getIdxFromRowPath(rowPath);
        if (idx == -1) {
            return false;
        }
        return datasets[idx]->getMatrixView()->knownRow(rowPath.tail());
    }

    virtual bool knownRowHash(const RowHash & rowHash) const
    {
        // Unused ?
        return rowIndex.find(rowHash) != rowIndex.end();
        //return rowIndex.getDefault(rowHash, 0) != 0;
    }

    virtual RowPath getRowPath(const RowHash & rowHash) const
    {
        const auto & it = rowIndex.find(rowHash);
        if (it == rowIndex.end()) {
            throw MLDB::Exception("Row not known");
        }
        const auto & idxAndHash = it->second;
        return datasets[idxAndHash.first]->getMatrixView()->getRowPath(idxAndHash.second);
    }

    // DEPRECATED
    virtual MatrixNamedRow getRow(const RowPath & rowPath) const
    {
        throw MLDB::Exception("Unimplemented %s : %d", __FILE__, __LINE__);
    }

    virtual bool knownColumn(const Path & column) const
    {
        for (const auto & d: datasets) {
            if (d->getMatrixView()->knownColumn(column)) {
                return true;
            }
        }
        return false;
    }

    virtual ColumnPath getColumnPath(ColumnHash columnHash) const
    {
        for (const auto & d: datasets) {
            try {
                return d->getMatrixView()->getColumnPath(columnHash);
            }
            catch (const MLDB::Exception & exc) {
            }
        }
        throw MLDB::Exception("Column not known");
    }

    /** Return a list of all columns. */
    virtual vector<ColumnPath> getColumnPaths() const
    {
        std::set<ColumnPath> preResult;
        for (const auto & d: datasets) {
            auto columnPaths = d->getColumnPaths();
            preResult.insert(columnPaths.begin(), columnPaths.end());
        }
        return vector<ColumnPath>(preResult.begin(), preResult.end());
    }

    virtual MatrixColumn getColumn(const ColumnPath & columnPath) const
    {
        MatrixColumn result;
        result.columnName = columnPath;
        result.columnHash = columnPath;
        vector<std::tuple<RowPath, CellValue> > res;
        for (int i = 0; i < datasets.size(); ++i) {
            const auto & d = datasets[i];
            const auto & subCol = d->getColumnIndex()->getColumn(columnPath);
            for (const auto & curr: subCol.rows) {
                result.rows.emplace_back(PathElement(i) + std::get<0>(curr),
                                         std::get<1>(curr),
                                         std::get<2>(curr));
            }
        }
        return result;
    }

    /** Return the value of the column for all rows and timestamps. */
    virtual vector<std::tuple<RowPath, CellValue> >
    getColumnValues(const ColumnPath & columnPath,
                    const std::function<bool (const CellValue &)> & filter) const
    {
        vector<std::tuple<RowPath, CellValue> > res;
        for (int i = 0; i < datasets.size(); ++i) {
            const auto & d = datasets[i];
            for (const auto curr: d->getColumnIndex()->getColumnValues(columnPath)) {
                res.emplace_back(
                    PathElement(i) + std::get<0>(curr).toUtf8String().rawString(),
                    std::get<1>(curr));
            }
        }
        return res;
    }

    virtual size_t getRowCount() const
    {
        size_t count = 0;
        for (const auto & d: datasets) {
            count += d->getRowCount();
        }
        return count;
    }

    virtual size_t getColumnCount() const
    {
        return getColumnPaths().size();
    }

    std::pair<Date, Date> getTimestampRange() const
    {
        std::pair<Date, Date> result(Date::notADate(), Date::notADate());
        bool first = true;

        for (auto & d: datasets) {
            std::pair<Date, Date> dsRange = d->getTimestampRange();
            if (!dsRange.first.isADate() || !dsRange.second.isADate()) {
                continue;
            }
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


UnionDataset::
UnionDataset(MldbServer * owner,
             PolyConfig config,
             const std::function<bool (const Json::Value &)> & onProgress)
    : Dataset(owner)
{
    auto unionConfig = config.params.convert<UnionDatasetConfig>();

    vector<std::shared_ptr<Dataset> > datasets;

    for (auto & d: unionConfig.datasets) {
        datasets.emplace_back(obtainDataset(owner, d, onProgress));
    }

    itl.reset(new Itl(server, datasets));
}

UnionDataset::
UnionDataset(MldbServer * owner,
              vector<std::shared_ptr<Dataset> > datasetsToMerge)
    : Dataset(owner)
{
    itl.reset(new Itl(server, datasetsToMerge));
}

UnionDataset::
~UnionDataset()
{
}

Any
UnionDataset::
getStatus() const
{
    vector<Any> result;
    for (auto & d: itl->datasets) {
        result.emplace_back(d->getStatus());
    }
    return result;
}

std::pair<Date, Date>
UnionDataset::
getTimestampRange() const
{
    return itl->getTimestampRange();
}

std::shared_ptr<MatrixView>
UnionDataset::
getMatrixView() const
{
    return itl;
}

std::shared_ptr<ColumnIndex>
UnionDataset::
getColumnIndex() const
{
    return itl;
}

std::shared_ptr<RowStream>
UnionDataset::
getRowStream() const
{
    return make_shared<UnionDataset::Itl::UnionRowStream>(itl.get());
}

ExpressionValue
UnionDataset::
getRowExpr(const RowPath & rowPath) const
{
    int idx = itl->getIdxFromRowPath(rowPath);
    if (idx == -1) {
        return ExpressionValue{};
    }
    return itl->datasets[idx]->getRowExpr(
        Path(rowPath.begin() + 1, rowPath.end()));
}

} // namespace MLDB
