/**                                                                 -*- C++ -*-
 * union_dataset.cc
 * Mich, 2016-09-14
 * This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
 **/
#include "union_dataset.h"

#include <thread>

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

extern std::shared_ptr<Dataset> (*createUnionDatasetFn) (MldbServer *, vector<std::shared_ptr<Dataset> > datasets);

std::shared_ptr<Dataset> createUnionDataset(MldbServer * server, vector<std::shared_ptr<Dataset> > datasets)
{
    return std::make_shared<UnionDataset>(server, datasets);
}

struct UnionDataset::Itl
    : public MatrixView, public ColumnIndex {

    // Datasets that it was constructed with
    vector<std::shared_ptr<Dataset> > datasets;

    Itl(MldbServer * server, vector<std::shared_ptr<Dataset> > datasets) {
        if (datasets.empty()) {
            throw ML::Exception("Attempt to unify no datasets together");
        }
        this->datasets = datasets;
    }

    struct UnionRowStream : public RowStream {

        UnionRowStream(const UnionDataset::Itl* source) : source(source)
        {
        }

        virtual std::shared_ptr<RowStream> clone() const
        {
            cerr << __FILE__ << ":" << __LINE__ << endl;
            return make_shared<UnionRowStream>(source);
        }

        /* set where the stream should start*/
        virtual void initAt(size_t start)
        {
            cerr << __FILE__ << ":" << __LINE__ << endl;
        }

        virtual RowName next()
        {
            cerr << __FILE__ << ":" << __LINE__ << endl;
            uint64_t hash = (*it).first;
            ++it;

            return source->getRowName(RowHash(hash));
        }

        virtual const RowName & rowName(RowName & storage) const
        {
            cerr << __FILE__ << ":" << __LINE__ << endl;
            uint64_t hash = (*it).first;
            return storage = source->getRowName(RowHash(hash));
        }

        const UnionDataset::Itl* source;
        IdHashes::const_iterator it;

    };

    string getStrFix(int count, bool begin) const {
        if (count == 0) {
            return begin ? "[" : "]";
        }
        string s = "";
        for (; count > 0; --count) {
            s += "[]-";
        }
        return begin ? s + "[" : "]-" + s.substr(0, s.size() - 1);
    }
       
    virtual vector<Path>
    getRowNames(ssize_t start = 0, ssize_t limit = -1) const
    {
        // Row names are []-[]-[]-...[] where the original row name goes into
        // the [] at the index of the dataset within the union.
        vector<Path> result;
        for (int i = 0; i < datasets.size(); ++i) {
            const auto & d = datasets[i];
            string prefix = getStrFix(i, true);
            string suffix = getStrFix(datasets.size() - i - 1, false);
            for (const auto & name: d->getMatrixView()->getRowNames()) {
                result.push_back(Path(prefix + name.toUtf8String().rawString() + suffix));
            }

        }
        return result;
    }

    virtual vector<RowHash>
    getRowHashes(ssize_t start = 0, ssize_t limit = -1) const
    {
            cerr << __FILE__ << ":" << __LINE__ << endl;
        vector<RowHash> result;
        return result;
    }

    virtual bool knownRow(const Path & rowName) const
    {
            cerr << __FILE__ << ":" << __LINE__ << endl;
        for (const auto & curr: getRowNames()) {
            if (rowName == curr) {
                return true;
            }
        }
        return false;
    }

    virtual bool knownRowHash(const RowHash & rowHash) const
    {
            cerr << __FILE__ << ":" << __LINE__ << endl;
        for (const auto & curr: getRowNames()) {
            if (rowHash == curr.hash()) {
                return true;
            }
        }
        return false;
    }

    virtual RowName getRowName(const RowHash & rowHash) const
    {
            cerr << __FILE__ << ":" << __LINE__ << endl;
        for (const auto & curr: getRowNames()) {
            if (rowHash == curr.hash()) {
                return curr;
            }
        }
        throw ML::Exception("Row not known");
    }

    virtual MatrixNamedRow getRow(const RowName & rowName) const
    {
        string name = rowName.toUtf8String().rawString();
        for (int i = 0; i < datasets.size(); ++i) {
            string prefix = getStrFix(i, true);
            string suffix = getStrFix(datasets.size() - i - 1, false);
            if (name.find(prefix) == 0 && name.rfind(suffix) == name.size() - suffix.size()) {
                string subRowName = name.substr(prefix.size(), name.size() - prefix.size() - suffix.size());
                MatrixNamedRow result =
                    datasets[i]->getMatrixView()->getRow(Path(subRowName));
                result.rowName = rowName;
                //result.rowHash = ??; //TODO
                return result;
            }
        }
        MatrixNamedRow result;
        return result;
    }

    virtual bool knownColumn(const Path & column) const
    {
            cerr << __FILE__ << ":" << __LINE__ << endl;
        for (const auto & curr: getColumnNames()) {
            if (curr == column) {
                return true;
            }
        }
        return false;
    }

    virtual ColumnName getColumnName(ColumnHash columnHash) const
    {
            cerr << __FILE__ << ":" << __LINE__ << endl;
        for (const auto & curr: getColumnNames()) {
            if (columnHash == curr.hash()) {
                return curr;
            }
        }
        throw ML::Exception("Column not known");
    }

    /** Return a list of all columns. */
    virtual vector<ColumnName> getColumnNames() const
    {
            cerr << __FILE__ << ":" << __LINE__ << endl;
        std::set<ColumnName> preResult;
        for (const auto & d: datasets) {
            auto columnNames = d->getColumnNames();
            preResult.insert(columnNames.begin(), columnNames.end());
        }
        return vector<ColumnName>(preResult.begin(), preResult.end());
    }

    virtual MatrixColumn getColumn(const ColumnName & columnName) const
    {
            cerr << __FILE__ << ":" << __LINE__ << endl;
        MatrixColumn result;
        result.columnName = columnName;
        //result.columnHash = columnName.hash(); //TODO
        vector<std::tuple<RowName, CellValue> > res;
        for (int i = 0; i < datasets.size(); ++i) {
            const auto & d = datasets[i];
            string prefix = getStrFix(i, true);
            string suffix = getStrFix(datasets.size() - i - 1, false);
            const auto & subCol = d->getColumnIndex()->getColumn(columnName);
            for (const auto & curr: subCol.rows) {
                result.rows.emplace_back(
                    Path(prefix + std::get<0>(curr).toUtf8String().rawString() + suffix),
                    std::get<1>(curr),
                    std::get<2>(curr));
            }
        }
        return result;
    }

    /** Return the value of the column for all rows and timestamps. */
    virtual vector<std::tuple<RowName, CellValue> >
    getColumnValues(const ColumnName & columnName,
                    const std::function<bool (const CellValue &)> & filter) const
    {
            cerr << __FILE__ << ":" << __LINE__ << endl;
        vector<std::tuple<RowName, CellValue> > res;
        for (int i = 0; i < datasets.size(); ++i) {
            const auto & d = datasets[i];
            string prefix = getStrFix(i, true);
            string suffix = getStrFix(datasets.size() - i - 1, false);
            for (const auto curr: d->getColumnIndex()->getColumnValues(columnName)) {
                res.emplace_back(
                    Path(prefix + std::get<0>(curr).toUtf8String().rawString() + suffix),
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
        return getColumnNames().size();
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
            cerr << __FILE__ << ":" << __LINE__ << endl;
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

namespace {
struct AtInit {
    AtInit()
    {
        createUnionDatasetFn = createUnionDataset;
    }
} atInit;
}

} // namespace MLDB
