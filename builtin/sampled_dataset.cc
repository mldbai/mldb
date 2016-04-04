/** sampled_dataset.cc                                              -*- C++ -*-
    Francois Maillet, 11 janvier 2016
    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

*/

#include "mldb/sql/sql_expression.h"
#include "sampled_dataset.h"
#include "mldb/jml/utils/lightweight_hash.h"
#include "mldb/types/any_impl.h"
#include "mldb/types/structure_description.h"
#include "mldb/server/dataset_context.h"
#include "mldb/http/http_exception.h"
#include <random>
#include <unordered_set>

using namespace std;


namespace Datacratic {
namespace MLDB {

    
/*****************************************************************************/
/* SAMPLED DATASET CONFIG                                                    */
/*****************************************************************************/

std::random_device rd;

SampledDatasetConfig::
SampledDatasetConfig() :
        rows(0), fraction(0), withReplacement(false)
{
    seed = rd();
}


/*****************************************************************************/
/* SAMPLED DATASET CONFIG                                                 */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(SampledDatasetConfig);

SampledDatasetConfigDescription::
SampledDatasetConfigDescription()
{
    nullAccepted = true;

    addField("rows", &SampledDatasetConfig::rows,
            "Number of rows to sample. This option cannot be set when fraction is "
            "set. This option cannot be set higher than the number of rows of "
            "input unless withReplacement is set to true.");
    addField("fraction", &SampledDatasetConfig::fraction,
            "Fraction of rows in dataset to sample. It cannot be set when "
            "rows is set.");
    addField("withReplacement", &SampledDatasetConfig::withReplacement,
            "Sample with or without replacement. Sampling with replacement "
            "means that the same input row can appear in the output more "
            "than once.", false);
    addField("dataset", &SampledDatasetConfig::dataset,
             "Dataset to sample");
    addField("seed", &SampledDatasetConfig::seed,
              "Seed value for the random number generator. The purpose of "
              "this parameter is to permit reproducible random samples. "
              "This parameter is optional, with the default value being "
              "selected randomly for each sample.");

    onPostValidate = [] (SampledDatasetConfig * config,
                         JsonParsingContext & context) {

        if (config->rows != 0 && config->fraction != 0) {
            throw ML::Exception("rows and fraction parameters cannot be set "
                    "at the same time");
        }
        if (config->rows != 0 && config->fraction != 0) {
            throw ML::Exception("rows or fraction parameters need to be set");
        }
        if(config->rows == 0 && (config->fraction >= 1 || config->fraction <= 0)) {
            throw ML::Exception("fraction needs to be between 0 and 1");
        }
    };

}


/*****************************************************************************/
/* SAMPLED INTERNAL REPRESENTATION                                        */
/*****************************************************************************/


struct SampledDataset::Itl
    : public MatrixView, public ColumnIndex {
    
    /// Dataset that it was constructed with
    std::shared_ptr<Dataset> dataset;

    std::shared_ptr<MatrixView> matrix;
    std::shared_ptr<ColumnIndex> index;
    size_t columnCount;

    std::unordered_set<RowName> sampledRowsIndex;
    std::vector<RowName> sampledRows;
    std::vector<RowHash> sampledRowsHash;

    Itl(MldbServer * server, std::shared_ptr<Dataset> dataset,
            const SampledDatasetConfig config)
        : dataset(dataset),
          matrix(dataset->getMatrixView()),
          index(dataset->getColumnIndex()),
          columnCount(matrix->getColumnNames().size())
    {
        // get all existing  rows
        auto rows = matrix->getRowHashes();

        unsigned numRows = config.rows != 0 ? config.rows
                                            : rows.size() * config.fraction;

        if(!config.withReplacement && numRows > rows.size()) {
            throw ML::Exception("Request more rows without replacement than "
                    "available number of rows.");
        }
        sampledRowsHash.reserve(numRows);
        sampledRows.reserve(numRows);

        // do the sampling
        std::mt19937 gen(config.seed);
        std::uniform_int_distribution<> dis(0, rows.size() - 1);

        unordered_set<unsigned> sampledIndexes;
        while(sampledRows.size() < numRows) {
            unsigned sample_index = dis(gen);

            // if we're not sampling with replacement, check if
            // we already sampled this index
            if(!config.withReplacement) {
                if(sampledIndexes.count(sample_index))
                    continue;

                sampledIndexes.insert(sample_index);
            }

            sampledRowsHash.emplace_back(rows[sample_index]);

            auto rowName = matrix->getRowName(rows[sample_index]);
            sampledRows.emplace_back(rowName);
            sampledRowsIndex.insert(rowName);
        }
    }
    
    virtual RowName getRowName(const RowHash & row) const 
    {
        auto rowName = matrix->getRowName(row);
        if(!knownRow(rowName))
            throw ML::Exception("Can't get name of unknown row");

        return rowName;
    }

    virtual std::vector<RowName>
    getRowNames(ssize_t start = 0, ssize_t limit = -1) const
    {
        std::vector<RowName> rtn;
        rtn.reserve(sampledRows.size() - start);

        for(int i=start; i<sampledRows.size(); i++) {
            rtn.emplace_back(sampledRows[i]);

            if(limit != -1 && rtn.size() > limit)
                break;
        }

        return rtn;
    }

    virtual std::vector<RowHash>
    getRowHashes(ssize_t start = 0, ssize_t limit = -1) const
    {
        std::vector<RowHash> rtn;
        rtn.reserve(sampledRows.size() - start);

        for(int i=start; i<sampledRowsHash.size(); i++) {
            rtn.emplace_back(sampledRowsHash[i]);

            if(limit != -1 && rtn.size() > limit)
                break;
        }

        return rtn;
    }
    
    virtual bool knownRow(const RowName & row) const
    {
        return sampledRowsIndex.count(row);
    }
    
    virtual MatrixNamedRow getRow(const RowName & rowName) const
    {
        if(!knownRow(rowName))
            return MatrixNamedRow();
        
        return matrix->getRow(rowName);
    }
    
    virtual bool knownColumn(const ColumnName & column) const
    {
        return matrix->knownColumn(column);
    }
    
    virtual ColumnName getColumnName(ColumnHash column) const
    {
        return matrix->getColumnName(column);
    }
    
    virtual std::vector<ColumnName> getColumnNames() const
    {
        return matrix->getColumnNames();
    }
    
    virtual uint64_t getRowColumnCount(const RowName & row) const
    {
        if(!knownRow(row))
            return 0;

        return matrix->getRowColumnCount(row);        
    }

    virtual size_t getRowCount() const
    {
        return sampledRows.size();
    }

    virtual size_t getColumnCount() const
    {
        return matrix->getColumnCount();
    }

    std::pair<Date, Date>
    getTimestampRange() const
    {
        // TODO MLDB-1262
        throw ML::Exception("not implemented");
        // return dataset->getTimestampRange();
    }
    
    virtual MatrixColumn getColumn(const ColumnName & column) const
    {
        auto col = index->getColumn(column);

        std::vector<std::tuple<RowName, CellValue, Date> > allRows = std::move(col.rows);
        map<RowName, unsigned> rowIndex;
        for(int i=0; i<allRows.size(); i++) {
            rowIndex.insert(make_pair(get<0>(allRows[i]), i));
        }

        
        // std::vector<std::tuple<RowName, CellValue, Date> > rows;
        for(auto rowName : sampledRows) {
            auto it = rowIndex.find(rowName);
            if(it == rowIndex.end())
                throw ML::Exception("Unknown row in index");

            col.rows.emplace_back(col.rows[it->second]);
        }

        return col;
    }

    virtual void recordRowItl(const RowName & rowName,
          const std::vector<std::tuple<ColumnName, CellValue, Date> > & vals)
    {
        throw ML::Exception("Dataset type doesn't allow recording");
    }

};


/*****************************************************************************/
/* SAMPLED DATASET                                                        */
/*****************************************************************************/

SampledDataset::
SampledDataset(MldbServer * owner,
                  PolyConfig config,
                  const std::function<bool (const Json::Value &)> & onProgress)
    : Dataset(owner)
{
    auto sampleConfig = config.params.convert<SampledDatasetConfig>();
    
    SqlExpressionMldbContext context(owner);
    bondTableExpression = sampleConfig.dataset->bind(context);

    itl.reset(new Itl(server, bondTableExpression.dataset, sampleConfig));
}

SampledDataset::
SampledDataset(MldbServer * owner,
               std::shared_ptr<Dataset> dataset,
               const ExpressionValue & options)
    : Dataset(owner)
{
    SampledDatasetConfig config
        = jsonDecode<SampledDatasetConfig>(options.extractJson());
    itl.reset(new Itl(server, dataset, config));
}

SampledDataset::
~SampledDataset()
{
}

Any
SampledDataset::
getStatus() const
{
    return Any();
}

std::pair<Date, Date>
SampledDataset::
getTimestampRange() const
{
    return itl->getTimestampRange();
}

std::shared_ptr<MatrixView>
SampledDataset::
getMatrixView() const
{
    return itl;
}

std::shared_ptr<ColumnIndex>
SampledDataset::
getColumnIndex() const
{
    return itl;
}

RegisterDatasetType<SampledDataset, SampledDatasetConfig> 
regSampled(builtinPackage(),
              "sampled",
              "Dataset that samples another dataset",
              "datasets/SampledDataset.md.html");

extern std::shared_ptr<Dataset> (*createSampledDatasetFn) (MldbServer *,
                                                           std::shared_ptr<Dataset> dataset,
                                                           const ExpressionValue & options);

std::shared_ptr<Dataset> createSampledDataset(MldbServer * server,
                                              std::shared_ptr<Dataset> dataset,
                                              const ExpressionValue & options)
{  
    return std::make_shared<SampledDataset>(server, dataset, options);
}

namespace {
struct AtInit {
    AtInit()
    {
        createSampledDatasetFn = createSampledDataset;
    }
} atInit;
}

} // namespace MLDB
} // namespace Datacratic
