/** embedding.h                                                    -*- C++ -*-
    Embedding dataset for MLDB.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Each row holds a coordinate vector.
*/

#pragma once


#include "mldb/core/dataset.h"
#include "mldb/types/value_description.h"
#include "metric_space.h"

namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* EMBEDDING DATASET CONFIG                                                  */
/*****************************************************************************/

struct EmbeddingDatasetConfig {
    EmbeddingDatasetConfig()
        : metric(METRIC_EUCLIDEAN)
    {
    }

    MetricSpace metric;
};

DECLARE_STRUCTURE_DESCRIPTION(EmbeddingDatasetConfig);


/*****************************************************************************/
/* EMBEDDING                                                                 */
/*****************************************************************************/

struct EmbeddingDataset: public Dataset {

    EmbeddingDataset(MldbServer * owner,
                     PolyConfig config,
                     const std::function<bool (const Json::Value &)> & onProgress);
    
    virtual ~EmbeddingDataset();

    virtual Any getStatus() const;

    virtual void recordRowItl(const RowName & rowName,
                           const std::vector<std::tuple<ColumnName, CellValue, Date> > & vals);

    virtual void recordEmbedding(const std::vector<ColumnName> & columnNames,
                                 const std::vector<std::tuple<RowName, std::vector<float>, Date> > & rows);

    virtual void commit();

    virtual std::shared_ptr<MatrixView> getMatrixView() const;
    virtual std::shared_ptr<ColumnIndex> getColumnIndex() const;
    virtual std::shared_ptr<RowStream> getRowStream() const;

    virtual BoundFunction
    overrideFunction(const Utf8String & functionName,
                     SqlBindingScope & context) const;

    virtual RestRequestMatchResult
    handleRequest(RestConnection & connection,
                  const RestRequest & request,
                  RestRequestParsingContext & context) const;

    virtual std::pair<Date, Date> getTimestampRange() const;
    virtual Date quantizeTimestamp(Date timestamp) const;

    virtual KnownColumn getKnownColumnInfo(const ColumnName & columnName) const;

    virtual std::vector<KnownColumn>
    getKnownColumnInfos(const std::vector<ColumnName> & columnNames) const;

private:
    EmbeddingDatasetConfig datasetConfig;
    struct Itl;
    std::shared_ptr<Itl> itl;
};


} // namespace MLDB
} // namespace Datacratic

