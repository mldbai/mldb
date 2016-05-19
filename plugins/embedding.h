/** embedding.h                                                    -*- C++ -*-
    Embedding dataset for MLDB.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Each row holds a coordinate vector.
*/

#pragma once


#include "mldb/core/dataset.h"
#include "mldb/core/value_function.h"
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

    virtual void
    recordEmbedding(const std::vector<ColumnName> & columnNames,
                    const std::vector<std::tuple<RowName, std::vector<float>, Date> > & rows);

    virtual void commit();

    virtual std::shared_ptr<MatrixView> getMatrixView() const;
    virtual std::shared_ptr<ColumnIndex> getColumnIndex() const;
    virtual std::shared_ptr<RowStream> getRowStream() const;

    virtual BoundFunction
    overrideFunction(const Utf8String & tableName,
                     const Utf8String & functionName,
                     SqlBindingScope & context) const;

    virtual std::pair<Date, Date> getTimestampRange() const;
    virtual Date quantizeTimestamp(Date timestamp) const;

    virtual KnownColumn getKnownColumnInfo(const ColumnName & columnName) const;

    virtual std::vector<KnownColumn>
    getKnownColumnInfos(const std::vector<ColumnName> & columnNames) const;
    
    std::vector<std::tuple<RowName, RowHash, float> >
    getNeighbors(const ML::distribution<float> & coord, int numNeighbors,
                 double maxDistance) const;
    
    std::vector<std::tuple<RowName, RowHash, float> >
    getRowNeighbors(const RowName & row, int numNeighbors,
                    double maxDistance) const;

private:
    EmbeddingDatasetConfig datasetConfig;
    struct Itl;
    std::shared_ptr<Itl> itl;
};



/*****************************************************************************/
/* NEAREST NEIGHBORS FUNCTION                                                */
/*****************************************************************************/

struct NearestNeighborsFunctionConfig {
    NearestNeighborsFunctionConfig()
        : defaultNumNeighbors(10), defaultMaxDistance(INFINITY),
          columnName()
    {
    }

    unsigned defaultNumNeighbors;
    double defaultMaxDistance;
    ColumnName columnName;
    std::shared_ptr<TableExpression> dataset;
};

DECLARE_STRUCTURE_DESCRIPTION(NearestNeighborsFunctionConfig);

struct NearestNeighborsInput {
    NearestNeighborsInput();
    ExpressionValue coords;
    CellValue numNeighbors; // positive integer or null
    CellValue maxDistance;  // double or null
};

DECLARE_STRUCTURE_DESCRIPTION(NearestNeighborsInput);

struct NearestNeighborsOutput {
    ExpressionValue neighbors;
};

DECLARE_STRUCTURE_DESCRIPTION(NearestNeighborsOutput);

struct NearestNeighborsFunction
    : public ValueFunctionT<NearestNeighborsInput, NearestNeighborsOutput> {

    NearestNeighborsFunction(MldbServer * owner,
                             PolyConfig config,
                             const std::function<bool (const Json::Value &)> & onProgress);

    virtual ~NearestNeighborsFunction();

    virtual NearestNeighborsOutput
    applyT(const ApplierT & applier, NearestNeighborsInput input) const override;
    
    virtual std::unique_ptr<ApplierT>
    bindT(SqlBindingScope & outerContext,
          const std::shared_ptr<RowValueInfo> & input) const override;
    
    NearestNeighborsFunctionConfig functionConfig;
};


} // namespace MLDB
} // namespace Datacratic

