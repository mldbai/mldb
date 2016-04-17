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

    virtual void recordEmbedding(const std::vector<ColumnName> & columnNames,
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
    getNeighbours(const ML::distribution<float> & coord, int numNeighbours, double maxDistance) const;
    
    std::vector<std::tuple<RowName, RowHash, float> >
    getRowNeighbours(const RowName & row, int numNeighbours, double maxDistance) const;

private:
    EmbeddingDatasetConfig datasetConfig;
    struct Itl;
    std::shared_ptr<Itl> itl;
};



/*****************************************************************************/
/* nearest.neighbors FUNCTION                                                */
/*****************************************************************************/

struct NearestNeighborsFunctionConfig {
    NearestNeighborsFunctionConfig()
        : default_num_neighbors(10), default_max_distance(INFINITY)
    {
    }

    unsigned default_num_neighbors;
    double default_max_distance;
    std::shared_ptr<TableExpression> dataset;
};

DECLARE_STRUCTURE_DESCRIPTION(NearestNeighborsFunctionConfig);

struct NearestNeighborInput {
    const ExpressionValue coords;
    const ExpressionValue num_neighbors; //can be empty
    const ExpressionValue max_distance; //can be empty
};

DECLARE_STRUCTURE_DESCRIPTION(NearestNeighborInput);

struct NearestNeighborOuput {
    ExpressionValue row;
};

DECLARE_STRUCTURE_DESCRIPTION(NearestNeighborOuput);

struct NearestNeighborsFunction: public ValueFunctionT<NearestNeighborInput, NearestNeighborOuput> {
    NearestNeighborsFunction(MldbServer * owner,
                  PolyConfig config,
                  const std::function<bool (const Json::Value &)> & onProgress);

    ~NearestNeighborsFunction();

    virtual NearestNeighborOuput applyT(const ApplierT & applier,
                          const NearestNeighborInput & input) const;
    
    virtual std::unique_ptr<FunctionApplierT<NearestNeighborInput, NearestNeighborOuput> >
    bindT(SqlBindingScope & outerContext,
          const FunctionValues & input) const;
    
    NearestNeighborsFunctionConfig functionConfig;
};


} // namespace MLDB
} // namespace Datacratic

