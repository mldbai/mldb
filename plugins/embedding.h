/** embedding.h                                                    -*- C++ -*-
    Embedding dataset for MLDB.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Each row holds a coordinate vector.
*/

#pragma once


#include "mldb/core/dataset.h"
#include "mldb/core/value_function.h"
#include "mldb/types/value_description_fwd.h"
#include "metric_space.h"


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
                     const ProgressFunc & onProgress);
    
    virtual ~EmbeddingDataset();

    virtual Any getStatus() const;

    virtual void recordRowItl(const RowPath & rowName,
                           const std::vector<std::tuple<ColumnPath, CellValue, Date> > & vals);

    virtual void
    recordEmbedding(const std::vector<ColumnPath> & columnNames,
                    const std::vector<std::tuple<RowPath, std::vector<float>, Date> > & rows);

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

    virtual KnownColumn getKnownColumnInfo(const ColumnPath & columnName) const;

    virtual std::vector<KnownColumn>
    getKnownColumnInfos(const std::vector<ColumnPath> & columnNames) const;
    
    virtual std::shared_ptr<RowValueInfo> getRowInfo() const;

    std::vector<std::tuple<RowPath, RowHash, float> >
    getNeighbors(const distribution<float> & coord, int numNeighbors,
                 double maxDistance) const;
    
    std::vector<std::tuple<RowPath, RowHash, float> >
    getRowNeighbors(const RowPath & row, int numNeighbors,
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
    ColumnPath columnName;
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
    ExpressionValue distances;
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
          const std::vector<std::shared_ptr<ExpressionValueInfo> > & input)
        const override;
    
    NearestNeighborsFunctionConfig functionConfig;

    ExpressionValue embedding;
};

/*****************************************************************************/
/* READ PIXELS FUNCTION                                                      */
/*****************************************************************************/

struct ReadPixelsFunctionConfig {
    ReadPixelsFunctionConfig()
    {
    }

    std::shared_ptr<SqlExpression> expression;
};

DECLARE_STRUCTURE_DESCRIPTION(ReadPixelsFunctionConfig);

struct ReadPixelsInput {
    ReadPixelsInput();
    ExpressionValue x;
    ExpressionValue y;
};

DECLARE_STRUCTURE_DESCRIPTION(ReadPixelsInput);

struct ReadPixelsOutput {
    ExpressionValue value;
};

DECLARE_STRUCTURE_DESCRIPTION(ReadPixelsOutput);

struct ReadPixelsFunction
    : public ValueFunctionT<ReadPixelsInput, ReadPixelsOutput> {

    ReadPixelsFunction(MldbServer * owner,
                             PolyConfig config,
                             const std::function<bool (const Json::Value &)> & onProgress);

    virtual ~ReadPixelsFunction();

    virtual ReadPixelsOutput
    applyT(const ApplierT & applier, ReadPixelsInput input) const override;
    
    virtual std::unique_ptr<ApplierT>
    bindT(SqlBindingScope & outerContext,
          const std::vector<std::shared_ptr<ExpressionValueInfo> > & input)
        const override;
    
    ReadPixelsFunctionConfig functionConfig;

    ExpressionValue embedding;
    DimsVector shape;
};

/*****************************************************************************/
/* PROXIMATE VOXELS                                                          */
/*****************************************************************************/

struct ProximateVoxelsFunctionConfig {
    ProximateVoxelsFunctionConfig()
    {
    }

    std::shared_ptr<SqlExpression> expression;
    int range;
};

DECLARE_STRUCTURE_DESCRIPTION(ProximateVoxelsFunctionConfig);

struct ProximateVoxelsInput {
    ProximateVoxelsInput();
    ExpressionValue x;
    ExpressionValue y;
    ExpressionValue z;
};

DECLARE_STRUCTURE_DESCRIPTION(ProximateVoxelsInput);

struct ProximateVoxelsOutput {
    ExpressionValue value;
};

DECLARE_STRUCTURE_DESCRIPTION(ProximateVoxelsOutput);

struct ProximateVoxelsFunction
    : public ValueFunctionT<ProximateVoxelsInput, ProximateVoxelsOutput> {

    ProximateVoxelsFunction(MldbServer * owner,
                             PolyConfig config,
                             const std::function<bool (const Json::Value &)> & onProgress);

    virtual ~ProximateVoxelsFunction();

    virtual ProximateVoxelsOutput
    applyT(const ApplierT & applier, ProximateVoxelsInput input) const override;
    
    virtual std::unique_ptr<ApplierT>
    bindT(SqlBindingScope & outerContext,
          const std::vector<std::shared_ptr<ExpressionValueInfo> > & input)
        const override;
    
    ProximateVoxelsFunctionConfig functionConfig;

    ExpressionValue embedding;

    int N;
};



} // namespace MLDB


