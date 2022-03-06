/** dataset_builder.h                                               -*- C++ -*-
    Jeremy Barnes, 24 January 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Build datasets from records of text.
*/

#pragma once
#include "mldb/core/dataset.h"
#include "mldb/core/procedure.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/utils/lightweight_hash.h"
#include "sql_csv_scope.h"
#include <memory>

namespace MLDB {

struct Decomposition;


struct DatasetBuilderConfig {

    PolyConfigT<Dataset> outputDataset = DefaultType("tabular");

    /// What to select from the CSV
    SelectExpression select = SelectExpression::STAR; 

    /// Filter for the CSV
    std::shared_ptr<SqlExpression> where = SqlExpression::TRUE;

    ///< Row name to output
    std::shared_ptr<SqlExpression> named
        = SqlExpression::parse("lineNumber()");

    ///< Timestamp for row    
    std::shared_ptr<SqlExpression> timestamp
        = SqlExpression::parse("fileTimestamp()");
};

DECLARE_STRUCTURE_DESCRIPTION(DatasetBuilderConfig);

struct BoundDatasetBuilder;

struct DatasetBuilderChunkRecorder {

    /// Pointer to our parent object (which owns us)
    const BoundDatasetBuilder * owner = nullptr;

    /// Recorder object for this thread that the dataset gives us
    /// to record into the dataset.
    std::unique_ptr<Recorder> threadRecorder;

    /// Special function to allow rapid insertion of fixed set of
    /// atom valued columns.  Only for isIdentitySelect.
    std::function<void (RowPath rowName,
                        Date timestamp,
                        CellValue * vals,
                        size_t numVals,
                        std::vector<std::pair<ColumnPath, CellValue> > extra)>
        specializedRecorder;

    void recordRow(SqlCsvScope::RowScope & row,
                   std::span<CellValue> fixedValues,
                   std::span<std::pair<RowPath, CellValue>> extraValues,
                   uint64_t actualLineNum);
    void finish();
};

struct BoundDatasetBuilder {
    std::shared_ptr<Dataset> dataset;
    std::shared_ptr<spdlog::logger> logger;
    std::function<bool (const Json::Value &)> onProgress;

    BoundSqlExpression whereBound;
    BoundSqlExpression selectBound;
    BoundSqlExpression namedBound;
    BoundSqlExpression timestampBound;
    bool isIdentitySelect = false;
    bool areAllOutputColumnsKnown = false;
    bool isNamedLineNumber = false;
    bool isWhereTrue = false;
    std::vector<ColumnPath> knownColumnNames;
    std::vector<ColumnPath> inputColumnNames;
    LightweightHash<ColumnHash, int> inputColumnIndex;
    LightweightHash<ColumnHash, int> columnIndex; //To check for duplicates column names
    const Dataset::MultiChunkRecorder * recorder = nullptr;

    std::unique_ptr<Decomposition> decomposition;
    bool canUseDecomposed = false;

    DatasetBuilderChunkRecorder newChunk(int chunkNumber) const;
};

struct DatasetBuilder {
    std::shared_ptr<Dataset> dataset;
    std::shared_ptr<spdlog::logger> logger;
    std::function<bool (const Json::Value &)> onProgress;
    MldbEngine * engine = nullptr;
    const DatasetBuilderConfig * config = nullptr;
    Dataset::MultiChunkRecorder recorder;

    void initialize(MldbEngine & engine,
                    std::shared_ptr<spdlog::logger> logger,
                    const DatasetBuilderConfig & config,
                    const std::function<bool (const Json::Value &)> & onProgress = nullptr);

    BoundDatasetBuilder bind(SqlBindingScope & scope,
                             std::span<const ColumnPath> inputColumnNames) const;

    void commit();
};

} // namespace MLDB
