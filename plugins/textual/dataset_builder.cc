/** dataset_builder.h                                               -*- C++ -*-
    Jeremy Barnes, 24 January 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Build datasets from records of text.
*/

#include "dataset_builder.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/types/span_description.h"
#include "mldb/core/dataset_scope.h"
#include "mldb/types/annotated_exception.h"
#include "mldb/sql/decompose.h"
#include "mldb/utils/log.h"

using namespace std;

namespace MLDB {

DEFINE_STRUCTURE_DESCRIPTION_INLINE(DatasetBuilderConfig)
{
    addField("outputDataset", &DatasetBuilderConfig::outputDataset,
             "Dataset to record the data into.",
             PolyConfigT<Dataset>().withType("tabular"));
    addField("select", &DatasetBuilderConfig::select,
             "Which columns to use.",
             SelectExpression::STAR);
    addField("where", &DatasetBuilderConfig::where,
             "Which lines to use to create rows.",
             SqlExpression::TRUE);
    addField("named", &DatasetBuilderConfig::named,
             "Row name expression for output dataset. Note that each row "
             "must have a unique name.",
             SqlExpression::parse("lineNumber()"));
    addField("timestamp", &DatasetBuilderConfig::timestamp,
             "Expression for row timestamp.",
             SqlExpression::parse("fileTimestamp()"));
}

void
DatasetBuilderChunkRecorder::
recordRow(SqlCsvScope::RowScope & row,
          std::span<CellValue> fixedValues,
          std::span<std::pair<RowPath, CellValue>> extraValues,
          uint64_t actualLineNum)
{
    //cerr << "recordRow with " << fixedValues.size() << " values and threadRecorder " << threadRecorder.get() << endl;

    ExpressionValue nameStorage;
    RowPath rowName;

    if (owner->isNamedLineNumber) {
        rowName = Path(actualLineNum);
    }
    else {
        rowName = owner->namedBound(row, nameStorage, GET_ALL).coerceToPath();
    }
    row.rowName = &rowName;
    
    // If it doesn't match the where, don't add it
    if (!owner->isWhereTrue) {
        ExpressionValue storage;
        if (!owner->whereBound(row, storage, GET_ALL).isTrue())
            return;
    }

    // Get the timestamp for the row
    ExpressionValue tsStorage;
    Date rowTs = owner->timestampBound(row, tsStorage, GET_ALL)
            .coerceToTimestamp().toTimestamp();

    //ExcAssert(!(isIdentitySelect && outputColumnNamesUnknown));

    if (owner->isIdentitySelect && extraValues.empty()) {
        cerr << "identity select" << endl;
        // If it's a select *, we don't really need to run the
        // select clause.  We simply record the values directly.
        specializedRecorder(std::move(rowName),
                            rowTs, fixedValues.data(),
                            fixedValues.size(), {});
    }
    else if (owner->decomposition && owner->decomposition->canUseDecomposed) {
        cerr << "owner->decomposition has " << owner->decomposition->knownColumnNames.size() << " columns" << endl;
        auto [outputValues, extra] = owner->decomposition->apply(row, fixedValues);
        //extra.insert(extra.end(),
        //             std::make_move_iterator(extraValues.begin())
        //             std::make_move_iterator(extraValues.end()));
        specializedRecorder(std::move(rowName),
                            rowTs, outputValues.data(), outputValues.size(),
                            std::move(extra));
    }
    else {
        //cerr << "non-optimized" << endl;
        //cerr << "has decomposition = " << (bool)owner->decomposition << endl;
        //cerr << "canUseDecomposed = " << owner->canUseDecomposed << endl;
        ExpressionValue selectStorage;
        const ExpressionValue & selectOutput
                = owner->selectBound(row, selectStorage, GET_ALL);

        if (&selectOutput == &selectStorage) {
            // We can destructively work with it
            threadRecorder
                ->recordRowExprDestructive(std::move(rowName),
                                           std::move(selectStorage));
            }
            else {
                // We don't own the output; we will need to copy
                // it.
                threadRecorder
                    ->recordRowExpr(std::move(rowName),
                                    selectOutput);
        }
    }
}

void
DatasetBuilderChunkRecorder::
finish()
{
    threadRecorder->finishedChunk();
}

DatasetBuilderChunkRecorder
BoundDatasetBuilder::
newChunk(int chunkNumber) const
{
    cerr << "BoundDatasetBuilder: newChunk with " << knownColumnNames.size() << " columns" << endl;
    
    DatasetBuilderChunkRecorder result;
    result.owner = this;
    result.threadRecorder = recorder->newChunk(chunkNumber);
    if (isIdentitySelect || canUseDecomposed) {
        ExcAssert(result.threadRecorder);
        result.specializedRecorder
            = result.threadRecorder
            ->specializeRecordTabular(knownColumnNames);
    }
    cerr << "  NewChunk recorder at " << result.threadRecorder.get() << endl;
    return result;
}

BoundDatasetBuilder
DatasetBuilder::
bind(SqlBindingScope & scope,
     std::span<const ColumnPath> inputColumnNames) const
{
    BoundDatasetBuilder result;
    result.recorder = &recorder;

    // Early check for duplicate column names in input
    for (unsigned i = 0;  i < inputColumnNames.size();  ++i) {
        const ColumnPath & c = inputColumnNames[i];
        ColumnHash ch(c);
        if (!result.inputColumnIndex.insert(make_pair(ch, i)).second)
            throw AnnotatedException(400, "Duplicate column name in CSV file",
                                          "columnName", c);
    }

    result.selectBound = config->select.bind(scope);
    result.whereBound = config->where->bind(scope);
    result.namedBound = config->named->bind(scope);
    result.timestampBound = config->timestamp->bind(scope);

    //for (size_t i = 0;  i < inputColumnNames.size();  ++i) {
    //    cerr << "column " << i << " name " << inputColumnNames[i]
    //         << " used " << scope.columnsUsed[i] << endl;
    //}
    
    // Do we have a "select *"?  In that case, we can perform various
    // optimizations to avoid calling into the SQL layer
    SqlExpressionDatasetScope noContext(*dataset, ""); //needs a context because x.* is ambiguous
    result.isIdentitySelect = config->select.isIdentitySelect(noContext);  

    // Figure out our output column names from the bound
    // select clause

    if (result.selectBound.info->getSchemaCompletenessRecursive() != SCHEMA_CLOSED) {
        result.areAllOutputColumnsKnown = false;
    }

    auto cols = result.selectBound.info->getKnownColumns();

    for (unsigned i = 0;  i < cols.size();  ++i) {
        const auto& col = cols[i];
        if (!col.valueInfo->isScalar())
            throw AnnotatedException
                (400,
                    "Import select expression cannot have row-valued columns.",
                    "select", config->select,
                    "selectOutputInfo", result.selectBound.info,
                    "columnName", col.columnName);

        ColumnHash ch(col.columnName);
        if (!result.columnIndex.insert(make_pair(ch, i)).second)
            throw AnnotatedException(400, "Duplicate column name in select expression",
                                        "columnName", col.columnName);

        result.knownColumnNames.emplace_back(col.columnName);
    }

    //if (isIdentitySelect)
    //    ExcAssertEqual(inputColumnNames, knownColumnNames);

    //INFO_MSG(logger)
    //    << "reading " << inputColumnNames.size() << " columns "
    //    << jsonEncodeStr(inputColumnNames);

    //INFO_MSG(logger)
    //    << "writing " << result.knownColumnNames.size() << " columns "
    //    << jsonEncodeStr(result.knownColumnNames);

    auto decomposition = decompose(result.selectBound, scope);
    result.decomposition.reset(new Decomposition(std::move(decomposition)));
    result.canUseDecomposed = result.decomposition->canUseDecomposed;

    //cerr << "with " << otherClauses.size() << " other clauses" << endl;
    //cerr << "canUseDecomposed = " << canUseDecomposed << endl;
    
    // Do we have a "where true'?  In that case, we don't need to
    // call the SQL parser
    result.isWhereTrue = config->where->isConstantTrue();

    // Do we have a "NAMED lineNumber()"?  In that case, we can short
    // circuit the evaluation of that expression.
    result.isNamedLineNumber = config->named->surface == "lineNumber()";

    return result;
}

void
DatasetBuilder::
initialize(MldbEngine & engine,
           std::shared_ptr<spdlog::logger> logger,
           const DatasetBuilderConfig & config,
           const std::function<bool (const Json::Value &)> & onProgress)
{
    cerr << "config = " << jsonEncode(config.outputDataset) << endl;

    dataset = createDataset(&engine, config.outputDataset, onProgress,
                            true /*overwrite*/);

    if(!dataset) {
        throw MLDB::Exception("Unable to obtain output dataset");
    }

    recorder = dataset->getChunkRecorder();
    this->config = &config;
    this->logger = logger;
    this->engine = &engine;
    this->onProgress = onProgress;
}

void
DatasetBuilder::
commit()
{
    recorder.commit();
}

} // namespace MLDB