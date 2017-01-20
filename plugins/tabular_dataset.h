/* tabular_dataset.h                                               -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Tabular dataset: one timestamp per row, dense values, known columns.

   An example is a CSV file or a relational database.
*/

#pragma once

#include "mldb/core/dataset.h"
#include "mldb/sql/sql_expression.h"


namespace MLDB {

/*****************************************************************************/
/* TABULAR DATASET                                                           */
/*****************************************************************************/

enum UnknownColumnAction {
    UC_IGNORE,   ///< Ignore unknown columns
    UC_ERROR,    ///< Unknown columns are an error
    UC_ADD       ///< Add unknown columns as a new column
};

DECLARE_ENUM_DESCRIPTION(UnknownColumnAction);

struct TabularDatasetConfig {
    TabularDatasetConfig();

    UnknownColumnAction unknownColumns;
};

DECLARE_STRUCTURE_DESCRIPTION(TabularDatasetConfig);

struct TabularDataset : public Dataset {

    TabularDataset(MldbServer * owner,
                   PolyConfig config,
                   const ProgressFunc & onProgress);

    virtual ~TabularDataset();
    
    virtual Any getStatus() const;

    virtual std::shared_ptr<MatrixView> getMatrixView() const;

    virtual std::shared_ptr<ColumnIndex> getColumnIndex() const;

    virtual std::shared_ptr<RowStream> getRowStream() const;

    virtual ExpressionValue getRowExpr(const RowPath & row) const;
    
    virtual std::pair<Date, Date> getTimestampRange() const;

    virtual GenerateRowsWhereFunction
    generateRowsWhere(const SqlBindingScope & context,
                      const Utf8String& alias,
                      const SqlExpression & where,
                      ssize_t offset,
                      ssize_t limit) const;

    virtual KnownColumn getKnownColumnInfo(const ColumnPath & columnName) const;

    /** Commit changes to the database. */
    virtual void commit();

    virtual MultiChunkRecorder getChunkRecorder();

    virtual void recordRowItl(const RowPath & rowName, const std::vector<std::tuple<ColumnPath, CellValue, Date> > & vals);

    virtual void recordRows(const std::vector<std::pair<RowPath, std::vector<std::tuple<ColumnPath, CellValue, Date> > > > & rows);

protected:
    // To initialize from a subclass
    TabularDataset(MldbServer * owner);

    struct TabularDataStore;
    std::shared_ptr<TabularDataStore> itl;
};



} // namespace MLDB

