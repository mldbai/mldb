/** sparse_matrix.h                                                -*- C++ -*-
    SparseMatrix dataset for MLDB.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Each row holds a coordinate vector.
*/

#pragma once


#include "mldb/types/value_description_fwd.h"
#include "mldb/core/dataset.h"



namespace MLDB {


/*****************************************************************************/
/* SPARSE MATRIX DATASET                                                     */
/*****************************************************************************/

struct SparseMatrixDataset: public Dataset {

    virtual ~SparseMatrixDataset();

    virtual Any getStatus() const override;

    /** Base database methods require us to be able to iterate through rows.
        All other views are built on top of this.
    */
    virtual void recordRowItl(const RowPath & rowName,
                              const std::vector<std::tuple<ColumnPath, CellValue, Date> > & vals) override;

    virtual void recordRows(const std::vector<std::pair<RowPath, std::vector<std::tuple<ColumnPath, CellValue, Date> > > > & rows) override;

    virtual void
    recordRowExpr(const RowPath & rowName,
                  const ExpressionValue & vals) override;

    virtual void
    recordRowsExpr(const std::vector<std::pair<RowPath, ExpressionValue> > & rows)
        override;

    /** Return what is known about the given column.  Default returns
        an "any value" result, ie nothing is known about the column.
    */
    virtual KnownColumn
    getKnownColumnInfo(const ColumnPath & columnName) const override;

    /** Commit changes to the database. */
    virtual void commit() override;

    // TODO: implement; the default version is very slow
    //virtual std::pair<Date, Date> getTimestampRange() const;

    virtual Date quantizeTimestamp(Date timestamp) const override;

    virtual std::shared_ptr<MatrixView> getMatrixView() const override;
    virtual std::shared_ptr<ColumnIndex> getColumnIndex() const override;
    virtual std::shared_ptr<RowStream> getRowStream() const override;

    virtual RestRequestMatchResult
    handleRequest(RestConnection & connection,
                  const RestRequest & request,
                  RestRequestParsingContext & context) const override;

    virtual ExpressionValue getRowExpr(const RowPath & rowName) const override;

protected:
    struct Itl;
    std::shared_ptr<Itl> itl;
    SparseMatrixDataset(MldbServer * owner);
};


/******************************************************************************/
/* MUTABLE SPARSE MATRIX DATASET CONFIG                                       */
/******************************************************************************/

enum WriteTransactionLevel {
    WT_READ_AFTER_WRITE,       ///< Data visible as soon as its read
    WT_READ_AFTER_COMMIT       ///< Data visible once it's committed
};

DECLARE_ENUM_DESCRIPTION(WriteTransactionLevel);

enum TransactionFavor {
    TF_FAVOR_READS,            ///< Favor read speed over write speed
    TF_FAVOR_WRITES            ///< Favor write speed over read speed
};

DECLARE_ENUM_DESCRIPTION(TransactionFavor);

struct MutableSparseMatrixDatasetConfig
{
    MutableSparseMatrixDatasetConfig();

    /// Precision for the timestamps
    double timeQuantumSeconds;

    /// Write transaction level.  Can data be read straight away or after commit
    WriteTransactionLevel consistencyLevel;

    /// Transaction favor.  When reads and writes are mixed, which do we favor?
    TransactionFavor favor;
};

DECLARE_STRUCTURE_DESCRIPTION(MutableSparseMatrixDatasetConfig);


/*****************************************************************************/
/* MUTABLE SPARSE MATRIX DATASET                                             */
/*****************************************************************************/

/** Live recordable and queryable sparse matrix dataset. */

struct MutableSparseMatrixDataset: public SparseMatrixDataset {
    MutableSparseMatrixDataset(MldbServer * owner,
                               PolyConfig config,
                               const ProgressFunc & onProgress);

    virtual MultiChunkRecorder getChunkRecorder();

    struct Itl;
};

} // namespace MLDB


