/** sparse_matrix.h                                                -*- C++ -*-
    SparseMatrix dataset for MLDB.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    Each row holds a coordinate vector.
*/

#pragma once


#include "mldb/types/value_description_fwd.h"
#include "mldb/core/dataset.h"


namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* SPARSE MATRIX DATASET                                                     */
/*****************************************************************************/

struct SparseMatrixDataset: public Dataset {

    virtual ~SparseMatrixDataset();

    virtual Any getStatus() const;

    /** Base database methods require us to be able to iterate through rows.
        All other views are built on top of this.
    */
    virtual void recordRowItl(const RowName & rowName,
                           const std::vector<std::tuple<ColumnName, CellValue, Date> > & vals);

    virtual void recordRows(const std::vector<std::pair<RowName, std::vector<std::tuple<ColumnName, CellValue, Date> > > > & rows);

    /** Return what is known about the given column.  Default returns
        an "any value" result, ie nothing is known about the column.
    */
    virtual KnownColumn getKnownColumnInfo(const ColumnName & columnName) const;

    /** Commit changes to the database.  Default is a no-op. */
    virtual void commit();

    // TODO: implement; the default version is very slow
    //virtual std::pair<Date, Date> getTimestampRange() const;
    virtual Date quantizeTimestamp(Date timestamp) const;

    virtual std::shared_ptr<MatrixView> getMatrixView() const;
    virtual std::shared_ptr<ColumnIndex> getColumnIndex() const;
    virtual std::shared_ptr<RowStream> getRowStream() const;

    virtual RestRequestMatchResult
    handleRequest(RestConnection & connection,
                  const RestRequest & request,
                  RestRequestParsingContext & context) const;

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
                               const std::function<bool (const Json::Value &)> & onProgress);

    struct Itl;
};

} // namespace MLDB
} // namespace Datacratic

