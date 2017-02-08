// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** sqlite_dataset.h                                               -*- C++ -*-
    Embedding dataset for MLDB.

    Each row holds a coordinate vector.
*/

#pragma once


#include "mldb/core/dataset.h"
#include "mldb/types/value_description_fwd.h"



namespace MLDB {


/*****************************************************************************/
/* SQLITE SPARSE DATASET CONFIG                                              */
/*****************************************************************************/

struct SqliteSparseDatasetConfig {
    SqliteSparseDatasetConfig()
    {
    }

    Url dataFileUrl;  /// must be file://
};

DECLARE_STRUCTURE_DESCRIPTION(SqliteSparseDatasetConfig);


/*****************************************************************************/
/* SQLITE SPARSE DATASET                                                     */
/*****************************************************************************/

struct SqliteSparseDataset: public Dataset {

    SqliteSparseDataset(MldbServer * owner,
                     PolyConfig config,
                     const ProgressFunc & onProgress);
    
    virtual ~SqliteSparseDataset();

    virtual Any getStatus() const;

    /** Base database methods require us to be able to iterate through rows.
        All other views are built on top of this.
    */
    virtual void recordRowItl(const RowPath & rowName,
                           const std::vector<std::tuple<ColumnPath, CellValue, Date> > & vals);

    virtual void recordRows(const std::vector<std::pair<RowPath, std::vector<std::tuple<ColumnPath, CellValue, Date> > > > & rows);

    /** Commit changes to the database.  Default is a no-op. */
    virtual void commit();

    virtual std::shared_ptr<MatrixView> getMatrixView() const;
    virtual std::shared_ptr<ColumnIndex> getColumnIndex() const;
    virtual std::shared_ptr<RowStream> getRowStream() const { return std::shared_ptr<RowStream>(); }

    virtual std::pair<Date, Date> getTimestampRange() const;
    virtual Date quantizeTimestamp(Date timestamp) const;

    virtual RestRequestMatchResult
    handleRequest(RestConnection & connection,
                  const RestRequest & request,
                  RestRequestParsingContext & context) const;

private:
    SqliteSparseDatasetConfig datasetConfig;
    struct Itl;
    std::shared_ptr<Itl> itl;
};


} // namespace MLDB


