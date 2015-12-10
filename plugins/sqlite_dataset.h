// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** sqlite_dataset.h                                               -*- C++ -*-
    Embedding dataset for MLDB.

    Each row holds a coordinate vector.
*/

#pragma once


#include "mldb/core/dataset.h"
#include "mldb/types/value_description.h"


namespace Datacratic {
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
                     const std::function<bool (const Json::Value &)> & onProgress);
    
    virtual ~SqliteSparseDataset();

    virtual Any getStatus() const;

    /** Base database methods require us to be able to iterate through rows.
        All other views are built on top of this.
    */
    virtual void recordRowItl(const RowName & rowName,
                           const std::vector<std::tuple<ColumnName, CellValue, Date> > & vals);

    virtual void recordRows(const std::vector<std::pair<RowName, std::vector<std::tuple<ColumnName, CellValue, Date> > > > & rows);

    /** Commit changes to the database.  Default is a no-op. */
    virtual void commit();

    virtual std::shared_ptr<MatrixView> getMatrixView() const;
    virtual std::shared_ptr<ColumnIndex> getColumnIndex() const;

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
} // namespace Datacratic

