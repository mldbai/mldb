/** continuous_dataset.h                                           -*- C++ -*-
    Jeremy Barnes, 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Dataset that continuously records and expires on a time window.
*/

#pragma once


#include "mldb/core/dataset.h"
#include "mldb/core/procedure.h"
#include "mldb/server/forwarded_dataset.h"
#include "mldb/types/value_description_fwd.h"
#include "mldb/types/periodic_utils.h"



namespace MLDB {


/*****************************************************************************/
/* CONTINUOUS DATASET CONFIG                                                 */
/*****************************************************************************/

struct ContinuousDatasetConfig {
    PolyConfigT<Dataset> metadataDataset;          ///< Dataset for metadata storage
    PolyConfigT<Procedure> createStorageDataset;   ///< Create a storage dataset
    PolyConfigT<Procedure> saveStorageDataset;     ///< Save a storage dataset
    TimePeriod commitInterval;                     ///< Frequency for auto-commit
};

DECLARE_STRUCTURE_DESCRIPTION(ContinuousDatasetConfig);


/*****************************************************************************/
/* CONTINUOUS DATASET                                                        */
/*****************************************************************************/

/** This is a continuously mutable, continuously available dataset that
    works by creating views over underlying datasets.
*/

struct ContinuousDataset: public Dataset {

    ContinuousDataset(MldbServer * owner,
                      PolyConfig config,
                      const ProgressFunc & onProgress);
    
    virtual ~ContinuousDataset();

    virtual Any getStatus() const;

    virtual void recordRowItl(const RowPath & rowName,
                              const std::vector<std::tuple<ColumnPath, CellValue, Date> > & vals);
    
    virtual void recordRows(const std::vector<std::pair<RowPath, std::vector<std::tuple<ColumnPath, CellValue, Date> > > > & rows);

    /** Commit changes to the database.  Default is a no-op. */
    virtual void commit();

    virtual std::pair<Date, Date> getTimestampRange() const;
    virtual Date quantizeTimestamp(Date timestamp) const;

    virtual std::shared_ptr<MatrixView> getMatrixView() const;
    virtual std::shared_ptr<ColumnIndex> getColumnIndex() const;
    virtual std::shared_ptr<RowStream> getRowStream() const;

    virtual RestRequestMatchResult
    handleRequest(RestConnection & connection,
                  const RestRequest & request,
                  RestRequestParsingContext & context) const;

private:
    ContinuousDatasetConfig datasetConfig;
    struct Itl;
    std::shared_ptr<Itl> itl;
};


/*****************************************************************************/
/* CONTINUOUS WINDOW DATASET CONFIG                                          */
/*****************************************************************************/

struct ContinuousWindowDatasetConfig {
    ContinuousWindowDatasetConfig();

    PolyConfigT<const Dataset> metadataDataset; ///< Metadata dataset
    Date from;                         ///< Earliest data point to use
    Date to;                           ///< Latest data point to use
    std::shared_ptr<SqlExpression> datasetFilter;  ///< Filter for datasets
};

DECLARE_STRUCTURE_DESCRIPTION(ContinuousWindowDatasetConfig);


/*****************************************************************************/
/* CONTINUOUS WINDOW DATASET                                                 */
/*****************************************************************************/

/** This is an immutable, frozen view of a continuous dataset that is
    typically used to run analytics queries on.
*/

struct ContinuousWindowDataset: public ForwardedDataset {
    ContinuousWindowDataset(MldbServer * owner,
                            PolyConfig config,
                            const ProgressFunc & onProgress);
    /// Dataset in which our metadata lives
    std::shared_ptr<Dataset> metadataDataset;

    PolyConfigT<const Dataset>
    getDatasetConfig(std::shared_ptr<SqlExpression> datasetFilter,
                     Date earliest,
                     Date latest);
};


} // namespace MLDB


