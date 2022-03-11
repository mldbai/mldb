/** behavior_dataset.h                                            -*- C++ -*-
    Jeremy Barnes, 16 December 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "mldb/core/dataset.h"
#include "mldb/types/periodic_utils.h"

#pragma once

namespace MLDB {

struct BehaviorDomain;
struct MutableBehaviorDomain;
struct BehaviorManager;
class BehaviorColumnIndex;
class BehaviorMatrixView;

// Static instance of a behavior manager, shared between all beh datasets
// in behavior_dataset.cc
extern BehaviorManager behManager;


/*****************************************************************************/
/* BEHAVIOR DATASET                                                          */
/*****************************************************************************/

struct BehaviorDatasetBase: public Dataset {

    friend class BehaviorDatasetRowStream;

    BehaviorDatasetBase(MldbEngine * owner);
    
    void initRoutes();

    virtual ~BehaviorDatasetBase();
    
    virtual Any getStatus() const override;

    virtual void recordRowItl(const RowPath & rowName,
          const std::vector<std::tuple<ColumnPath, CellValue, Date> > & vals) override
    {
        throw MLDB::Exception("Dataset type doesn't allow recording");
    }

    virtual std::shared_ptr<MatrixView> getMatrixView() const override;

    virtual std::shared_ptr<ColumnIndex> getColumnIndex() const override;

    virtual std::shared_ptr<RowStream> getRowStream() const override;

    virtual std::pair<Date, Date> getTimestampRange() const override;
    virtual Date quantizeTimestamp(Date timestamp) const override;

    virtual RestRequestMatchResult
    handleRequest(RestConnection & connection,
                  const RestRequest & request,
                  RestRequestParsingContext & context) const override;

    PolyConfigT<Dataset> save(Url dataFileUrl) const;

protected:
    /// Handler for special routes called on this dataset
    std::unique_ptr<RestRequestRouter> router;

    std::shared_ptr<BehaviorDomain> behs;
    std::shared_ptr<BehaviorColumnIndex> columns;
    std::shared_ptr<BehaviorMatrixView> matrix;
};


/*****************************************************************************/
/* BEHAVIOR DATASET CONFIG                                                  */
/*****************************************************************************/

struct BehaviorDatasetConfig
{
    Url dataFileUrl;  ///< Address (URI) of artifact to load the data from
};

DECLARE_STRUCTURE_DESCRIPTION(BehaviorDatasetConfig);


/*****************************************************************************/
/* BEHAVIOR DATASET                                                          */
/*****************************************************************************/

struct BehaviorDataset: public BehaviorDatasetBase {

    BehaviorDataset(MldbEngine * owner,
                    PolyConfig config,
                    const ProgressFunc & onProgress);
    
    virtual ~BehaviorDataset();
};


/*****************************************************************************/
/* MUTABLE BEHAVIOR DATASET CONFIG                                          */
/*****************************************************************************/

struct MutableBehaviorDatasetConfig : BehaviorDatasetConfig
{
    TimePeriod timeQuantum = "1s";
    bool recordNulls = false;
};

DECLARE_STRUCTURE_DESCRIPTION(MutableBehaviorDatasetConfig);


/*****************************************************************************/
/* MUTABLE BEHAVIOR DATASET                                                 */
/*****************************************************************************/

struct MutableBehaviorDataset: public BehaviorDatasetBase {

    MutableBehaviorDataset(MldbEngine * owner,
                           PolyConfig config,
                           const ProgressFunc & onProgress);
    
    virtual ~MutableBehaviorDataset();
    
    virtual Any getStatus() const;

    virtual void recordRowItl(const RowPath & rowName,
                           const std::vector<std::tuple<ColumnPath, CellValue, Date> > & vals);

    virtual void recordRows(const std::vector<std::pair<RowPath, std::vector<std::tuple<ColumnPath, CellValue, Date> > > > & rows);

    virtual void commit();
    
    virtual std::shared_ptr<MatrixView> getMatrixView() const;

    virtual std::shared_ptr<ColumnIndex> getColumnIndex() const;

    virtual std::shared_ptr<RowStream> getRowStream() const;

    virtual std::pair<Date, Date> getTimestampRange() const;
    virtual Date quantizeTimestamp(Date timestamp) const;
    
private:

    friend struct MutableBehaviorDatasetRowStream;

    std::string address;
    bool recordNulls = false;
    std::shared_ptr<MutableBehaviorDomain> behs;  // Shadows and must be equal to that in the base
};


} // namespace MLDB

