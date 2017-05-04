/** behavior_dataset.h                                            -*- C++ -*-
    Jeremy Barnes, 16 December 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "mldb/core/dataset.h"

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
/* BEHAVIOR DATASET CONFIG                                                  */
/*****************************************************************************/

struct BehaviorDatasetConfig
{
    Url dataFileUrl;  ///< Address (URI) of artifact to load the data from
};

DECLARE_STRUCTURE_DESCRIPTION(BehaviorDatasetConfig);


/*****************************************************************************/
/* BEHAVIOR DATASET                                                         */
/*****************************************************************************/

struct BehaviorDataset: public Dataset {

    friend class PandasRollupPlugin;
    friend class BehaviorDatasetRowStream;

    BehaviorDataset(MldbServer * owner,
                     PolyConfig config,
                     const ProgressFunc & onProgress);
    
    virtual ~BehaviorDataset();
    
    virtual Any getStatus() const;

    virtual void recordRowItl(const RowPath & rowName,
          const std::vector<std::tuple<ColumnPath, CellValue, Date> > & vals)
    {
        throw MLDB::Exception("Dataset type doesn't allow recording");
    }

    virtual std::shared_ptr<MatrixView> getMatrixView() const;

    virtual std::shared_ptr<ColumnIndex> getColumnIndex() const;

    virtual std::shared_ptr<RowStream> getRowStream() const;

    virtual std::pair<Date, Date> getTimestampRange() const;
    virtual Date quantizeTimestamp(Date timestamp) const;

private:
    std::shared_ptr<BehaviorDomain> behs;
    std::shared_ptr<BehaviorColumnIndex> columns;
    std::shared_ptr<BehaviorMatrixView> matrix;
};

/*****************************************************************************/
/* MUTABLE BEHAVIOR DATASET CONFIG                                          */
/*****************************************************************************/

struct MutableBehaviorDatasetConfig : BehaviorDatasetConfig
{
    MutableBehaviorDatasetConfig();
    double timeQuantumSeconds; 
};

DECLARE_STRUCTURE_DESCRIPTION(MutableBehaviorDatasetConfig);

/*****************************************************************************/
/* MUTABLE BEHAVIOR DATASET                                                 */
/*****************************************************************************/

struct MutableBehaviorDataset: public Dataset {

    MutableBehaviorDataset(MldbServer * owner,
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
    std::shared_ptr<MutableBehaviorDomain> behs;
    std::shared_ptr<BehaviorColumnIndex> columns;
    std::shared_ptr<BehaviorMatrixView> matrix;
};


} // namespace MLDB

