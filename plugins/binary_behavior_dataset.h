// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** binary_behavior_dataset.h                                     -*- C++ -*-
    Jeremy Barnes, 16 December 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

*/

#include "mldb/core/dataset.h"

#pragma once



namespace MLDB {


/*****************************************************************************/
/* BINARY BEHAVIOR DATASET                                                  */
/*****************************************************************************/

/** An extremely efficient, memory mappable dataset for when the only
    values recorded are zeros (nulls) and 1s.
*/

struct BinaryBehaviorDataset: public Dataset {

    BinaryBehaviorDataset(MldbServer * owner,
                           PolyConfig config,
                           const ProgressFunc & onProgress);
    
    virtual ~BinaryBehaviorDataset();
    
    virtual Any getStatus() const;

    virtual void recordRowItl(const RowPath & rowName,
          const std::vector<std::tuple<ColumnPath, CellValue, Date> > & vals)
    {
        throw MLDB::Exception("Dataset type doesn't allow recording");
    }

    virtual std::shared_ptr<MatrixView> getMatrixView() const;

    virtual std::shared_ptr<ColumnIndex> getColumnIndex() const;

    virtual RestRequestMatchResult
    handleRequest(RestConnection & connection,
                  const RestRequest & request,
                  RestRequestParsingContext & context) const;

    virtual std::pair<Date, Date> getTimestampRange() const;
    virtual Date quantizeTimestamp(Date timestamp) const;

    virtual std::shared_ptr<RowStream> getRowStream() const;

    virtual std::shared_ptr<RowValueInfo> getRowInfo() const;

protected:
    // To initialize from a subclass
    BinaryBehaviorDataset(MldbServer * owner);

    struct Itl;
    std::shared_ptr<Itl> itl;
};

/******************************************************************************/
/* MUTABLE BINARY BEHAVIOR DATASET CONFIG                                     */
/******************************************************************************/

struct MutableBinaryBehaviorDatasetConfig : BehaviorDatasetConfig
{
    MutableBinaryBehaviorDatasetConfig();
    double timeQuantumSeconds; 
};

DECLARE_STRUCTURE_DESCRIPTION(MutableBinaryBehaviorDatasetConfig);

/*****************************************************************************/
/* MUTABLE BINARY BEHAVIOR DATASET                                          */
/*****************************************************************************/

/** An extremely efficient, memory mappable dataset for when the only
    values recorded are zeros (nulls) and 1s.

    Allows for atomic updates.
*/

struct MutableBinaryBehaviorDataset: public BinaryBehaviorDataset {

    MutableBinaryBehaviorDataset(MldbServer * owner,
                                  PolyConfig config,
                                  const ProgressFunc & onProgress);
    
    virtual ~MutableBinaryBehaviorDataset();
    
    virtual void recordRowItl(const RowPath & rowName,
                           const std::vector<std::tuple<ColumnPath, CellValue, Date> > & vals);

    virtual void recordRows(const std::vector<std::pair<RowPath, std::vector<std::tuple<ColumnPath, CellValue, Date> > > > & rows);

    virtual void commit();

    std::string address;
};

} // namespace MLDB


