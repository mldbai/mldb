// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** accuracy.h                                                   -*- C++ -*-
    Jeremy Barnes, 22 January 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    Accuracy procedure and functions.
*/

#pragma once

#include "mldb/server/dataset.h"
#include "mldb/server/procedure.h"
#include "mldb/server/algorithm.h"
#include "mldb/server/function.h"
#include "matrix.h"
#include "mldb/types/value_description.h"
#include "mldb/types/optional.h"

namespace Datacratic {
namespace MLDB {


class SqlExpression;


struct AccuracyConfig : public ProcedureConfig {
    AccuracyConfig()
        :  when(WhenExpression::TRUE),
           where(SqlExpression::TRUE),
           weight(SqlExpression::ONE),
           orderBy(OrderByExpression::ROWHASH),
           offset(0), limit(-1)
    {
    }

    /// Dataset with testing data
    std::shared_ptr<TableExpression> testingDataset;

    /// Dataset we output to
    Optional<PolyConfigT<Dataset> > outputDataset;
    static constexpr char const * defaultOutputDatasetType = "sparse.mutable";

    /// The WHEN clause for the timespan the tuples must belong to
    WhenExpression when;

    /// The WHERE clause for which rows to include from the dataset
    std::shared_ptr<SqlExpression> where;

    /// The expression to generate the score, normally the name of the score value
    std::shared_ptr<SqlExpression> score;

    /// The expression to generate the label
    std::shared_ptr<SqlExpression> label;

    /// The expression to generate the weight
    std::shared_ptr<SqlExpression> weight;

    /// How to order the rows when using an offset and a limit
    OrderByExpression orderBy;

    /// Where to start running
    ssize_t offset;

    /// Maximum number of rows to use
    ssize_t limit;
};

DECLARE_STRUCTURE_DESCRIPTION(AccuracyConfig);


/*****************************************************************************/
/* ACCURACY PROCEDURE                                                         */
/*****************************************************************************/

/** Procedure that calculates the accuracy of a classifier. */

struct AccuracyProcedure: public Procedure {

    AccuracyProcedure(MldbServer * owner,
                     PolyConfig config,
                     const std::function<bool (const Json::Value &)> & onProgress);
    
    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const;
    
    virtual Any getStatus() const;
    
    AccuracyConfig accuracyConfig;
};


} // namespace MLDB
} // namespace Datacratic
