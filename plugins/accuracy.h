/** accuracy.h                                                   -*- C++ -*-
    Jeremy Barnes, 22 January 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Accuracy procedure and functions.
*/

#pragma once

#include "mldb/core/dataset.h"
#include "mldb/core/procedure.h"
#include "mldb/core/function.h"
#include "matrix.h"
#include "mldb/types/value_description_fwd.h"
#include "mldb/types/optional.h"
#include "mldb/plugins/classifier.h"


namespace MLDB {


class SqlExpression;


struct AccuracyConfig : public ProcedureConfig {
    static constexpr const char * name = "classifier.test";

    AccuracyConfig()
          : mode(CM_BOOLEAN), uniqueScoresOnly(false)
    {
    }

    /// Sql query to select the testing data
    InputQuery testingData;

    /// What mode to run in
    ClassifierMode mode;

    bool uniqueScoresOnly;

    /// Dataset we output to
    Optional<PolyConfigT<Dataset> > outputDataset;
    static constexpr char const * defaultOutputDatasetType = "tabular";
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

