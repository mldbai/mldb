/** datasetsplit_procedure.h                                                   -*- C++ -*-
    Mathieu Marquis Bolduc, April 3rd 2017
    This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "types/value_description_fwd.h"
#include "core/mldb_engine.h"
#include "mldb/core/procedure.h"
#include "sql/sql_expression.h"


namespace MLDB {


/*****************************************************************************/
/*  DATASET SPLIT PROCEDURE CONFIG                                           */
/*****************************************************************************/


struct SplitProcedureConfig : public ProcedureConfig {
    static constexpr const char * name = "split";

    InputQuery labels;
    std::vector<PolyConfigT<Dataset>> outputDatasets;
    std::vector<float> splits;
    float foldImportance = 1.0f;
    bool reproducible = true;
    uint64_t randomSeed = 0;
};

DECLARE_STRUCTURE_DESCRIPTION(SplitProcedureConfig);



/*****************************************************************************/
/* MELT PROCEDURE                                                            */
/*****************************************************************************/

struct SplitProcedure: public Procedure {

    SplitProcedure(MldbEngine * owner,
                PolyConfig config,
                const std::function<bool (const Json::Value &)> & onProgress);

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const;

    virtual Any getStatus() const;

    SplitProcedureConfig procConfig;
};

} // namespace MLDB

