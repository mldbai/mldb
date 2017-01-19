// 

/** permutation_procedure.h                                                   -*- C++ -*-
    Francois Maillet, 16 septembre2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "mldb/types/value_description_fwd.h"
#include "mldb/server/mldb_server.h"
#include "mldb/core/procedure.h"


namespace MLDB {


/*****************************************************************************/
/* SCRIPT PROCEDURE CONFIG                                                   */
/*****************************************************************************/

struct PermutationProcedureConfig : public ProcedureConfig {
    static constexpr const char * name = "permuter.run";

    PermutationProcedureConfig()
    {
    }

    PolyConfig procedure;
    Any permutations;
};

DECLARE_STRUCTURE_DESCRIPTION(PermutationProcedureConfig);


/*****************************************************************************/
/* SCRIPT PROCEDURE                                                          */
/*****************************************************************************/

struct PermutationProcedure: public Procedure {

    PermutationProcedure(MldbServer * owner,
                PolyConfig config,
                const std::function<bool (const Json::Value &)> & onProgress);

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const;

    virtual Any getStatus() const;


    typedef std::function<void (const Json::Value&)> ForEachPermutationFunc;
    static void
    forEachPermutation(Json::Value baseConfig,
                       const Json::Value & permutations,
                       ForEachPermutationFunc doForPermutation);

    PermutationProcedureConfig procConf;
};

} // namespace MLDB

