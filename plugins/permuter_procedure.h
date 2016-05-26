// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** permutation_procedure.h                                                   -*- C++ -*-
    Francois Maillet, 16 septembre2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.
*/

#pragma once

#include "mldb/types/value_description.h"
#include "mldb/server/mldb_server.h"
#include "mldb/core/procedure.h"

namespace Datacratic {
namespace MLDB {

struct Step;

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
                          const std::function<bool (const Step &)> & onProgress) const;

    virtual Any getStatus() const;


    typedef std::function<void (const Json::Value&)> ForEachPermutationFunc;
    static void
    forEachPermutation(Json::Value baseConfig,
                       const Json::Value & permutations,
                       ForEachPermutationFunc doForPermutation);

    PermutationProcedureConfig procConf;
};

} // namespace MLDB
} // namespace Datacratic
