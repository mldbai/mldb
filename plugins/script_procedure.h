/** script.h                                                   -*- C++ -*-
    Francois Maillet, 10 juillet 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Script procedure
*/

#pragma once

#include "mldb/types/value_description_fwd.h"
#include "mldb/server/plugin_resource.h"
#include "mldb/server/mldb_server.h"
#include "mldb/core/procedure.h"


namespace MLDB {


/*****************************************************************************/
/* SCRIPT PROCEDURE CONFIG                                                   */
/*****************************************************************************/

struct ScriptProcedureConfig : ProcedureConfig {
    static constexpr const char * name = "script.run";

    ScriptProcedureConfig()
    {
    }

    std::string language;
    ScriptResource scriptConfig;

    Any args;
};

DECLARE_STRUCTURE_DESCRIPTION(ScriptProcedureConfig);


/*****************************************************************************/
/* SCRIPT PROCEDURE                                                          */
/*****************************************************************************/

struct ScriptProcedure: public Procedure {

    ScriptProcedure(MldbServer * owner,
                    PolyConfig config,
                    const std::function<bool (const Json::Value &)> & onProgress);

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const;

    virtual Any getStatus() const;

    ScriptProcedureConfig scriptProcedureConfig;
};

} // namespace MLDB

