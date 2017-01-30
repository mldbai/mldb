/** external_python_procedure.h                                     -*- C++ -*-

    Francois Maillet, 31 aout 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    External python procedure
*/

#pragma once

#include "mldb/core/procedure.h"
#include "mldb/types/value_description_fwd.h"
#include "mldb/server/plugin_resource.h"


namespace MLDB {

struct ExternalPythonProcedureConfig: public ProcedureConfig {
    static constexpr const char * name = "experimental.external.procedure";

    ExternalPythonProcedureConfig()
    {
    }

    std::string stdInData;
    ScriptResource scriptConfig;
};

DECLARE_STRUCTURE_DESCRIPTION(ExternalPythonProcedureConfig);



/*****************************************************************************/
/* EXTERNAL PYTHON PROCEDURE                                                 */
/*****************************************************************************/

struct ExternalPythonProcedure: public Procedure {

    ExternalPythonProcedure(MldbServer * owner,
                   PolyConfig config,
                   const std::function<bool (const Json::Value &)> & onProgress);

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const;

    virtual Any getStatus() const;

    ExternalPythonProcedureConfig procedureConfig;

    std::shared_ptr<LoadedPluginResource> pluginRes;
};


} // namespace MLDB

