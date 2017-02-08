/** melt_procedure.h                                                   -*- C++ -*-
    Francois Maillet, 21 janvier 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "types/value_description_fwd.h"
#include "server/mldb_server.h"
#include "mldb/core/procedure.h"
#include "sql/sql_expression.h"


namespace MLDB {


/*****************************************************************************/
/*  MELT PROCEDURE CONFIG                                                    */
/*****************************************************************************/


struct MeltProcedureConfig : public ProcedureConfig {
    static constexpr const char * name = "melt";

    MeltProcedureConfig() :
        keyColumnName("key"),
        valueColumnName("value")
    {
        outputDataset.withType("tabular");
    }

    InputQuery inputData;
    PolyConfigT<Dataset> outputDataset;

    std::string keyColumnName;
    std::string valueColumnName;
};

DECLARE_STRUCTURE_DESCRIPTION(MeltProcedureConfig);



/*****************************************************************************/
/* MELT PROCEDURE                                                            */
/*****************************************************************************/

struct MeltProcedure: public Procedure {

    MeltProcedure(MldbServer * owner,
                PolyConfig config,
                const std::function<bool (const Json::Value &)> & onProgress);

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const;

    virtual Any getStatus() const;

    MeltProcedureConfig procConfig;
};

} // namespace MLDB

