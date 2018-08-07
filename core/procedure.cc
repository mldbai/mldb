/* procedure.cc
   Jeremy Barnes, 21 January 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Procedure support.
*/

#include "mldb/core/procedure.h"
#include "mldb/core/mldb_engine.h"
#include "mldb/types/basic_value_descriptions.h"
#include <mutex>
#include "mldb/types/any_impl.h"
#include "mldb/rest/cancellation_exception.h"


using namespace std;



namespace MLDB {

std::shared_ptr<Procedure>
obtainProcedure(MldbEngine * engine,
                const PolyConfig & config,
                const MldbEngine::OnProgress & onProgress)
{
    return engine->obtainProcedureSync(config, onProgress);
}

std::shared_ptr<Procedure>
createProcedure(MldbEngine * engine,
                const PolyConfig & config,
                const std::function<bool (const Json::Value & progress)> & onProgress,
                bool overwrite)
{
    return engine->createProcedureSync(config, onProgress, overwrite);
}


/*****************************************************************************/
/* PROCEDURE TRAINING                                                         */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(ProcedureRunConfig);

ProcedureRunConfigDescription::
ProcedureRunConfigDescription()
{
    nullAccepted = true;

    addField("id", &ProcedureRunConfig::id, "ID of run");
    addField("params", &ProcedureRunConfig::params, "Parameters of run");
}

DEFINE_STRUCTURE_DESCRIPTION(ProcedureRunState);

ProcedureRunStateDescription::
ProcedureRunStateDescription()
{
    addField("state", &ProcedureRunState::state, "State of the run");
}

DEFINE_STRUCTURE_DESCRIPTION(ProcedureRunStatus);

ProcedureRunStatusDescription::
ProcedureRunStatusDescription()
{
    addParent<PolyStatus>();
    addField("runStarted", &ProcedureRunStatus::runStarted,
             "Timestamp at which the run started");
    addField("runFinished", &ProcedureRunStatus::runFinished,
             "Timestamp at which the run finished");
}

ProcedureRun::
ProcedureRun(Procedure * owner,
             ProcedureRunConfig config,
             const std::function<bool (const Json::Value & progress)> & onProgress)
{
    runStarted = Date::now();
    ExcAssert(owner);
    this->config.reset(new ProcedureRunConfig(std::move(config)));
    try {
        RunOutput output = owner->run(*this->config, onProgress);
        this->results = std::move(output.results);
        this->details = std::move(output.details);
    } catch (...) {
        runFinished = Date::now();
        throw;
    }
    runFinished = Date::now();
}

DEFINE_STRUCTURE_DESCRIPTION(ProcedureRun);

ProcedureRunDescription::
ProcedureRunDescription()
{
    addField("config", &ProcedureRun::config,
             "Configuration of the procedure run");
    addField("runStarted", &ProcedureRun::runStarted,
             "Timestamp at which the run started");
    addField("runFinished", &ProcedureRun::runFinished,
             "Timestamp at which the run finished");
    addField("results", &ProcedureRun::results,
             "Result of running the procedure");
    addField("details", &ProcedureRun::details,
             "Details on the procedure output");
}


DEFINE_STRUCTURE_DESCRIPTION(RunOutput);

RunOutputDescription::
RunOutputDescription()
{
    addField("results", &RunOutput::results,
             "Result of running the procedure");
    addField("details", &RunOutput::details,
             "Details on the procedure output");
}


/*****************************************************************************/
/* PROCEDURE                                                                  */
/*****************************************************************************/

Procedure::
Procedure(MldbEngine * engine)
    : engine(static_cast<MldbEngine *>(engine)),
      runs(engine->createProcedureRunCollection(this))
{
}

Procedure::
~Procedure()
{
}

Any
Procedure::
getStatus() const
{
    return Any();
}

bool
Procedure::
isCollection() const
{
    return true;
}

Utf8String
Procedure::
getDescription() const
{
    return "A procedure description";
}

Utf8String
Procedure::
getName() const
{
    ExcAssert(config_);
    return config_->id;
}

RestEntity *
Procedure::
getParent() const
{
    return engine->getProcedureCollection();
}

Any
Procedure::
getRunDetails(const ProcedureRun * run) const
{
    ExcAssert(run);
    return run->details;
}

/*****************************************************************************/
/* PROCEDURE CONFIG                                                          */
/*****************************************************************************/

ProcedureConfig::
ProcedureConfig() : runOnCreation(true)
{
}

DEFINE_STRUCTURE_DESCRIPTION(ProcedureConfig);

ProcedureConfigDescription::
ProcedureConfigDescription()
    : StructureDescription(true /*nullAccepted*/)
{
    addField("runOnCreation", &ProcedureConfig::runOnCreation,
             "If true, the procedure will be run immediately. The response will contain an "
             "extra field called `firstRun` pointing to the URL of the run.",
             true);

    // ignore unknown fields
    onUnknownField = [] (const ProcedureConfig * conf, JsonParsingContext & ctx) { };
}

DEFINE_STRUCTURE_DESCRIPTION_NAMED(ProcedurePolyConfigDescription, PolyConfigT<Procedure>);

ProcedurePolyConfigDescription::
ProcedurePolyConfigDescription()
{
    addParent<PolyConfig>();
    setTypeName("Procedure");
    documentationUri = "/doc/builtin/procedures/ProcedureConfig.md";
}

} // namespace MLDB

