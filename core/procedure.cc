// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* procedure.cc
   Jeremy Barnes, 21 January 2014
   Copyright (c) 2014 Datacratic Inc.  All rights reserved.

   Procedure support.
*/

#include "mldb/core/procedure.h"
#include "mldb/server/procedure_collection.h"
#include "mldb/server/procedure_run_collection.h"
#include "mldb/types/basic_value_descriptions.h"
#include <mutex>
#include "mldb/server/mldb_server.h"
#include "mldb/core/dataset.h"
#include "mldb/core/plugin.h"
#include "mldb/core/function.h"
#include "mldb/types/any_impl.h"


using namespace std;


namespace Datacratic {
namespace MLDB {


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
Procedure(MldbServer * server)
    : server(static_cast<MldbServer *>(server)),
      runs(new ProcedureRunCollection(server, this))
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
    return server->procedures.get();
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
ProcedureConfig() : runOnCreation(false)
{
}

DEFINE_STRUCTURE_DESCRIPTION(ProcedureConfig);

ProcedureConfigDescription::
ProcedureConfigDescription() 
    : StructureDescription(true /*nullAccepted*/)
{
    addField("runOnCreation", &ProcedureConfig::runOnCreation,
             "If true, perform a first run of the procedure after creation. The response will contain an "
             "extra field `firstRun` pointing to the location of the run.  All run's "
             "artefacts will also be available after a successful call.",
             false);

    // ignore unknown fields
    onUnknownField = [] (const ProcedureConfig * conf, JsonParsingContext & ctx) { };
}

/*****************************************************************************/
/* NULL PROCEDURE                                                             */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(NullProcedureConfig);

NullProcedureConfigDescription::
NullProcedureConfigDescription()
{
    addParent<ProcedureConfig>();
}

NullProcedure::
NullProcedure(MldbServer * server, const PolyConfig & config,
             const std::function<bool (const Json::Value &)> & onProgress)
    : Procedure(server)
{
}

NullProcedure::
~NullProcedure()
{
}

Any
NullProcedure::
getStatus() const
{
    return Any();
}

RunOutput
NullProcedure::
run(const ProcedureRunConfig & run,
      const std::function<bool (const Json::Value &)> & onProgress) const
{
    return Any();
}

RegisterProcedureType<NullProcedure, NullProcedureConfig>
regNullProcedure(builtinPackage(),
                 "null",
                 "Testing procedure type that does nothing",
                 "procedures/NullProcedure.md.html");


/*****************************************************************************/
/* SERIAL PROCEDURE                                                           */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(ProcedureStepConfig);

ProcedureStepConfigDescription::
ProcedureStepConfigDescription()
{
    addField("name", &ProcedureStepConfig::name,
             "Name of the step.  Used to make the error messages and log "
             "information more comprehensible.");
    addParent<PolyConfig>();
}

DEFINE_STRUCTURE_DESCRIPTION(SerialProcedureConfig);

SerialProcedureConfigDescription::
SerialProcedureConfigDescription()
{
    addField("steps", &SerialProcedureConfig::steps,
             "Steps that will be run (in order) when the procedure is run.");
    addParent<ProcedureConfig>();
}

DEFINE_STRUCTURE_DESCRIPTION(SerialProcedureStatus);

SerialProcedureStatusDescription::
SerialProcedureStatusDescription()
{
    addField("steps", &SerialProcedureStatus::steps,
             "Status of each of the steps in the procedure");
}

SerialProcedure::
SerialProcedure(MldbServer * server, const PolyConfig & config,
               const std::function<bool (const Json::Value &)> & onProgress)
    : Procedure(server)
{
    this->config = config.params.convert<SerialProcedureConfig>();

    for (auto & s: this->config.steps) {
        if (s.id == config.id)
            throw HttpReturnException(400, "Procedure contains itself as a child",
                                      "id", config.id,
                                      "procedureConfig", config,
                                      "childConfig", s);

        this->steps.emplace_back(obtainProcedure(server, s, onProgress));
    }
}

SerialProcedure::
~SerialProcedure()
{
}

Any
SerialProcedure::
getStatus() const
{
    SerialProcedureStatus result;
    for (auto & s: steps) {
        result.steps.emplace_back(s->getStatus());
    }
    return result;
}

RunOutput
SerialProcedure::
run(const ProcedureRunConfig & run,
      const std::function<bool (const Json::Value &)> & onProgress) const
{
    SerialProcedureStatus result;
    SerialProcedureStatus detail;

    for (auto & s: steps) {
        RunOutput output = s->run(run, onProgress);
        result.steps.emplace_back(std::move(output.results));
        detail.steps.emplace_back(std::move(output.details));
    }
    
    return { result, detail };
}

static RegisterProcedureType<SerialProcedure, SerialProcedureConfig>
regSerialProcedure(builtinPackage(),
                   "serial",
                   "Train multiple procedures in sequence",
                   "procedures/SerialProcedure.md.html");


/*****************************************************************************/
/* CREATE ENTITY PROCEDURE                                                    */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(CreateEntityProcedureConfig);

CreateEntityProcedureConfigDescription::
CreateEntityProcedureConfigDescription()
{
    addParent<PolyConfig>();
    addField("kind", &CreateEntityProcedureConfig::kind,
             "Kind of entity to create.  Valid values are 'function', 'procedure', "
             "'dataset' or 'plugin'.");
    addParent<ProcedureConfig>();
}

DEFINE_STRUCTURE_DESCRIPTION(CreateEntityProcedureOutput);

CreateEntityProcedureOutputDescription::
CreateEntityProcedureOutputDescription()
{
    addField("kind", &CreateEntityProcedureOutput::kind,
             "Kind of entity created");
    addField("config", &CreateEntityProcedureOutput::config,
             "Configuration of the entity created");
    addField("status", &CreateEntityProcedureOutput::status,
             "Status of entity creation");
}

DECLARE_STRUCTURE_DESCRIPTION(CreateEntityProcedureOutput);

CreateEntityProcedure::
CreateEntityProcedure(MldbServer * server, const PolyConfig & config,
                     const std::function<bool (const Json::Value &)> & onProgress)
    : Procedure(server)
{
    this->config = config.params.convert<CreateEntityProcedureConfig>();
}

CreateEntityProcedure::
~CreateEntityProcedure()
{
}

Any
CreateEntityProcedure::
getStatus() const
{
    Json::Value result;
    result["config"] = jsonEncode(this->config);
    return result;
}

RunOutput
CreateEntityProcedure::
run(const ProcedureRunConfig & run,
      const std::function<bool (const Json::Value &)> & onProgress) const
{
    auto makeResult = [&] (std::shared_ptr<MldbEntity> entity)
        {
            CreateEntityProcedureOutput result;
            result.kind = entity->getKind();
            result.config = entity->getConfig();
            result.status = entity->getStatus();
            return Any(result);
        };
    
    if (config.kind == "dataset") {
        return makeResult(obtainDataset(server, config, onProgress));
    }
    else if (config.kind == "plugin") {
        return makeResult(obtainPlugin(server, config, onProgress));
    }
    else if (config.kind == "procedure") {
        return makeResult(obtainProcedure(server, config, onProgress));
    }
    else if (config.kind == "function") {
        return makeResult(obtainFunction(server, config, onProgress));
    }
    else throw HttpReturnException(400, "Attempt to create unknown entity kind '" + config.kind + "'");
}

static RegisterProcedureType<CreateEntityProcedure,
                             CreateEntityProcedureConfig>
regCreateEntityProcedure(builtinPackage(),
                         "createEntity",
                         "Create an entity as part of a procedure application",
                         "procedures/CreateEntityProcedure.md.html");



DEFINE_STRUCTURE_DESCRIPTION_NAMED(ProcedurePolyConfigDescription, PolyConfigT<Procedure>);

ProcedurePolyConfigDescription::
ProcedurePolyConfigDescription()
{
    addParent<PolyConfig>();
    setTypeName("Procedure");
    documentationUri = "/doc/builtin/procedures/ProcedureConfig.md";
}

} // namespace MLDB
} // namespace Datacratic

