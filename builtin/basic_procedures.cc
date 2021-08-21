/* procedure.cc
   Jeremy Barnes, 21 January 2014
   Copyright (c) 2014 mldb.ai inc.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Procedure support.
*/

#include "basic_procedures.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/types/annotated_exception.h"
#include "mldb/core/function.h"
#include "mldb/core/plugin.h"
#include "mldb/core/dataset.h"
#include "mldb/utils/progress.h"
#include "mldb/rest/cancellation_exception.h"
#include "mldb/types/any_impl.h"


using namespace std;


namespace MLDB {

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
NullProcedure(MldbEngine * engine, const PolyConfig & config,
             const std::function<bool (const Json::Value &)> & onProgress)
    : Procedure(engine)
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
                 "Testing procedure type that does nothing",
                 "procedures/NullProcedure.md.html",
                 nullptr /* static route */,
                 { MldbEntity::INTERNAL_ENTITY });


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
SerialProcedure(MldbEngine * engine, const PolyConfig & config,
               const std::function<bool (const Json::Value &)> & onProgress)
    : Procedure(engine)
{
    this->config = config.params.convert<SerialProcedureConfig>();

    for (auto & s: this->config.steps) {
        if (s.id == config.id)
            throw AnnotatedException(400, "Procedure contains itself as a child",
                                      "id", config.id,
                                      "procedureConfig", config,
                                      "childConfig", s);

        this->steps.emplace_back(obtainProcedure(engine, s, onProgress));
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

    Progress serialProgress;
    std::vector<std::pair<std::string, std::string>> progressSteps;
    for (int i = 0; i < steps.size(); ++i ) {
        progressSteps.emplace_back(make_pair("step " + to_string(i), "done"));
    }
    auto iterationStep = serialProgress.steps(progressSteps);

    auto onProg = [&] (const Json::Value & data) {
        Json::Value progress = jsonEncode(serialProgress);
        if (!data.empty()) {
            progress["subProgress"] = data;
        }
        return onProgress(progress);
    };

    for (int i = 0; i < steps.size(); ++i ) {
        auto & s = steps[i];
        bool keepGoing = onProg(Json::Value{});
        if (!keepGoing) {
            throw MLDB::CancellationException("Procedure serial cancelled");
        }

        RunOutput output = s->run(run, onProg);
        result.steps.emplace_back(std::move(output.results));
        detail.steps.emplace_back(std::move(output.details));

        if (i < steps.size() - 1) {
            iterationStep = iterationStep->nextStep(1);
        }
    }

    return { result, detail };
}

static RegisterProcedureType<SerialProcedure, SerialProcedureConfig>
regSerialProcedure(builtinPackage(),
                   "Train multiple procedures in sequence",
                   "procedures/SerialProcedure.md.html",
                    nullptr /* static route */,
                    { MldbEntity::INTERNAL_ENTITY });


/*****************************************************************************/
/* CREATE ENTITY PROCEDURE                                                    */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(CreateEntityProcedureConfig);

CreateEntityProcedureConfigDescription::
CreateEntityProcedureConfigDescription()
{
    addParent<PolyConfig>();
    addParent<ProcedureConfig>();
    addField("kind", &CreateEntityProcedureConfig::kind,
             "Kind of entity to create.  Valid values are 'function', 'procedure', "
             "'dataset' or 'plugin'.");
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
CreateEntityProcedure(MldbEngine * engine, const PolyConfig & config,
                     const std::function<bool (const Json::Value &)> & onProgress)
    : Procedure(engine)
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
        return makeResult(obtainDataset(engine, config, onProgress));
    }
    else if (config.kind == "plugin") {
        return makeResult(obtainPlugin(engine, config, onProgress));
    }
    else if (config.kind == "procedure") {
        return makeResult(obtainProcedure(engine, config, onProgress));
    }
    else if (config.kind == "function") {
        return makeResult(obtainFunction(engine, config, onProgress));
    }
    else throw AnnotatedException(400, "Attempt to create unknown entity kind '" + config.kind + "'");
}

static RegisterProcedureType<CreateEntityProcedure,
                             CreateEntityProcedureConfig>
regCreateEntityProcedure(builtinPackage(),
                         "Create an entity as part of a procedure application",
                         "procedures/CreateEntityProcedure.md.html",
                         nullptr /* static route */,
                         { MldbEntity::INTERNAL_ENTITY });

} // namespace MLDB
