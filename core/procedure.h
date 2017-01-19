/** procedure.h                                                       -*- C++ -*-
    Jeremy Barnes, 4 December 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Interface for procedures into MLDB.
*/

#include "mldb/types/value_description_fwd.h"
#include "mldb/core/mldb_entity.h"
#include "mldb/rest/rest_entity.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/sql/sql_expression_operations.h"
#include <set>
#include <iostream>
#include <typeinfo>

// NOTE TO MLDB DEVELOPERS: This is an API header file.  No includes
// should be added, especially value_description.h.


#pragma once



struct RestDirectory;

namespace MLDB {

struct MldbServer;
struct Procedure;

typedef EntityType<Procedure> ProcedureType;

constexpr char GENERIC_OUTPUT_DS_DESC[] =
    "Output dataset configuration. This may refer either to an "
    "existing dataset, or a fully specified but non-existing dataset "
    "which will be created by the procedure.";

/*****************************************************************************/
/* PROCEDURE TRAINING                                                        */
/*****************************************************************************/

struct ProcedureRunConfig {
    Utf8String id;
    Any params;
};

DECLARE_STRUCTURE_DESCRIPTION(ProcedureRunConfig);

struct ProcedureRunState {
    Utf8String state;
};

DECLARE_STRUCTURE_DESCRIPTION(ProcedureRunState);

struct ProcedureRunStatus: public PolyStatus {
    Date runStarted;   ///< Timestamp at which run of the procedure started
    Date runFinished;  ///< Timestamp at which run of the procedure finished
};

DECLARE_STRUCTURE_DESCRIPTION(ProcedureRunStatus);

struct ProcedureRun {
    ProcedureRun()
    {
    }

    ProcedureRun(Procedure * owner,
                 ProcedureRunConfig config,
                 const std::function<bool (const Json::Value & progress)> & onProgress);

    std::shared_ptr<ProcedureRunConfig> config;
    Date runStarted;
    Date runFinished;
    Any results;
    Any details;
};

DECLARE_STRUCTURE_DESCRIPTION(ProcedureRun);

struct ProcedureRunCollection;

struct RunOutput {
    RunOutput(Any results = Any(), Any details = Any()) noexcept
        : results(std::move(results)), details(std::move(details))
    {
    }

    Any results;
    Any details;
};

DECLARE_STRUCTURE_DESCRIPTION(RunOutput);


/*****************************************************************************/
/* PROCEDURE                                                                 */
/*****************************************************************************/

/** Abstraction of a procedure.  Note that since it has child entities (its
    runs), it is necessarily a RestEntity and has to implement that
    interface.
*/

struct Procedure: public MldbEntity, public RestEntity {
    Procedure(MldbServer * server);

    virtual ~Procedure();

    MldbServer * server;

    virtual Any getStatus() const = 0;

    virtual std::string getKind() const
    {
        return "procedure";
    }

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress)
        const = 0;

    virtual bool isCollection() const;

    virtual Utf8String getDescription() const;

    virtual Utf8String getName() const;

    virtual RestEntity * getParent() const;

    /** Return details about a run.  Default gets the details from the
        Run object and returns directly.
    */
    virtual Any getRunDetails(const ProcedureRun * run) const;

    /**
        Apply all existing keys from the run config over the procedure config
    */
    template<typename ProcConfigType>
    ProcConfigType
    applyRunConfOverProcConf(const ProcConfigType & procConf,
                             const ProcedureRunConfig & run) const
    {
        // Recursively copy the values of b into a. Both a and b must be objects.
        typedef std::function<void (Json::Value&, const Json::Value&)> UpdateFunc;
        UpdateFunc update = [&update] (Json::Value & a, const Json::Value & b) {
            if (!a.isObject() || !b.isObject()) return;

            for (const auto & key : b.getMemberNames()) {
                if (b[key].isObject()) {
                    if(!a.isMember(key)) {
                        a[key] = Json::Value();
                    }
                    update(a[key], b[key]);
                } else {
                    a[key] = b[key];
                }
            }
        };

        Json::Value modifiedRun(jsonEncode(procConf));
        update(modifiedRun, jsonEncode(run.params));

        return jsonDecode<ProcConfigType>(modifiedRun);
    }

    std::shared_ptr<ProcedureRunCollection> runs;

};



/*****************************************************************************/
/* PROCEDURE CONFIG                                                          */
/*****************************************************************************/

/*
 * Keep all the shared config parameters for procedure here.
 */
struct ProcedureConfig
{
    ProcedureConfig();
    bool runOnCreation; // force a run of the procedure upon creation
};

DECLARE_STRUCTURE_DESCRIPTION(ProcedureConfig);

/*****************************************************************************/
/* NULL PROCEDURE                                                            */
/*****************************************************************************/

struct NullProcedureConfig : public ProcedureConfig
{
    static constexpr const char * name = "null";
};

DECLARE_STRUCTURE_DESCRIPTION(NullProcedureConfig);

/** Null procedure, that does nothing when run. */

struct NullProcedure: public Procedure {
    NullProcedure(MldbServer * server, const PolyConfig & config,
                 const std::function<bool (const Json::Value &)> & onProgress);

    virtual ~NullProcedure();

    virtual Any getStatus() const;

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress)
        const;
};


/*****************************************************************************/
/* SERIAL PROCEDURE                                                          */
/*****************************************************************************/

/** A serial procedure, that runs multiple procedure steps one after the
    other.
*/

struct ProcedureStepConfig: public PolyConfig {
    /// Name of the step
    Utf8String name;
};

DECLARE_STRUCTURE_DESCRIPTION(ProcedureStepConfig);

struct SerialProcedureConfig : public ProcedureConfig {
    static constexpr const char * name = "serial";
    std::vector<ProcedureStepConfig> steps;
};

DECLARE_STRUCTURE_DESCRIPTION(SerialProcedureConfig);

struct SerialProcedureStatus {
    std::vector<Any> steps;
};

DECLARE_STRUCTURE_DESCRIPTION(SerialProcedureStatus);

struct SerialProcedure: public Procedure {
    SerialProcedure(MldbServer * server,
                   const PolyConfig & config,
                   const std::function<bool (const Json::Value &)> & onProgress);

    SerialProcedureConfig config;

    virtual ~SerialProcedure();

    virtual Any getStatus() const;

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress)
        const;

    std::vector<std::shared_ptr<Procedure> > steps;
};


/*****************************************************************************/
/* CREATE ENTITY PROCEDURE                                                   */
/*****************************************************************************/

/** A procedure that creates an entity as its operation.
*/

struct CreateEntityProcedureConfig: public PolyConfig, ProcedureConfig {
    static constexpr const char * name = "createEntity";

    std::string kind;  ///< function, procedure, plugin, dataset, ...
};

DECLARE_STRUCTURE_DESCRIPTION(CreateEntityProcedureConfig);

/** Output of the createEntity procedure. */
struct CreateEntityProcedureOutput {
    std::string kind;
    PolyConfig config;
    Any status;
};

DECLARE_STRUCTURE_DESCRIPTION(CreateEntityProcedureOutput);

struct CreateEntityProcedure: public Procedure {
    CreateEntityProcedure
        (MldbServer * server,
         const PolyConfig & config,
         const std::function<bool (const Json::Value &)> & onProgress);

    CreateEntityProcedureConfig config;

    virtual ~CreateEntityProcedure();

    virtual Any getStatus() const;

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress)
        const;
};


/*****************************************************************************/
/* UTILITIES                                                                 */
/*****************************************************************************/

std::shared_ptr<Procedure>
createProcedure(MldbServer * server,
                const PolyConfig & config,
                const std::function<bool (const Json::Value & progress)> & onProgress,
                bool overwrite);

std::shared_ptr<Procedure>
obtainProcedure(MldbServer * server,
                const PolyConfig & config,
                const std::function<bool (const Json::Value & progress)> & onProgress
                    = nullptr);


DECLARE_STRUCTURE_DESCRIPTION_NAMED(ProcedurePolyConfigDescription, PolyConfigT<Procedure>);


std::shared_ptr<ProcedureType>
registerProcedureType(const Package & package,
                      const Utf8String & name,
                      const Utf8String & description,
                      std::function<Procedure * (RestDirectory *,
                                                 PolyConfig,
                                                 const std::function<bool (const Json::Value)> &)>
                      createEntity,
                      TypeCustomRouteHandler docRoute,
                      TypeCustomRouteHandler customRoute,
                      std::shared_ptr<const ValueDescription> config,
                      std::set<std::string> registryFlags);

/** Register a new procedure kind.  This takes care of registering everything behind
    the scenes.
*/
template<typename ProcedureT, typename Config>
std::shared_ptr<ProcedureType>
registerProcedureType(const Package & package,
                      const Utf8String & description,
                      const Utf8String & docRoute,
                      TypeCustomRouteHandler customRoute = nullptr,
                      std::set<std::string> registryFlags = {})
{
    static_assert(std::is_convertible<Config, ProcedureConfig>::value,
                  "Procedure configuration type must derive from ProcedureConfig");

    return registerProcedureType
        (package, Config::name, description,
         [] (RestDirectory * server,
             PolyConfig config,
             const std::function<bool (const Json::Value)> & onProgress)
         {
             std::shared_ptr<spdlog::logger> logger = MLDB::getMldbLog<ProcedureT>();
             auto procedure = new ProcedureT(ProcedureT::getOwner(server), config, onProgress);
             procedure->logger = std::move(logger); // noexcept
             return procedure;
         },
         makeInternalDocRedirect(package, docRoute),
         customRoute,
         getDefaultDescriptionSharedT<Config>(),
         registryFlags);
}

template<typename ProcedureT, typename Config>
struct RegisterProcedureType {
    RegisterProcedureType(const Package & package,
                          const Utf8String & description,
                          const Utf8String & docRoute,
                          TypeCustomRouteHandler customRoute = nullptr,
                          std::set<std::string> registryFlags = {})
    {
        handle = registerProcedureType<ProcedureT, Config>
            (package, description, docRoute, customRoute,
             registryFlags);
    }

    std::shared_ptr<ProcedureType> handle;
};

} // namespace MLDB

