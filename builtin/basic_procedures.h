/** basic_procedures.h                                            -*- C++ -*-
    Jeremy Barnes, 4 December 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Interface for procedures into MLDB.
*/

#include "mldb/core/procedure.h"

namespace MLDB {

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
    NullProcedure(MldbEngine * engine, const PolyConfig & config,
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
    SerialProcedure(MldbEngine * engine,
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
        (MldbEngine * engine,
         const PolyConfig & config,
         const std::function<bool (const Json::Value &)> & onProgress);

    CreateEntityProcedureConfig config;

    virtual ~CreateEntityProcedure();

    virtual Any getStatus() const;

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress)
        const;
};

} // namespace MLDB

