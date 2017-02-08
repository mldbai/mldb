// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* command.h                                                       -*- C++ -*-
   Jeremy Barnes, 29 August 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   Command.  The base thing that we run as a job.
*/

#pragma once

#include <vector>
#include <string>
#include <iostream>
#include <limits>
#include <memory>
#include "mldb/ext/jsoncpp/json.h"
#include "mldb/types/value_description_fwd.h"
#include "mldb/utils/runner.h"
#include "mldb/types/periodic_utils.h"
#include "mldb/types/optional.h"
#include "mldb/types/any.h"


namespace MLDB {


/*****************************************************************************/
/* COMMAND METADATA                                                          */
/*****************************************************************************/

/** Data about how a command should be executed. */

struct CommandMetadata {
    CommandMetadata()
        : memGb(std::numeric_limits<float>::quiet_NaN()),
          numCores(-1),
          startTimeout(0),
          runTimeout(TimeGranularity::SECONDS, 0)
    {
    }

    float memGb;           //< Amount of memory that the command needs
    int numCores;          //< Number of cores that the command needs

    int startTimeout;      //< The time allowed for a job to start before
                           //  cancellation
    TimePeriod runTimeout; //< The time allowed for a job to run before being
                           //  killed, once started (in seconds)

    Optional<int> priority;
};

DECLARE_STRUCTURE_DESCRIPTION(CommandMetadata);


/*****************************************************************************/
/* COMMAND                                                                   */
/*****************************************************************************/

/** This class encapsulates a command that can be run on either a local or a
    remote machine.
*/

struct Command {
    Command()
        : redirectedOutputStream(0),
          redirectedErrStream(0)
    {
    }

    Command(const std::vector<std::string> & cmdLine)
        : cmdLine(cmdLine), redirectedOutputStream(0), redirectedErrStream(0)
    {
    }

    /// Job name of the command that enables to easily identity a job with
    /// human eyes
    std::string jobName;

    /// Category of the job to aggregate stats
    std::string jobCategory;

    /// The group to which the command belongs, if any
    std::string groupName;

    /// Environment variables to set for the command
    std::map<std::string, std::string> env;

    /// Command line
    std::vector<std::string> cmdLine;

    /// Files that need to be made available for the command to run
    /// (files may need to be copied remotely if not accessible)
    std::vector<std::string> filesToMakeAvailable;

    /// Data that needs to be made available for the command to run
    /// (needs to be copied remotely into a temporary file)
    std::vector<std::string> dataToMakeAvaialble;

    CommandMetadata metadata;    ///< Metadata passed to the command

    std::string stdinData;
    std::ostream *redirectedOutputStream;
    std::ostream *redirectedErrStream;
    std::string s3LogUrl;        ///< S3 url where the output of remote jobs
                                 /// is uploaded
    // ...

    void redirectStdoutToStream(std::ostream & stream);
    void redirectStderrToStream(std::ostream & stream);
    void setStdinData(const std::string &data);
    
    /// Render the command as a shell command
    std::string renderShell() const;
};

/** Escapes the given string for use in a shell argument. */
std::string shellEscape(const std::string & str);

inline std::ostream & operator << (std::ostream & stream, const Command & command)
{
    return stream << command.renderShell();
}

DECLARE_STRUCTURE_DESCRIPTION(Command);


/*****************************************************************************/
/* COMMAND STATUS                                                            */
/*****************************************************************************/

/** This object provides the status of a job that was run using the command
    handler interface.
*/

struct CommandStatus {
    std::string handler;                    ///< Which handler was used to run it
    Json::Value handlerConfig;              ///< Configuration of the handler

    Command command;                        ///< Command that was requested

    Any jobResult;                          ///< Result of the command execution
    int returnCode;                         ///< Did the command succeed?  0 means
                                            ///< yes, -1 is launch error, otherwise
                                            ///< positive error code

    Date localStartDate;                    ///< Date/time when the command
                                            ///was launched
    std::vector<std::string> localCommand;  ///< Command that was executed locally
    RunResult localRunResult;               ///< Result of the local command execution

    bool succeeded() const
    {
        return returnCode == 0;
    }
};

DECLARE_STRUCTURE_DESCRIPTION(CommandStatus);

} // namespace MLDB
