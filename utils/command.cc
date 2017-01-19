// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* command.cc
   Jeremy Barnes, 29 August 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.
   
   Command handler.
*/

#include "command.h"
#include "mldb/arch/exception.h"
#include "mldb/types/map_description.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/vector_description.h"
#include "mldb/types/optional_description.h"


using namespace std;


namespace MLDB {

std::string
shellEscape(const std::string & str)
{
    if (str.empty())
        return "''";

    int numSpaces = 0;
    int numSpecial = 0;
    int numSingleQuotes = 0;
    int numDoubleQuotes = 0;
    int numNormal = 0;
    
    for (char c: str) {
        switch (c) {
        case '\\':
        case '!':
        case '$':
        case '*':
        case '?':
        case '[':
        case ']':
        case '{':
        case '}':
        case '~':
        case '&':
        case ';':
        case '(':
        case ')':
        case '<':
        case '>':
        case '|':
            ++numSpecial;
            break;

        case '\'':
            ++numSingleQuotes;
            break;

        case '\"':
            ++numDoubleQuotes;
            break;

        case ' ':
        case '\t':
        case '\r':
        case '\n':
            ++numSpaces;
            break;

        case '/':
        case '-':
        default:
            ++numNormal;
            break;
        }
    }
    
    if (numSpaces == 0 && numSpecial == 0 && numSingleQuotes == 0 && numDoubleQuotes == 0)
        return str;

    if (numSingleQuotes == 0)
        return '\'' + str + '\'';
    
    if (numSpecial == 0 && numDoubleQuotes == 0)
        return '\"' + str + '\"';

    string result;
    result.reserve(str.size() + 2 + 4 * numSingleQuotes);
    result += '\'';
    for (auto c: str) {
        if (c == '\'')
            result += "\'\"\'\"\'";
        else result += c;
    }

    result += '\'';

    return result;
}

/*****************************************************************************/
/* COMMAND                                                                   */
/*****************************************************************************/

void 
Command::redirectStdoutToStream(std::ostream &stream)
{
    redirectedOutputStream = &stream;
}

void
Command::redirectStderrToStream(std::ostream &stream)
{
    redirectedErrStream = &stream;
}

void
Command::setStdinData(const std::string &data)
{
    stdinData = data ;
}

std::string
Command::
renderShell() const
{
    std::string result;

    for (auto & c: cmdLine) {
        if (result != "")
            result += " ";
        result += shellEscape(c);
    }

    return result;
}

DEFINE_STRUCTURE_DESCRIPTION(Command);

CommandDescription::
CommandDescription()
{
    addField("env", &Command::env, "environment variables");
    addField("cmdLine", &Command::cmdLine, "command line to run");
    addField("metadata", &Command::metadata, "metadata about the command");
    addField("s3LogUrl", &Command::s3LogUrl, "S3 url where output is logged");
}


/*****************************************************************************/
/* COMMAND METADATA                                                          */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(CommandMetadata);

CommandMetadataDescription::
CommandMetadataDescription()
{
    addField("memGb", &CommandMetadata::memGb,
             "Amount of memory required in GB",
             std::numeric_limits<float>::quiet_NaN());
    addField("numCores", &CommandMetadata::numCores,
             "Number of CPU cores that the command takes to run",
             -1);
    addField("priority", &CommandMetadata::priority,
             "Command priority (higher is greater priority");
    addField("startTimeout", &CommandMetadata::startTimeout,
             "Time allowed for a job to start");
    addField("runTimeout", &CommandMetadata::runTimeout,
             "Time in TimePeriod format allowed for a job to execute "
             "entirely");
}


/*****************************************************************************/
/* COMMAND STATUS                                                            */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(CommandStatus);

CommandStatusDescription::
CommandStatusDescription()
{
    addField("handler", &CommandStatus::handler,
             "Command handler used to run the command");
    addField("handlerConfig", &CommandStatus::handlerConfig,
             "Configuration used for the command handler");
    addField("command", &CommandStatus::command,
             "Command that was requested");
    addField("jobResult", &CommandStatus::jobResult,
             "The result of the job that was actually run");
    addField("localCommand", &CommandStatus::localCommand,
             "Command executed locally");
    addField("localRunResult", &CommandStatus::localRunResult,
             "Result of the command executed locally");
}

} // namespace MLDB
