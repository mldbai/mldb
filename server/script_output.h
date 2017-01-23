/** script_output.h                                                -*- C++ -*-
    Jeremy Barnes, 13 October 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Common definitions for the output of scripts.  Used to enforce some
    commonality over the different languages available.
*/

#include "mldb/types/value_description_fwd.h"
#include "mldb/types/any.h"
#include "mldb/types/string.h"
#include "mldb/types/date.h"
#include "mldb/ext/jsoncpp/json.h"
#include "mldb/arch/exception.h"
#include <vector>

#pragma once



namespace MLDB {


/*****************************************************************************/
/* SCRIPT STACK FRAME                                                        */
/*****************************************************************************/

/** Represents a frame of a stack trace that occurs in a script.  This is
    designed to be generic enough to work with all scripting languages, but
    allows for extra language-specific information to be recorded in the
    extra field.
*/
struct ScriptStackFrame {
    ScriptStackFrame()
        : lineNumber(-1), columnStart(-1), columnEnd(-1)
    {
    }

    Utf8String scriptUri;       ///< URI that the script runs
    Utf8String functionName;    ///< Name of the function in the stack trace
    Utf8String where;           ///< Where is the frame, in natural format
    int64_t lineNumber;         ///< Line number in the scriptUri, or -1 if unknown
    int64_t columnStart;        ///< Starting column number or -1 if unknown
    int64_t columnEnd;          ///< Ending column number or -1 if unknown
    Any extra;                  ///< Extra language-specific information
};

DECLARE_STRUCTURE_DESCRIPTION(ScriptStackFrame);


/*****************************************************************************/
/* SCRIPT EXCEPTION                                                          */
/*****************************************************************************/

/** Represents an exception that's thrown in a script. It includes the stack
    of the call site and other information about the exception.
*/
struct ScriptException {
    ScriptException()
        : lineNumber(-1), columnStart(-1), columnEnd(-1)
    {
    }

    Utf8String message;
    Utf8String where;
    Utf8String scriptUri;
    int64_t lineNumber;
    int64_t columnStart;
    int64_t columnEnd;
    Utf8String lineContents;
    std::vector<Utf8String> context;
    std::vector<ScriptStackFrame> stack;
    Any extra;
};

DECLARE_STRUCTURE_DESCRIPTION(ScriptException);


/*****************************************************************************/
/* SCRIPT LOG CONTENT                                                        */
/*****************************************************************************/

/** Structure used to hold the content of a stream, such as stdout or
    stderror.

    Currently it only holds text, but is designed to allow for a more
    structured format like JSON or markup.
*/

struct ScriptLogContent {
    ScriptLogContent() noexcept
    {
    }

    ScriptLogContent(std::string rawBytes) noexcept
        : rawBytes(std::move(rawBytes))
    {
    }

    std::string rawBytes;
};

PREDECLARE_VALUE_DESCRIPTION(ScriptLogContent);


/*****************************************************************************/
/* SCRIPT LOG ENTRY                                                          */
/*****************************************************************************/

/** Log file entry.  This is a timestamped entry that describes the channel
    and the content of a log file entry, which allows for logs to be
    kept.
*/

struct ScriptLogEntry {
    ScriptLogEntry() noexcept
        : ts(Date::notADate()), closed(false)
    {
    }

    ScriptLogEntry(Date ts, std::string stream, std::string content) noexcept
        : ts(ts), stream(std::move(stream)), content(std::move(content)),
          closed(false)
    {
    }

    ScriptLogEntry(Date ts, std::string stream, Utf8String content) noexcept
        : ts(ts),
          stream(std::move(stream)),
          content(content.stealRawString()),
          closed(false)
    {
    }

    ScriptLogEntry(Date ts, std::string stream, bool closed) noexcept
        : ts(ts), stream(std::move(stream)), closed(closed)
    {
    }

    Date ts;
    std::string stream;
    ScriptLogContent content;
    bool closed;

    bool operator < (const ScriptLogEntry & other) const;
};

DECLARE_STRUCTURE_DESCRIPTION(ScriptLogEntry);


/*****************************************************************************/
/* SCRIPT OUTPUT                                                             */
/*****************************************************************************/

/** Describes the output of a script run. */

struct ScriptOutput {
    ScriptOutput() : returnCode(0) {}

    Json::Value result;                   ///< Return value of the script
    std::vector<ScriptLogEntry> logs;     ///< Log entries produced by the script
    std::shared_ptr<ScriptException> exception;  ///< Exception thrown, if any
    Any extra;


    void setReturnCode(unsigned rtnCode)
    {
        if (rtnCode == 0) {
            throw MLDB::Exception("Script return_code cannot be set to 0");
        }
        returnCode = rtnCode;
    }

    unsigned getReturnCode() const
    {
        return returnCode;
    }

    private:
        unsigned returnCode;                   ///< Return code of the script
};

DECLARE_STRUCTURE_DESCRIPTION(ScriptOutput);


} // namespace MLDB


