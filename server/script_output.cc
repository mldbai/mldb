// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** script_output.cc
    Jeremy Barnes, 13 October 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    
*/

#include "script_output.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/vector_description.h"
#include "mldb/types/pointer_description.h"


using namespace std;



namespace MLDB {


/*****************************************************************************/
/* SCRIPT STACK FRAME                                                        */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(ScriptStackFrame);

ScriptStackFrameDescription::
ScriptStackFrameDescription()
{
    addField("scriptUri", &ScriptStackFrame::scriptUri,
             "URI of script in this frame");
    addField("functionName", &ScriptStackFrame::functionName,
             "Name of function in this frame");
    addField("where", &ScriptStackFrame::where,
             "Where is the frame, in natural format for language");
    addField("lineNumber", &ScriptStackFrame::lineNumber,
             "Line number", (int64_t)-1);
    addField("columnStart", &ScriptStackFrame::columnStart,
             "Column number of error", (int64_t)-1);
    addField("columnEnd", &ScriptStackFrame::columnEnd,
             "End column number of error", (int64_t)-1);
    addField("extra", &ScriptStackFrame::extra,
             "Extra stack from information from language");
}


/*****************************************************************************/
/* SCRIPT EXCEPTION                                                          */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(ScriptException);

ScriptExceptionDescription::
ScriptExceptionDescription()
{
    addField("message", &ScriptException::message,
             "Exception description");
    addField("where", &ScriptException::where,
             "Full description of where exception came from");
    addField("scriptUri", &ScriptException::scriptUri,
             "URI of script that caused the exception");
    addField("lineNumber", &ScriptException::lineNumber,
             "Number of the line that caused the exception",
             (int64_t)-1);
    addField("columnStart", &ScriptException::columnStart,
             "Start column in the line", (int64_t)-1);
    addField("columnEnd", &ScriptException::columnEnd,
             "End column in the line", (int64_t)-1);
    addField("lineContents", &ScriptException::lineContents,
             "Contents of the line indicating where the exception was");
    addField("context", &ScriptException::context,
             "What we were doing when we threw the exception");
    addField("stack", &ScriptException::stack,
             "Call stack for exception");
    addField("extra", &ScriptException::extra,
             "Extra information from language about exception");
}


/*****************************************************************************/
/* STREAM CONTENT                                                            */
/*****************************************************************************/

struct ScriptLogContentDescription: public ValueDescriptionT<ScriptLogContent> {
    virtual void parseJsonTyped(ScriptLogContent * val,
                                JsonParsingContext & context) const
    {
        if (context.isNull()) {
            context.expectNull();
            *val = ScriptLogContent();
        }
        else if (context.isString()) {
            val->rawBytes = context.expectStringUtf8().stealRawString();
        }
        else if (context.isArray()) {
            Json::Value jval = context.expectJson();
            std::string result;
            for (auto & v: jval) {
                if (v.isIntegral()) {
                    result += char(v.asInt());
                }
                else {
                    result += v.toString();
                }
            }
            val->rawBytes = std::move(result);
        }
    }

    virtual void printJsonTyped(const ScriptLogContent * val,
                                JsonPrintingContext & context) const
    {
        vector<Json::Value> result;
        auto it = val->rawBytes.begin(), end = val->rawBytes.end();
        while (it != end) {
            auto nextIt = utf8::find_invalid(it, end);
            if (it != nextIt) {
                result.push_back(Utf8String(string(it, nextIt)));
                it = nextIt;
            }
            if (it != end) {
                result.push_back((int)*it++);
            }
        }

        if (result.empty())
            context.writeNull();
        else if (result.size() == 1)
            context.writeStringUtf8(Utf8String(result[0].asString()));
        else {
            context.startArray(result.size());
            for (auto & r: result) {
                context.newArrayElement();
                context.writeJson(r);
            }
            context.endArray();
        }
    }

    virtual bool isDefaultTyped(const ScriptLogContent * val) const
    {
        return val->rawBytes.empty();
    }
};

DEFINE_VALUE_DESCRIPTION_NS(ScriptLogContent, ScriptLogContentDescription);


/*****************************************************************************/
/* SCRIPT LOG ENTRY                                                          */
/*****************************************************************************/

bool
ScriptLogEntry::
operator < (const ScriptLogEntry & other) const
{
    // NaNs compare < is always true, and so we don't have a proper
    // partial order which can crash the sort unless we do this
    if (!ts.isADate() || !other.ts.isADate()) {
        if (!ts.isADate() && other.ts.isADate())
            return true;
        if (ts.isADate() && !other.ts.isADate())
            return false;
        return stream < other.stream;
    }

    // No NaDs... standard comparison
    return ts < other.ts
                || (ts == other.ts && stream < other.stream);
}

DEFINE_STRUCTURE_DESCRIPTION(ScriptLogEntry);

ScriptLogEntryDescription::
ScriptLogEntryDescription()
{
    addField("ts", &ScriptLogEntry::ts, "Timestamp at which message was received");
    addField("s", &ScriptLogEntry::stream, "Stream on which message was received");
    addField("c", &ScriptLogEntry::content, "Content of stream");
    addField("closed", &ScriptLogEntry::closed, "Stream is closed", false);
}



/*****************************************************************************/
/* SCRIPT OUTPUT                                                             */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(ScriptOutput);

ScriptOutputDescription::
ScriptOutputDescription()
{
    addField("result", &ScriptOutput::result, "Result of running script");
    addField("logs", &ScriptOutput::logs, "Log entries created by script");
    addField("exception", &ScriptOutput::exception, "Exception thrown by script");
    addField("extra", &ScriptOutput::extra, "Extra information from language");
}


} // namespace MLDB

