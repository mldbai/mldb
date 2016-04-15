// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** js_common.cc
    Jeremy Barnes, 12 June 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

*/

#include "js_common.h"
#include "mldb/sql/cell_value.h"
#include "mldb/sql/expression_value.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb_js.h"
#include <boost/algorithm/string.hpp>
#include <boost/regex.hpp>


using namespace std;


namespace Datacratic {
namespace MLDB {

Logging::Category mldbJsCategory("javascript");

void
JsIsolate::
init(bool forThisThreadOnly)
{
    this->isolate = v8::Isolate::New();

    this->isolate->Enter();
    v8::V8::SetCaptureStackTraceForUncaughtExceptions(true, 50 /* frame limit */,
                                                      v8::StackTrace::kDetailed);
    this->isolate->Exit();

    if (forThisThreadOnly) {
        // Lock this isolate to our thread for ever
        locker.reset(new v8::Locker(this->isolate));

        // We have a stack of currently entered isolates, and what we
        // want to put this one on the bottom of the stack
        //
        // This allows, when v8 has exited the last one, for our thread
        // specific isolate to stay entered, so that there is no cost to
        // enter it for a function that's called on a worker thread.
        //
        // current isolate
        // next oldest
        // next oldest
        // oldest
        // <ours should go here>
        // ---------
        // 
        // To do so, we need to pop them all off momentarily, push ours,
        // and push all the old ones back again.

        // Pop them all off momentarily
        std::vector<v8::Isolate *> oldIsolates;
        oldIsolates.reserve(128);
        while (v8::Isolate::GetCurrent()) {
            oldIsolates.push_back(v8::Isolate::GetCurrent());
            v8::Isolate::GetCurrent()->Exit();
        }

        // Push ours on
        this->isolate->Enter();

        // Push the others back on top
        while (!oldIsolates.empty()) {
            oldIsolates.back()->Enter();
            oldIsolates.pop_back();
        }
    }
}

V8Init::
V8Init()
{
    const char * v8_argv[] = {
        "--lazy", "false"
        ,"--always_opt", "true"
        ,"--always_full_compiler", "true"
        //,"--trace-opt", "true"
    };
    int v8_argc = sizeof(v8_argv) / sizeof(const char *);

    v8::V8::SetFlagsFromCommandLine(&v8_argc, (char **)&v8_argv, false);
}

CellValue from_js(const JS::JSValue & value, CellValue *)
{
    if (value->IsNull() || value->IsUndefined())
        return CellValue();
    else if (value->IsNumber())
        return CellValue(value->NumberValue());
    else if (value->IsDate())
        return CellValue(Date::fromSecondsSinceEpoch(value->NumberValue() / 1000.0));
    else if (value->IsObject()) {
        // Look if it's already a CellValue
        JsPluginContext * cxt = JsContextScope::current();
        if (cxt->CellValue->HasInstance(value)) {
            return CellValueJS::getShared(value);
        }
        else {
            // Try to go through JSON
            Json::Value val = JS::fromJS(value);
            return jsonDecode<CellValue>(val);
        }
    }
    else return CellValue(utf8str(value));
}

void to_js(JS::JSValue & value, const CellValue & val)
{
    if (val.empty())
        value = v8::Null();
    else if (val.isExactDouble())
        to_js(value, val.toDouble());
    else if (val.isUtf8String())
        to_js(value, val.toUtf8String());
    else if (val.isTimestamp()) {
        to_js(value, val.toTimestamp());
    }
    else if (val.isString()) {
        to_js(value, val.toString());
    }
    else {
        // Get our context so we can return a proper object
        JsPluginContext * cxt = JsContextScope::current();
        value = CellValueJS::create(val, cxt);
    }
}

Coord from_js(const JS::JSValue & value, Coord *)
{
    if (value->IsNull() || value->IsUndefined())
        return Coord();
    return JS::from_js(value, (Utf8String *)0);
}

Coord from_js_ref(const JS::JSValue & value, Coord *)
{
    if (value->IsNull() || value->IsUndefined())
        return Coord();
    return JS::from_js(value, (Utf8String *)0);
}

void to_js(JS::JSValue & value, const Coord & val)
{
    if (val.empty())
        value = v8::Null();
    return to_js(value, val.toUtf8String());
}

Coords from_js(const JS::JSValue & value, Coords *)
{
    if (value->IsNull() || value->IsUndefined())
        return Coords();
    return jsonDecode<Coords>(JS::from_js(value, (Json::Value *)0));
}

Coords from_js_ref(const JS::JSValue & value, Coords *)
{
    if (value->IsNull() || value->IsUndefined())
        return Coords();
    return jsonDecode<Coords>(JS::from_js(value, (Json::Value *)0));
}

void to_js(JS::JSValue & value, const Coords & val)
{
    if (val.empty())
        value = v8::Null();
    return to_js(value, vector<Coord>(val.begin(), val.end()));
}

void to_js(JS::JSValue & value, const ExpressionValue & val)
{
    to_js(value, val.getAtom());
}

ExpressionValue from_js(const JS::JSValue & value, ExpressionValue *)
{
    // NOTE: we currently pretend that CellValue and ExpressionValue
    // are the same thing; they are not.  We will eventually need to
    // allow proper JS access to full-blown ExpressionValue objects,
    // backed with a JS object.

    CellValue val = from_js(value, (CellValue *)0);
    return ExpressionValue(val, Date::notADate());
}

ScriptStackFrame
parseV8StackFrame(const std::string & v8StackFrameMessage)
{
    ScriptStackFrame result;

    static boost::regex format1("[ ]*at (.*) \\((.*):([0-9]+):([0-9]+)\\)");
    static boost::regex format2("[ ]*at (.*):([0-9]+):([0-9]+)");

    boost::smatch what;
    if (boost::regex_match(v8StackFrameMessage, what, format1)) {
        ExcAssertEqual(what.size(), 5);
        result.functionName = what[1];
        result.scriptUri = what[2];
        result.lineNumber = std::stoi(what[3]);
        result.columnStart = std::stoi(what[4]);
    }
    else if (boost::regex_match(v8StackFrameMessage, what, format2)) {
        ExcAssertEqual(what.size(), 4);
        result.scriptUri = what[1];
        result.lineNumber = std::stoi(what[2]);
        result.columnStart = std::stoi(what[3]);
    }
    else {
        result.functionName = v8StackFrameMessage;
    }

    return result;
}



/** Convert an exception to its representation. */
ScriptException convertException(const v8::TryCatch & trycatch,
                                 const Utf8String & context)
{
    using namespace v8;

    if (!trycatch.HasCaught())
        throw ML::Exception("function didn't return but no result");

    Handle<Value> exception = trycatch.Exception();
    String::AsciiValue exception_str(exception);
    
    ScriptException result;
    result.context.push_back(context);
    string where = "(unknown error location)";

    Handle<Message> message = trycatch.Message();

    Json::Value jsonException = JS::fromJS(exception);

    result.extra = jsonException;

    if (!message.IsEmpty()) {
        Utf8String msgStr = JS::utf8str(message->Get());
        Utf8String sourceLine = JS::utf8str(message->GetSourceLine());
        Utf8String scriptUri = JS::utf8str(message->GetScriptResourceName());
        
        //cerr << "msgStr = " << msgStr << endl;
        //cerr << "sourceLine = " << sourceLine << endl;
        
        int line = message->GetLineNumber();
        int column = message->GetStartColumn();
        int endColumn = message->GetEndColumn();
        
        // Note: in the case of backslashed lines, the columns may go past
        // the length of the text in sourceLine (MLDB-980)
        if (column <= sourceLine.length())
            sourceLine.replace(column, 0, "[[[[");
        if (endColumn + 4 <= sourceLine.length())
            sourceLine.replace(endColumn + 4, 0, "]]]]");

        where = ML::format("file '%s', line %d, column %d, source '%s': %s",
                           scriptUri.rawData(),
                           line, column,
                           sourceLine.rawData(),
                           msgStr.rawData());

        result.message = msgStr;
        result.where = where;
        result.scriptUri = JS::utf8str(message->GetScriptResourceName());
        result.lineNumber = line;
        result.columnStart = column;
        result.columnEnd = endColumn;
        result.lineContents = sourceLine;

        auto stack = message->GetStackTrace();

        if (!stack.IsEmpty()) {

            for (unsigned i = 0;  i < stack->GetFrameCount();  ++i) {
                auto frame = stack->GetFrame(i);
                ScriptStackFrame frameRep;
                frameRep.scriptUri = JS::utf8str(frame->GetScriptNameOrSourceURL());
                frameRep.functionName = JS::utf8str(frame->GetFunctionName());
                frameRep.lineNumber = frame->GetLineNumber();
                frameRep.columnStart = frame->GetColumn();

                Json::Value extra;
                extra["isEval"] = frame->IsEval();
                extra["isConstructor"] = frame->IsConstructor();
                
                frameRep.extra = std::move(extra);

                result.stack.emplace_back(std::move(frameRep));
            }
        }
        else {
            auto stack2 = trycatch.StackTrace();

            if (!stack2.IsEmpty()) {
                string traceMessage = JS::cstr(stack2);

                vector<string> traceLines;
                boost::split(traceLines, traceMessage,
                             boost::is_any_of("\n"));

                for (unsigned i = 1;  i < traceLines.size();  ++i) {
                    ScriptStackFrame frame = parseV8StackFrame(traceLines[i]);
                    result.stack.emplace_back(std::move(frame));
                }
            }
        }
        
    }

    return result;
}


/*****************************************************************************/
/* JS OBJECT BASE                                                            */
/*****************************************************************************/

JsObjectBase::
~JsObjectBase()
{
    if (js_object_.IsEmpty()) return;
    ExcAssert(js_object_.IsNearDeath());
    js_object_->SetInternalField(0, v8::Undefined());
    js_object_->SetInternalField(1, v8::Undefined());
    js_object_.Dispose();
    js_object_.Clear();
}

JsPluginContext *
JsObjectBase::
getContext(const v8::Handle<v8::Object> & val)
{
    return reinterpret_cast<JsPluginContext *>
        (v8::Handle<v8::External>::Cast
         (val->GetInternalField(1))->Value());
}

void
JsObjectBase::
wrap(v8::Handle<v8::Object> handle, JsPluginContext * context)
{
    ExcAssert(js_object_.IsEmpty());

    if (handle->InternalFieldCount() == 0) {
        throw ML::Exception("InternalFieldCount is zero; are you forgetting "
                            "to use 'new'?");
    }

    ExcAssertEqual(handle->InternalFieldCount(), 2);

    js_object_ = v8::Persistent<v8::Object>::New(handle);
    js_object_->SetInternalField(0, v8::External::New(this));
    js_object_->SetInternalField(1, v8::External::New(context));
    registerForGarbageCollection();
}

/** Set this object up to be garbage collected once there are no more
    references to it in the javascript. */
void
JsObjectBase::
registerForGarbageCollection()
{
    js_object_.MakeWeak(this, garbageCollectionCallback);
}
    
v8::Handle<v8::Value>
JsObjectBase::
NoConstructor(const v8::Arguments & args)
{
    try {
        return args.This();
        //throw ML::Exception("Type has no constructor");
    } HANDLE_JS_EXCEPTIONS;
}


v8::Handle<v8::FunctionTemplate>
JsObjectBase::
CreateFunctionTemplate(const char * name,
                       v8::InvocationCallback constructor)
{
    using namespace v8;
        
    v8::Handle<v8::FunctionTemplate> t
        = FunctionTemplate::New(constructor);

    t->InstanceTemplate()->SetInternalFieldCount(2);
    t->SetClassName(v8::String::NewSymbol(name));

    return t;
}

    
void
JsObjectBase::
garbageCollectionCallback(v8::Persistent<v8::Value> value, void *data)
{
    JsObjectBase * obj = reinterpret_cast<JsObjectBase *>(data);
    ExcAssert(value == obj->js_object_);
    if (value.IsNearDeath()) {
        delete obj;
    }
}



/*****************************************************************************/
/* JS CONTEXT SCOPE                                                          */
/*****************************************************************************/

JsContextScope::
JsContextScope(JsPluginContext * context)
    : context(context)
{
    enter(context);
}

JsContextScope::
JsContextScope(const v8::Handle<v8::Object> & val)
    : context(JsObjectBase::getContext(val))
{
    enter(context);
}

JsContextScope::
~JsContextScope()
{
    exit(context);
}

static __thread std::vector<JsPluginContext *> * jsContextStack = nullptr;

JsPluginContext *
JsContextScope::
current()
{
    if (!jsContextStack || jsContextStack->empty())
        throw ML::Exception("attempt to retrieve JS context stack with nothing on it");
    return jsContextStack->back();
}

void
JsContextScope::
enter(JsPluginContext * context)
{
    if (!jsContextStack)
        jsContextStack = new std::vector<JsPluginContext *>();
    jsContextStack->push_back(context);
}

void
JsContextScope::
exit(JsPluginContext * context)
{
    if (current() != context)
        throw ML::Exception("JS context stack consistency error");
    jsContextStack->pop_back();
}

} // namespace MLDB

namespace JS {


Json::Value
fromJsForRestParams(const JSValue & val)
{
    //cerr << "converting " << cstr(val) << " to rest params" << endl;

    if (val->IsDate()) {
        //cerr << "date" << endl;
        double ms = val->NumberValue();
        MLDB::CellValue cv(Date::fromSecondsSinceEpoch(ms / 1000.0));
        return jsonEncode(cv);
    }
    else if (val->IsObject()) {

        auto objPtr = v8::Object::Cast(*val);

        Json::Value result;

        v8::Local<v8::Array> properties = objPtr->GetOwnPropertyNames();
        
        for (int i=0; i<properties->Length(); ++i) {
            v8::Local<v8::Value> key = properties->Get(i);
            v8::Local<v8::Value> val = objPtr->Get(key);
            result[utf8str(key)] = fromJsForRestParams(val);
        }
        
        return result;
    }
    else {
        Json::Value jval = JS::fromJS(val);
        //cerr << "got val " << jval << endl;
        return jval;
    }
}

RestParams
from_js(const JSValue & val, const RestParams *)
{
    RestParams result;

    if (val->IsArray()) {

        auto arrPtr = v8::Array::Cast(*val);

        for (int i=0; i<arrPtr->Length(); ++i) {
            auto arrPtr2 = v8::Array::Cast(*arrPtr->Get(i));
            if(arrPtr2->Length() != 2) {
                throw ML::Exception("invalid length for pair extraction");
            }
            
            Json::Value param = fromJsForRestParams(arrPtr2->Get(1));

            if (param.isString())
                result.emplace_back(utf8str(arrPtr2->Get(0)),
                                    param.asString());
            else
                result.emplace_back(utf8str(arrPtr2->Get(0)),
                                    param.toStringNoNewLine());
        }

        return result;
    }
    else if (val->IsObject()) {

        //cerr << "rest params from object " << cstr(val) << endl;

        auto objPtr = v8::Object::Cast(*val);

        v8::Local<v8::Array> properties = objPtr->GetOwnPropertyNames();
        
        for(int i=0; i<properties->Length(); ++i) {
            v8::Local<v8::Value> key = properties->Get(i);
            v8::Local<v8::Value> val = objPtr->Get(key);
            Json::Value param = fromJsForRestParams(val);

            if (param.isString())
                result.emplace_back(utf8str(key),
                                    param.asString());
            else
                result.emplace_back(utf8str(key),
                                    param.toStringNoNewLine());
        }
        
        //cerr << "got " << jsonEncode(result) << endl;

        return result;
    }
    else throw ML::Exception("couldn't convert JS value '%s' to REST parameters",
                             cstr(val).c_str());
}

} // namespace JS

} // namespace Datacratic

