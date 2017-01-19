// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** js_common.cc
    Jeremy Barnes, 12 June 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

*/

#include "js_common.h"
#include "js_utils.h"
#include "mldb/sql/cell_value.h"
#include "mldb/sql/expression_value.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb_js.h"
#include "mldb/ext/v8-cross-build-output/include/libplatform/libplatform.h"
#include "mldb/base/thread_pool.h"
#include <boost/algorithm/string.hpp>
#include <boost/regex.hpp>
#include <boost/filesystem.hpp>
#include <thread>
#include <queue>
#include <condition_variable>


using namespace std;
using namespace v8;



namespace MLDB {

Logging::Category mldbJsCategory("javascript");

/**
 * V8 Platform abstraction layer.
 *
 * The embedder has to provide an implementation of this interface before
 * initializing the rest of V8.
 */
struct V8MldbPlatform: public v8::Platform {
    V8MldbPlatform(MldbServer * server)
        : start(std::chrono::steady_clock::now()),
          shutdown(false),
          foregroundMessageLoop(std::bind(&V8MldbPlatform::runForegroundLoop,
                                          this))
    {
    }

    ~V8MldbPlatform()
    {
        shutdown = true;
        foregroundLoopCondition.notify_all();
        std::unique_lock<std::mutex> guard(mutex);
        foregroundMessageLoop.join();

        // Delete outstanding tasks (don't run them)
        while (!queue.empty()) {
            auto entry = queue.top();
            Task * task = std::get<2>(entry);
            delete task;
            queue.pop();
        };
    }

    std::chrono::time_point<std::chrono::steady_clock> start;
    MldbServer * server;

    /**
     * Gets the number of threads that are used to execute background tasks. Is
     * used to estimate the number of tasks a work package should be split into.
     * A return value of 0 means that there are no background threads available.
     * Note that a value of 0 won't prohibit V8 from posting tasks using
     * |CallOnBackgroundThread|.
     */
    virtual size_t NumberOfAvailableBackgroundThreads() override
    {
        return ThreadPool::instance().numThreads();
    }

    /**
     * Schedules a task to be invoked on a background thread. |expected_runtime|
     * indicates that the task will run a long time. The Platform implementation
     * takes ownership of |task|. There is no guarantee about order of execution
     * of tasks wrt order of scheduling, nor is there a guarantee about the
     * thread the task will be run on.
     */
    virtual void CallOnBackgroundThread(Task* task,
                                        ExpectedRuntime expected_runtime) override
    {
        std::shared_ptr<Task> taskPtr(task);
        auto lambda = [=] ()
            {
                taskPtr->Run();
            };

        if (expected_runtime == Platform::kShortRunningTask) {
            ThreadPool::instance().add(lambda);
        }
        else {
            std::thread(lambda).detach();
        }
    }

    /**
     * Schedules a task to be invoked on a foreground thread wrt a specific
     * |isolate|. Tasks posted for the same isolate should be execute in order of
     * scheduling. The definition of "foreground" is opaque to V8.
     */
    virtual void CallOnForegroundThread(Isolate* isolate, Task* task) override
    {
        CallDelayedOnForegroundThread(isolate, task, 0.0);
    }

    /**
     * Schedules a task to be invoked on a foreground thread wrt a specific
     * |isolate| after the given number of seconds |delay_in_seconds|.
     * Tasks posted for the same isolate should be execute in order of
     * scheduling. The definition of "foreground" is opaque to V8.
     */
    virtual void CallDelayedOnForegroundThread(Isolate* isolate, Task* task,
                                               double delay_in_seconds) override
    {
        auto deadline = std::chrono::steady_clock::now()
            + std::chrono::nanoseconds((long long)delay_in_seconds * 1000000000);
        std::unique_lock<std::mutex> guard(mutex);
        queue.emplace(deadline, isolate, task);
        if (queue.empty() || deadline < std::get<0>(queue.top())) {
            foregroundLoopCondition.notify_one();
        }
    }

    /**
     * Monotonically increasing time in seconds from an arbitrary fixed point in
     * the past. This function is expected to return at least
     * millisecond-precision values. For this reason,
     * it is recommended that the fixed point be no further in the past than
     * the epoch.
     **/
    virtual double MonotonicallyIncreasingTime() override
    {
        std::chrono::duration<double> diff
            = std::chrono::steady_clock::now() - start;
        return diff.count();
    }

    /**
     * Called by TRACE_EVENT* macros, don't call this directly.
     * The name parameter is a category group for example:
     * TRACE_EVENT0("v8,parse", "V8.Parse")
     * The pointer returned points to a value with zero or more of the bits
     * defined in CategoryGroupEnabledFlags.
     **/
    virtual const uint8_t* GetCategoryGroupEnabled(const char* name) override
    {
        static uint8_t no = 0;
        return &no;
    }

    /**
     * Gets the category group name of the given category_enabled_flag pointer.
     * Usually used while serliazing TRACE_EVENTs.
     **/
    virtual const char*
    GetCategoryGroupName(const uint8_t* category_enabled_flag) override
    {
        static const char dummy[] = "dummy";
        return dummy;
    }

    /**
     * Adds a trace event to the platform tracing system. This function call is
     * usually the result of a TRACE_* macro from trace_event_common.h when
     * tracing and the category of the particular trace are enabled. It is not
     * advisable to call this function on its own; it is really only meant to be
     * used by the trace macros. The returned handle can be used by
     * UpdateTraceEventDuration to update the duration of COMPLETE events.
     */
    virtual uint64_t
    AddTraceEvent(char phase,
                  const uint8_t* category_enabled_flag,
                  const char* name,
                  const char* scope,
                  uint64_t id, uint64_t bind_id,
                  int32_t num_args, const char** arg_names,
                  const uint8_t* arg_types, const uint64_t* arg_values,
                  unsigned int flags) override
    {
        // TODO: hook into MLDB logging framework
        return 0;
    }

    /**
     * Sets the duration field of a COMPLETE trace event. It must be called with
     * the handle returned from AddTraceEvent().
     **/
    virtual void
    UpdateTraceEventDuration(const uint8_t* category_enabled_flag,
                             const char* name, uint64_t handle) override
    {
    }

    void runForegroundLoop()
    {
        std::unique_lock<std::mutex> guard(mutex);

        while (!shutdown.load()) {
            if (queue.empty()) {
                foregroundLoopCondition.wait(guard);
            }
            else {
                TimePoint nextWakeup = std::get<0>(queue.top());
                foregroundLoopCondition.wait_until(guard, nextWakeup);
            }

            if (shutdown.load())
                return;

            while (!queue.empty()
                   && (std::chrono::steady_clock::now()
                       < std::get<0>(queue.top()))) {
                auto entry = queue.top();
                queue.pop();
                guard.unlock();
                Task * task = std::get<2>(entry);
                task->Run();
                delete task;
                guard.lock();
            }
        };
    }

    std::mutex mutex;
    std::atomic<bool> shutdown;
    std::condition_variable foregroundLoopCondition;

    typedef std::chrono::time_point<std::chrono::steady_clock> TimePoint;
    typedef std::tuple<TimePoint, v8::Isolate *, Task*> DelayedEntry;
    std::priority_queue<DelayedEntry, std::vector<DelayedEntry>,
                        std::greater<DelayedEntry> >
        queue;
    //tracing::TracingController* tracing_controller_;

    std::thread foregroundMessageLoop;
};

void
JsIsolate::
init(bool forThisThreadOnly)
{
    Isolate::CreateParams create_params;
    create_params.array_buffer_allocator =
        v8::ArrayBuffer::Allocator::NewDefaultAllocator();

    this->isolate = v8::Isolate::New(create_params);


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
V8Init(MldbServer * server)
{
    static std::atomic<bool> alreadyDone(false);
    if (alreadyDone)
        return;

    static std::mutex mutex;
    std::unique_lock<std::mutex> guard(mutex);

    if (alreadyDone)
        return;

    const char * v8_argv[] = {
        "--lazy", "false"
        ,"--always_opt", "true"
    };
    int v8_argc = sizeof(v8_argv) / sizeof(const char *);

    v8::V8::SetFlagsFromCommandLine(&v8_argc, (char **)&v8_argv, false);

    // TODO: linuxisms...
    char exePath[PATH_MAX];
    ssize_t pathLen = readlink("/proc/self/exe", exePath, PATH_MAX);
    if (pathLen == -1)
        throw HttpReturnException
            (400, "Couldn't path to the MLDB executable; "
             "is the /proc filesystem mounted?  ("
             + string(strerror(errno)) + ")");
    boost::filesystem::path path(exePath, exePath + pathLen);
    auto libPath = path.parent_path().parent_path() / "lib" / "libv8.so";

    v8::V8::InitializeExternalStartupData(libPath.c_str());
    v8::V8::InitializePlatform(new V8MldbPlatform(server));
    v8::V8::Initialize();

    alreadyDone = true;
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
        v8::Isolate* isolate = v8::Isolate::GetCurrent();
        // Look if it's already a CellValue
        JsPluginContext * cxt = JsContextScope::current();
        if (cxt->CellValue.Get(isolate)->HasInstance(value)) {
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
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    if (val.empty())
        value = v8::Null(isolate);
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

PathElement from_js(const JS::JSValue & value, PathElement *)
{
    if (value->IsNull() || value->IsUndefined())
        return PathElement();
    return JS::from_js(value, (Utf8String *)0);
}

PathElement from_js_ref(const JS::JSValue & value, PathElement *)
{
    if (value->IsNull() || value->IsUndefined())
        return PathElement();
    return JS::from_js(value, (Utf8String *)0);
}

void to_js(JS::JSValue & value, const PathElement & val)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    if (val.null())
        value = v8::Null(isolate);
    return to_js(value, val.toUtf8String());
}

Path from_js(const JS::JSValue & value, Path *)
{
    if (value->IsNull() || value->IsUndefined())
        return Path();
    if (value->IsArray()) {
        auto vals = JS::from_js(value, (std::vector<PathElement> *)0);
        return Path(vals.data(), vals.size());
    }
    return jsonDecode<Path>(JS::from_js(value, (Json::Value *)0));
}

Path from_js_ref(const JS::JSValue & value, Path *)
{
    return from_js(value, (Path *)0);
}

void to_js(JS::JSValue & value, const Path & val)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    if (val.empty())
        value = v8::Null(isolate);
    return to_js(value, vector<PathElement>(val.begin(), val.end()));
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
        throw MLDB::Exception("function didn't return but no result");
    
    Handle<Value> exception = trycatch.Exception();
    String::Utf8Value exception_str(exception);
    
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

        where = MLDB::format("file '%s', line %d, column %d, source '%s': %s",
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
    if (!js_object_.IsNearDeath()) {
        ::fprintf(stderr, "JS object is not near death");
        std::terminate();
    }
    //v8::Isolate* isolate = v8::Isolate::GetCurrent();
    //js_object_->SetInternalField(0, v8::Undefined(isolate));
    //js_object_->SetInternalField(1, v8::Undefined(isolate));
    js_object_.Reset();
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
    v8::Isolate* isolate = v8::Isolate::GetCurrent();

    ExcAssert(js_object_.IsEmpty());

    if (handle->InternalFieldCount() == 0) {
        throw MLDB::Exception("InternalFieldCount is zero; are you forgetting "
                            "to use 'new'?");
    }

    ExcAssertEqual(handle->InternalFieldCount(), 2);

    handle->SetInternalField(0, v8::External::New(isolate, this));
    handle->SetInternalField(1, v8::External::New(isolate, context));

    js_object_.Reset(isolate, handle);
    registerForGarbageCollection();
}

/** Set this object up to be garbage collected once there are no more
    references to it in the javascript. */
void
JsObjectBase::
registerForGarbageCollection()
{
    js_object_.SetWeak(this, garbageCollectionCallback,
                       v8::WeakCallbackType::kParameter);
}
    
void
JsObjectBase::
NoConstructor(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    args.GetReturnValue().Set(args.This());
}


v8::Handle<v8::FunctionTemplate>
JsObjectBase::
CreateFunctionTemplate(const char * name,
                       v8::FunctionCallback constructor)
{
    using namespace v8;
        
    v8::Isolate* isolate = v8::Isolate::GetCurrent();

    v8::Handle<v8::FunctionTemplate> t
        = FunctionTemplate::New(isolate, constructor);

    t->InstanceTemplate()->SetInternalFieldCount(2);
    t->SetClassName(v8::String::NewFromUtf8(isolate, name));

    return t;
}

    
void
JsObjectBase::
garbageCollectionCallback(const v8::WeakCallbackInfo<JsObjectBase> & info)
{
    JsObjectBase * obj = info.GetParameter();
    delete obj;
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
        throw MLDB::Exception("attempt to retrieve JS context stack with nothing on it");
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
        throw MLDB::Exception("JS context stack consistency error");
    jsContextStack->pop_back();
}

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
    else if (val->IsArray()) {
        auto arrayPtr = v8::Array::Cast(*val);

        Json::Value result;

        for (int i=0; i<arrayPtr->Length(); ++i) {
            v8::Local<v8::Value> val = arrayPtr->Get(i);
            result[i] = fromJsForRestParams(val);
        }
        
        return result;
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
                throw MLDB::Exception("invalid length for pair extraction");
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
    else throw MLDB::Exception("couldn't convert JS value '%s' to REST parameters",
                             cstr(val).c_str());
}

} // namespace JS

} // namespace MLDB
