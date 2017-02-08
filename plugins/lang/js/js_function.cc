/* js_function.cc
   Jeremy Barnes, 12 June 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "js_common.h"
#include "mldb_js.h"
#include "mldb/arch/thread_specific.h"
#include "mldb/http/http_exception.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/sql/expression_value.h"
#include "mldb/sql/sql_expression.h"

#include <boost/algorithm/string.hpp>

using namespace std;


namespace MLDB {

struct JsFunctionData;

/** Data for a JS function for each thread. */
struct JsFunctionThreadData {
    JsFunctionThreadData()
        : isolate(0), data(0)
    {
    }

    bool initialized() const
    {
        return isolate;
    }

    JsIsolate * isolate;
    v8::Persistent<v8::Context> context;
    v8::Persistent<v8::Script> script;
    v8::Persistent<v8::Function> function;
    const JsFunctionData * data;

    void initialize(const JsFunctionData & data);

    ExpressionValue run(const std::vector<ExpressionValue> & args,
                        const SqlRowScope & context) const;
};

struct JsFunctionData {
    MldbServer * server;
    ThreadSpecificInstanceInfo<JsFunctionThreadData, void> threadInfo;
    Utf8String scriptSource;
    std::string filenameForErrorMessages;
    std::vector<std::string> params;
    std::shared_ptr<JsPluginContext> context;
};

void
JsFunctionThreadData::
initialize(const JsFunctionData & data)
{
    using namespace v8;

    if (isolate)
        return;

    isolate = JsIsolate::getIsolateForMyThread();
    this->data = &data;

    //v8::Locker locker(this->isolate->isolate);
    v8::Isolate::Scope isolate(this->isolate->isolate);

    HandleScope handle_scope(this->isolate->isolate);

    // Create a new context.
    this->context.Reset(this->isolate->isolate,
                        Context::New(this->isolate->isolate));

    // Enter the created context for compiling and
    // running the hello world script. 
    Context::Scope context_scope(this->context.Get(this->isolate->isolate));

    // Add the mldb object to the context
    auto mldb = MldbJS::registerMe()->NewInstance();
    mldb->SetInternalField(0, v8::External::New(this->isolate->isolate,
                                                data.server));
    mldb->SetInternalField(1, v8::External::New(this->isolate->isolate,
                                                data.context.get()));
    this->context.Get(this->isolate->isolate)
        ->Global()
        ->Set(String::NewFromUtf8(this->isolate->isolate,
                                 "mldb"), mldb);
    
    Utf8String jsFunctionSource = data.scriptSource;

    // Create a string containing the JavaScript source code.
    Handle<String> source
        = String::NewFromUtf8(this->isolate->isolate,
                              jsFunctionSource.rawString().c_str());

    TryCatch trycatch;
    //trycatch.SetVerbose(true);

    // This is equivalent to fntocall = new Function('arg1', ..., 'script');
    auto function
        = this->context.Get(this->isolate->isolate)
        ->Global()
        ->Get(v8::String::NewFromUtf8(this->isolate->isolate, "Function"))
        .As<v8::Object>();
    
    std::vector<v8::Handle<v8::Value> > argv;
    for (unsigned i = 0;  i != data.params.size();  ++i)
        argv.push_back(v8::String::NewFromUtf8(this->isolate->isolate,
                                               data.params[i].c_str()));
    argv.push_back(source);

    v8::Local<v8::Function> compiled 
        = function->CallAsConstructor(argv.size(), &argv[0])
        .As<v8::Function>();

    if (compiled.IsEmpty()) {  
        auto rep = convertException(trycatch, "Compiling jseval script");
        MLDB_TRACE_EXCEPTIONS(false);
        throw HttpReturnException(400, "Exception compiling jseval script",
                                  "exception", rep,
                                  "scriptSource", data.scriptSource,
                                  "provenance", data.filenameForErrorMessages);
    }

    this->function.Reset(this->isolate->isolate, compiled);
}

ExpressionValue
JsFunctionThreadData::
run(const std::vector<ExpressionValue> & args,
    const SqlRowScope & context) const
{
    using namespace v8;

    ExcAssert(initialized());

    //v8::Locker locker(this->isolate->isolate);
    v8::Isolate::Scope isolate(this->isolate->isolate);

    HandleScope handle_scope(this->isolate->isolate);

    // Enter the created context for compiling and
    // running the hello world script. 
    Context::Scope context_scope(this->context.Get(this->isolate->isolate));

    Date ts = Date::negativeInfinity();

    std::vector<v8::Handle<v8::Value> > argv;
    for (unsigned i = 2;  i < args.size();  ++i) {
        if (args[i].isRow()) {
            RowValue row;
            args[i].appendToRow(Path(), row);
            argv.push_back(JS::toJS(row));
        }
        else {
            argv.push_back(JS::toJS(args[i].getAtom()));
        }
        ts.setMax(args[i].getEffectiveTimestamp());
    }

    TryCatch trycatch;
    //trycatch.SetVerbose(true);

    auto result = this->function.Get(this->isolate->isolate)
        ->Call(this->context.Get(this->isolate->isolate)->Global(),
               argv.size(), &argv[0]);
    
    if (result.IsEmpty()) {  
        auto rep = convertException(trycatch, "Running jseval script");
        MLDB_TRACE_EXCEPTIONS(false);
        throw HttpReturnException(400, "Exception running jseval script",
                                  "exception", rep,
                                  "scriptSource", data->scriptSource,
                                  "provenance", data->filenameForErrorMessages,
                                  "arguments", args);
    }

    if (result->IsUndefined()) {
        return ExpressionValue::null(Date::notADate());
    }
    else if (result->IsString() || result->IsNumber() || result->IsNull() || result->IsDate()) {
        CellValue res = JS::fromJS(result);

        return ExpressionValue(res, ts);
    }
    else if (result->IsObject()) {
        std::map<Utf8String, CellValue> cols = JS::fromJS(result);

        std::vector<std::tuple<PathElement, ExpressionValue> > row;
        row.reserve(cols.size());
        for (auto & c: cols) {
            row.emplace_back(c.first, ExpressionValue(std::move(c.second),
                                                      ts));
        }
        return ExpressionValue(std::move(row));
    }
    else {
        throw HttpReturnException(400, "Don't understand expression");
    }
}

ExpressionValue
runJsFunction(const std::vector<ExpressionValue> & args,
              const SqlRowScope & context,
              const shared_ptr<JsFunctionData> & data)
{
    // 1.  Find the JS function for this isolate
    JsFunctionThreadData * threadData = data->threadInfo.get();

    if (!threadData->initialized())
        threadData->initialize(*data);

    // 2.  Run the function
    return threadData->run(args, context);
}

BoundFunction bindJsEval(const Utf8String & name,
                         const std::vector<BoundSqlExpression> & args,
                         const SqlBindingScope & context)
{
    if (args.size() < 2)
        throw HttpReturnException(400, "jseval expected at least 2 arguments, got " + to_string(args.size()));

    // 1.  Get the constant source value
    Utf8String scriptSource = args[0].constantValue().toUtf8String();

    // 2.  Create the runner, including test compiling the script
    auto runner = std::make_shared<JsFunctionData>();
    runner->server = context.getMldbServer();
    runner->scriptSource = scriptSource;
    runner->filenameForErrorMessages = "<<eval>>";
    runner->context.reset(new JsPluginContext(name, runner->server,
                                              nullptr /* no plugin context */));
                          
    string params = args[1].constantValue().toString();
    boost::split(runner->params, params,
                 boost::is_any_of(","));
    
    // 3.  We don't know what it returns; TODO: allow it to be specified
    auto info = std::make_shared<AnyValueInfo>();

    // 4.  Do the binding
    auto fn =  [=] (const std::vector<ExpressionValue> & args,
                    const SqlRowScope & context) -> ExpressionValue
        {
            return runJsFunction(args, context, runner);
        };

    // 5.  Return it
    return { std::move(fn), std::move(info) };
}

RegisterFunction registerJs(Utf8String("jseval"), bindJsEval);


} // namespace MLDB
