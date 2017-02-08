/** procedure_js.cc
    Jeremy Barnes, 14 June 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    JS interface for procedures.
*/

#include "procedure_js.h"
#include "mldb/core/procedure.h"


using namespace std;



namespace MLDB {


/*****************************************************************************/
/* PROCEDURE JS                                                              */
/*****************************************************************************/

v8::Handle<v8::Object>
ProcedureJS::
create(std::shared_ptr<Procedure> procedure, JsPluginContext * context)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    auto obj = context->Procedure.Get(isolate)->GetFunction()->NewInstance();
    auto * wrapped = new ProcedureJS();
    wrapped->procedure = procedure;
    wrapped->wrap(obj, context);
    return obj;
}

Procedure *
ProcedureJS::
getShared(const v8::Handle<v8::Object> & val)
{
    return reinterpret_cast<ProcedureJS *>
        (v8::Handle<v8::External>::Cast
         (val->GetInternalField(0))->Value())->procedure.get();
}

v8::Local<v8::FunctionTemplate>
ProcedureJS::
registerMe()
{
    using namespace v8;

    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    EscapableHandleScope scope(isolate);

    auto fntmpl = CreateFunctionTemplate("Procedure");
    auto prototmpl = fntmpl->PrototypeTemplate();

    prototmpl->Set(String::NewFromUtf8(isolate, "status"),
                   FunctionTemplate::New(isolate, status));
    prototmpl->Set(String::NewFromUtf8(isolate, "id"),
                   FunctionTemplate::New(isolate, id));
    prototmpl->Set(String::NewFromUtf8(isolate, "type"),
                   FunctionTemplate::New(isolate, type));
    prototmpl->Set(String::NewFromUtf8(isolate, "config"),
                   FunctionTemplate::New(isolate, config));
    prototmpl->Set(String::NewFromUtf8(isolate, "run"),
                   FunctionTemplate::New(isolate, run));
        
    return scope.Escape(fntmpl);
}


void
ProcedureJS::
status(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    try {
        Procedure * procedure = getShared(args.This());
            
        args.GetReturnValue().Set(JS::toJS(jsonEncode(procedure->getStatus())));
    } HANDLE_JS_EXCEPTIONS(args);
}
    
void
ProcedureJS::
id(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    try {
        Procedure * procedure = getShared(args.This());
            
        args.GetReturnValue().Set(JS::toJS(procedure->getId()));
    } HANDLE_JS_EXCEPTIONS(args);
}
    
void
ProcedureJS::
type(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    try {
        Procedure * procedure = getShared(args.This());
            
        args.GetReturnValue().Set(JS::toJS(procedure->getType()));
    } HANDLE_JS_EXCEPTIONS(args);
}
    
void
ProcedureJS::
config(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    try {
        Procedure * procedure = getShared(args.This());
        
        args.GetReturnValue().Set(JS::toJS(jsonEncode(procedure->getConfig())));
    } HANDLE_JS_EXCEPTIONS(args);
}

void
ProcedureJS::
run(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    try {
        Procedure * procedure = getShared(args.This());
        Json::Value configJson = JS::getArg<Json::Value>(args, 0, "config");
        ProcedureRunConfig config = jsonDecode<ProcedureRunConfig>(configJson);

        auto onProgress = [&] (const Json::Value & progress)
            {
                return true;
            };

        auto result = procedure->run(config, onProgress);
        
        args.GetReturnValue().Set(JS::toJS(jsonEncode(result)));

    } HANDLE_JS_EXCEPTIONS(args);
}

} // namespace MLDB

