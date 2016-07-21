/** procedure_js.cc
    Jeremy Barnes, 14 June 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    JS interface for procedures.
*/

#include "procedure_js.h"
#include "mldb/core/procedure.h"
#include "id_js.h"


using namespace std;


namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* PROCEDURE JS                                                              */
/*****************************************************************************/

v8::Handle<v8::Object>
ProcedureJS::
create(std::shared_ptr<Procedure> procedure, JsThreadContext * context)
{
    auto obj = context->Procedure->GetFunction()->NewInstance();
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

    HandleScope scope;

    auto fntmpl = CreateFunctionTemplate("Procedure");
    auto prototmpl = fntmpl->PrototypeTemplate();

    prototmpl->Set(String::New("status"), FunctionTemplate::New(status));
    prototmpl->Set(String::New("id"), FunctionTemplate::New(id));
    prototmpl->Set(String::New("type"), FunctionTemplate::New(type));
    prototmpl->Set(String::New("config"), FunctionTemplate::New(config));
    prototmpl->Set(String::New("run"), FunctionTemplate::New(run));
        
    return scope.Close(fntmpl);
}


v8::Handle<v8::Value>
ProcedureJS::
status(const v8::Arguments & args)
{
    try {
        Procedure * procedure = getShared(args.This());
            
        return JS::toJS(jsonEncode(procedure->getStatus()));
    } HANDLE_JS_EXCEPTIONS;
}
    
v8::Handle<v8::Value>
ProcedureJS::
id(const v8::Arguments & args)
{
    try {
        Procedure * procedure = getShared(args.This());
            
        return JS::toJS(procedure->getId());
    } HANDLE_JS_EXCEPTIONS;
}
    
v8::Handle<v8::Value>
ProcedureJS::
type(const v8::Arguments & args)
{
    try {
        Procedure * procedure = getShared(args.This());
            
        return JS::toJS(procedure->getType());
    } HANDLE_JS_EXCEPTIONS;
}
    
v8::Handle<v8::Value>
ProcedureJS::
config(const v8::Arguments & args)
{
    try {
        Procedure * procedure = getShared(args.This());
        
        return JS::toJS(jsonEncode(procedure->getConfig()));
    } HANDLE_JS_EXCEPTIONS;
}

v8::Handle<v8::Value>
ProcedureJS::
run(const v8::Arguments & args)
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
        
        return JS::toJS(jsonEncode(result));

    } HANDLE_JS_EXCEPTIONS;
}

} // namespace MLDB
} // namespace Datacratic
