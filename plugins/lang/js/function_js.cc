/** function_js.cc
    Jeremy Barnes, 14 June 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    JS interface for functions.
*/

#include "function_js.h"
#include "mldb/core/function.h"
#include "mldb/types/js/id_js.h"


using namespace std;


namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* FUNCTION JS                                                                */
/*****************************************************************************/

v8::Handle<v8::Object>
FunctionJS::
create(std::shared_ptr<Function> function, JsPluginContext * context)
{
    auto obj = context->Function->GetFunction()->NewInstance();
    auto * wrapped = new FunctionJS();
    wrapped->function = function;
    wrapped->wrap(obj, context);
    return obj;
}

Function *
FunctionJS::
getShared(const v8::Handle<v8::Object> & val)
{
    return reinterpret_cast<FunctionJS *>
        (v8::Handle<v8::External>::Cast
         (val->GetInternalField(0))->Value())->function.get();
}

v8::Local<v8::FunctionTemplate>
FunctionJS::
registerMe()
{
    using namespace v8;

    HandleScope scope;

    auto fntmpl = CreateFunctionTemplate("Function");
    auto prototmpl = fntmpl->PrototypeTemplate();

    prototmpl->Set(String::New("status"), FunctionTemplate::New(status));
    prototmpl->Set(String::New("details"), FunctionTemplate::New(details));
    prototmpl->Set(String::New("id"), FunctionTemplate::New(id));
    prototmpl->Set(String::New("type"), FunctionTemplate::New(type));
    prototmpl->Set(String::New("config"), FunctionTemplate::New(config));
    prototmpl->Set(String::New("call"), FunctionTemplate::New(call));
        
    return scope.Close(fntmpl);
}


v8::Handle<v8::Value>
FunctionJS::
status(const v8::Arguments & args)
{
    try {
        Function * function = getShared(args.This());
            
        return JS::toJS(jsonEncode(function->getStatus()));
    } HANDLE_JS_EXCEPTIONS;
}
    
v8::Handle<v8::Value>
FunctionJS::
details(const v8::Arguments & args)
{
    try {
        Function * function = getShared(args.This());
            
        return JS::toJS(jsonEncode(function->getDetails()));
    } HANDLE_JS_EXCEPTIONS;
}

v8::Handle<v8::Value>
FunctionJS::
id(const v8::Arguments & args)
{
    try {
        Function * function = getShared(args.This());
            
        return JS::toJS(function->getId());
    } HANDLE_JS_EXCEPTIONS;
}
    
v8::Handle<v8::Value>
FunctionJS::
type(const v8::Arguments & args)
{
    try {
        Function * function = getShared(args.This());
            
        return JS::toJS(function->getType());
    } HANDLE_JS_EXCEPTIONS;
}
    
v8::Handle<v8::Value>
FunctionJS::
config(const v8::Arguments & args)
{
    try {
        Function * function = getShared(args.This());
        
        return JS::toJS(jsonEncode(function->getConfig()));
    } HANDLE_JS_EXCEPTIONS;
}

v8::Handle<v8::Value>
FunctionJS::
call(const v8::Arguments & args)
{
    try {
        JsContextScope scope(args.This());
        
        Function * function = getShared(args.This());
        auto input
            = JS::getArg<std::map<Utf8String, ExpressionValue> >(args, 0, "config");

        auto result = function->call(input);
        
        return JS::toJS(jsonEncode(result));

    } HANDLE_JS_EXCEPTIONS;
}

} // namespace MLDB
} // namespace Datacratic
