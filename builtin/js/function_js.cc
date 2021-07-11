/** function_js.cc
    Jeremy Barnes, 14 June 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    JS interface for functions.
*/

#include "function_js.h"
#include "mldb/core/function.h"


using namespace std;



namespace MLDB {


/*****************************************************************************/
/* FUNCTION JS                                                                */
/*****************************************************************************/

v8::Handle<v8::Object>
FunctionJS::
create(std::shared_ptr<Function> function, JsPluginContext * pluginContext)
{
    return doCreateWrapper<FunctionJS>(std::move(function), pluginContext, pluginContext->Function);
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

    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    EscapableHandleScope scope(isolate);

    auto fntmpl = CreateFunctionTemplate("Function");
    auto prototmpl = fntmpl->PrototypeTemplate();

#define ADD_METHOD(name) JS::addMethod(isolate, prototmpl, #name, FunctionTemplate::New(isolate, name))
    ADD_METHOD(status);
    ADD_METHOD(details);
    ADD_METHOD(id);
    ADD_METHOD(type);
    ADD_METHOD(config);
    ADD_METHOD(call);
    ADD_METHOD(callJson);
    
    return scope.Escape(fntmpl);
}


void
FunctionJS::
status(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    try {
        Function * function = getShared(args.This());
            
        args.GetReturnValue().Set(JS::toJS(jsonEncode(function->getStatus())));
    } HANDLE_JS_EXCEPTIONS(args);
}
    
void
FunctionJS::
details(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    try {
        Function * function = getShared(args.This());
            
        args.GetReturnValue().Set(JS::toJS(jsonEncode(function->getDetails())));
    } HANDLE_JS_EXCEPTIONS(args);
}

void
FunctionJS::
id(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    try {
        Function * function = getShared(args.This());
            
        args.GetReturnValue().Set(JS::toJS(function->getId()));
    } HANDLE_JS_EXCEPTIONS(args);
}
    
void
FunctionJS::
type(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    try {
        Function * function = getShared(args.This());
            
        args.GetReturnValue().Set(JS::toJS(function->getType()));
    } HANDLE_JS_EXCEPTIONS(args);
}
    
void
FunctionJS::
config(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    try {
        Function * function = getShared(args.This());
        
        args.GetReturnValue().Set(JS::toJS(jsonEncode(function->getConfig())));
    } HANDLE_JS_EXCEPTIONS(args);
}

void
FunctionJS::
call(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    try {
        JsContextScope scope(args.This());
        
        Function * function = getShared(args.This());
        auto input
            = JS::getArg<std::map<Utf8String, ExpressionValue> >(args, 0, "input");

        // Convert to a row, which can then be converted to an ExpressionValue
        StructValue row;
        row.reserve(input.size());
        for (auto & r: input) {
            row.emplace_back(PathElement(std::move(r.first)), std::move(r.second));
        }

        auto result = function->call(std::move(row));
        
        args.GetReturnValue().Set(JS::toJS(jsonEncode(result)));

    } HANDLE_JS_EXCEPTIONS(args);
}

void
FunctionJS::
callJson(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    try {
        JsContextScope scope(args.This());
        
        Function * function = getShared(args.This());

        Json::Value json = JS::getArg<Json::Value>(args, 0, "inputJson");
        StructuredJsonParsingContext context(json);
        ExpressionValue input
            = ExpressionValue::parseJson(context, Date::now());
        
        auto result = function->call(std::move(input));
        
        args.GetReturnValue().Set(JS::toJS(result.extractJson()));
        
    } HANDLE_JS_EXCEPTIONS(args);
}

} // namespace MLDB

