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
create(std::shared_ptr<Function> function, JsPluginContext * context)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    auto obj = context->Function.Get(isolate)->GetFunction()->NewInstance();
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

    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    EscapableHandleScope scope(isolate);

    auto fntmpl = CreateFunctionTemplate("Function");
    auto prototmpl = fntmpl->PrototypeTemplate();

    prototmpl->Set(String::NewFromUtf8(isolate, "status"),
                   FunctionTemplate::New(isolate, status));
    prototmpl->Set(String::NewFromUtf8(isolate, "details"),
                   FunctionTemplate::New(isolate, details));
    prototmpl->Set(String::NewFromUtf8(isolate, "id"),
                   FunctionTemplate::New(isolate, id));
    prototmpl->Set(String::NewFromUtf8(isolate, "type"),
                   FunctionTemplate::New(isolate, type));
    prototmpl->Set(String::NewFromUtf8(isolate, "config"),
                   FunctionTemplate::New(isolate, config));
    prototmpl->Set(String::NewFromUtf8(isolate, "call"),
                   FunctionTemplate::New(isolate, call));
    prototmpl->Set(String::NewFromUtf8(isolate, "callJson"),
                   FunctionTemplate::New(isolate, callJson));
    
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

