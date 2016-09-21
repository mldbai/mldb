/** sensor_js.cc
    Jeremy Barnes, 14 June 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    JS interface for sensors.
*/

#include "sensor_js.h"
#include "mldb/core/sensor.h"
#include "mldb/sql/expression_value.h"

using namespace std;


namespace MLDB {


/*****************************************************************************/
/* SENSOR JS                                                                */
/*****************************************************************************/

v8::Handle<v8::Object>
SensorJS::
create(std::shared_ptr<Sensor> sensor, JsPluginContext * context)
{
    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    auto obj = context->Sensor.Get(isolate)->GetFunction()->NewInstance();
    auto * wrapped = new SensorJS();
    wrapped->sensor = sensor;
    wrapped->wrap(obj, context);
    return obj;
}

Sensor *
SensorJS::
getShared(const v8::Handle<v8::Object> & val)
{
    return reinterpret_cast<SensorJS *>
        (v8::Handle<v8::External>::Cast
         (val->GetInternalField(0))->Value())->sensor.get();
}

v8::Local<v8::FunctionTemplate>
SensorJS::
registerMe()
{
    using namespace v8;

    v8::Isolate* isolate = v8::Isolate::GetCurrent();
    EscapableHandleScope scope(isolate);

    auto fntmpl = CreateFunctionTemplate("Sensor");
    auto prototmpl = fntmpl->PrototypeTemplate();

    prototmpl->Set(String::NewFromUtf8(isolate, "status"),
                   FunctionTemplate::New(isolate, status));
    prototmpl->Set(String::NewFromUtf8(isolate, "id"),
                   FunctionTemplate::New(isolate, id));
    prototmpl->Set(String::NewFromUtf8(isolate, "type"),
                   FunctionTemplate::New(isolate, type));
    prototmpl->Set(String::NewFromUtf8(isolate, "config"),
                   FunctionTemplate::New(isolate, config));
    prototmpl->Set(String::NewFromUtf8(isolate, "latest"),
                   FunctionTemplate::New(isolate, latest));
    
    return scope.Escape(fntmpl);
}


void
SensorJS::
status(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    try {
        Sensor * sensor = getShared(args.This());
            
        args.GetReturnValue().Set(JS::toJS(jsonEncode(sensor->getStatus())));
    } HANDLE_JS_EXCEPTIONS(args);
}
    
void
SensorJS::
id(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    try {
        Sensor * sensor = getShared(args.This());
            
        args.GetReturnValue().Set(JS::toJS(sensor->getId()));
    } HANDLE_JS_EXCEPTIONS(args);
}
    
void
SensorJS::
type(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    try {
        Sensor * sensor = getShared(args.This());
            
        args.GetReturnValue().Set(JS::toJS(sensor->getType()));
    } HANDLE_JS_EXCEPTIONS(args);
}
    
void
SensorJS::
config(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    try {
        Sensor * sensor = getShared(args.This());
        
        args.GetReturnValue().Set(JS::toJS(jsonEncode(sensor->getConfig())));
    } HANDLE_JS_EXCEPTIONS(args);
}

void
SensorJS::
latest(const v8::FunctionCallbackInfo<v8::Value> & args)
{
    try {
        JsContextScope scope(args.This());
        
        Sensor * sensor = getShared(args.This());
        auto result = sensor->latest();
        
        args.GetReturnValue().Set(JS::toJS(result));

    } HANDLE_JS_EXCEPTIONS(args);
}

} // namespace MLDB
