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
create(std::shared_ptr<Sensor> sensor, JsPluginContext * pluginContext)
{
    return doCreateWrapper<SensorJS>(std::move(sensor), pluginContext, pluginContext->Sensor);
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

#define ADD_METHOD(name) JS::addMethod(isolate, prototmpl, #name, FunctionTemplate::New(isolate, name))
    ADD_METHOD(status);
    ADD_METHOD(id);
    ADD_METHOD(type);
    ADD_METHOD(config);
    ADD_METHOD(latest);

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
