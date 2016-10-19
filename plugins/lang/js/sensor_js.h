/** sensor_js.h                                                   -*- C++ -*-
    Jeremy Barnes, 14 June 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    JS interface for sensors.
*/

#pragma once

#include "js_common.h"


namespace Datacratic {
namespace MLDB {

struct Sensor;


/*****************************************************************************/
/* SENSOR JS                                                                */
/*****************************************************************************/

struct SensorJS: public JsObjectBase {

    std::shared_ptr<Sensor> sensor;

    static v8::Handle<v8::Object>
    create(std::shared_ptr<Sensor> sensor, JsPluginContext * context);

    static Sensor *
    getShared(const v8::Handle<v8::Object> & val);

    static v8::Local<v8::FunctionTemplate>
    registerMe();

    static void
    status(const v8::FunctionCallbackInfo<v8::Value> & args);
    
    static void
    id(const v8::FunctionCallbackInfo<v8::Value> & args);

    static void
    type(const v8::FunctionCallbackInfo<v8::Value> & args);
    
    static void
    config(const v8::FunctionCallbackInfo<v8::Value> & args);

    static void
    latest(const v8::FunctionCallbackInfo<v8::Value> & args);
};

} // namespace MLDB
} // namespace Datacratic
