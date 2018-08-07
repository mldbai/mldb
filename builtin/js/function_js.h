/** function_js.h                                                   -*- C++ -*-
    Jeremy Barnes, 14 June 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    JS interface for functions.
*/

#pragma once

#include "js_common.h"



namespace MLDB {

struct Function;


/*****************************************************************************/
/* FUNCTION JS                                                                */
/*****************************************************************************/

struct FunctionJS: public JsObjectBase {

    std::shared_ptr<Function> function;

    static v8::Handle<v8::Object>
    create(std::shared_ptr<Function> function, JsPluginContext * context);

    static Function *
    getShared(const v8::Handle<v8::Object> & val);

    static v8::Local<v8::FunctionTemplate>
    registerMe();

    static void
    status(const v8::FunctionCallbackInfo<v8::Value> & args);
    
    static void
    details(const v8::FunctionCallbackInfo<v8::Value> & args);
    
    static void
    id(const v8::FunctionCallbackInfo<v8::Value> & args);

    static void
    type(const v8::FunctionCallbackInfo<v8::Value> & args);
    
    static void
    config(const v8::FunctionCallbackInfo<v8::Value> & args);

    static void
    call(const v8::FunctionCallbackInfo<v8::Value> & args);

    static void
    callJson(const v8::FunctionCallbackInfo<v8::Value> & args);
};

} // namespace MLDB

