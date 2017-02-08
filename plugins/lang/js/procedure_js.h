/** procedure_js.h                                                   -*- C++ -*-
    Jeremy Barnes, 14 June 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    JS interface for procedures.
*/

#pragma once

#include "js_common.h"



namespace MLDB {

struct Procedure;


/*****************************************************************************/
/* PROCEDURE JS                                                              */
/*****************************************************************************/

struct ProcedureJS: public JsObjectBase {

    std::shared_ptr<Procedure> procedure;

    static v8::Handle<v8::Object>
    create(std::shared_ptr<Procedure> procedure, JsPluginContext * context);

    static Procedure *
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
    run(const v8::FunctionCallbackInfo<v8::Value> & args);
};

} // namespace MLDB

