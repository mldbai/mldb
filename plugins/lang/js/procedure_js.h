/** procedure_js.h                                                   -*- C++ -*-
    Jeremy Barnes, 14 June 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    JS interface for procedures.
*/

#pragma once

#include "js_common.h"


namespace Datacratic {
namespace MLDB {

struct Procedure;


/*****************************************************************************/
/* PROCEDURE JS                                                              */
/*****************************************************************************/

struct ProcedureJS: public JsObjectBase {

    std::shared_ptr<Procedure> procedure;

    static v8::Handle<v8::Object>
    create(std::shared_ptr<Procedure> procedure, JsThreadContext * context);

    static Procedure *
    getShared(const v8::Handle<v8::Object> & val);

    static v8::Local<v8::FunctionTemplate>
    registerMe();

    static v8::Handle<v8::Value>
    status(const v8::Arguments & args);
    
    static v8::Handle<v8::Value>
    id(const v8::Arguments & args);

    static v8::Handle<v8::Value>
    type(const v8::Arguments & args);
    
    static v8::Handle<v8::Value>
    config(const v8::Arguments & args);

    static v8::Handle<v8::Value>
    run(const v8::Arguments & args);
};

} // namespace MLDB
} // namespace Datacratic
