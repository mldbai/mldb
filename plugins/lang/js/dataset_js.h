/** dataset_js.h                                                   -*- C++ -*-
    Jeremy Barnes, 14 June 2015
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    JS interface for datasets.
*/

#pragma once

#include "js_common.h"



namespace MLDB {

struct Dataset;


/*****************************************************************************/
/* DATASET JS                                                                */
/*****************************************************************************/

struct DatasetJS: public JsObjectBase {

    std::shared_ptr<Dataset> dataset;

    static v8::Handle<v8::Object>
    create(std::shared_ptr<Dataset> dataset, JsPluginContext * context);

    static Dataset *
    getShared(const v8::Handle<v8::Object> & val);

    static v8::Local<v8::FunctionTemplate>
    registerMe();

    static void
    recordRow(const v8::FunctionCallbackInfo<v8::Value> & args);

    static void
    recordRows(const v8::FunctionCallbackInfo<v8::Value> & args);

    static void
    recordColumn(const v8::FunctionCallbackInfo<v8::Value> & args);

    static void
    recordColumns(const v8::FunctionCallbackInfo<v8::Value> & args);

    static void
    commit(const v8::FunctionCallbackInfo<v8::Value> & args);

    static void
    status(const v8::FunctionCallbackInfo<v8::Value> & args);
    
    static void
    id(const v8::FunctionCallbackInfo<v8::Value> & args);

    static void
    type(const v8::FunctionCallbackInfo<v8::Value> & args);
    
    static void
    config(const v8::FunctionCallbackInfo<v8::Value> & args);
    
    static void
    getColumnPaths(const v8::FunctionCallbackInfo<v8::Value> & args);

    static void
    getTimestampRange(const v8::FunctionCallbackInfo<v8::Value> & args);
};

} // namespace MLDB

