/** dataset_js.h                                                   -*- C++ -*-
    Jeremy Barnes, 14 June 2015
    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    JS interface for datasets.
*/

#pragma once

#include "js_common.h"


namespace Datacratic {
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

    static v8::Handle<v8::Value>
    recordRow(const v8::Arguments & args);

    static v8::Handle<v8::Value>
    recordRows(const v8::Arguments & args);

    static v8::Handle<v8::Value>
    recordColumn(const v8::Arguments & args);

    static v8::Handle<v8::Value>
    recordColumns(const v8::Arguments & args);

    static v8::Handle<v8::Value>
    commit(const v8::Arguments & args);

    static v8::Handle<v8::Value>
    status(const v8::Arguments & args);
    
    static v8::Handle<v8::Value>
    id(const v8::Arguments & args);

    static v8::Handle<v8::Value>
    type(const v8::Arguments & args);
    
    static v8::Handle<v8::Value>
    config(const v8::Arguments & args);
    
    static v8::Handle<v8::Value>
    getColumnNames(const v8::Arguments & args);

    static v8::Handle<v8::Value>
    getTimestampRange(const v8::Arguments & args);
};

} // namespace MLDB
} // namespace Datacratic
