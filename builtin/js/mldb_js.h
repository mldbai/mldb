/** mldb_js.h                                                   -*- C++ -*-
    Jeremy Barnes, 14 June 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    JS interface for mldb.
*/

#pragma once

#include "js_common.h"
#include "mldb/sql/cell_value.h"


namespace MLDB {

struct MldbEngine;


/*****************************************************************************/
/* CELL VALUE JS                                                             */
/*****************************************************************************/

struct CellValueJS: public JsObjectBase {
    CellValueJS(CellValue val)
        : val(std::move(val))
    {
    }

    CellValue val;

    static v8::Handle<v8::Object>
    create(CellValue value, JsPluginContext * context);

    static CellValue &
    getShared(const v8::Handle<v8::Object> & val);

    static v8::Local<v8::FunctionTemplate>
    registerMe();

    struct Methods;
};


/*****************************************************************************/
/* STREAM JS                                                                 */
/*****************************************************************************/

/** Interface around an istream. */

struct StreamJS: public JsObjectBase {

    StreamJS(std::shared_ptr<std::istream> stream = nullptr)
        : stream(std::move(stream))
        {
        }

    std::shared_ptr<std::istream> stream;

    static v8::Handle<v8::Object>
    create(std::shared_ptr<std::istream> stream, JsPluginContext * context);

    static std::istream *
    getShared(const v8::Handle<v8::Object> & val);

    static v8::Local<v8::FunctionTemplate>
    registerMe();

    struct Methods;
};


/*****************************************************************************/
/* RANDOM NUMBER GENERATOR                                                   */
/*****************************************************************************/

struct RandomNumberGenerator;


/*****************************************************************************/
/* RANDOM NUMBER GENERATOR JS                                                */
/*****************************************************************************/

struct RandomNumberGeneratorJS: public JsObjectBase {

    RandomNumberGeneratorJS(std::shared_ptr<RandomNumberGenerator> randomNumberGenerator = nullptr)
        : randomNumberGenerator(std::move(randomNumberGenerator))
    {
    }

    std::shared_ptr<RandomNumberGenerator> randomNumberGenerator;

    static v8::Handle<v8::Object>
    create(std::shared_ptr<RandomNumberGenerator> randomNumberGenerator,
           JsPluginContext * context);

    static RandomNumberGenerator *
    getShared(const v8::Handle<v8::Object> & val);

    static v8::Local<v8::FunctionTemplate>
    registerMe();

    struct Methods;
};




/*****************************************************************************/
/* MLDB JS                                                                   */
/*****************************************************************************/

struct MldbJS: public JsObjectBase {

    MldbJS(std::shared_ptr<MldbEngine> mldb)
        : mldb(std::move(mldb))
    {
    }

    std::shared_ptr<MldbEngine> mldb;

    static v8::Handle<v8::Object>
    create(std::shared_ptr<MldbEngine> mldb, JsPluginContext * context);

    static MldbEngine *
    getShared(const v8::Handle<v8::Object> & val);

    static JsPluginContext * getContext(const v8::Handle<v8::Object> & val);

    static v8::Local<v8::ObjectTemplate>
    registerMe();

    static void
    New(const v8::FunctionCallbackInfo<v8::Value> & args);

    struct Methods;
};

} // namespace MLDB

