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

struct MldbServer;


/*****************************************************************************/
/* CELL VALUE JS                                                             */
/*****************************************************************************/

struct CellValueJS: public JsObjectBase {
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

    std::shared_ptr<MldbServer> mldb;

    static v8::Handle<v8::Object>
    create(std::shared_ptr<MldbServer> mldb, JsPluginContext * context);

    static MldbServer *
    getShared(const v8::Handle<v8::Object> & val);

    static JsPluginContext * getContext(const v8::Handle<v8::Object> & val);

    static v8::Local<v8::ObjectTemplate>
    registerMe();

    static void
    New(const v8::FunctionCallbackInfo<v8::Value> & args);

    struct Methods;
};

} // namespace MLDB

