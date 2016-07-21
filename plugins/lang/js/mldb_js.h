/** mldb_js.h                                                   -*- C++ -*-
    Jeremy Barnes, 14 June 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    JS interface for mldb.
*/

#pragma once

#include "js_common.h"
#include "mldb/sql/cell_value.h"
#include "mldb/sql/expression_value.h"

namespace Datacratic {
namespace MLDB {

struct MldbServer;


/*****************************************************************************/
/* CELL VALUE JS                                                             */
/*****************************************************************************/

struct CellValueJS: public JsObjectBase {
    CellValue val;

    static v8::Handle<v8::Object>
    create(CellValue value, JsThreadContext * context);

    static CellValue &
    getShared(const v8::Handle<v8::Object> & val);

    static v8::Local<v8::FunctionTemplate>
    registerMe();

    struct Methods;
};


/*****************************************************************************/
/* EXPRESSION VALUE JS                                                       */
/*****************************************************************************/

struct ExpressionValueJS: public JsObjectBase {
    ExpressionValue val;

    static v8::Handle<v8::Object>
    create(ExpressionValue value, JsThreadContext * context);

    static ExpressionValue &
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
    create(std::shared_ptr<std::istream> stream, JsThreadContext * context);

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
           JsThreadContext * context);

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
    create(std::shared_ptr<MldbServer> mldb, JsThreadContext * context);

    static MldbServer *
    getShared(const v8::Handle<v8::Object> & val);

    static JsThreadContext * getContext(const v8::Handle<v8::Object> & val);

    static v8::Local<v8::ObjectTemplate>
    registerMe();

    static v8::Handle<v8::Value>
    New(const v8::Arguments & args);

    struct Methods;
};

} // namespace MLDB
} // namespace Datacratic
