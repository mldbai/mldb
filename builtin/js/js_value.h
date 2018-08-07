/* js_value.h                                                      -*- C++ -*-
   Jeremy Barnes, 21 July 2010
   Copyright (c) 2010 mldb.ai inc.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Definition of what a javascript value is, in a way that we can forward
   declare without needing to include all of v8.
*/

#pragma once

#include "js_value_fwd.h"
#include "mldb/ext/v8-cross-build-output/include/v8.h"

namespace MLDB {

namespace JS {


/*****************************************************************************/
/* JSVALUE                                                                   */
/*****************************************************************************/

/** Opaque type used to represent a Javascript value
*/

struct JSValue : public v8::Handle<v8::Value> {
    JSValue()
    {
    }

    template<typename T>
    JSValue(const v8::Handle<T> & val)
        : v8::Handle<v8::Value>(val)
    {
    }

    operator v8::Handle<v8::Object>() const;
};


/*****************************************************************************/
/* JSVALUE                                                                   */
/*****************************************************************************/

/** Opaque type used to represent a Javascript object.  Convertible to a
    Javscript value.
*/

struct JSObject : public v8::Handle<v8::Object> {
    JSObject()
    {
    }

    template<typename T>
    JSObject(const v8::Handle<T> & val)
        : v8::Handle<v8::Object>(val)
    {
    }

    void initialize();
    
    void add(const std::string & key, const std::string & value);
    void add(const std::string & key, const JSValue & value);
};

} // namespace JS

} // namespace MLDB

