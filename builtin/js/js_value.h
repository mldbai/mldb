/* js_value.h                                                      -*- C++ -*-
   Jeremy Barnes, 21 July 2010
   Copyright (c) 2010 mldb.ai inc.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Definition of what a javascript value is, in a way that we can forward
   declare without needing to include all of v8.
*/

#pragma once

#include "js_value_fwd.h"
#include "v8.h"
#include "mldb/arch/exception.h"


namespace MLDB {

namespace JS {

template<typename T>
v8::Local<T> toLocalChecked(const v8::MaybeLocal<T> & val)
{
    v8::Local<T> result;
    if (!val.ToLocal(&result)) {
        // TODO: throw JS exception instead?
        throw MLDB::Exception("JS Expected value was not returned; JS exception in process");
    }
    return result;
}

template<typename T>
T check(const v8::Maybe<T> & val)
{
    if (val.IsNothing()) {
        // TODO: throw JS exception instead?
        throw MLDB::Exception("JS expected check did not pass; JS exception in process");
    }
    return val.ToChecked();
}

// Some API methods change from X to Maybe<X>; this allows us to operate on them
// over the different versions of the API
template<typename T>
T check(const T & val)
{
    return val;
}


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

    template<typename T>
    JSValue(const v8::MaybeLocal<T> & val)
        : v8::Handle<v8::Value>(toLocalChecked(val))
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

