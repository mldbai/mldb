/** callback.h                                 -*- C++ -*-
    Rémi Attab, 16 Jan 2013
    Copyright (c) 2013 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    std::function and std::function compatible python callbacks.

    Note that we can't have an automatic python-callback to std::function
    converter because we have to determine at runtime whether a callable python
    object matches the signature of the target std::function. Since python is a
    dymanic language, there is no realistic way of doing that.

    What this essentially means is that to wrap function that take callbacks we
    have to add another function on top which will convert the PyObject into a
    SafeCallback or UnsafeCallback object forwarding the call to the wrapped
    function. Might be possible to generate using template magic. I leave that
    to the reader as an exercise*.

    Not ideal, but that's life.
*/

#pragma once

#include <boost/python.hpp>
#include <iostream>

namespace MLDB {

namespace Python {

/******************************************************************************/
/* LOCK GIL                                                                   */
/******************************************************************************/

/** RAII object to acquire the python GIL.

    Required to safely handle python objects from multiple threads.
 */
struct LockGil
{

    LockGil() : state(PyGILState_Ensure()) {}
    ~LockGil() { 
        PyGILState_Release(state); 
    }

private:

    PyGILState_STATE state;

};


/******************************************************************************/
/* SAFE CALLBACK                                                              */
/******************************************************************************/

/** Python callback that can safely be called from multiple threads.

    Thread safety is accomplished by grabbing the GIL before every call.
 */
template<typename R, typename... Args>
struct SafeCallback
{
    SafeCallback(PyObject* callable) : callback(callable) {}

    template<typename... FnArgs>
    R operator() (FnArgs&&... args) const
    {
        // std::cout << "locking GIL: " << sizeof...(FnArgs) << std::endl;
        LockGil lock;
        try {
            return boost::python::call<R>(callback, std::forward<FnArgs>(args)...);
        }
        catch (...) {
            std::cout << "Python function threw an error." << std::endl;
            return 0;
        }
    }


private:
    PyObject* callback;
};


/******************************************************************************/
/* UNSAFE CALLBACK                                                            */
/******************************************************************************/

/** Python callback that is not thread safe.*/
template<typename R, typename... Args>
struct UnsafeCallback
{
    UnsafeCallback(PyObject* callable) : callback(callable) {}

    template<typename... FnArgs>
    R operator() (FnArgs&&... args) const
    {
        try {
            return boost::python::call<R>(callback, std::forward<FnArgs>(args)...);
        }
        catch (...) {
            std::cout << "Python function threw an error." << std::endl;
            return 0;
        }
    }

private:
    PyObject* callback;
};


} // namespace Python

} // namespace MLDB
