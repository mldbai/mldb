/** capture_stream.cc
    Jeremy Barnes, 17 August 2018
    Copyright (c) 2018 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2018 mldb.ai inc. All rights reserved.

    Functionality to capture Python streams.
*/

// Original file:
//
// Copyright (C) 2011 Mateusz Loskot <mateusz@loskot.net>
// Distributed under the Boost Software License, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)
//
// Blog article: http://mateusz.loskot.net/?p=2819

#include "capture_stream.h"
#include <functional>
#include <iostream>
#include <string>
#include <Python.h>
#include "python_interpreter.h"
#include "mldb/arch/exception.h"

using namespace std;


namespace MLDB {

struct Stdstream
{
    PyObject_HEAD
    stdstream_write_type write;
};

PyObject* Stdstream_write(PyObject* self, PyObject* args)
{
    auto token = assertGilAlreadyHeld();

    std::size_t written(0);
    Stdstream* selfimpl = reinterpret_cast<Stdstream*>(self);
    if (selfimpl->write)
    {
        char* data;
        if (!PyArg_ParseTuple(args, "s", &data))
            return 0;

        std::string str(data);
        selfimpl->write(*token, str);
        written = str.size();
    }
    return PyLong_FromSize_t(written);
}

PyObject* Stdstream_flush(PyObject* self, PyObject* args)
{
    // no-op
    return Py_BuildValue("");
}

// This will be reliably initialized when the module is loaded as a shared library,
// unlike the PyTypeObject which is sometimes initialized AFTER we attempt to register
// it with the Python runtime.
static PyTypeObject * initializedStdstreamType = nullptr;

PyMODINIT_FUNC PyInit_streamcapture(void)
{
    static PyMethodDef Stdstream_methods[] =
    {
        {"write", Stdstream_write, METH_VARARGS, "sys.stdstream.write"},
        {"flush", Stdstream_flush, METH_VARARGS, "sys.stdstream.flush"},
        {0, 0, 0, 0} // sentinel
    };

    static PyTypeObject StdstreamType =
    {
        PyVarObject_HEAD_INIT(0, 0)
        "streamcapture.StdstreamType",     /* tp_name */
        sizeof(Stdstream),       /* tp_basicsize */
        0,                    /* tp_itemsize */
        0,                    /* tp_dealloc */
        0,                    /* tp_print */
        0,                    /* tp_getattr */
        0,                    /* tp_setattr */
        0,                    /* tp_reserved */
        0,                    /* tp_repr */
        0,                    /* tp_as_number */
        0,                    /* tp_as_sequence */
        0,                    /* tp_as_mapping */
        0,                    /* tp_hash  */
        0,                    /* tp_call */
        0,                    /* tp_str */
        0,                    /* tp_getattro */
        0,                    /* tp_setattro */
        0,                    /* tp_as_buffer */
        Py_TPFLAGS_DEFAULT,   /* tp_flags */
        "streamcapture.Stdstream objects", /* tp_doc */
        0,                    /* tp_traverse */
        0,                    /* tp_clear */
        0,                    /* tp_richcompare */
        0,                    /* tp_weaklistoffset */
        0,                    /* tp_iter */
        0,                    /* tp_iternext */
        Stdstream_methods,    /* tp_methods */
        0,                    /* tp_members */
        0,                    /* tp_getset */
        0,                    /* tp_base */
        0,                    /* tp_dict */
        0,                    /* tp_descr_get */
        0,                    /* tp_descr_set */
        0,                    /* tp_dictoffset */
        0,                    /* tp_init */
        0,                    /* tp_alloc */
        0,                    /* tp_new */
    };

    static PyModuleDef streamcapturemodule =
    {
        PyModuleDef_HEAD_INIT,
        "streamcapture", 0, -1, 0,
    };

    StdstreamType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&StdstreamType) < 0) {
        cerr << "type not ready" << endl;
        abort();
    }

    PyObject* m = PyModule_Create(&streamcapturemodule);
    if (m)
    {
        Py_INCREF(&StdstreamType);
        PyModule_AddObject(m, "Stdstream", reinterpret_cast<PyObject*>(&StdstreamType));
    }

    initializedStdstreamType = &StdstreamType;
    
    return m;
}

namespace {

RegisterPythonInitializer registerMe([] (auto & thr) { PyInit_streamcapture(); });

} // file scope

std::shared_ptr<const void>
setStdStream(const EnterThreadToken & token,
             stdstream_write_type write,
             const std::string & streamName)
{
    PyObject * oldStream = PySys_GetObject(streamName.c_str()); // borrowed
    assert(initializedStdstreamType);
    PyObject * newStream = initializedStdstreamType->tp_new(initializedStdstreamType, 0, 0);
    Stdstream* impl = reinterpret_cast<Stdstream*>(newStream);
    impl->write = std::move(write);
    
    auto resetStream = [=] (const void *)
        {
            int err = PySys_SetObject(streamName.c_str(), oldStream);
            if (err != 0) {
                cerr << "error resetting Python stream" << endl;
                abort();
            }
            Py_XDECREF(newStream);
        };

    int err = PySys_SetObject(streamName.c_str(), newStream);
    if (err != 0) {
        cerr << "error setting " << streamName << " stream" << endl;
        Py_XDECREF(newStream);
        throw Exception("error capturing Python stream " + streamName);
    }

    static const char * NOT_NULL = "stdstream";
    return std::shared_ptr<const void>(NOT_NULL, resetStream);
}

} // namespace MLDB
