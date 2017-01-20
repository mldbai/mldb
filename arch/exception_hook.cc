// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* exception_hook.cc
   Jeremy Barnes, 7 February 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   

   
   Object file to install the exception tracer.
*/

#include <iostream>
#include <dlfcn.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <errno.h>
#include <stdlib.h>

#include "exception_handler.h"
#include "exception_hook.h"

using namespace std;

namespace MLDB {

/** Hook for the function to call when we throw an exception.  The first
    argument is a pointer to the object thrown; the second is the type
    info block for the thrown object.

    Starts off at null which means that no hook is installed.
*/
bool (*exception_tracer) (void *, const std::type_info *) = 0;

namespace {

/** The signature of __cxa_throw. */
typedef void (*cxa_throw_type) (void *, std::type_info *, void (*) (void *));

void * handle = 0;
cxa_throw_type old_handler = 0;
bool done_init = false;

struct Init {

    Init()
    {
        //cerr << "exception hook init" << endl;

        if (done_init) return;

        /* Find the __cxa_throw function from libstdc++.so.  This is the
           original function that would have been used if we hadn't overridden
           it here.
        */
        
        /** Fist, we dynamically load the C++ standard library */
        handle = dlopen(0, RTLD_LAZY | RTLD_GLOBAL);
        if (!handle) {
            cerr << "in __cxa_throw override:" << endl;
            cerr << "error loading libstdc++.so.6: " << dlerror() << endl;
            abort();
        }
        
        /** Secondly, we lookup the symbol __cxa_throw */
        old_handler = (cxa_throw_type) dlvsym(handle, "__cxa_throw", "CXXABI_1.3");
        
        if (!old_handler) {
            cerr << "in __cxa_throw override:" << endl;
            cerr << "error finding __cxa_throw: " << dlerror() << endl;
            abort();
        }
        
        /** Close the dynamic library. */
        dlclose(handle);

        done_init = true;
    }
} init;

} // file scope

} // namespace MLDB

/** Our overridden version of __cxa_throw.  The first argument is the
    object that was thrown.  The second is the type info block for the
    object.  The third is the destructor for the object that should be
    called with thrown_object as an argument when we are finished with
    the exception object.
*/
extern "C"
void
__cxa_throw (void *thrown_object, std::type_info *tinfo,
             void (*destructor) (void *) )
{
    using namespace MLDB;

    //cerr << "exception was thrown" << endl;
    //cerr << "exception_tracer = " << exception_tracer << endl;
    
    /** If we have installed an exception tracing hook, we follow it here. */
    if (!exception_tracer || !exception_tracer(thrown_object, tinfo)) {
        MLDB::default_exception_tracer(thrown_object, tinfo);
    }

    if (!MLDB::done_init) {
        MLDB::Init();
    }

    /** Now we finish by calling the old handler which will propegate the
        exception as usual. */
    MLDB::old_handler(thrown_object, tinfo, destructor);

    /** This should never happen, as __cxa_throw is a noreturn() function.
        If for some reason it returns, we abort out. */
    cerr << "got back from __cxa_throw; error" << endl;
    abort();
}
