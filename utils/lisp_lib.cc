/* lisp_lib.h                                                        -*- C++ -*-
   Jeremy Barnes, 22 February 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "lisp_lib.h"
#include "lisp_predicate.h"
#include "lisp_visitor.h"
#include "safe_clamp.h"
#include <shared_mutex>
#include <map>

using namespace std;

namespace MLDB {
namespace Lisp {

namespace {

struct FunctionNamespace {
    Path ns;
    mutable std::shared_mutex mutex;
    std::map<PathElement, FunctionCompiler> functionCompilers;

    void addFunctionCompiler(PathElement name, FunctionCompiler compiler)
    {
        std::unique_lock guard { mutex };
        auto [it, inserted] = functionCompilers.emplace(std::move(name), std::move(compiler));
        if (!inserted) {
            throw MLDB::Exception("function compiler " + it->first.toUtf8String().rawString()
                                  + " already registered in namespace " + ns.toUtf8String().rawString());
        }
    }
};

std::shared_mutex lispNamespacesMutex;
std::map<PathElement, FunctionNamespace> lispNamespaces;

} // file scope

void addFunctionCompiler(PathElement ns, PathElement name, FunctionCompiler compiler)
{
    std::unique_lock guard { lispNamespacesMutex };
    auto it = lispNamespaces.find(ns);
    if (it == lispNamespaces.end()) {
        // Insert and set the namespace
        lispNamespaces[ns].ns = ns;
        it = lispNamespaces.find(ns);
    }
    it->second.addFunctionCompiler(std::move(name), std::move(compiler));
}

std::optional<FunctionCompiler>
tryLookupFunction(const PathElement & fn, const std::vector<PathElement> & importedNamespaces)
{
    std::shared_lock guard { lispNamespacesMutex };
    for (auto & n: importedNamespaces) {
        auto it = lispNamespaces.find(n);
        if (it == lispNamespaces.end()) {
            throw MLDB::Exception("Looking up function: unknown namespace " + n.toUtf8String());
        }
        const FunctionNamespace & ns = it->second;
        std::shared_lock guard { ns.mutex };
        auto it2 = ns.functionCompilers.find(fn);
        if (it2 != ns.functionCompilers.end()) {
            return it2->second;
        }
    }
    return std::nullopt;
}

FunctionCompiler
lookupFunction(const PathElement & fn,
               const std::vector<PathElement> & importedNamespaces)
{
    auto tried = tryLookupFunction(fn, importedNamespaces);
    if (!tried)
        throw MLDB::Exception("Couldn't find function " + fn.toUtf8String() + " in any namespace");
    return *tried;
}

#if 0
const FunctionCompiler & getFunctionCompiler(const PathElement & name)
{
    std::shared_lock guard { FunctionCompilersMutex };
    auto it = FunctionCompilers.find(name);
    if (it == FunctionCompilers.end()) {
        throw MLDB::Exception("function compiler " + name.toUtf8String().rawString() + " not registered");
    }
    return it->second;
}
#endif

} // namespace Lisp
} // namespace MLDB
