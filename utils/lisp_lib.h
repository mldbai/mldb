/* lisp_lib.h                                                        -*- C++ -*-
   Jeremy Barnes, 22 February 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "lisp.h"
#include <optional>

namespace MLDB {
namespace Lisp {

// TODO: hide this implementation detail
void addFunctionCompiler(PathElement ns, PathElement name, FunctionCompiler compiler);

std::optional<FunctionCompiler>
tryLookupFunction(const PathElement & fn, const std::vector<PathElement> & importedNamespaces);
FunctionCompiler lookupFunction(const PathElement & fn, const std::vector<PathElement> & importedNamespaces);

struct RegisterFunctionCompiler {
    RegisterFunctionCompiler(const char * ns, std::vector<std::string> names, FunctionCompiler fn)
    {
        for (auto name: names) {
            addFunctionCompiler(ns, name, fn);
        }
    };
};


#define DEFINE_LISP_FUNCTION_COMPILER(identifier, ns, name, ...) \
static inline CompiledExpression compile_ ## ns ## _ ## identifier(const List & expr, const CompilationScope & scope); \
static const RegisterFunctionCompiler register ## identifier(#ns, {name, __VA_ARGS__}, &compile_ ## ns ## _ ## identifier); \
CompiledExpression compile_ ## ns ## _ ## identifier(const List & expr, const CompilationScope & scope)

#define DEFINE_LISP_FUNCTION(identifier, ns, name, ...) \
Value exec_ ## ns ## _ ## identifier(ParsingContext & context); \
DEFINE_LISP_FUNCTION_COMPILER(identifier, ns, name, __VA_ARGS__) { return { exec_ ## ns ## identifier}, context }; } \
Value exec_ ## identifier(ParsingContext & context)

} // namespace Lisp
} // namespace MLDB
