/* lisp.cc                                                  -*- C++ -*-
   Jeremy Barnes, 22 February 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "lisp.h"
#include "lisp_lib.h"
#include "mldb/types/json_parsing.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/base/scope.h"
#include "mldb/types/any_impl.h"
#include "mldb/base/parse_context.h"

using namespace std;

namespace MLDB {
namespace Lisp {


/*******************************************************************************/
/* LISP EXECUTION SCOPE                                                        */
/*******************************************************************************/

ExecutionScope::
ExecutionScope(Context & context)
    : context_(&context)
{
    ExcAssert(context_);
}


/*******************************************************************************/
/* LISP COMPILATION SCOPE                                                      */
/*******************************************************************************/

CompilationScope::
CompilationScope(Context & lcontext)
    : context_(&lcontext)
{
}

CompilationScope::
CompilationScope()
{
}

CompilationScope::
CompilationScope(CompilationScope & parent)
    : context_(parent.context_)
{

}

CompiledExpression
CompilationScope::
compile(const Value & program) const
{
    ExcAssert(context_);
    program.verifyContext(context_);

    if (program.is<List>()) {
        const List & list = program.as<List>();
        if (!list.empty()) {
#if 0
            std::vector<CreateExecutionScope> scopeCreators;
            std::vector<Executor> argExecutors;

            for (auto & item: list) {
                auto [itemExecutor, createItemScope] = compile(item);
                scopeCreators.emplace_back(std::move(createItemScope));
                argExecutors.emplace_back(std::move(itemExecutor));
            }

            //auto createScope = [] (const ExecutionScope & scope) -> std::shared_ptr<const ExecutionScope>
            //{
            //    MLDB_THROW_UNIMPLEMENTED();
            //};
#endif

            if (!list.empty() && list.front().is<Function>()) {
                const Function & fn = list.front().as<Function>();
                auto compiler = this->getFunctionCompiler(fn.fn);
                return compiler(list, *this);
            }
            else {
                // Just a list, keep it as it was
                // ...
                MLDB_THROW_UNIMPLEMENTED();
            }
        }
    }
    else {
        Executor result = [program] (ExecutionScope & scope) -> Value
        {
            return program;
        };

        return { std::move(result), nullptr };
    }

    return { };
}

FunctionCompiler
CompilationScope::
getFunctionCompiler(const Path & fn) const
{
    if (fn.size() != 1)
        MLDB_THROW_UNIMPLEMENTED("paths with size() != 1");
    return lookupFunction(fn.front(), importedNamespaces);
}


/*******************************************************************************/
/* LISP CONTEXT                                                                */
/*******************************************************************************/

Value Context::list(PathElement head)
{
    List l;
    l.emplace_back(*this, Function{std::move(head), nullptr});
    return { *this, std::move(l) };
}

Value Context::list(PathElement head, std::vector<Value> vals)
{
    List l;
    l.emplace_back(*this, Function{std::move(head), nullptr});
    l.insert(l.end(), std::make_move_iterator(vals.begin()), std::make_move_iterator(vals.end()));
    return { *this, std::move(l) };
}

} // namespace Lisp
} // namespace MLDB
