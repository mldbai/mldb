/* lisp.h                                                        -*- C++ -*-
   Jeremy Barnes, 22 February 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "mldb/types/value_description_fwd.h"

namespace MLDB {

struct ParseContext;
struct Path;
struct PathElement;
struct Utf8String;

namespace Lisp {

struct Context;
struct CompilationScope;
struct ExecutionScope;
struct Value;
struct Symbol;
struct Null;
struct Wildcard;
struct List;
struct Predicate;
struct Substitution;
struct Pattern;
struct Ellipsis;

DECLARE_STRUCTURE_DESCRIPTION(Symbol);
DECLARE_STRUCTURE_DESCRIPTION(Null);
PREDECLARE_VALUE_DESCRIPTION(Wildcard);
DECLARE_STRUCTURE_DESCRIPTION(List);
DECLARE_STRUCTURE_DESCRIPTION(Value);
DECLARE_STRUCTURE_DESCRIPTION(Substitution);
DECLARE_STRUCTURE_DESCRIPTION(Predicate);
DECLARE_STRUCTURE_DESCRIPTION(Ellipsis);

} // namespace Lisp
} // namespace MLDB
