/* lisp_jit.h                                                        -*- C++ -*-
   Jeremy Barnes, 22 February 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "lisp_fwd.h"
#include "mldb/types/path.h"

namespace MLDB {
namespace Lisp {

struct Opcode {
    uint8_t code;
    uint8_t length;
    PathElement name;
};

#define MLDB_LISP_ENUMERATE_OPCODES(OP) \
    OP(NOP,         "nop",          0) \
    OP(ADD,         "add",          1) \
    OP(SUB,         "sub",          2) \
    OP(TOSIGNED,    "tosigned",     3) \
    OP(TOUNSIGNED,  "tounsigned",   4) \
    OP(CCALL,       "ccall",        5) \

#define MLDB_LISP_DEFINE_OPCODE(Name, str, num) extern Opcode Name;
MLDB_LISP_ENUMERATE_OPCODES(MLDB_LISP_DEFINE_OPCODE)
#undef MLDB_LISP_DEFINE_OPCODE

/*******************************************************************************/
/* LISP JIT                                                                    */
/*******************************************************************************/

Value jit(CompilationScope & scope, Value program);


} // namespace Lisp
} // namespace MLDB
