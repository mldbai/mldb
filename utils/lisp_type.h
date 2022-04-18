/* lisp_type.h                                                  -*- C++ -*-
   Jeremy Barnes, 22 February 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "lisp_fwd.h"
#include <optional>
#include <vector>
#include <memory>
#include <map>
#include "mldb/types/string.h"
#include "mldb/types/path.h"
#include "mldb/types/any.h"

namespace MLDB {
namespace Lisp {

/*******************************************************************************/
/* LISP TYPE                                                                   */
/*******************************************************************************/

// A type is a value, but we allow it to have a distinct type for matching
// and some methods.
struct Type: public Value {
};

} // namespace LISP
} // namespace MLDB

