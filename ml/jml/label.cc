// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* label.cc
   Jeremy Barnes, 18 May 2010
   Copyright (c) 2010 Jeremy Barnes.  All rights reserved.

   Implementation of the label class.
*/

#include "mldb/ml/jml/label.h"
#include "mldb/jml/db/persistent.h"

using namespace std;

namespace ML {


/*****************************************************************************/
/* LABEL                                                                     */
/*****************************************************************************/

void Label::serialize(DB::Store_Writer & store) const
{
    store << label_;
}

void Label::reconstitute(DB::Store_Reader & store)
{
    store >> label_;
}

} // file scope
