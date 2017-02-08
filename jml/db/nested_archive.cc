// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* nested_archive.cc
   Jeremy Barnes, 20 March 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.



   ---

   Implementation of the nested archive classes.
*/

#include "nested_archive.h"


namespace ML {
namespace DB {


/*****************************************************************************/
/* NESTED_READER                                                             */
/*****************************************************************************/

Nested_Reader::Nested_Reader()
{
    open(stream);
}


/*****************************************************************************/
/* NESTED_WRITER                                                             */
/*****************************************************************************/

Nested_Writer::Nested_Writer()
{
    open(stream);
}

} // namespace DB
} // namespace ML
