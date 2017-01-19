// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* portable_oarchive.cc
   Jeremy Barnes, 20 March 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   


   ---

   Implementation of the portable oarchive.
*/


#include "portable_oarchive.h"
#include "nested_archive.h"
#include "mldb/vfs/filter_streams.h"

using namespace std;


namespace ML {
namespace DB {


/*****************************************************************************/
/* PORTABLE_BIN_OARCHIVE                                                     */
/*****************************************************************************/

portable_bin_oarchive::portable_bin_oarchive()
    : stream(0), offset_(0)
{
}

portable_bin_oarchive::portable_bin_oarchive(const std::string & filename)
    : stream(new MLDB::filter_ostream(filename)), owned_stream(stream),
      offset_(0)
{
}

portable_bin_oarchive::portable_bin_oarchive(std::ostream & stream)
    : stream(&stream), offset_(0)
{
}

void portable_bin_oarchive::open(const std::string & filename)
{
    stream = new MLDB::filter_ostream(filename.c_str());
    owned_stream.reset(stream);
    offset_ = 0;
}

void portable_bin_oarchive::open(std::ostream & stream)
{
    this->stream = &stream;
    owned_stream.reset();
    offset_ = 0;
}

void
portable_bin_oarchive::
save(const Nested_Writer & writer)
{
    writer.serialize(*this);
}

} // namespace DB
} // namespace ML
