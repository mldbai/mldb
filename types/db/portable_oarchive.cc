/* portable_oarchive.cc
   Jeremy Barnes, 20 March 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   ---

   Implementation of the portable oarchive.
*/


#include "portable_oarchive.h"
#include "nested_archive.h"
#include <fstream>

using namespace std;


namespace MLDB {
namespace DB {

static std::ostream * open_fstream_write(const std::string & filename)
{
    return new std::ofstream(filename.c_str());
}

std::function<std::ostream * (const std::string )> defaultOpenOutputStream
    = open_fstream_write;



/*****************************************************************************/
/* PORTABLE_BIN_OARCHIVE                                                     */
/*****************************************************************************/

portable_bin_oarchive::portable_bin_oarchive()
    : stream(0), offset_(0)
{
}

portable_bin_oarchive::portable_bin_oarchive(const std::string & filename)
    : stream(defaultOpenOutputStream(filename)), owned_stream(stream),
      offset_(0)
{
}

portable_bin_oarchive::portable_bin_oarchive(std::ostream & stream)
    : stream(&stream), offset_(0)
{
}

void portable_bin_oarchive::open(const std::string & filename)
{
    stream = defaultOpenOutputStream(filename);
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
} // namespace MLDB
