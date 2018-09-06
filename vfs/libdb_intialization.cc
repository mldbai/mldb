/* libdb_initialization.cc
   Jeremy Barnes, 13 March 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Initialization of stream handlers in the presence of filter_xstream.
*/

#include "mldb/types/db/portable_iarchive.h"
#include "mldb/types/db/portable_oarchive.h"
#include "filter_streams.h"

namespace MLDB {

namespace {

// Plug in more capable filter_stream classes instead of the ofstream that
// libdb comes with.
struct AtInit {
    AtInit()
    {
        oldOpenInputStream = defaultOpenInputStream;
        oldOpenOutputStream = defaultOpenOutputStream;
        
        defaultOpenInputStream = open_input;
        defaultOpenOutputStream = open_output;
    }

    ~AtInit()
    {
        defaultOpenInputStream = oldOpenInputStream;
        defaultOpenOutputStream = oldOpenOutputStream;
    }

    static std::istream * open_input(const std::string & filename)
    {
        return new filter_istream(filename);
    }
    
    static std::ostream * open_output(const std::string & filename)
    {
        return new filter_ostream(filename);
    }
    
    std::function<std::istream * (const std::string &)> oldOpenInputStream;
    std::function<std::ostream * (const std::string &)> oldOpenOutputStream;

} atInit;

} // file scope

} // namespace MLDB
