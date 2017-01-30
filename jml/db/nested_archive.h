/* nested_archive.h                                                -*- C++ -*-
   Jeremy Barnes, 30 January 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   ---

   A nested archive, that allows one to be embedded within another.
*/

#pragma once

#include "persistent.h"
#include <sstream>

namespace ML {
namespace DB {


/*****************************************************************************/
/* NESTED_READER                                                             */
/*****************************************************************************/

/** A store reader that can be used to read an archive nested within another
    archive.
*/

class Nested_Reader : public Store_Reader {
public:
    Nested_Reader();

    template<class Archive>
    void serialize(Archive & archive)
    {
        // reconstitute
        std::string data;
        archive >> data;
        stream.str(data);
    }

private:
    std::istringstream stream;
};


/*****************************************************************************/
/* NESTED_WRITER                                                             */
/*****************************************************************************/

/** A store writer that can be used to write an archive and nest it within
    another archive.
*/

class Nested_Writer : public Store_Writer {
public:
    Nested_Writer();

    template<class Archive>
    void serialize(Archive & archive) const
    {
        archive << stream.str();
    }
    
private:
    std::ostringstream stream;
};

} // namespace DB
} // namespace ML
