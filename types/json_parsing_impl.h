/** json_parsing_impl.h                                            -*- C++ -*-
    Jeremy Barnes, 28 November 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
*/

#pragma once

#include "json_parsing.h"
#include "mldb/jml/utils/compact_vector.h"

namespace Datacratic {

/*****************************************************************************/
/* JSON PATH                                                                 */
/*****************************************************************************/

/** Internal class used to keep track of where inside a JSON object we
    are currently parsing.
*/

struct JsonPath: public ML::compact_vector<JsonPathEntry, 8> {
    JsonPath();

    /// Print the path out as a dotted series of elements
    std::string print(bool includeLeadingDot = true) const;

    /// Return the name of the outermost element.  Throws if in an array.
    std::string fieldName() const;

    /// Zero allocation return of name of the outermost element.  Throws if
    /// in an array.  String is owned by this and reference must not outlive it.
    const char * fieldNamePtr() const;

    /// Push a new path element
    void push(JsonPathEntry entry, int fieldNum = 0);

    /// Replace the top-most element with a new one
    void replace(JsonPathEntry entry);

    /// Pop the top-most element
    void pop();
};


} // file scope
