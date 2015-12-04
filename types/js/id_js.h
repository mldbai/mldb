// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* id_js.h                                                         -*- C++ -*-
   JS encoding/decoding for IDs.
*/

#pragma once

#include "mldb/soa/js/js_utils.h"
#include "mldb/types/id.h"

namespace Datacratic {

class Id;

namespace JS {

inline void to_js(JS::JSValue & value, const Id & id)
{
    to_js(value, id.toString());
}

inline Id from_js(const JSValue & value, Id * = 0)
{
    return Id(utf8str(value));
}

inline Id from_js_ref(const JSValue & value, Id * = 0)
{
    return from_js(value, (Id *)0);
}

} // namespace JS
} // namespace Datacratic
