// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/* json_codec.h                                                    -*- C++ -*-
   Jeremy Banres, 26 November 2012
   Copyright (c) 2012 Datacratic.  All rights reserved.

   JSON encoding/decoding code.
*/

#pragma once

#include "mldb/ext/jsoncpp/json.h"
#include <vector>
#include <map>
#include <unordered_map>
#include "mldb/base/exc_assert.h"
#include "mldb/types/basic_value_descriptions.h"

namespace Datacratic {

template<typename T, typename Enable = void>
struct JsonCodec {
    static T decode(const Json::Value & val)
    {
        return jsonDecode(val, (T *)0);
    }

    static Json::Value encode(const T & val)
    {
        return jsonEncode(val);
    }
};

template<typename T>
void getParam(const Json::Value & parameters,
              T & val,
              const std::string & name)
{
    if (parameters.isMember(name)) {
        Json::Value j = parameters[name];
        if (j.isNull())
            return;
        val = jsonDecode(j, &val);
    }
}

} // namespace Datacratic
