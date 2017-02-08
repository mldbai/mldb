// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** rest_request_params_types.h                                    -*- C++ -*-
    Wolfgang Sourdeau, 8 January 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

*/

#pragma once

#include <string>

#include "mldb/types/date.h"
#include "mldb/rest/rest_request_params.h"


namespace MLDB {

template<>
struct RestCodec<MLDB::Date> {
    static Date decode(const std::string & str)
    {
        return Date::parseIso8601DateTime(str);
    }

    static Date decode(const MLDB::Utf8String & str)
    {
        return Date::parseIso8601DateTime(str.rawString());
    }

    static std::string encode(const Date & val)
    {
        return val.printIso8601();
    }
};

} // namespace MLDB
