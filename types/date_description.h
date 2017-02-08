/** date_description.h                                             -*- C++ -*-
    Jeremy Barnes, 23 August 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Value descriptions for date.
*/

#pragma once

#include "date.h"
#include "mldb/types/value_description.h"

namespace MLDB {

struct DateDescription
    : public ValueDescriptionI<Date, ValueKind::ATOM, DateDescription> {

    virtual void parseJsonTyped(Date * val,
                                JsonParsingContext & context) const override;
    virtual void printJsonTyped(const Date * val,
                                JsonPrintingContext & context) const override;
    virtual bool isDefaultTyped(const Date * val) const override;
};

extern template struct ValueDescriptionT<MLDB::Date>;
extern template struct ValueDescriptionI<MLDB::Date, ValueKind::ATOM, DateDescription>;

struct JavaTimestampValueDescription: public DateDescription {

    virtual void parseJsonTyped(Date * val,
                                JsonParsingContext & context) const override;
    virtual void printJsonTyped(const Date * val,
                                JsonPrintingContext & context) const override;
};

struct Iso8601TimestampValueDescription: public DateDescription {

    virtual void parseJsonTyped(Date * val,
                                JsonParsingContext & context) const override;
    virtual void printJsonTyped(const Date * val,
                                JsonPrintingContext & context) const override;
};


} // namespace MLDB
