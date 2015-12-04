// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** date_description.h                                             -*- C++ -*-
    Jeremy Barnes, 23 August 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    Value descriptions for date.
*/

#pragma once

#include "date.h"
#include "mldb/types/value_description.h"

namespace Datacratic {

struct DateDescription
    : public ValueDescriptionI<Date, ValueKind::ATOM, DateDescription> {

    virtual void parseJsonTyped(Date * val,
                                JsonParsingContext & context) const JML_OVERRIDE;
    virtual void printJsonTyped(const Date * val,
                                JsonPrintingContext & context) const JML_OVERRIDE;
    virtual bool isDefaultTyped(const Date * val) const JML_OVERRIDE;
};

extern template class ValueDescriptionT<Datacratic::Date>;
extern template class ValueDescriptionI<Datacratic::Date, ValueKind::ATOM, DateDescription>;

struct JavaTimestampValueDescription: public DateDescription {

    virtual void parseJsonTyped(Date * val,
                                JsonParsingContext & context) const JML_OVERRIDE;
    virtual void printJsonTyped(const Date * val,
                                JsonPrintingContext & context) const JML_OVERRIDE;
};

struct Iso8601TimestampValueDescription: public DateDescription {

    virtual void parseJsonTyped(Date * val,
                                JsonParsingContext & context) const JML_OVERRIDE;
    virtual void printJsonTyped(const Date * val,
                                JsonPrintingContext & context) const JML_OVERRIDE;
};


} // namespace Datacratic
