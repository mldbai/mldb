// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** meta_value_description_impl.h                                  -*- C++ -*-
    Jeremy Barnes, 23 August 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    Value descriptions for meta.
*/

#include "mldb/types/meta_value_description.h"
#include "mldb/types/value_description.h"

namespace MLDB {

struct ValueDescriptionPtrDescription
    : public ValueDescriptionT<std::shared_ptr<ValueDescription > > {

    ValueDescriptionPtrDescription(bool detailed = false)
        : detailed(detailed)
    {
    }

    bool detailed;

    virtual void parseJsonTyped(std::shared_ptr<ValueDescription> * val,
                                JsonParsingContext & context) const;

    virtual void printJsonTyped(const std::shared_ptr<ValueDescription> * val,
                                JsonPrintingContext & context) const;

    virtual bool isDefaultTyped(const std::shared_ptr<ValueDescription> * val) const;
};

struct ValueDescriptionConstPtrDescription
    : public ValueDescriptionT<std::shared_ptr<const ValueDescription > > {

    ValueDescriptionConstPtrDescription(bool detailed = false)
        : detailed(detailed)
    {
    }

    bool detailed;

    virtual void parseJsonTyped(std::shared_ptr<const ValueDescription> * val,
                                JsonParsingContext & context) const;

    virtual void printJsonTyped(const std::shared_ptr<const ValueDescription> * val,
                                JsonPrintingContext & context) const;

    virtual bool isDefaultTyped(const std::shared_ptr<const ValueDescription> * val) const;
};

struct ValueDescriptionNakedPtrDescription
    : public ValueDescriptionT<ValueDescription *> {

    ValueDescriptionNakedPtrDescription(bool detailed = false)
        : detailed(detailed)
    {
    }

    bool detailed;

    virtual void parseJsonTyped(ValueDescription * * val,
                                JsonParsingContext & context) const;

    virtual void printJsonTyped(ValueDescription * const * val,
                                JsonPrintingContext & context) const;

    virtual bool isDefaultTyped(ValueDescription * const * val) const;
};

struct ValueDescriptionNakedConstPtrDescription
    : public ValueDescriptionT<ValueDescription const *> {

    ValueDescriptionNakedConstPtrDescription(bool detailed = false)
        : detailed(detailed)
    {
    }

    bool detailed;

    virtual void parseJsonTyped(ValueDescription const * * val,
                                JsonParsingContext & context) const;

    virtual void printJsonTyped(ValueDescription const * const * val,
                                JsonPrintingContext & context) const;

    virtual bool isDefaultTyped(ValueDescription const * const * val) const;
};

} // namespace MLDB
