/* exception_description.cc                                            -*- C++ -*-
   Jeremy Barnes, 29 March 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "exception_description.h"
#include "value_description.h"
#include "annotated_exception.h"

namespace MLDB {

struct ExceptionPtrDescription: public ValueDescriptionT<std::exception_ptr> {


    virtual void parseJsonTyped(std::exception_ptr * val,
                                JsonParsingContext & context) const override
    {
        MLDB_THROW_UNIMPLEMENTED("can't parse exception pointers");
    }

    virtual void printJsonTyped(const std::exception_ptr * val,
                                JsonPrintingContext & context) const override
    {
        if (!(*val)) {
            context.writeNull();
            return;
        }

        MLDB_TRACE_EXCEPTIONS(false);

        context.startObject();

        try {
            std::rethrow_exception(*val);
        } catch (const AnnotatedException & exc) {
            context.startMember("message");
            context.writeStringUtf8(getUtf8ExceptionString());
            context.startMember("type");
            context.writeString(demangle(typeid(exc).name()));
            context.startMember("httpCode");
            context.writeInt(exc.httpCode);
            if (!exc.details.empty()) {
                context.startMember("details");
                std::shared_ptr<const ValueDescription> desc = getDefaultDescriptionSharedT<Any>();
                desc->printJson(&exc.details, context);
            }
        } catch (const std::exception & exc) {
            context.startMember("message");
            context.writeStringUtf8(getUtf8ExceptionString());
            context.startMember("type");
            context.writeString(demangle(typeid(exc).name()));
        } catch (...) {
            context.startMember("message");
            context.writeStringUtf8(getUtf8ExceptionString());
        }

        context.endObject();
    }

    virtual bool isDefaultTyped(const std::exception_ptr * val) const override
    {
        return !(*val);
    }
};

} // namespace MLDB

namespace std {

DEFINE_VALUE_DESCRIPTION_NS(std::exception_ptr, MLDB::ExceptionPtrDescription);

}  // namespace std
