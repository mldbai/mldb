/** generic_enum_description.h                                             -*- C++ -*-
    Jeremy Barnes, 21 August 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Value description for an enum.
*/

#pragma once

#include "value_description.h"
#include "mldb/compiler/compiler.h"

namespace MLDB {


/*****************************************************************************/
/* GENERIC ENUM DESCRIPTION                                                  */
/*****************************************************************************/

/** Enum description that's not based on a C++ type. */

struct GenericEnumDescription: public ValueDescription {
    GenericEnumDescription(std::shared_ptr<const ValueDescription> underlying, std::string typeName,
                           const std::type_info * type = nullptr)
        : ValueDescription(ValueKind::ENUM, type, underlying->width, underlying->align, std::move(typeName)),
          underlying(std::move(underlying))
    {
    }

    using Enum = int;
    std::shared_ptr<const void> defaultValue;
    std::shared_ptr<const ValueDescription> underlying;
    std::unordered_map<std::string, Enum> parse;
    std::map<Enum, std::pair<std::string, std::string> > print;

    /** Parse the value of the enum which is already in a string. */
    Enum parseString(const std::string & s) const
    {
        auto it = parse.find(s);
        if (it == parse.end())
            throw MLDB::Exception("unknown value for " + this->typeName
                                + ": " + s);
        return it->second;
    }

    /** Convert the value of the enum to a string that represents the value. */
    std::string printString(Enum e) const
    {
        auto it = print.find(e);
        if (it == print.end())
            return std::to_string(e);
        else return it->second.first;
    }

    Enum getValue(const void * val) const
    {
        auto jval = underlying->printJsonStructured(val);
        return jval.asInt();
    }

    void setValue(void * val, Enum to) const
    {
        // TODO: anything but JSON...
        Json::Value jval(to);
        StructuredJsonParsingContext context(jval);
        underlying->parseJson(val, context);
    }

    virtual void parseJson(void * val, JsonParsingContext & context) const override
    {
        if (context.isNull()) {
            context.exception("NULL value found parsing enumeration "
                              + this->typeName + "; expected either a "
                              "string or an integer");
        }

        if (context.isString()) {
            std::string s = context.expectStringAscii();
            auto it = parse.find(s);
            if (it == parse.end())
                context.exception("unknown value for " + this->typeName
                                  + ": " + s);
            setValue(val, it->second);
            return;
        }

        // Otherwise, it was serialized directly as an integer
        underlying->parseJson(val, context);
    }

    virtual void printJson(const void * val, JsonPrintingContext & context) const override
    {
        auto val2 = getValue(val);
        auto it = print.find(val2);
        if (it == print.end())
            underlying->printJson(val, context);
        else context.writeString(it->second.first);
    }

    virtual bool isDefault(const void * val) const override
    {
        if (!defaultValue)
            return false;
        return underlying->compareEquality(val, defaultValue.get());
    }

    virtual void setDefault(void * val) const override
    {
        if (!defaultValue)
            underlying->setDefault(val);
    }

    virtual void copyValue(const void * from, void * to) const override
    {
        underlying->copyValue(from, to);
    }

    virtual void moveValue(void * from, void * to) const override
    {
        underlying->moveValue(from, to);
    }

    virtual void swapValues(void * from, void * to) const override
    {
        underlying->swapValues(from, to);
    }

    virtual void initializeDefault(void * obj) const override
    {
        if (!defaultValue) {
            underlying->initializeDefault(obj);
            return;
        }
        underlying->initializeCopy(obj, defaultValue.get());
    }

    virtual void initializeCopy(void * obj, const void * other) const override
    {
        underlying->initializeCopy(obj, other);
    }

    virtual void initializeMove(void * obj, void * other) const override
    {
        underlying->initializeMove(obj, other);
    }

    virtual void destruct(void * obj) const override
    {
        underlying->destruct(obj);
    }

    void setDefaultValue(Enum value)
    {
        throw MLDB::Exception("setDefaultValue");
    }

    void addValue(const std::string & name, Enum value)
    {
        if (!parse.insert(make_pair(name, value)).second)
            throw MLDB::Exception("double added name '" + name + "' to enum '"
                                  + this->typeName + "'");
        
        print.insert({ value, { name, "" } });
    }

    void addValue(const std::string & name, Enum value,
                  const std::string & description)
    {
        if (!parse.insert(make_pair(name, value)).second)
            throw MLDB::Exception("double added name to enum");
        print.insert({ value, { name, description } });
    }

    virtual std::vector<std::tuple<int, std::string, std::string> >
    getEnumValues() const
    {
        std::vector<std::tuple<int, std::string, std::string> > result;
        for (auto & v: print)
            result.emplace_back((int)v.first, v.second.first, v.second.second);
        return result;
    }

    virtual const std::vector<std::string> getEnumKeys() const
    {
        std::vector<std::string> res;
        for (const auto & it: print) {
            res.push_back(it.second.first);
        }
        return res;
    }
};


} // namespace MLDB
