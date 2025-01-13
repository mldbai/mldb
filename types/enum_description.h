/** enum_description.h                                             -*- C++ -*-
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
/* ENUM DESCRIPTION                                                          */
/*****************************************************************************/

template<typename Enum, typename Underlying = std::underlying_type_t<Enum>>
struct EnumDescription: public ValueDescriptionT<Enum> {

    EnumDescription(std::shared_ptr<const ValueDescription> underlying = getDefaultDescriptionSharedT<Underlying>())
        : ValueDescriptionT<Enum>(ValueKind::ENUM),
          hasDefault(false), defaultValue(Enum(0)),
          underlying(std::move(underlying))
    {
    }

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
            return std::to_string((Underlying)e);
        else return it->second.first;
    }

    virtual void parseJsonTyped(Enum * val, JsonParsingContext & context) const override
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
            *val = it->second;
            return;
        }

        *val = (Enum)context.expectInt();
    }

    virtual void printJsonTyped(const Enum * val, JsonPrintingContext & context) const override ATTRIBUTE_NO_SANITIZE_UNDEFINED
    {
        auto it = print.find(static_cast<Underlying>(*val));
        if (it == print.end())
            context.writeInt((int)*val);
        else context.writeString(it->second.first);
    }
    
    virtual bool isDefaultTyped(const Enum * val) const override
    {
        if (!hasDefault)
            return false;
        return *val == defaultValue;
    }

    virtual void setDefaultTyped(Enum * val) const override
    {
        *val = defaultValue;
    }

    virtual const ValueDescription & contained() const override
    {
        return *underlying;
    }

    virtual std::shared_ptr<const ValueDescription> containedPtr() const override
    {
        return underlying;
    }

    bool hasDefault;
    Enum defaultValue;
    std::shared_ptr<const ValueDescription> underlying;

    void setDefaultValue(Enum value)
    {
        this->hasDefault = true;
        this->defaultValue = value;
    }

    void addValue(const std::string & name, Enum value)
    {
        if (!parse.insert(make_pair(name, value)).second)
            throw MLDB::Exception("double added name '" + name + "' to enum '"
                                  + this->typeName + "'");
        
        print.insert({ static_cast<Underlying>(value), { name, "" } });
    }

    void addValue(const std::string & name, Enum value,
                  const std::string & description)
    {
        if (!parse.insert(make_pair(name, value)).second)
            throw MLDB::Exception("double added name to enum");
        print.insert({ static_cast<Underlying>(value), { name, description } });
    }

    virtual std::vector<std::tuple<int, std::string, std::string> >
    getEnumValues() const override
    {
        std::vector<std::tuple<int, std::string, std::string> > result;
        for (auto & v: print)
            result.emplace_back((int)v.first, v.second.first, v.second.second);
        return result;
    }

    virtual const std::vector<std::string> getEnumKeys() const override
    {
        std::vector<std::string> res;
        for (const auto & it: print) {
            res.push_back(it.second.first);
        }
        return res;
    }

    virtual void extractBitField(const void * from, void * to, uint32_t bitOffset, uint32_t bitWidth) const override
    {
        underlying->extractBitField(from, to, bitOffset, bitWidth);
    }

    virtual void insertBitField(const void * from, void * to, uint32_t bitOffset, uint32_t bitWidth) const override
    {
        underlying->insertBitField(from, to, bitOffset, bitWidth);
    }

    std::unordered_map<std::string, Enum> parse;
    std::map<Underlying, std::pair<std::string, std::string> > print;
};

} // namespace MLDB
