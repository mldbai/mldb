/** enum_description.h                                             -*- C++ -*-
    Jeremy Barnes, 21 August 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Value description for an enum.
*/

#pragma once

#include "value_description.h"

namespace MLDB {


/*****************************************************************************/
/* ENUM DESCRIPTION                                                          */
/*****************************************************************************/

template<typename Enum>
struct EnumDescription: public ValueDescriptionT<Enum> {

    EnumDescription()
        : ValueDescriptionT<Enum>(ValueKind::ENUM),
          hasDefault(false), defaultValue(Enum(0))
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
            return std::to_string((int)e);
        else return it->second.first;
    }

    virtual void parseJsonTyped(Enum * val, JsonParsingContext & context) const
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

    virtual void printJsonTyped(const Enum * val, JsonPrintingContext & context) const
    {
        auto it = print.find(*val);
        if (it == print.end())
            context.writeInt((int)*val);
        else context.writeString(it->second.first);
    }
    
    virtual bool isDefaultTyped(const Enum * val) const
    {
        if (!hasDefault)
            return false;
        return *val == defaultValue;
    }

    virtual void setDefaultTyped(Enum * val) const
    {
        *val = defaultValue;
    }

    bool hasDefault;
    Enum defaultValue;

    void setDefaultValue(Enum value)
    {
        this->hasDefault = true;
        this->defaultValue = value;
    }

    void addValue(const std::string & name, Enum value)
    {
        if (!parse.insert(make_pair(name, value)).second)
            throw MLDB::Exception("double added name to enum");
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
        for (const auto it: print) {
            res.push_back(it.second.first);
        }
        return res;
    }

    std::unordered_map<std::string, Enum> parse;
    std::map<Enum, std::pair<std::string, std::string> > print;
};

} // namespace MLDB
