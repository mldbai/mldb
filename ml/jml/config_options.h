// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* config_options.h                                                -*- C++ -*-
   Jeremy Barnes, 15 March 2006
   Copyright (c) 2006 Jeremy Barnes.  All rights reserved.

   Structure to hold a set of configuration options.
*/

#pragma once


#include <string>
#include <vector>
#include <boost/lexical_cast.hpp>
#include "mldb/jml/utils/configuration.h"
#include "mldb/jml/utils/enum_info.h"
#include "mldb/ml/jml/registry.h"


namespace ML {


class Config_Options;


/** A single configuration option. */

struct Config_Option {
    std::string name;   ///< Name of the option
    std::string type;   ///< Type of the option
    std::string value;  ///< Current or default value
    std::string range;  ///< Accepted range of values
    std::string doc;    ///< Documentation

    ///< If this is a nested option group, this contains their documentation
    std::shared_ptr<Config_Options> group;

    std::string print() const;
};

Config_Option
option(const std::string & name, const std::string & type,
       const std::string & value, const std::string & range,
       const std::string & doc);

/** Generate an option from something with a type. */
template<class T>
Config_Option
option(const std::string & name, const T & value, const std::string & range,
       const std::string & doc)
{
    return option(name, demangle(typeid(T).name()),
                  boost::lexical_cast<std::string>(value), range, doc);
}

inline Config_Option
option(const std::string & name, bool value, const std::string & doc)
{
    return option(name, "bool", (value ? "true" : "false"), "true|false|1|0",
                  doc);
}

/** Generate an option from an enum. */
template<class Enum>
Config_Option
option(const std::string & name, const Enum & value, const std::string & doc)
{
    return option(name, Enum_Info<Enum>::NAME, enum_value(value),
                  enum_values<Enum>(), doc);
}


/** A group of configuration options. */

class Config_Options : public std::vector<Config_Option> {
public:
    void add(const std::string & name, const std::string & type,
             const std::string & value, const std::string & range,
             const std::string & doc);

    template<class T>
    Config_Options &
    add(const std::string & name, const T & value,
        const std::string & range, const std::string & doc)
    {
        push_back(option(name, value, range, doc));
        return *this;
    }

    template<class Enum>
    Config_Options &
    add(const std::string & name, const Enum & value,
        const std::string & doc)
    {
        push_back(option(name, value, doc));
        return *this;
    }

    Config_Options &
    add(const Config_Options & opt)
    {
        for (unsigned i = 0;  i < opt.size();  ++i)
            push_back(opt[i]);
        return *this;
    }

    template<class Object>
    Config_Options &
    subconfig(const std::string & name, const std::shared_ptr<Object> & obj,
              const std::string & doc)
    {
        Config_Option option;
        option.name = name;
        option.type = demangle(typeid(Object).name());
        option.value = (obj ? obj->type() : "<not set>");
        option.range = Registry<Object>::entry_list();

        if (obj)
            option.group.reset(new Config_Options(obj->options()));

        push_back(option);
        return *this;
    }

#if 0
    template<class Object>
    Config_Options &
    subconfig(const std::string & name, const Object & obj,
              const std::string & doc)
    {
        Config_Option option;
        option.name = name;
        option.type = demangle(typeid(Object).name());
        option.value = obj.type();
        option.range = Registry<Object>::entry_list();

        if (obj)
            option.group.reset(new Config_Options(obj->options()));

        push_back(option);
        return *this;
    }
#endif
    
    void dump(std::ostream & stream, int indent = 0, int cols = -1) const;
};

} // namespace ML
