/* enum_info.h                                                     -*- C++ -*-
   Jeremy Barnes, 3 April 2006
   Copyright (c) 2006 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Functions to allow us to know the contents of enumerations.
*/

#pragma once

#include <string>
#include <map>
#include <atomic>
#include "mldb/arch/exception.h"

namespace ML {

struct Enum_Tag {};
struct Not_Enum_Tag {};

template<typename Enum>
struct Enum_Opt {
    const char * name;
    Enum val;
};

template<typename Enum>
struct Enum_Info {
    enum { IS_SPECIALIZED = false };
    typedef Not_Enum_Tag Tag;
};

template<typename Enum>
std::string
enum_values()
{
    std::string result;
    for (unsigned i = 0;  i < Enum_Info<Enum>::NUM;  ++i)
        result += std::string(i > 0, ' ') + Enum_Info<Enum>::OPT[i].name;
    return result;
}

template<typename Enum>
std::string
enum_value(Enum val)
{
    typedef std::map<Enum, std::string> values_type; 
    static std::atomic<values_type *> values(nullptr);
    if (!values) {
        values_type * new_values = new values_type;

        for (unsigned i = 0;  i < Enum_Info<Enum>::NUM;  ++i) {
            const Enum_Opt<Enum> & opt = Enum_Info<Enum>::OPT[i];
            if (!new_values->count(opt.val))
                (*new_values)[opt.val] = opt.name;
        }

        values_type * old_values = nullptr;
        if (!values.compare_exchange_strong(old_values, new_values))
            delete new_values;
    }
    typename values_type::const_iterator found = values.load()->find(val);
    if (found == values.load()->end())
        return "";
    return found->second;
}

template<typename Enum>
Enum
enum_value(const std::string & name)
{
    typedef std::map<std::string, Enum> values_type; 
    static std::atomic<values_type *> values(nullptr);
    if (!values) {
        values_type * new_values = new values_type;

        for (unsigned i = 0;  i < Enum_Info<Enum>::NUM;  ++i) {
            const Enum_Opt<Enum> & opt = Enum_Info<Enum>::OPT[i];
            if (!new_values->count(opt.name))
                (*new_values)[opt.name] = opt.val;
        }
        
        values_type * old_values = nullptr;
        if (!values.compare_exchange_strong(old_values, new_values))
            delete new_values;
    }
    typename values_type::const_iterator found = values.load()->find(name);
    if (found == values.load()->end())
        throw MLDB::Exception("couldn't parse '" + name + "' as "
                        + ML::Enum_Info<Enum>::NAME + " (possibilities are "
                        + ::ML::enum_values<Enum>());
    return found->second;
}

#define DECLARE_ENUM_INFO(type, num_values) \
namespace ML { \
template<> \
struct Enum_Info<type> { \
    enum { NUM = num_values, IS_SPECIALIZED = true }; \
    typedef Enum_Tag Tag; \
    static const Enum_Opt<type> OPT[num_values]; \
    static const char * NAME; \
}; \
\
} // namespace ML

} // namespace ML
