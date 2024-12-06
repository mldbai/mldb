/* environment.h                                                   -*- C++ -*-
   Jeremy Barnes, 1 February 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
      
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   ---

   Environment access functions.
*/

#pragma once

#include <unordered_map>
#include <string>
#include <iostream>


namespace MLDB {


/*****************************************************************************/
/* ENVIRONMENT                                                               */
/*****************************************************************************/

/** Simple class to allow access to environment variables. */

class Environment : public std::unordered_map<std::string, std::string> {
    typedef std::unordered_map<std::string, std::string> base_type;

public:
    Environment();

    using base_type::operator [];

    const std::string & operator [] (const std::string & key) const
    {
        static const std::string NONE;
        const_iterator it = find(key);
        if (it == end()) return NONE;
        return it->second;
    }

    static const Environment & instance();
};

inline std::string from_string(std::string s, std::string *)
{
    return s;
}

inline int from_string(const std::string & s, int *)
{
    return std::stol(s);
}

inline bool from_string(const std::string & s, bool *)
{
    if (s == "true")
        return true;
    if (s == "false")
        return false;
    return from_string(s, (int *)0);
}

/*****************************************************************************/
/* ENV_OPTION                                                                */
/*****************************************************************************/

template<bool Trace> struct EnvOptionTracer;

template<>
struct EnvOptionTracer<true> {

    static std::ostream & start_trace(const std::string & var_name);
    static std::ostream & end_trace(std::ostream & stream);
#if 0
    {
        using namespace std;
        cerr << "Environment option " << var_name << " set to ";
    }
#endif

    template<typename T>
    static void trace(const std::string & var_name, const T & t)
    {
        end_trace(start_trace(var_name) << t);
    }
};

template<>
struct EnvOptionTracer<false> {
    template<typename T>
    static void trace(const std::string & var_name, const T & t)
    {
    }
};

/** An environment option variable */

template<typename T, bool Trace = false>
class EnvOption {
public:
    EnvOption(const std::string & var_name, const T & def)
        : t_(def), specified_(false)
    {
        const char * p = getenv(var_name.c_str());
        if (p) {
            t_ = from_string(p, (T*)0);
            specified_ = true;
            EnvOptionTracer<Trace>::trace(var_name, t_);
        }
    }

    operator T () const { return t_; }

    T get() const { return t_; }

    bool specified() const { return specified_; }

    void set(T t, bool specified = true)
    {
        t_ = t;
        specified_ = specified;
    }

private:
    T t_;
    bool specified_;

};
} // namespace MLDB
