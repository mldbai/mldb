/* environment.h                                                   -*- C++ -*-
   Jeremy Barnes, 1 February 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
      
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   ---

   Environment access functions.
*/

#pragma once

#include <map>
#include <string>
#include <iostream>


namespace MLDB {


/*****************************************************************************/
/* ENVIRONMENT                                                               */
/*****************************************************************************/

/** Simple class to allow access to environment variables. */

class Environment : public std::map<std::string, std::string> {
    typedef std::map<std::string, std::string> base_type;

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

/** An environment option variable */

template<typename T, bool Trace = false>
class EnvOption {
public:
    EnvOption(const std::string & var_name, const T & def)
        : t_(def), specified_(false)
    {
        const Environment & env = Environment::instance();
        if (env.count(var_name)) {
            using MLDB::from_string;
            t_ = from_string(env[var_name], (T*)0);
            specified_ = true;
            if (Trace) {
                using namespace std;
                cerr << "Environment option " << var_name << " set to "
                     << t_ << endl;
            }
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
