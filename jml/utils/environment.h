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
#include <boost/lexical_cast.hpp>
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
            t_ = boost::lexical_cast<T>(env[var_name]);
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
