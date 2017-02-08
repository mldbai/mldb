/* config.h                                                           -*- C++ -*-
   Guy Dumais, 8 March 2016

   This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

   Config interface.
*/

#pragma once

#include <string>
#include <unordered_map>
#include <memory>

namespace boost {
namespace program_options {
template <typename ValueType> class basic_parsed_options;
class variables_map;
typedef basic_parsed_options<char> parsed_options;
}
}

namespace MLDB {

struct Config {
    typedef std::shared_ptr<Config> ConfigPtr;

    // return the requested value if the key is present or the provided default value otherwise
    virtual std::string getString(const std::string & key, const std::string & defaultValue)  = 0;
    virtual bool getBool(const std::string & key, bool defaultValue)  = 0;
    virtual int getInt(const std::string & key, int defaultValue)  = 0;

    virtual ~Config(){}

    // create from a standard map
    static ConfigPtr createFromMap(const std::unordered_map<std::string, std::string> & map);
    
    // create from boost::program_options structure
    static ConfigPtr createFromProgramOptions(const boost::program_options::parsed_options & map );
    static ConfigPtr createFromProgramOptions(const boost::program_options::variables_map & map );

    // chain configs - lookup in first and if not found lookup in second
    //static ConfigPtr chainConfigs(ConfigPtr first, ConfigPtr second);

    static ConfigPtr get();
private:
    static ConfigPtr config;
};

} // MLDB
