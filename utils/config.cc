/* config.h                                                           -*- C++ -*-
   Guy Dumais, 8 March 2016

   This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

   Config interface.
*/

#include "config.h"
#include "mldb/arch/exception.h"
#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/lexical_cast.hpp>
#include <set>
#include <boost/locale.hpp>

namespace {
    static std::set<std::string> trueValues({"true", "1"});
}

namespace boost {
    template<> 
    bool lexical_cast<bool, std::string>(const std::string& arg) {
        boost::locale::generator gen;
        std::locale loc=gen("en_US.UTF-8"); 
        return trueValues.find(boost::locale::to_lower(arg, loc)) != trueValues.end();
    }
}

namespace MLDB {

struct BaseConfig : public Config {
    virtual bool getBool(const std::string & key, bool defaultValue) {
        std::string defaultStrValue = (defaultValue ? "true" : "false");
        try {
            return boost::lexical_cast<bool>(getString(key, defaultStrValue));
        } catch (std::exception &) {
            return defaultValue;
        }
    }
    virtual int getInt(const std::string & key, int defaultValue) {
        try {
            std::string defaultStrValue = boost::lexical_cast<std::string>(defaultValue);
            return boost::lexical_cast<int>(getString(key, defaultStrValue));
        } catch (std::exception &) {
            return defaultValue;
        }
    }
};

struct MapConfig : public BaseConfig {

    MapConfig(std::unordered_map<std::string, std::string> map)
        : map(map)
    {}
    virtual std::string getString(const std::string & key, const std::string & defaultValue) {
        auto value = map.find(key);
        if (value != map.end())
            return value->second;
        else
            return defaultValue;
    }
    
    std::unordered_map<std::string, std::string> map;
};

Config::ConfigPtr
Config:: 
createFromMap(const std::unordered_map<std::string,std::string> & map) {
    config = std::make_shared<MapConfig>(map);
    return config;
}

struct ParsedOptionsConfig : public BaseConfig {

    ParsedOptionsConfig(const boost::program_options::parsed_options & options)
        : options(options)
    {}
    virtual std::string getString(const std::string & key, const std::string & defaultValue) {
        // this is really sub-optimal 
        // if we feel the pain we can convert the vector into a unordered_map
        for (const auto & option : options.options) {
            if (option.string_key == key)
                return option.value[0];
        }
        return defaultValue;
    }

    boost::program_options::parsed_options options;
};

Config::ConfigPtr
Config::
createFromProgramOptions(const boost::program_options::parsed_options & map) {
    config = std::make_shared<ParsedOptionsConfig>(map);
    return config;
}

struct VariablesMapConfig : public BaseConfig {

    VariablesMapConfig(const boost::program_options::variables_map & map)
        : map(map)
    {}
    virtual std::string getString(const std::string & key, const std::string & defaultValue) {
        if (map.count(key))
            return map[key].as<std::string>();
        return defaultValue;
    }
  
    boost::program_options::variables_map map;
};

Config::ConfigPtr
Config::
createFromProgramOptions(const boost::program_options::variables_map & map) {
    config = std::make_shared<VariablesMapConfig>(map);
    return config;
}

struct ChainConfig : public BaseConfig {

    ChainConfig(const std::pair<ConfigPtr, ConfigPtr> configs)
        : configs(configs)
    {}
    virtual std::string getString(const std::string & key, const std::string & defaultValue) {
        return "";
    }
  
    std::pair<ConfigPtr, ConfigPtr> configs;
};

// Config::ConfigPtr
// Config::
// chainConfigs(ConfigPtr first, ConfigPtr second) {
//     return std::make_shared<ChainConfig>({first, second});
// }

Config::ConfigPtr
Config::
get() {
    return config;
}

Config::ConfigPtr Config::config;

} // MLDB
