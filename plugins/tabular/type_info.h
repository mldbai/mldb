/* type_info.h                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#error don't include me

#pragma once
#include <type_traits>
#include <map>
#include <string>
#include "mldb/arch/demangle.h"
#include "mldb/types/value_description_fwd.h"

#if 0
namespace MLDB {
namespace Tabular {

enum MetaType: uint32_t {
    INTEGER,
    ENUM,
    STRUCT
};

struct TabularTypeInfo;

struct TabularTypeInfo {
    TabularTypeInfo(const std::type_info * typeId, std::string name, const MetaType & metaType);

    const std::type_info * typeId;
    const std::string name;
    const MetaType metaType;
};

struct TabularEnumInfo: public TabularTypeInfo {
    TabularEnumInfo(const std::type_info * typeId, std::string name, const TabularTypeInfo * underlyingType)
        : TabularTypeInfo(typeId, std::move(name), MetaType::ENUM), underlyingType(underlyingType)
    {
    }

    const TabularTypeInfo * underlyingType;
};

struct IntegralTabularTypeInfo: public TabularTypeInfo {
    IntegralTabularTypeInfo(const std::type_info * typeId, std::string name)
        : TabularTypeInfo(typeId, std::move(name), MetaType::INTEGER)
    {
    }
};

#define DECLARE_INBUILT_TYPE_INFO(type) \
const TabularTypeInfo & getTabularTypeInfo(type *);

DECLARE_INBUILT_TYPE_INFO(uint8_t);
DECLARE_INBUILT_TYPE_INFO(int8_t);
DECLARE_INBUILT_TYPE_INFO(uint16_t);
DECLARE_INBUILT_TYPE_INFO(int16_t);
DECLARE_INBUILT_TYPE_INFO(uint32_t);
DECLARE_INBUILT_TYPE_INFO(int32_t);
DECLARE_INBUILT_TYPE_INFO(uint64_t);
DECLARE_INBUILT_TYPE_INFO(int64_t);

template<typename T, typename Return = decltype(getTabularTypeInfo((T *)0))>
const auto & getTabularTypeInfoFor() { return getTabularTypeInfo((T *)0); }

template<typename Enum>
struct TabularEnumInfoT: public TabularEnumInfo {
    std::multimap<Enum, std::string> valueToName;
    std::map<std::string, Enum> nameToValue;

    template<typename... Vals>
    TabularEnumInfoT(const char * typeName, Vals&&... vals)
        : TabularEnumInfo(&typeid(Enum), typeName, &getTabularTypeInfoFor<std::underlying_type_t<Enum>>())
    {
        addValues(std::forward<Vals>(vals)...);
    }

    template<typename... Rest>
    void addValues(Enum value, const char * name, Rest&&... rest)
    {
        addValue(name, value);
        addValues(std::forward<Rest>(rest)...);
    }

    void addValues()
    {
    }

    void addValue(const char * name, Enum value)
    {
        if (this->nameToValue.count(name)) {
            MLDB_THROW_LOGIC_ERROR("enum value added twice");
        }
        this->nameToValue[name] = value;
        this->valueToName.emplace(value, name);
    }
};

#define DECLARE_TABULAR_ENUM_INFO(type) \
const TabularEnumInfoT<type> & getTabularTypeInfo(type *)

struct TabularStructInfo: public TabularTypeInfo {
    TabularStructInfo(const std::type_info * typeId, std::string name)
        : TabularTypeInfo(typeId, std::move(name), MetaType::STRUCT)
    {
    }
};

template<typename T> struct TabularStructInfoT: public TabularStructInfo {
    TabularStructInfoT(std::string name = demangle(typeid(T).name()))
        : TabularStructInfo(&typeid(T), std::move(name))
    {
    }

    template<typename Field>
    void addField(Field T::* field);
};

#define DECLARE_TABULAR_STRUCT_INFO(type) \
const TabularStructInfoT<type> & getTabularTypeInfo(type *)

const TabularTypeInfo * tryGetTabularTypeInfo(const std::type_info * type);
template<typename T> const TabularTypeInfo * tryGetTabularTypeInfoT() { return tryGetTabularTypeInfo(&typeid(T)); }
const TabularTypeInfo & getTabularTypeInfo(const std::type_info * type);

//template<typename T>
//bool hasTabularTypeInfo = std::is_convertible_v<getTabularTypeInfo((T*)0)), const TabularTypeInfo &>>;

} // namespace Tabular

using namespace Tabular;

} // namespace MLDB
#endif