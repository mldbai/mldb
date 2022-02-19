/* type_info.cc                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "type_info_impl.h"
#include <unordered_map>
#include <mutex>

#if 0
namespace MLDB {
namespace Tabular {

namespace {

std::unordered_map<const std::type_info *, const TabularTypeInfo *> tabularTypeInfoRegistry;
std::mutex tabularTypeInfoRegistryMutex;

void registerTabularTypeInfo(const std::type_info * typeId, const TabularTypeInfo * info)
{
    return;
    std::unique_lock<std::mutex> lock(tabularTypeInfoRegistryMutex);
    if (!tabularTypeInfoRegistry.emplace(typeId, info).second) {
        MLDB_THROW_LOGIC_ERROR("attempted to register TabularTypeInfo for the same type twice");
    }
}

} // file scope

const TabularTypeInfo * tryGetTabularTypeInfo(const std::type_info * type)
{
    return nullptr;
    std::unique_lock<std::mutex> lock(tabularTypeInfoRegistryMutex);
    auto it = tabularTypeInfoRegistry.find(type);
    if (it == tabularTypeInfoRegistry.end())
        return nullptr;
    return it->second;
}

const TabularTypeInfo & getTabularTypeInfo(const std::type_info * type)
{
    auto p = tryGetTabularTypeInfo(type);
    if (!p) {
        MLDB_THROW_RUNTIME_ERROR("couldn't find type info");
    }
    return *p;
}

TabularTypeInfo::TabularTypeInfo(const std::type_info * typeId, std::string name, const MetaType & metaType)
    : typeId(typeId), name(std::move(name)), metaType(metaType)
{
    registerTabularTypeInfo(typeId, this);
}

template<typename T> struct IntegerTabularTypeInfo: public TabularTypeInfo {
    IntegerTabularTypeInfo(std::string name)
        : TabularTypeInfo(&typeid(T), std::move(name), INTEGER)
    {
    }
};

#define DEFINE_INBUILT_TYPE_INFO(type, shortname) \
static const IntegerTabularTypeInfo<type> inbuilt_type_info_##type(#shortname); \
const TabularTypeInfo & getTabularTypeInfo(type *) { return inbuilt_type_info_##type; }

DEFINE_INBUILT_TYPE_INFO(uint8_t, u8);
DEFINE_INBUILT_TYPE_INFO(int8_t, i8);
DEFINE_INBUILT_TYPE_INFO(uint16_t, u16);
DEFINE_INBUILT_TYPE_INFO(int16_t, i16);
DEFINE_INBUILT_TYPE_INFO(uint32_t, u32);
DEFINE_INBUILT_TYPE_INFO(int32_t, i32);
DEFINE_INBUILT_TYPE_INFO(uint64_t, u64);
DEFINE_INBUILT_TYPE_INFO(int64_t, i64);

} // namespace Tabular
} // namespace MLDB

#endif