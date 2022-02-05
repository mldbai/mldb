/** compute_kernel_call_utils.h                                                -*- C++ -*-
    Jeremy Barnes, 2 February 2022
    This file is part of MLDB.

    Utilities for calling compute kernels behind a generic (type erased) interface.
*/

#pragma once

#include <memory>
#include <span>
#include "mldb/types/value_description_fwd.h"
#include "mldb/arch/exception.h"

namespace MLDB {

namespace details {

// Implemented in .cc file to avoid including value_description.h
// Copies from to to via the value description
void copyUsingValueDescription(const ValueDescription * desc,
                               std::span<const std::byte> from, void * to,
                               const std::type_info & toType);

const std::type_info & getTypeFromValueDescription(const ValueDescription * desc);

std::shared_ptr<ValueDescription>
makeGenericEnumDescription(std::shared_ptr<const ValueDescription> underlying, std::string typeName, const std::type_info * tinfo);
std::shared_ptr<ValueDescription>
makeGenericAtomDescription(size_t width, size_t align, std::string typeName, const std::type_info * tinfo);

using Pin = std::shared_ptr<const void>;

template<typename T>
auto getBestValueDescription(T *, std::enable_if_t<MLDB::has_default_description_v<T>> * = 0)
{
    return getDefaultDescriptionSharedT<T>();
}

std::shared_ptr<const ValueDescription> getValueDescriptionForType(const std::type_info & type);

template<typename T>
std::shared_ptr<const ValueDescription>
getBestValueDescription(T *, std::enable_if_t<!MLDB::has_default_description_v<T>> * = 0)
{
    auto result = getValueDescriptionForType(typeid(T));

    if (result) {
        return result;
    }

    if constexpr (std::is_enum_v<T>) {
        return makeGenericEnumDescription(getDefaultDescriptionSharedT<std::underlying_type_t<T>>(), type_name<T>(), &typeid(T));
    }
    else {
        return makeGenericAtomDescription(sizeof(T), alignof(T), type_name<T>(), &typeid(T));
    }

    throw MLDB::Exception("Couldn't find any kind of value description for type " + type_name<T>());
}

template<typename T>
auto getBestValueDescriptionT()
{
    return getBestValueDescription((T*)0);
}

} // namespace details
} // namespace MLDB