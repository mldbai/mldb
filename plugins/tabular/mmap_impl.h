/* mmap.h                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include "mmap.h"
#include "mldb/types/structure_description.h"

namespace MLDB {

#if 0
struct MappedVectorDescriptionBase: public StructureDescriptionBase, public ValueDescription {
    MappedVectorDescriptionBase(const std::type_info * type, uint32_t width, uint32_t align,
                                const std::string & structName,
                                std::shared_ptr<const ValueDescription> underlying)
        : StructureDescriptionBase(type, this, structName, false /* allow null */),
          ValueDescription(ValueKind::STRUCTURE, type, width, align, typeName)
    {
    }

    std::shared_ptr<const ValueDescription> underlying;
};
#endif

template<typename T, typename Ofs>
struct MappedPtrDescription: public StructureDescription<MappedPtr<T, Ofs>> {
    MappedPtrDescription(ConstructOnly)
    {
    }

    MappedPtrDescription()
        : StructureDescription<MappedPtr<T, Ofs>>(true /* null accepted */)
    {
        this->addField("ofs", &MappedPtr<T, Ofs>::ofs, "Offset of pointed to data from pointer's address");
    }

    void initialize()
    {
        MappedPtrDescription newMe;
        *this = std::move(newMe);
    }
};

template<typename T, typename Ofs>
struct MappedVectorDescription: public StructureDescription<MappedVector<T, Ofs>> {
    MappedVectorDescription(ConstructOnly)
    {
    }

    MappedVectorDescription()
        : StructureDescription<MappedVector<T, Ofs>>(true /* null accepted */)
    {
        this->addField("size", &MappedVector<T, Ofs>::size_, "size of the vector");
        this->addField("data", &MappedVector<T, Ofs>::data_, "pointer to data");
    }

    void initialize()
    {
        MappedVectorDescription newMe;
        *this = std::move(newMe);
    }
};

} // namespace MLDB
