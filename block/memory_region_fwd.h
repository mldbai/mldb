/** memory_region.h                                                -*- C++ -*-
    Jeremy Barnes, 27 March 2016
    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.

    Basic primitives around memory regions.  Once frozen, these are the
    representation that covers CPU memory, device memory and remote
    memory and implements the primitives that allow data to be made
    available and brought to the compute resources required.
*/

#pragma once

namespace MLDB {

struct FrozenMemoryRegion;
template<typename T>
struct FrozenMemoryRegionT;
struct MutableMemoryRegion;
template<typename T>
struct MutableMemoryRegionT;

struct MappedSerializer;
struct StructuredSerializer;
struct StructuredReconstituter;

struct MemoryRegionHandleInfo;
struct MemoryRegionHandle;
template<typename T>
struct MemoryArrayHandleT;

} // namespace MLDB
