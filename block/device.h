/** device.h                                                       -*- C++ -*-
    Jeremy Barnes, 2 April 2018
    Copyight (c) 2018 Element AI Inc.  All rights reserved.
    This file is part of MLDB. Copyright 2016-2018 mldb.ai inc. All rights reserved.

    Device enumeration and memory space functionality for blocks.
*/

#pragma once

namespace MLDB {

struct MemorySpaceInfo {
};

struct DeviceInfo {
};

enum Endianness {
    BIG_ENDIAN,    // Power and most ARM CPUs
    LITTLE_ENDIAN  // Intel CPUs, most GPUs
};

struct MemorySpace {
};

struct Device {
    virtual ~Device();

    virtual std::vector<MemorySpace> getMemorySpaces() = 0;
    virtual Endianness getEndianness() const = 0;
};


}  // namespace MLDB
