/** thrift.h                                                       -*- C++ -*-
    Jeremy Barnes, 30 May 2017

    This file is part of MLDB. Copyright 2017 mldb.ai inc. All rights reserved.

    Definitions to allow us to understand the Thrift serialization protocol.

    Not a complete implementation; being filled in as required.
*/

#pragma once

#include <functional>
#include <iostream>
#include <cstring>
#include "mldb/types/value_description_fwd.h"
#include "mldb/arch/endian.h"
#include "mldb/compiler/compiler.h"

namespace MLDB {


/*****************************************************************************/
/* THRIFT                                                                    */
/*****************************************************************************/

typedef std::function<void (void *, std::istream &)> ThriftDeserializer;

enum ThriftSerializationProtocol {
    THRIFT_COMPACT_PROTOCOL
};

DECLARE_ENUM_DESCRIPTION(ThriftSerializationProtocol);

// https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md

inline uint32_t intToZigZag(int32_t n)
{
    return (n << 1) ^ (n >> 31);
}

inline uint64_t longToZigZag(int64_t n)
{
    return (n << 1) ^ (n >> 63);
}

inline int32_t zigZagToInt(uint32_t n)
{
    return (n >> 1) ^ - (n & 1);
}

inline int64_t zigZagToLong(uint64_t n)
{
    return (n >> 1) ^ - (n & 1);
}

uint64_t thriftCompactDeserializeUnsigned(std::istream & stream);

int64_t thriftCompactDeserializeSigned(std::istream & stream);

float thriftCompactDeserializeFloat(std::istream & stream);

double thriftCompactDeserializeDouble(std::istream & stream);

bool thriftCompactDeserializeBoolean(std::istream & stream);

std::string thriftCompactDeserializeString(std::istream & stream);

void throwThriftException(const std::string & message) MLDB_NORETURN;

template<typename T>
T
thriftStructureDeserializeLiteralT(std::istream & stream)
{
    BigEndian<T> val;
    
    static constexpr size_t NUM_BYTES = sizeof(val);
    
    char b[NUM_BYTES];
    stream.read(b, NUM_BYTES);

    if (!stream) {
        throwThriftException("Error reading thrift type");
    }

    std::memcpy(&val, b, NUM_BYTES);

    return val;
}

enum struct ThriftStructureFieldType: uint8_t {
    BOOLEAN_TRUE = 1,
    BOOLEAN_FALSE = 2,
    BYTE = 3,
    I16 = 4,
    I32 = 5,
    I64 = 6,
    DOUBLE = 7,
    BINARY = 8,
    LIST = 9,
    SET = 10,
    MAP = 11,
    STRUCT = 12
};

DECLARE_ENUM_DESCRIPTION(ThriftStructureFieldType);

enum struct ThriftArrayFieldType: uint8_t {
    BOOL = 2,
    BYTE = 3,
    DOUBLE = 4,
    I16 = 6,
    I32 = 8,
    I64 = 10,
    STRING = 11,
    STRUCT = 12,
    MAP = 13,
    SET = 14,
    LIST = 15
};

DECLARE_ENUM_DESCRIPTION(ThriftArrayFieldType);

ThriftDeserializer
createThriftDeserializer(std::shared_ptr<const ValueDescription> desc,
                         ThriftSerializationProtocol protocol);

template<typename T>
std::function<T (std::istream &)>
createThriftDeserializerT(ThriftSerializationProtocol protocol
                         = THRIFT_COMPACT_PROTOCOL,
                         const std::shared_ptr<const ValueDescription> & desc
                          = getDefaultDescriptionSharedT<T>())
{
    auto deserialize = createThriftDeserializer(desc, protocol);
    return [=] (std::istream & stream)
        {
            T result;
            deserialize(&result, stream);
            return result;
        };
}

} // namespace MLDB

