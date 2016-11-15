/** endian.h                                                       -*- C++ -*-
    Jeremy Barnes, 1 August 2016
    Copyright (c) 2016 Datacratic Inc.  All rights reserved.

    Code to deal with endianness in MLDB.
*/

#pragma once

#include <cstdint>

namespace MLDB {

inline constexpr uint8_t host_to_be(uint8_t v)
{
    return v;
}

inline constexpr uint8_t be_to_host(uint8_t v)
{
    return v;
}

inline constexpr int8_t host_to_be(int8_t v)
{
    return v;
}

inline constexpr int8_t be_to_host(int8_t v)
{
    return v;
}

inline constexpr uint8_t host_to_le(uint8_t v)
{
    return v;
}

inline constexpr int8_t host_to_le(int8_t v)
{
    return v;
}

inline constexpr uint8_t le_to_host(uint8_t v)
{
    return v;
}

inline constexpr int8_t le_to_host(int8_t v)
{
    return v;
}

#if (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)

inline constexpr uint16_t host_to_be(uint16_t v)
{
    return __builtin_bswap16(v);
}

inline constexpr uint16_t be_to_host(uint16_t v)
{
    return __builtin_bswap16(v);
}

inline constexpr int16_t host_to_be(int16_t v)
{
    return __builtin_bswap16(v);
}

inline constexpr int16_t be_to_host(int16_t v)
{
    return __builtin_bswap16(v);
}

inline constexpr uint32_t host_to_be(uint32_t v)
{
    return __builtin_bswap32(v);
}

inline constexpr uint32_t be_to_host(uint32_t v)
{
    return __builtin_bswap32(v);
}

inline constexpr int32_t host_to_be(int32_t v)
{
    return __builtin_bswap32(v);
}

inline constexpr int32_t be_to_host(int32_t v)
{
    return __builtin_bswap32(v);
}

inline constexpr uint64_t host_to_be(uint64_t v)
{
    return __builtin_bswap64(v);
}

inline constexpr uint64_t be_to_host(uint64_t v)
{
    return __builtin_bswap64(v);
}

inline constexpr int64_t host_to_be(int64_t v)
{
    return __builtin_bswap64(v);
}

inline constexpr int64_t be_to_host(int64_t v)
{
    return __builtin_bswap64(v);
}

inline constexpr uint16_t host_to_le(uint16_t v)
{
    return v;
}

inline constexpr uint16_t le_to_host(uint16_t v)
{
    return v;
}

inline constexpr int16_t host_to_le(int16_t v)
{
    return v;
}

inline constexpr int16_t le_to_host(int16_t v)
{
    return v;
}

inline constexpr uint32_t host_to_le(uint32_t v)
{
    return v;
}

inline constexpr uint32_t le_to_host(uint32_t v)
{
    return v;
}

inline constexpr int32_t host_to_le(int32_t v)
{
    return v;
}

inline constexpr int32_t le_to_host(int32_t v)
{
    return v;
}

inline constexpr uint64_t host_to_le(uint64_t v)
{
    return v;
}

inline constexpr uint64_t le_to_host(uint64_t v)
{
    return v;
}

inline constexpr int64_t host_to_le(int64_t v)
{
    return v;
}

inline constexpr int64_t le_to_host(int64_t v)
{
    return v;
}

#else

inline constexpr uint16_t host_to_be(uint16_t v)
{
    return __builtin_bswap16(v);
}

inline constexpr uint16_t be_to_host(uint16_t v)
{
    return __builtin_bswap16(v);
}

inline constexpr uint32_t host_to_be(uint32_t v)
{
    return __builtin_bswap32(v);
}

inline constexpr uint32_t be_to_host(uint32_t v)
{
    return __builtin_bswap32(v);
}

inline constexpr uint64_t host_to_be(uint64_t v)
{
    return __builtin_bswap64(v);
}

inline constexpr uint64_t be_to_host(uint64_t v)
{
    return __builtin_bswap64(v);
}

inline constexpr uint16_t host_to_le(uint16_t v)
{
    return v;
}

inline constexpr uint16_t le_to_host(uint16_t v)
{
    return v;
}

inline constexpr int16_t host_to_le(int16_t v)
{
    return v;
}

inline constexpr int16_t le_to_host(int16_t v)
{
    return v;
}

inline constexpr uint32_t host_to_le(uint32_t v)
{
    return v;
}

inline constexpr uint32_t le_to_host(uint32_t v)
{
    return v;
}

inline constexpr int32_t host_to_le(int32_t v)
{
    return v;
}

inline constexpr int32_t le_to_host(int32_t v)
{
    return v;
}

inline constexpr uint64_t host_to_le(uint64_t v)
{
    return v;
}

inline constexpr uint64_t le_to_host(uint64_t v)
{
    return v;
}

inline constexpr int64_t host_to_le(int64_t v)
{
    return v;
}

inline constexpr int64_t le_to_host(int64_t v)
{
    return v;
}

#endif
template<typename Base>
struct BigEndian {
#if 0
    constexpr BigEndian(Base val = Base())
        : val(host_to_be(val))
    {
    }
#endif

    Base val;

    constexpr operator Base () const
    {
        return be_to_host(val);
    }

    BigEndian & operator = (Base val)
    {
        this->val = host_to_be(val);
        return *this;
    }
};

template<typename Base>
struct LittleEndian {
#if 0
    constexpr LittleEndian(Base val = Base())
        : val(host_to_le(val))
    {
    }
#endif

    Base val;

    constexpr operator Base () const
    {
        return le_to_host(val);
    }

    LittleEndian & operator = (Base val)
    {
        this->val = host_to_le(val);
        return *this;
    }
};

typedef LittleEndian<uint16_t> uint16_le;
typedef LittleEndian<int16_t> int16_le;
typedef LittleEndian<uint32_t> uint32_le;
typedef LittleEndian<int32_t> int32_le;
typedef LittleEndian<uint64_t> uint64_le;
typedef LittleEndian<int64_t> int64_le;

typedef BigEndian<uint16_t> uint16_be;
typedef BigEndian<int16_t> int16_be;
typedef BigEndian<uint32_t> uint32_be;
typedef BigEndian<int32_t> int32_be;
typedef BigEndian<uint64_t> uint64_be;
typedef BigEndian<int64_t> int64_be;

} // namespace MLDB
