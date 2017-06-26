/* portable_oarchive.h                                             -*- C++ -*-
   Jeremy Barnes, 17 March 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.   


   ---

   Portable output archive.
*/

#pragma once


#include "mldb/arch/exception.h"
#include "mldb/arch/endian.h"
#include "compact_size_types.h"
#include <cstring>
#include <memory>

namespace std {

template<class T, class A>
class vector;

template<class K, class V, class L, class A>
class map;

template<class K, class V, class H, class P, class A>
class unordered_map;

template<class V, class L, class A>
class set;

} // namespace std

namespace boost {

template <typename T, std::size_t NumDims, typename TPtr>
class const_multi_array_ref;

} // namespace boost

namespace ML {
using namespace MLDB;

namespace DB {


class Nested_Writer;


/*****************************************************************************/
/* PORTABLE_BIN_OARCHIVE                                                     */
/*****************************************************************************/

class portable_bin_oarchive {
public:
    portable_bin_oarchive();
    portable_bin_oarchive(const std::string & filename);
    portable_bin_oarchive(std::ostream & stream);

    portable_bin_oarchive(const portable_bin_oarchive &) = delete;
    void operator = (const portable_bin_oarchive &) = delete;

    void open(const std::string & filename);
    void open(std::ostream & stream);

    void save(const Nested_Writer & writer);

    void save_binary(const void * address, size_t size)
    {
        if (!stream)
            throw Exception("Writing to unopened portable_bin_oarchive");
        stream->write((char *)address, size);
        offset_ += size;
        if (!stream)
            throw Exception("Error writing to stream");
    }

    /** Warning: doesn't do byte order conversions or anything like that. */
    template<typename T>
    void save_binary(const T & val)
    {
        save_binary(&val, sizeof(T));
    }

#if 0
    template<typename T>
    void save(const T & obj)
    {
        obj.serialize(*this);
    }
#endif

    size_t offset() const { return offset_; }

private:
    std::ostream * stream;
    std::shared_ptr<std::ostream> owned_stream;
    size_t offset_;
};

inline void save(portable_bin_oarchive & archive, unsigned char x)
{
    archive.save_binary(&x, 1);
}

inline void save(portable_bin_oarchive & archive, signed char x)
{
    archive.save_binary(&x, 1);
}

inline void save(portable_bin_oarchive & archive, char x)
{
    archive.save_binary(&x, 1);
}

inline void save(portable_bin_oarchive & archive, unsigned short x)
{
    uint16_t xx = x;
    if (xx != x) throw Exception("truncated");
    xx = MLDB::host_to_le(xx);
    archive.save_binary(&xx, 2);
}

inline void save(portable_bin_oarchive & archive, signed short x)
{
    int16_t xx = x;
    if (xx != x) throw Exception("truncated");
    xx = MLDB::host_to_le(xx);
    archive.save_binary(&xx, 2);
}

inline void save(portable_bin_oarchive & archive, unsigned int x)
{
    uint32_t xx = x;
    if (xx != x) throw Exception("truncated");
    xx = MLDB::host_to_le(xx);
    archive.save_binary(&xx, 4);
}

inline void save(portable_bin_oarchive & archive, signed int x)
{
    int32_t xx = x;
    if (xx != x) throw Exception("truncated");
    xx = MLDB::host_to_le(xx);
    archive.save_binary(&xx, 4);
}
    
inline void save(portable_bin_oarchive & archive, unsigned long x)
{
    compact_size_t sz(x);
    sz.serialize(archive);
}

inline void save(portable_bin_oarchive & archive, signed long x)
{
    compact_int_t sz(x);
    sz.serialize(archive);
}

inline void save(portable_bin_oarchive & archive, unsigned long long x)
{
    compact_size_t sz(x);
    sz.serialize(archive);
}

inline void save(portable_bin_oarchive & archive, signed long long x)
{
    compact_int_t sz(x);
    sz.serialize(archive);
}
    
inline void save(portable_bin_oarchive & archive, bool x)
{
    unsigned char xx = x;
    archive.save_binary(&xx, 1);
}
    
inline void save(portable_bin_oarchive & archive, float x)
{
    // Saved as the 4 bytes in network order
    x = MLDB::host_to_le(x);
    archive.save_binary(&x, 4);
}

inline void save(portable_bin_oarchive & archive, double x)
{
    // Saved as the 8 bytes in network order
    x = MLDB::host_to_le(x);
    archive.save_binary(&x, 8);
}
    
inline void save(portable_bin_oarchive & archive, long double x)
{
    throw Exception("long double not supported yet");
}

inline void save(portable_bin_oarchive & archive, const std::string & str)
{
    compact_size_t size(str.length());
    size.serialize(archive);
    archive.save_binary(&str[0], size);
}

inline void save(portable_bin_oarchive & archive, const char * str)
{
    compact_size_t size(std::strlen(str));
    size.serialize(archive);
    archive.save_binary(str, size);
}

template<class T, class A>
void save(portable_bin_oarchive & archive, const std::vector<T, A> & vec)
{
    compact_size_t size(vec.size());
    size.serialize(archive);
    for (unsigned i = 0;  i < vec.size();  ++i)
        archive << vec[i];
}

template<class K, class V, class L, class A>
void save(portable_bin_oarchive & archive, const std::map<K, V, L, A> & m)
{
    compact_size_t size(m.size());
    size.serialize(archive);
    for (auto it = m.begin(), end = m.end(); it != end;  ++it)
        archive << it->first << it->second;
}
    
template<class K, class V, class H, class P, class A>
void save(portable_bin_oarchive & archive,
          const std::unordered_map<K, V, H, P, A> & m)
{
    compact_size_t size(m.size());
    size.serialize(archive);
    for (auto it = m.begin(), end = m.end(); it != end;  ++it)
        archive << it->first << it->second;
}

template<class V, class L, class A>
void save(portable_bin_oarchive & archive, const std::set<V, L, A> & m)
{
    compact_size_t size(m.size());
    size.serialize(archive);
    for (auto it = m.begin(), end = m.end();  it != end;  ++it)
        archive << *it;
}

template<typename T, std::size_t NumDims, typename TPtr>
void save(portable_bin_oarchive & archive,
          const boost::const_multi_array_ref<T, NumDims, TPtr> & arr)
{
    char version = 1;
    using ML::DB::save;
    save(archive, version);
    char nd = NumDims;
    save(archive, nd);
    for (unsigned i = 0;  i < NumDims;  ++i) {
        compact_size_t dim(arr.shape()[i]);
        dim.serialize(archive);
    }

    size_t ne = arr.num_elements();
    const T * el = arr.data();
    for (unsigned i = 0;  i < ne;  ++i, ++el)
        archive << *el;
}

template<typename T1, typename T2>
void save(portable_bin_oarchive & archive,
          const std::pair<T1, T2> & p)
{
    using ML::DB::save;
    save(archive, p.first);
    save(archive, p.second);
}

// Anything with a serialize() method gets to be serialized
template<typename T>
void save(portable_bin_oarchive & archive,
          const T & obj,
          decltype(((T *)0)->serialize(*(portable_bin_oarchive *)0)) * = 0)
{
    obj.serialize(archive);
}


} // namespace DB
} // namespace ML
