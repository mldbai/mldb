/* portable_iarchive.h                                             -*- C++ -*-
   Jeremy Barnes, 13 March 2005
   Copyright (c) 2005 Jeremy Barnes.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.


   ---

   Portable format for iarchives.
*/

#pragma once

#include <algorithm>
#include "mldb/arch/exception.h"
#include "mldb/arch/endian.h"
#include "compact_size_types.h"
#include <memory>
#include <sstream>

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

template<typename T, std::size_t NumDims, class Allocator>
class multi_array;

} // namespace boost

namespace ML {

using namespace MLDB;
class File_Read_Buffer;

namespace DB {

/*****************************************************************************/
/* BINARY_INPUT                                                              */
/*****************************************************************************/

/* This is a class that can get its input from an istream or a buffer. */

struct Binary_Input {
public:
    Binary_Input();
    Binary_Input(const File_Read_Buffer & buf);
    Binary_Input(const std::string & filename);
    Binary_Input(std::istream & stream);
    Binary_Input(const char * c, size_t len);

    void open(const File_Read_Buffer & buf);
    void open(const std::string & filename);
    void open(std::istream & stream);
    void open(const char * c, size_t len);

    size_t avail() const { return end_ - pos_; }

    size_t must_have(size_t amount)
    {
        if (avail() < amount) make_avail(amount);
        return avail();
    }

    /** Trys to make it available, but if it's not then it just returns
        what is available. */
    size_t try_to_have(size_t amount)
    {
        if (avail() < amount) return try_make_avail(amount);
        return avail();
    }

    void skip(size_t amount)
    {
        if (amount > avail())
            make_avail(amount);

        if (avail() < amount)
            throw Exception("skipped past end of store");

        offset_ += amount;
        pos_ += amount;
    }

    //const char * start() const { return start_; }
    const char * pos() const { return pos_; }
    const char * end() const { return end_; }

    char operator * () const { return *pos_; }

    char operator [] (int idx) const { return pos_[idx]; }

    size_t offset() const { return offset_; }

private:
    size_t offset_;       ///< Offset of start from archive start
    const char * pos_;    ///< Position in memory region
    const char * end_;    ///< End of memory region

    void make_avail(size_t min_avail);
    size_t try_make_avail(size_t amount);    

    struct Source;
    struct Buffer_Source;
    struct Stream_Source;
    struct No_Source;
    std::shared_ptr<Source> source;
};


/*****************************************************************************/
/* PORTABLE_BIN_IARCHIVE                                                     */
/*****************************************************************************/

class portable_bin_iarchive
    : public Binary_Input {
public:
    portable_bin_iarchive();
    portable_bin_iarchive(const File_Read_Buffer & buf);
    portable_bin_iarchive(const std::string & filename);
    portable_bin_iarchive(std::istream & stream);
    portable_bin_iarchive(const char * c, size_t sz);

    void load_binary(void * address, size_t size)
    {
        must_have(size);
        std::copy(pos(), pos() + size,
                  reinterpret_cast<char *>(address));
        skip(size);
    }

#if 0
    template<typename T>
    void load(T & obj)
    {
        obj.reconstitute(*this);
    }
#endif
};

inline void load(portable_bin_iarchive & archive, unsigned char & x)
{
    archive.load_binary(&x, 1);
}

inline void load(portable_bin_iarchive & archive, signed char & x)
{
    archive.load_binary(&x, 1);
}

inline void load(portable_bin_iarchive & archive, char & x)
{
    archive.load_binary(&x, 1);
}

inline void load(portable_bin_iarchive & archive, unsigned short & x)
{
    uint16_t xx;
    archive.load_binary(&xx, 2);
    x = le_to_host(xx);
}

inline void load(portable_bin_iarchive & archive, signed short & x)
{
    int16_t xx;
    archive.load_binary(&xx, 2);
    x = le_to_host(xx);
}

inline void load(portable_bin_iarchive & archive, unsigned int & x)
{
    uint32_t xx;
    archive.load_binary(&xx, 4);
    x = le_to_host(xx);
}

inline void load(portable_bin_iarchive & archive, signed int & x)
{
    int32_t xx;
    archive.load_binary(&xx, 4);
    x = le_to_host(xx);
}

inline void load(portable_bin_iarchive & archive, unsigned long & x)
{
    compact_size_t sz(archive);
    x = sz;
}

inline void load(portable_bin_iarchive & archive, signed long & x)
{
    compact_int_t sz(archive);
    x = sz;
}

inline void load(portable_bin_iarchive & archive, unsigned long long & x)
{
    compact_size_t sz(archive);
    x = sz;
}

inline void load(portable_bin_iarchive & archive, signed long long & x)
{
    compact_int_t sz(archive);
    x = sz;
}
    
inline void load(portable_bin_iarchive & archive, bool & x)
{
    unsigned char xx;
    archive.load_binary(&xx, 1);
    x = xx;
}
    
inline void load(portable_bin_iarchive & archive, float & x)
{
    archive.load_binary(&x, 4);
    x = le_to_host(x);
}
    
inline void load(portable_bin_iarchive & archive, double & x)
{
    archive.load_binary(&x, 8);
    x = le_to_host(x);
}
    
inline void load(portable_bin_iarchive & archive, long double & x)
{
    throw Exception("long double not supported yet");
}

inline void load(portable_bin_iarchive & archive, std::string & str)
{
    compact_size_t size(archive);
    str.resize(size);
    archive.load_binary(&str[0], size);
}

inline void load(portable_bin_iarchive & archive, const char * & str)
{
    compact_size_t size(archive);
    char * res = new char[size];  // keep track of this?
    archive.load_binary(res, size);
    str = res;
}

template<class T, class A>
void load(portable_bin_iarchive & archive, std::vector<T, A> & vec)
{
    compact_size_t sz(archive);

    std::vector<T, A> v;
    v.reserve(sz);
    for (unsigned i = 0;  i < sz;  ++i) {
        T t;
        archive >> t;
        v.push_back(t);
    }
    vec.swap(v);
}

template<class K, class V, class L, class A>
void load(portable_bin_iarchive & archive, std::map<K, V, L, A> & res)
{
    compact_size_t sz(archive);

    std::map<K, V, L, A> m;
    for (unsigned i = 0;  i < sz;  ++i) {
        K k;
        archive >> k;
        V v;
        archive >> v;
        m.insert(std::make_pair(k, v));
    }
    res.swap(m);
}

template<class K, class V, class H, class P, class A>
void load(portable_bin_iarchive & archive,
          std::unordered_map<K, V, H, P, A> & res)
{
    compact_size_t sz(archive);

    std::unordered_map<K, V, H, P, A> m;
    for (unsigned i = 0;  i < sz;  ++i) {
        K k;
        archive >> k;
        V v;
        archive >> v;
        m.insert(std::make_pair(k, v));
    }
    res.swap(m);
}

template<class V, class L, class A>
void load(portable_bin_iarchive & archive, std::set<V, L, A> & res)
{
    compact_size_t sz(archive);

    std::set<V, L, A> m;
    for (unsigned i = 0;  i < sz;  ++i) {
        V v;
        archive >> v;
        m.insert(v);
    }
    res.swap(m);
}

template<typename T1, typename T2>
void load(portable_bin_iarchive & archive, std::pair<T1, T2> & p)
{
    load(archive, p.first);
    load(archive, p.second);
}

template<typename T, std::size_t NumDims, class Allocator>
void load(portable_bin_iarchive & archive,
          boost::multi_array<T, NumDims, Allocator> & arr)
{
    using namespace std;
    char version;
    load(archive, version);
    if (version != 1)
        throw Exception("unknown multi array version");

    char nd;
    load(archive, nd);
    if (nd != NumDims)
        throw Exception("NumDims wrong");

    std::array<size_t, NumDims> sizes;
    for (unsigned i = 0;  i < NumDims;  ++i) {
        compact_size_t sz(archive);
        sizes[i] = sz;
    }

    arr.resize(sizes);

    size_t ne = arr.num_elements();
    T * el = arr.data();
    for (unsigned i = 0;  i < ne;  ++i, ++el)
        archive >> *el;
}

// Anything with a serialize() method gets to be serialized
template<typename T>
void load(portable_bin_iarchive & archive,
          T & obj,
          decltype(((T *)0)->reconstitute(*(portable_bin_iarchive *)0)) * = 0)
{
    obj.reconstitute(archive);
}


} // namespace DB
} // namespace ML
