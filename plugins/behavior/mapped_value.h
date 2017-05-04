// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/** mapped_value.h                                                 -*- C++ -*-
    Jeremy Barnes, 28 March 2012
    Copyright (c) 2012 mldb.ai inc.  All rights reserved.

    Basic memory mapped structure support.
*/

#pragma once


#include "mldb/jml/utils/file_functions.h"
#include "mldb/arch/exception.h"
#include "mldb/plugins/behavior/id.h"
#include "mldb/compiler/compiler.h"
#include "mldb/arch/demangle.h"
#include <algorithm>


namespace MLDB {

/** Return the last 8 bytes of the buffer as an offset. */
uint64_t readTrailingOffset(const ML::File_Read_Buffer & buf);


/*****************************************************************************/
/* MAPPED VALUE                                                              */
/*****************************************************************************/

template<typename T>
struct MappedValue {

    MappedValue()
        : ptr(0)
    {
    }

    void init(const char * start, uint64_t offset)
    {
        ptr = reinterpret_cast<const T *>(start + offset);
    }

    void init(const ML::File_Read_Buffer & buf, uint64_t offset)
    {
        init(buf.start(), offset);
        if (buf.start() + offset + sizeof(T) > buf.end())
            throw MLDB::Exception("invalid mapped value initialization for "
                                + MLDB::type_name<T>());
    }

    const T & operator * () const
    {
        return *ptr;
    }

    const T * operator -> () const
    {
        return ptr;
    }

    const T * ptr;
};


/*****************************************************************************/
/* MAPPED ARRAY                                                              */
/*****************************************************************************/

template<typename T>
struct MappedArray {
    MappedArray()
        : ptr(0), len(0)
    {
    }

    void init(const ML::File_Read_Buffer & buf,
              uint64_t offset,
              int len)
    {
        this->ptr = reinterpret_cast<const T *>(buf.start() + offset);
        this->len = len;
    }

    void init(const char * start, uint64_t offset, int len)
    {
        this->ptr = reinterpret_cast<const T *>(start + offset);
        this->len = len;
    }

    const T & operator [] (int index) const
    {
        if (index < 0 || index >= len)
            throw MLDB::Exception("accessing invalid index in mapped array: "
                                "%d not in [0-%d]", index, len);
        return ptr[index];
    }
    
    const T & at(int index) const
    {
        return operator [] (index);
    }

    size_t size() const { return len; }
    
    const T * ptr;
    int len;

    typedef const T * const_iterator;
    const_iterator begin() const { return ptr; }
    const_iterator end() const { return ptr + len; }
};


/*****************************************************************************/
/* MAPPED SORTED ARRAY                                                       */
/*****************************************************************************/

template<typename T,
         typename Less = std::less<T>,
         typename Equal = std::equal_to<T> >
struct MappedSortedArray
    : public MappedArray<T> {

    typedef typename MappedArray<T>::const_iterator const_iterator;
    using MappedArray<T>::begin;
    using MappedArray<T>::end;
    using MappedArray<T>::operator [];
    using MappedArray<T>::at;

    template<typename V>
    const_iterator find(V val) const
    {
        int i = indexOf(val);
        if (i == -1) return end();
        return begin() + i;
    }

    template<typename V>
    const_iterator lower_bound(V val) const
    {
        return std::lower_bound(begin(), end(), val, Less());
    }

    template<typename V>
    const_iterator upper_bound(V val) const
    {
        return std::upper_bound(begin(), end(), val, Less());
    }

    template<typename V>
    bool count(V val)
    {
        return indexOf(val) != -1;
    }
    
    template<typename V>
    int indexOf(V val) const
    {
        int i = lowerIndexOf(val);
        //cerr << "i = " << i << " len = " << this->len << endl;
        if (i == -1 || i == this->len || !(Equal()(at(i), val))) return -1;
        return i;
    }

    template<typename V>
    int lowerIndexOf(V val) const
    {
        return std::lower_bound(begin(), end(), val, Less()) - begin();
    }

    template<typename V>
    int upperIndexOf(V val) const
    {
        return std::upper_bound(begin(), end(), val, Less()) - begin();
    }
};


/*****************************************************************************/
/* KV ENTRY                                                                  */
/*****************************************************************************/

template<typename K, typename V>
struct KVEntry {

    K key;
    V value;

    bool operator < (const KVEntry & other) const
    {
        return key < other.key;
    }

    static K extractKey(K key)
    {
        return key;
    }

    static K extractKey(const KVEntry & kv)
    {
        return kv.key;
    }

    struct Less {
        template<typename K1, typename K2>
        bool operator () (const K1 & key1, const K2 & key2) const
        {
            return extractKey(key1) < extractKey(key2);
        }
    };

    struct Equal {
        template<typename K1, typename K2>
        bool operator () (const K1 & key1, const K2 & key2) const
        {
            return extractKey(key1) == extractKey(key2);
        }
    };
} MLDB_PACKED;


/*****************************************************************************/
/* MAPPED SORTED KEY VALUE ARRAY                                             */
/*****************************************************************************/

template<typename K, typename V, class Entry = KVEntry<K, V> >
struct MappedSortedKeyValueArray
    : public MappedSortedArray<Entry,
                               typename Entry::Less,
                               typename Entry::Equal> {

    V get(K key, V def) const
    {
        int idx = this->indexOf(key);
        if (idx == -1) return def;
        return this->at(idx).value;
    }
};

} // namespace MLDB
