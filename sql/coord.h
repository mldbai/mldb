/** coord.h                                                        -*- C++ -*-
    Jeremy Barnes, 29 January 2016
    Copyright (c) 2016 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.
*/

#include "dataset_fwd.h"
#include "mldb/types/string.h"
#include "mldb/types/value_description_fwd.h"

// NOTE TO MLDB DEVELOPERS: This is an API header file.  No includes
// should be added, especially value_description.h.

#pragma once

namespace Datacratic {
namespace MLDB {

/*****************************************************************************/
/* COORD                                                                     */
/*****************************************************************************/

/** This is a coordinate: a list of discrete (integer or string) values
    that are used to index rows, columns, etc within MLDB.

    It can deal with string representations (with dotted values) as well
    as their destructured versions.

    It takes up 32 bytes, and will do its best to inline whatever coordinates
    it is storing.
*/

struct Coord {
    Coord();
    Coord(Utf8String str);
    Coord(std::string str);
    Coord(const char * str);
    Coord(const char * str, size_t len);

    bool stringEqual(const std::string & other) const;
    bool stringEqual(const Utf8String & other) const;
    bool stringEqual(const char * other) const;

    bool stringLess(const std::string & other) const;
    bool stringLess(const Utf8String & other) const;
    bool stringLess(const char * other) const;

    bool stringGreaterEqual(const std::string & other) const;
    bool stringGreaterEqual(const Utf8String & other) const;
    bool stringGreaterEqual(const char * other) const;
    
    bool operator == (const Coord & other) const;
    bool operator != (const Coord & other) const;
    bool operator <  (const Coord & other) const;

    Utf8String toUtf8String() const;
    std::string toString() const;  // TODO: will disappear

    uint64_t hash() const;

    bool empty() const;

    Coord operator + (const Coord & other) const;
    Coord operator + (Coord && other) const;

    operator RowHash() const;
    operator ColumnHash() const;

private:
    Utf8String str;
};

std::ostream & operator << (std::ostream & stream, const Coord & id);

std::istream & operator >> (std::istream & stream, Coord & id);

inline Utf8String to_string(const Coord & coord)
{
    return coord.toUtf8String();
}

inline Coord stringToKey(const Utf8String & str, Coord *)
{
    return Coord(str);
}

inline Coord stringToKey(const std::string & str, Coord *)
{
    return Coord(str);
}

PREDECLARE_VALUE_DESCRIPTION(Coord);

} // namespace MLDB

} // namespace Datacratic

namespace std {

template<typename T> struct hash;

template<>
struct hash<Datacratic::MLDB::Coord> : public std::unary_function<Datacratic::MLDB::Coord, size_t>
{
    size_t operator()(const Datacratic::MLDB::Coord & coord) const
    {
        return coord.hash();
    }
};

} // namespace std
