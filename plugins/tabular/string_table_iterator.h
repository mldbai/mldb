#pragma once

#include <cctype>
#include <iterator>
#include <compare>

// This is a STL-compatible Random Access iterator over a string table.  It enables us
// to use std::lower_bound on StringTable implementations when the strings are sorted.
template<typename StringTableT, typename ValueT>
struct StringTableIterator {
    const StringTableT * owner;
    size_t pos;

    using iterator_category = std::random_access_iterator_tag;
    using value_type = ValueT;
    using difference_type = ssize_t;
    using pointer = const ValueT*;
    using reference = const ValueT&;

    int position() const { return pos; }

    auto operator <=> (const StringTableIterator & other) const = default;

    StringTableIterator & operator++() { pos += 1; return *this; }
    StringTableIterator & operator--() { pos -= 1; return *this; }
    StringTableIterator & operator += (int n) { pos += n;  return *this; }
    StringTableIterator & operator -= (int n) { pos -= n;  return *this; }

    value_type operator * () const { return owner->get(pos); }
    difference_type operator - (const StringTableIterator & other) const { return pos - other.pos; }
};

