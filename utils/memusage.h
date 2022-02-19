// Memory usage calculations for C++ data structures
// Allows one to estimate the direct (base structure) and indirect (pointed-to members)
// memory consumption of a structure.

#pragma once


#include <type_traits>
#include <string>
#include <string_view>
#include <map>
#include <unordered_map>
#include <utility>

// This file contains a set of functions to help calculate the actual memory usage of a data
// structure.
//
// The basic premise is that a data structure in C++ has two types of memory usage:
// - Direct memory usage, which is the sizeof(object)
// - Indirect memory usage, which is memory that is owned and allocated by the object
//
// Plain old data types contain only direct memory usage.  Typically, for something to have
// indirect memory, it needs to have a pointer.
//
// Each type of object needs to define a memUsageIndirect() implementation that gives
// the indirect memory usage.
//
// This is done automatically for trivial types.  For others, either:
// * a memUsageIndirect(object, MemUsageOptions) function needs to be defined in a namespace
//   that will allow it to be found by argument dependent lookup, or
// * the object needs to implement a object.memUsageIndirect(MemUsageOptions) const member
//   function.

namespace MLDB {

// Options to pass to MemUsage.
struct MemUsageOptions {
    bool includeUnused = false;  ///< Include allocated but unused capacity
};

// Trivial types have no indirect memory usage as they can contain no pointers.
template<typename T>
inline constexpr size_t memUsageIndirect(const T & /* obj */, const MemUsageOptions & /* opt */,
                               typename std::enable_if<std::is_trivial_v<T>>::type * = 0)
{
    return 0;
}

// The total memory usage of an object is its direct plus indirect memory usage
template<typename T>
inline size_t memUsage(const T & obj, const MemUsageOptions & opt = MemUsageOptions())
{
    return sizeof(obj) + memUsageIndirect(obj, opt);
}

// Strings have optimizations that cache shorter strings inside the structure, so we need
// to handle it carefully here to get an accurate result.
inline size_t memUsageIndirect(const std::string & s, const MemUsageOptions & opt)
{
    size_t result = opt.includeUnused ? s.capacity() : s.size();
    // Short string optimization
    // https://stackoverflow.com/questions/27631065/why-does-libcs-implementation-of-stdstring-take-up-3x-memory-as-libstdc/28003328#28003328
    if (result <= 22)
        result = 0;
    return result;
 }

// Strings have optimizations that cache shorter strings inside the structure, so we need
// to handle it carefully here to get an accurate result.
inline size_t memUsageIndirect(const std::u16string & s, const MemUsageOptions & opt)
{
    size_t result = opt.includeUnused ? s.capacity() : s.size();
    // Short string optimization
    // https://stackoverflow.com/questions/27631065/why-does-libcs-implementation-of-stdstring-take-up-3x-memory-as-libstdc/28003328#28003328
    if (result <= 11)
        result = 0;
    return result * 2;
 }

// Indirect memory usage of an array is the indirect usage of each of its elements.
template<typename T>
inline size_t memUsageIndirectArray(const T * first, size_t n,
                                    const MemUsageOptions & opt,
                                    typename std::enable_if<!std::is_trivial_v<T>>::type * = 0)
{
    size_t result = 0;
    for (size_t i = 0;  i < n;  ++i)
        result += memUsageIndirect(first[i], opt);
    return result;
}

// Indirect memory usage of an array of trivial objects is zero
template<typename T>
inline constexpr size_t
memUsageIndirectArray(const T * /* first */, size_t /* n */,
                      const MemUsageOptions & /* opt */,
                      typename std::enable_if<std::is_trivial_v<T>>::type * = 0)
{
    return 0;
}

// Helper function for indirect memory usage of a range
template<typename It>
inline size_t memUsageIndirectRange(It first, It last,
                                    const MemUsageOptions & opt,
                                    typename std::enable_if<!std::is_trivial_v<decltype(*std::declval<It>())>>::type * = 0)
{
    size_t result = 0;
    for (; first != last;  ++first)
        result += memUsageIndirect(*first, opt);
    return result;
}

// Helper function for indirect memory usage of a range of trivial objects, which is zero
template<typename It>
inline constexpr size_t
memUsageIndirectRange(It /* first */, It /* last */,
                      const MemUsageOptions & /* opt */,
                      typename std::enable_if<std::is_trivial_v<decltype(*std::declval<It>())>>::type * = 0)
{
    return 0;
}

// Indirect usage of a vector is that of its elements
template<typename T, typename Alloc>
inline size_t memUsageIndirect(const std::vector<T, Alloc> & v, const MemUsageOptions & opt)
{
    size_t result = (opt.includeUnused ? v.capacity() : v.size()) * sizeof(T);
    result += memUsageIndirectArray(v.data(), v.size(), opt);
    return result;
}

// Indirect usage of a pair is that of each of its elements
template<typename T1, typename T2>
inline size_t memUsageIndirect(const std::pair<T1, T2> & p, const MemUsageOptions & opt)
{
    return memUsageIndirectMany(opt, p.first, p.second);
}

// Indirect usage of an optional is that of the value, if it has one
template<typename T>
inline size_t memUsageIndirect(const std::optional<T> & o, const MemUsageOptions & opt)
{
    return o.has_value() ? memUsageIndirect(o.value(), opt) : 0;
}

// Sring views don't own their memory, so they have no indirect memory usage
constexpr inline size_t memUsageIndirect(const std::string_view &, const MemUsageOptions & /* opt */)
{
    return 0;
}

// A map has the memory usage of its internal nodes along with the indirect usage
// of its keys and values.
template<typename K, typename V, typename Cmp, typename Alloc>
inline size_t memUsageIndirect(const std::map<K, V, Cmp, Alloc> & m, const MemUsageOptions & opt)
{
    size_t result = m.size() * sizeof(typename std::map<K, V, Cmp, Alloc>::value_type);
    // Assume internal nodes have 3 pointers
    result += 3 * sizeof(void *) * m.size();
    result += memUsageIndirectRange(m.begin(), m.end(), opt);
    return result;
}

// A map has the memory usage of its buckets along with the indirect usage
// of its keys and values.
template<typename K, typename V, typename Hash, typename Cmp, typename Alloc>
inline size_t memUsageIndirect(const std::unordered_map<K, V, Hash, Cmp, Alloc> & m, const MemUsageOptions & opt)
{
    size_t result = m.size() * sizeof(typename std::unordered_map<K, V, Hash, Cmp, Alloc>::value_type);
    result += m.bucket_count() * sizeof(void *);  // buckets include a pointer to values?
    result += memUsageIndirectRange(m.begin(), m.end(), opt);
    return result;
}

// If an object has a memUsageIndirect() const member function, then use that
template<typename T,
         typename E = std::enable_if<std::is_convertible_v<decltype(std::declval<const T>().memUsageIndirect(MemUsageOptions())), size_t>>>
inline size_t memUsageIndirect(const T & val, const MemUsageOptions & opt)
{
    return val.memUsageIndirect(opt);
}

// Indirect usage of many objects is the sum of the indirect usage of each of them
template<typename First, typename... Rest>
inline size_t memUsageIndirectMany(const MemUsageOptions & opt, const First & first, Rest&&... rest)
{
    return memUsageIndirect(first, opt)
         + memUsageIndirectMany(opt, std::forward<Rest>(rest)...);
}

// Multiple objects, recursive base case
inline constexpr size_t memUsageIndirectMany(const MemUsageOptions & /* opt */)
{
    return 0;
}

} // namespace MLDB
