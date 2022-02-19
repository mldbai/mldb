/* int_table.cc                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "int_table.h"
#include "mldb/types/enum_description.h"
//#include "type_info_impl.hpp"

#if 0

#define FIRST_(a, ...) a
#define SECOND_(a, b, ...) b

#define FIRST(...) FIRST_(__VA_ARGS__,)
#define SECOND(...) SECOND_(__VA_ARGS__,)

#define EMPTY()

#define EVAL(...) EVAL4(__VA_ARGS__)
#define EVAL1024(...) EVAL512(EVAL512(__VA_ARGS__))
#define EVAL512(...) EVAL256(EVAL256(__VA_ARGS__))
#define EVAL256(...) EVAL128(EVAL128(__VA_ARGS__))
#define EVAL128(...) EVAL64(EVAL64(__VA_ARGS__))
#define EVAL64(...) EVAL32(EVAL32(__VA_ARGS__))
#define EVAL32(...) EVAL16(EVAL16(__VA_ARGS__))
#define EVAL16(...) EVAL8(EVAL8(__VA_ARGS__))
#define EVAL8(...) EVAL4(EVAL4(__VA_ARGS__))
#define EVAL4(...) EVAL2(EVAL2(__VA_ARGS__))
#define EVAL2(...) EVAL1(EVAL1(__VA_ARGS__))
#define EVAL1(...) __VA_ARGS__

#define DEFER1(m) m EMPTY()
#define DEFER2(m) m EMPTY EMPTY()()

#define IS_PROBE(...) SECOND(__VA_ARGS__, 0)
#define PROBE() ~, 1

#define CAT(a,b) a ## b

#define NOT(x) IS_PROBE(CAT(_NOT_, x))
#define _NOT_0 PROBE()

#define BOOL(x) NOT(NOT(x))

#define IF_ELSE(condition) _IF_ELSE(BOOL(condition))
#define _IF_ELSE(condition) CAT(_IF_, condition)

#define _IF_1(...) __VA_ARGS__ _IF_1_ELSE
#define _IF_0(...)             _IF_0_ELSE

#define _IF_1_ELSE(...)
#define _IF_0_ELSE(...) __VA_ARGS__

#define HAS_ARGS(...) BOOL(FIRST(_END_OF_ARGUMENTS_ __VA_ARGS__)())
#define _END_OF_ARGUMENTS_() 0

#define MAP(m, first, ...)           \
  m(first)                           \
  IF_ELSE(HAS_ARGS(__VA_ARGS__))(    \
    DEFER2(_MAP)()(m, __VA_ARGS__)   \
  )(                                 \
    /* Do nothing, just terminate */ \
  )

#define MAP2(m, common, first, ...)           \
  m(common, first)                           \
  IF_ELSE(HAS_ARGS(__VA_ARGS__))(    \
    DEFER2(_MAP2)()(m, common, __VA_ARGS__)   \
  )(                                 \
    /* Do nothing, just terminate */ \
  )

#define _MAP() MAP

#define _MAP2() MAP2

#define DO_ENUM_ARG(type, val) , type::val, #val

#define ENUM_INFO_ARGS(type, ...) \
EVAL(MAP2(DO_ENUM_ARG, type, __VA_ARGS__))

#define DEFINE_ENUM_INFO(type, ...) \
static const TabularEnumInfoT<type> enum_info_##type(#type ENUM_INFO_ARGS(type, __VA_ARGS__)); \
const TabularEnumInfoT<type> & getTabularTypeInfo(type *) { return enum_info_##type; }

#endif

namespace MLDB {

DEFINE_ENUM_DESCRIPTION_INLINE(IntTableType)
{
    addValue("BIT_COMPRESSED", IntTableType::BIT_COMPRESSED, "Compressed with bit ranges");
    addValue("FACTORED", IntTableType::FACTORED, "Compressed with factors");
    addValue("RLE", IntTableType::RLE, "Compressed with run length encoding");
}

template<typename Int>
void IntTableStats<Int>::add(Int value)
{
    size_t i = size;

    if (value < minValue || i == 0) {
        minValue = value;
        minElement = i;
        minElementCount = 0;
    }
    minElementCount += value == minValue;

    if (value > maxValue || i == 0) {
        maxValue = value;
        maxElement = i;
        maxElementCount = 0;
    }
    maxElementCount += value == maxValue;

    if (i != 0) {
        if (lastValue == value) {
            // run continues
            ++runLength;
        }
        else {
            // run has finished
            runLength = 1;
            ++numRuns;
        }
    } else {
        numRuns = 1;  // zero runs only with zero values
    }

    maxRunLength = std::max(maxRunLength, runLength);
    ++size;
    lastValue = value;
}

template<typename Int>
void IntTableStats<Int>::init(const std::span<const Int> & inputs)
{
    for (auto v: inputs) {
        add(v);
    }
}

std::ostream & operator << (std::ostream & stream, const IntTableStats<int64_t> & stats)
{
    auto printCount = [&] (size_t count)
    {
        if (count > 1) {
            stream << "*" << count;
        }
        return "";
    };

    auto printValueElementCount = [&] (auto label, auto value, auto element, auto count)
    {
        stream << " " << label << ": " << value << "@" << element << printCount(count);
        return "";
    };

    stream << "{ sz: " << stats.size
           << printValueElementCount("min", stats.minValue, stats.minElement, stats.minElementCount)
           << printValueElementCount("max", stats.maxValue, stats.maxElement, stats.maxElementCount)
           << " runs: " << stats.numRuns << "(" << stats.maxRunLength << ")}";
    return stream;
}

template struct IntTableStats<uint64_t>;
template struct IntTableStats<int64_t>;
template struct IntTableStats<uint32_t>;
template struct IntTableStats<int32_t>;
template struct IntTableStats<uint16_t>;
template struct IntTableStats<int16_t>;
template struct IntTableStats<uint8_t>;
template struct IntTableStats<int8_t>;

std::ostream & operator << (std::ostream & stream, IntTableType type)
{
    static constexpr const char * NAMES[] = { "BIT_COMPRESSED", "RLE", "FACTORED"};
    static constexpr size_t NUM_NAMES = sizeof(NAMES) / sizeof(NAMES[0]);
    return stream << (type >= NUM_NAMES ? "UNKNOWN" : NAMES[type]);
}

} // namespace MLDB