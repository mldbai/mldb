/* safe_clamp.h                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once

#include <algorithm>
#include <limits>
#include <type_traits>

namespace MLDB {

template<typename Float, typename Int>
struct FloatIntegerClamper {
    static constexpr bool specialized = false;
    static constexpr Float min = static_cast<Float>(std::numeric_limits<Int>::min());
    static constexpr Float max = static_cast<Float>(std::numeric_limits<Int>::max());
    static constexpr int64_t clamp(Float val)
    {
        if (val < min) return std::numeric_limits<Int>::min();
        if (val > max) return std::numeric_limits<Int>::max();
        return val;
        //return static_cast<int64_t>(std::clamp(val, min, max));
    }
};

template<>
struct FloatIntegerClamper<double, int64_t> {
    static constexpr bool specialized = true;
    static constexpr double min = -0x1p+63;
    static constexpr double max = 0x1.fffffffffffffp+62;
    static constexpr int64_t clamp(double val)
    {
        if (val < min) return std::numeric_limits<int64_t>::min();
        if (val > max) return std::numeric_limits<int64_t>::max();
        return val;
        //return static_cast<int64_t>(std::clamp(val, min, max));
    }
};

template<>
struct FloatIntegerClamper<double, uint64_t> {
    static constexpr bool specialized = true;
    static constexpr double min = 0;
    static constexpr double max = 0x1.fffffffffffffp+63;
    static constexpr uint64_t clamp(double val)
    {
        if (val < min) return std::numeric_limits<uint64_t>::min();
        if (val > max) return std::numeric_limits<uint64_t>::max();
        return val;
        //return static_cast<uint64_t>(std::clamp(val, min, max));
    }
};

template<>
struct FloatIntegerClamper<float, int64_t> {
    static constexpr bool specialized = true;
    static constexpr float min = -0x1p+63;
    static constexpr float max = 0x1.fffffep+62;
    static constexpr int64_t clamp(float val)
    {
        if (val < min) return std::numeric_limits<int64_t>::min();
        if (val > max) return std::numeric_limits<int64_t>::max();
        return val;
        //return static_cast<int64_t>(std::clamp(val, min, max));
    }
};

template<>
struct FloatIntegerClamper<float, uint64_t> { // here
    static constexpr bool specialized = true;
    static constexpr float min = 0;
    static constexpr float max = 0x1.fffffep+63;
    static constexpr uint64_t clamp(float val)
    {
        if (val < min) return std::numeric_limits<uint64_t>::min();
        if (val > max) return std::numeric_limits<uint64_t>::max();
        return val;
        //return static_cast<uint64_t>(std::clamp(val, min, max));
    }
};

template<>
struct FloatIntegerClamper<float, int32_t> {
    static constexpr bool specialized = true;
    static constexpr float min = -0x1p+31;
    static constexpr float max = 0x1.fffffep+30;
    static constexpr int32_t clamp(float val)
    {
        if (val < min) return std::numeric_limits<int32_t>::min();
        if (val > max) return std::numeric_limits<int32_t>::max();
        return val;
        //return static_cast<int32_t>(std::clamp(val, min, max));
    }
};

template<>
struct FloatIntegerClamper<float, uint32_t> {
    static constexpr bool specialized = true;
    static constexpr float min = 0;
    static constexpr float max = 0x1.fffffep+31;
    static constexpr uint32_t clamp(float val)
    {
        if (val < min) return std::numeric_limits<uint32_t>::min();
        if (val > max) return std::numeric_limits<uint32_t>::max();
        return val;
        //return static_cast<uint32_t>(std::clamp(val, min, max));
    }
};

// Conversion that avoids undefined behavior if out of range
template<typename Int, typename Float>
constexpr Int safe_clamp(Float val)
{
    return FloatIntegerClamper<Float, Int>::clamp(val);
}

template<typename Float>
struct SafeClamper {
    Float f;

    template<typename Int, typename Enable = std::enable_if_t<std::is_integral_v<Int>>>
    constexpr operator Int () const { return safe_clamp<Int, Float>(f); }
};

template<typename Float>
constexpr SafeClamper<Float> safely_clamped(Float f)
{
    return SafeClamper<Float>{f};
}

} // namespace MLDB
