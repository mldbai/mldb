#include "catch2/catch_all.hpp"
#include "mldb/utils/safe_clamp.h"
#include "mldb/arch/demangle.h"
#include <iostream>
#include <iomanip>

using namespace std;
using namespace MLDB;

template<typename Float, typename Int>
struct ConvertibleLimits {
    static constexpr Float min = static_cast<Float>(std::numeric_limits<Int>::min());
    static constexpr Float max = static_cast<Float>(std::numeric_limits<Int>::max());
};

template<>
struct ConvertibleLimits<double, int64_t> {
    static constexpr double min = -0x1p+63;
    static constexpr double max = 0x1.fffffffffffffp+62;
};

template<>
struct ConvertibleLimits<double, uint64_t> {
    static constexpr double min = 0;
    static constexpr double max = 0x1.fffffffffffffp+63;
};

template<>
struct ConvertibleLimits<float, int64_t> {
    static constexpr float min = -0x1p+63;
    static constexpr float max = 0x1.fffffep+62;
};

template<>
struct ConvertibleLimits<float, uint64_t> { // here
    static constexpr float min = 0;
    static constexpr float max = 0x1.fffffep+63;
};

template<>
struct ConvertibleLimits<float, int32_t> {
    static constexpr float min = -0x1p+31;
    static constexpr float max = 0x1.fffffep+30;
};

template<>
struct ConvertibleLimits<float, uint32_t> {
    static constexpr float min = 0;
    static constexpr float max = 0x1.fffffep+31;
};

#if 0
template <typename T> struct IntUnderlyingFloat;
template<> struct IntUnderlyingFloat<double> { using type = int64_t; };
template<> struct IntUnderlyingFloat<float> { using type = int32_t; };

template <typename T> struct DoubleIntUnderlyingFloat;
template<> struct DoubleIntUnderlyingFloat<double> { using type = __int128; };
template<> struct DoubleIntUnderlyingFloat<float> { using type = int64_t; };

template<typename Float, typename Int>
void find_min_max()
{
    using Limits = ConvertibleLimits<Float, Int>;
    static_assert(Int(Limits::min) >= std::numeric_limits<Int>::min());
    static_assert(Int(Limits::max) <= std::numeric_limits<Int>::max());

    using FloatInt = typename IntUnderlyingFloat<Float>::type;
    using DoubleFloatInt = typename DoubleIntUnderlyingFloat<Float>::type;

    union {
        FloatInt i;
        Float f;
    } min, max;

    min.f = static_cast<Float>(std::numeric_limits<Int>::min());
    max.f = static_cast<Float>(std::numeric_limits<Int>::max());

    // Find the highest float that converts to max
    for (;;) {
        DoubleFloatInt i = max.f;
        if (i > DoubleFloatInt(std::numeric_limits<Int>::max()))
            --max.i;
        else break;
    }

    // Find the lowest float that converts to min
    for (;;) {
        DoubleFloatInt i = min.f;
        if (i < DoubleFloatInt(std::numeric_limits<Int>::min()))
            --min.i;
        else break;        
    }

    cerr << demangle(typeid(Float).name()) << " to " << demangle(typeid(Int).name()) << ": min " << std::setprecision(24) << std::scientific << std::hexfloat << min.f << endl;
    cerr << demangle(typeid(Float).name()) << " to " << demangle(typeid(Int).name()) << ": max " << std::setprecision(24) << std::scientific << std::hexfloat << max.f << endl;
}
#endif

template <typename T> struct DoubleIntUnderlyingFloat;
template<> struct DoubleIntUnderlyingFloat<double> { using type = __int128; };
template<> struct DoubleIntUnderlyingFloat<float> { using type = int64_t; };

template<typename F, typename I>
struct Checker {
    static void check()
    {
        // If not specialized, there is no way that it can be out of range so it's moot
        if (!FloatIntegerClamper<F, I>::specialized)
            return;

        std::string section = demangle(typeid(F).name()) + " -> " + demangle(typeid(I).name());
        using DI = __int128;//typename DoubleIntUnderlyingFloat<F>::type;

        SECTION(section) {
            //cerr << "type " << section << endl;
            //cerr << "range " << hex << std::numeric_limits<I>::min() << " to " << std::numeric_limits<I>::max() << dec << endl;
            //cerr << "max = " << hex << int64_t(FloatIntegerClamper<F, I>::max) << dec << endl;
            CHECK(DI(FloatIntegerClamper<F, I>::max) <= std::numeric_limits<I>::max());
            CHECK(DI(std::nextafter(FloatIntegerClamper<F, I>::max, INFINITY)) > DI(std::numeric_limits<I>::max()));
            if (std::numeric_limits<I>::min() < 0) {
                CHECK(DI(FloatIntegerClamper<F, I>::min) >= std::numeric_limits<I>::min());
                CHECK(DI(std::nextafter(FloatIntegerClamper<F, I>::min, -INFINITY)) < std::numeric_limits<I>::min());
            }
        }
    }
};

#define TEST_TYPES int64_t, uint64_t, int32_t, uint32_t, int16_t, uint16_t, int8_t, uint8_t

TEMPLATE_TEST_CASE("safe float conversion limits", "[none]", TEST_TYPES)
{
    Checker<double, TestType>::check();
    Checker<float, TestType>::check();
}

template<typename Float, typename Int>
void do_safe_int_conversion_test()
{
    SECTION("to " + demangle(typeid(Float).name())) {
        CHECK(safe_clamp<Int, Float>(INFINITY) == std::numeric_limits<Int>::max());
        CHECK(safe_clamp<Int, Float>(0) == 0);
        CHECK(safe_clamp<Int, Float>(-INFINITY) == std::numeric_limits<Int>::min());
    }
}

TEMPLATE_TEST_CASE("safe int conversion", "[none]", TEST_TYPES)
{
    //find_min_max<double, TestType>();
    //find_min_max<float, TestType>();
    do_safe_int_conversion_test<double, TestType>();
    do_safe_int_conversion_test<float, TestType>();
}
