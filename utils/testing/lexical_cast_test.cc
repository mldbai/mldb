/* lexical_cast_test.cc
 * Jeremy Barnes, 22 April 2018
 * This file is part of MLDB.  See the accompanying file LICENSE for licensing details.
 */

#include "catch2/catch_all.hpp"
#include "mldb/utils/lexical_cast.h"
#include "mldb/types/string.h"

using namespace std;
using namespace MLDB;

// Test against boost lexical cast if available
#if __has_include(<boost/lexical_cast.hpp>)
#include <boost/lexical_cast.hpp>
#define MLDB_BOOST_LEXICAL_CAST_INCLUDED 1
#endif

template<typename To, typename From> // avoid ambiguous overload
To do_lexical_cast(const From & from, std::enable_if_t<!std::is_floating_point_v<To>> * = nullptr)
{
    auto res = MLDB::lexical_cast<To>(from);
#if MLDB_BOOST_LEXICAL_CAST_INCLUDED
    CHECK(res == boost::lexical_cast<To>(from));
#endif
    return res;
}

template<typename To, typename From>
To do_lexical_cast(const From & from, std::enable_if_t<std::is_floating_point_v<To>> * = nullptr)
{
    auto res = MLDB::lexical_cast<To>(from);
#if MLDB_BOOST_LEXICAL_CAST_INCLUDED
    CHECK(lexical_cast<std::string>(res) == lexical_cast<std::string>(boost::lexical_cast<To>(from)));
#endif
    return res;
}

TEST_CASE("dtoa")
{
    SECTION("double to string")
    {
        CHECK(dtoa(0.0) == "0");
        CHECK(dtoa(-0.0) == "-0");
        CHECK(dtoa(123.0) == "123");
        CHECK(dtoa(-123.0) == "-123");
        CHECK(dtoa(123.456) == "123.456");
        CHECK(dtoa(-123.456789123456) == "-123.456789123456");
        CHECK(dtoa(1.234567890123456789) == "1.2345678901234567");
        CHECK(dtoa(INFINITY) == "inf");
        CHECK(dtoa(-INFINITY) == "-inf");
        CHECK(dtoa(NAN) == "nan");
        CHECK(dtoa(-NAN) == "-nan");
    }

    SECTION("float to string")
    {
        CHECK(ftoa(0.0f) == "0");
        CHECK(ftoa(-0.0f) == "-0");
        CHECK(ftoa(123.0f) == "123");
        CHECK(ftoa(-123.0f) == "-123");
        CHECK(ftoa(123.456f) == "123.456");
        CHECK(ftoa(-123.456f) == "-123.456");
        CHECK(ftoa(1.234567890123456789f) == "1.2345679");
        CHECK(ftoa(float(INFINITY)) == "inf");
        CHECK(ftoa(float(-INFINITY)) == "-inf");
        CHECK(ftoa(float(NAN)) == "nan");
        CHECK(ftoa(float(-NAN)) == "-nan");
    }

    SECTION("biasAgainstScientific")
    {
        CHECK(dtoa(10.0, 0) == "10");
        CHECK(dtoa(100.0, 0) == "100");
        CHECK(dtoa(1000.0, 0) == "1e3");
        CHECK(dtoa(1000.0, 1) == "1000");
        CHECK(dtoa(10000.0, 1) == "1e4");
        CHECK(dtoa(10000.0, 2) == "10000");
        CHECK(dtoa(100000.0, 2) == "1e5");
        CHECK(dtoa(100000.0, 3) == "100000");

        CHECK(dtoa(0.1, 0) == "0.1");
        CHECK(dtoa(0.01, 0) == "0.01");
        CHECK(dtoa(0.001, 0) == "1e-3");
        CHECK(dtoa(0.001, 1) == "0.001");
        CHECK(dtoa(0.0001, 1) == "1e-4");
        CHECK(dtoa(0.0001, 2) == "0.0001");
    }
}

TEST_CASE("to string")
{
    SECTION("string to string")
    {
        CHECK(do_lexical_cast<std::string>("") == "");
        CHECK(do_lexical_cast<std::string>("Hello") == "Hello");
        CHECK(do_lexical_cast<std::string>("123") == "123");
        CHECK(do_lexical_cast<std::string>("123.456") == "123.456");
    }

    SECTION("int to string")
    {
        CHECK(do_lexical_cast<std::string>(0) == "0");
        CHECK(do_lexical_cast<std::string>(123) == "123");
        CHECK(do_lexical_cast<std::string>(-123) == "-123");
        CHECK(do_lexical_cast<std::string>(INT_MAX) == to_string(INT_MAX));
        CHECK(do_lexical_cast<std::string>(INT_MIN) == to_string(INT_MIN));
    }

    SECTION("double to string")
    {
        CHECK(do_lexical_cast<std::string>(0.0) == "0");
        CHECK(do_lexical_cast<std::string>(-0.0) == "-0");
        CHECK(do_lexical_cast<std::string>(123.0) == "123");
        CHECK(do_lexical_cast<std::string>(-123.0) == "-123");
        CHECK(do_lexical_cast<std::string>(123.456) == "123.456");
        CHECK(do_lexical_cast<std::string>(-123.456) == "-123.456");
        CHECK(do_lexical_cast<std::string>(1.234567890123456789) == "1.2345678901234567");
        CHECK(do_lexical_cast<std::string>(45000.0) == "45000");
        CHECK(   lexical_cast<std::string>(45678.90) == "45678.9");
        CHECK(   lexical_cast<std::string>(0.00045) == "0.00045");
        CHECK(   lexical_cast<std::string>(1.00045) == "1.00045");
        CHECK(do_lexical_cast<std::string>(INFINITY) == "inf");
        CHECK(do_lexical_cast<std::string>(-INFINITY) == "-inf");
        CHECK(do_lexical_cast<std::string>(NAN) == "nan");
        CHECK(do_lexical_cast<std::string>(-NAN) == "-nan");

        // Make sure the biasAgainstScientific works
        CHECK(do_lexical_cast<std::string>(1.0) == "1");
        CHECK(do_lexical_cast<std::string>(10.0) == "10");
        CHECK(do_lexical_cast<std::string>(50.0) == "50");
        CHECK(do_lexical_cast<std::string>(100.0) == "100");
        CHECK(do_lexical_cast<std::string>(1000.0) == "1000");
        CHECK(do_lexical_cast<std::string>(1e6) == "1000000");
        CHECK(do_lexical_cast<std::string>(1e7) == "10000000");
        CHECK(   lexical_cast<std::string>(1e8) == "1e8");
        CHECK(   lexical_cast<std::string>(1e9) == "1e9");
        CHECK(   lexical_cast<std::string>(1e10) == "1e10");
        CHECK(   lexical_cast<std::string>(1e13) == "1e13");
    }

    SECTION("float to string")
    {
        CHECK(do_lexical_cast<std::string>(0.0f) == "0");
        CHECK(do_lexical_cast<std::string>(-0.0f) == "-0");
        CHECK(do_lexical_cast<std::string>(123.0f) == "123");
        CHECK(do_lexical_cast<std::string>(-123.0f) == "-123");
        CHECK(   lexical_cast<std::string>(123.456f) == "123.456");
        CHECK(   lexical_cast<std::string>(-123.456f) == "-123.456");
        CHECK(   lexical_cast<std::string>(1.234567890123456789f) == "1.2345679");
        CHECK(do_lexical_cast<std::string>(45000.0f) == "45000");
        CHECK(do_lexical_cast<std::string>(float(INFINITY)) == "inf");
        CHECK(do_lexical_cast<std::string>(float(-INFINITY)) == "-inf");
        CHECK(do_lexical_cast<std::string>(float(NAN)) == "nan");
        CHECK(do_lexical_cast<std::string>(float(-NAN)) == "-nan");

        // Make sure the biasAgainstScientific works
        CHECK(do_lexical_cast<std::string>(1.0f) == "1");
        CHECK(do_lexical_cast<std::string>(10.0f) == "10");
        CHECK(do_lexical_cast<std::string>(50.0f) == "50");
        CHECK(do_lexical_cast<std::string>(100.0f) == "100");
        CHECK(do_lexical_cast<std::string>(1000.0f) == "1000");
        CHECK(do_lexical_cast<std::string>(1e6f) == "1000000");
        CHECK(do_lexical_cast<std::string>(1e7f) == "10000000");
        CHECK(   lexical_cast<std::string>(1e8f) == "1e8");
        CHECK(   lexical_cast<std::string>(1e9f) == "1e9");
        CHECK(   lexical_cast<std::string>(1e10f) == "1e10");
        //CHECK(   lexical_cast<std::string>(1e13f) == "1e13"); // TODO: num digits needs tuning
    }
}

TEST_CASE("to int")
{
    SECTION("string to int")
    {
        CHECK(do_lexical_cast<int>("0") == 0);
        CHECK(do_lexical_cast<int>("123") == 123);
        CHECK(do_lexical_cast<int>("-123") == -123);
        CHECK(do_lexical_cast<int>(std::to_string(std::numeric_limits<int>::min())) == std::numeric_limits<int>::min());
        CHECK(do_lexical_cast<int>(std::to_string(std::numeric_limits<int>::max())) == std::numeric_limits<int>::max());

        MLDB_TRACE_EXCEPTIONS(false);
        CHECK_THROWS(lexical_cast<int>("123.456"));
        CHECK_THROWS(lexical_cast<int>(""));
        CHECK_THROWS(lexical_cast<int>("hello"));
        CHECK_THROWS(lexical_cast<int>("1e10"));
        CHECK_THROWS(lexical_cast<int>("0."));
        CHECK_THROWS(lexical_cast<int>("100000000000000000"));
        CHECK_THROWS(lexical_cast<int>(std::to_string((long long)std::numeric_limits<int>::max() + 1)));
        CHECK_THROWS(lexical_cast<int>(std::to_string((long long)std::numeric_limits<int>::min() - 1)));
    }

    SECTION("int to int")
    {
        CHECK(do_lexical_cast<int>(0) == 0);
        CHECK(do_lexical_cast<int>(123) == 123);
        CHECK(do_lexical_cast<int>(-123) == -123);
        CHECK(do_lexical_cast<int>(INT_MAX) == INT_MAX);
        CHECK(do_lexical_cast<int>(INT_MIN) == INT_MIN);
    }
}

TEST_CASE("to float")
{
    SECTION("string to float")
    {
        CHECK(do_lexical_cast<float>("0") == 0.0f);
        CHECK(do_lexical_cast<float>("-0") == -0.0f);
        CHECK(signbit(do_lexical_cast<float>("-0")) == 1);
        CHECK(do_lexical_cast<float>("123") == 123.0f);
        CHECK(do_lexical_cast<float>("-123") == -123.0f);
        CHECK(do_lexical_cast<float>("123.456") == 123.456f);
        CHECK(do_lexical_cast<float>("-123.456789123456") == -123.456789123456f);
        CHECK(do_lexical_cast<float>("inf") == INFINITY);
        CHECK(do_lexical_cast<float>("Inf") == INFINITY);
        CHECK(do_lexical_cast<float>("INF") == INFINITY);
        CHECK(do_lexical_cast<float>("-inf") == -INFINITY);
        CHECK(lexical_cast<std::string>(do_lexical_cast<float>("NaN")) == "nan");
        CHECK(lexical_cast<std::string>(do_lexical_cast<float>("-NaN")) == "-nan");
        CHECK(lexical_cast<std::string>(do_lexical_cast<float>("-nan")) == "-nan");
        CHECK(lexical_cast<std::string>(do_lexical_cast<float>("NAN")) == "nan");

        MLDB_TRACE_EXCEPTIONS(false);
        CHECK_THROWS(lexical_cast<float>("NANNY"));
        CHECK_THROWS(lexical_cast<float>("1e40000"));
    }

    SECTION("int to float")
    {
        CHECK(do_lexical_cast<float>(0) == 0.0f);
        CHECK(do_lexical_cast<float>(123) == 123.0f);
        CHECK(do_lexical_cast<float>(-123) == -123.0f);
        CHECK(do_lexical_cast<float>(INT_MAX) == float(INT_MAX));
        CHECK(do_lexical_cast<float>(INT_MIN) == float(INT_MIN));
    }

#if 0
    SECTION("double to float")
    {
        CHECK(do_lexical_cast<float>(0.0) == 0.0f);
        CHECK(do_lexical_cast<float>(-0.0) == 0.0f);
        CHECK(do_lexical_cast<float>(123.0) == 123.0f);
        CHECK(do_lexical_cast<float>(-123.0) == -123.0f);
        CHECK(do_lexical_cast<float>(123.456) == 123.456f);
        CHECK(do_lexical_cast<float>(-123.456) == -123.456f);
        CHECK(do_lexical_cast<float>(1.234567890123456789) == 1.2345679f);
        CHECK(do_lexical_cast<float>(INFINITY) == INFINITY);
        CHECK(do_lexical_cast<float>(-INFINITY) == -INFINITY);
        CHECK(isnan(do_lexical_cast<float>(NAN)));
        CHECK(isnan(do_lexical_cast<float>(-NAN)));
    }

    SECTION("float to float")
    {
        CHECK(do_lexical_cast<float>(0.0f) == 0.0f);
        CHECK(do_lexical_cast<float>(-0.0f) == 0.0f);
        CHECK(do_lexical_cast<float>(123.0f) == 123.0f);
        CHECK(do_lexical_cast<float>(-123.0f) == -123.0f);
        CHECK(do_lexical_cast<float>(123.456f) == 123.456f);
        CHECK(do_lexical_cast<float>(-123.456f) == -123.456f);
        CHECK(do_lexical_cast<float>(1.234567890123456789f) == 1.2345679f);
        CHECK(do_lexical_cast<float>(float(INFINITY)) == INFINITY);
        CHECK(do_lexical_cast<float>(float(-INFINITY)) == -INFINITY);
        CHECK(isnan(do_lexical_cast<float>(float(NAN))));
        CHECK(isnan(do_lexical_cast<float>(float(-NAN))));
    }
#endif
}


