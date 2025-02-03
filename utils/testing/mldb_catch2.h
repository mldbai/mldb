#pragma once

#include "catch2/catch_all.hpp"
#include "mldb/arch/exception.h"

// Save the Catch2 CHECK_THROWS macro...
#define CATCH2_CHECK_THROWS(...) INTERNAL_CATCH_THROWS( "CHECK_THROWS", Catch::ResultDisposition::ContinueOnFailure, __VA_ARGS__ )
#define CATCH2_REQUIRE_THROWS(...) INTERNAL_CATCH_THROWS( "REQUIRE_THROWS", Catch::ResultDisposition::Normal, __VA_ARGS__ )

// ...and replace it with our own that disables exceptions
#undef CHECK_THROWS
#define CHECK_THROWS(...) do { MLDB_TRACE_EXCEPTIONS(false); CATCH2_CHECK_THROWS(__VA_ARGS__); } while (0)

#undef REQUIRE_THROWS
#define REQUIRE_THROWS(...) do { MLDB_TRACE_EXCEPTIONS(false); CATCH2_REQUIRE_THROWS(__VA_ARGS__); } while (0)

#define BOOST_CHECK_EQUAL(x, y) CHECK(x == y)
#define BOOST_CHECK(x) CHECK(x)
#define BOOST_CHECK_GT(x, y) CHECK(x > y)
#define BOOST_CHECK_LT(x, y) CHECK(x < y)
#define BOOST_CHECK_GE(x, y) CHECK(x >= y)
#define BOOST_CHECK_LE(x, y) CHECK(x <= y)

#define BOOST_REQUIRE_EQUAL(x, y) REQUIRE(x == y)
#define BOOST_REQUIRE(x) REQUIRE(x)
#define BOOST_REQUIRE_GT(x, y) REQUIRE(x > y)
#define BOOST_REQUIRE_LT(x, y) REQUIRE(x < y)
#define BOOST_REQUIRE_GE(x, y) REQUIRE(x >= y)
#define BOOST_REQUIRE_LE(x, y) REQUIRE(x <= y)

#define BOOST_AUTO_TEST_CASE(name) TEST_CASE(#name)

namespace boost {
    namespace unit_test {
        struct test_suite {
            test_suite(const char * name = "") { }
        };
    }
}
