/** mmap_test.cc
    Jeremy Barnes, 10 April 2016
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
*/

#include "catch2/catch_all.hpp"
#include "mldb/plugins/tabular/fit_accumulator.h"
#include "mldb/utils/ostream_vector.h"
#include "mldb/utils/ostream_pair.h"
#include "mldb/arch/exception.h"

using namespace std;
using namespace Catch::literals;
using namespace MLDB;

TEST_CASE("factorial")
{
    CHECK(factorial(0) == 1);
    CHECK(factorial(1) == 1);
    CHECK(factorial(2) == 2);
    CHECK(factorial(3) == 6);
    CHECK(factorial(4) == 24);
    CHECK(factorial(5) == 120);

    MLDB_TRACE_EXCEPTIONS(false);
    CHECK_THROWS(factorial(50));
    CHECK_THROWS(factorial(-1));
}

TEST_CASE("bernoulli numbers")
{
    // These are the coefficients of the taylor series for tan(x) around zero
    // we use that to test them

    using Expansion = std::vector<std::pair<int64_t, int64_t> >;

    auto taylor_tan_coefficients = [] (int order) -> Expansion
    {
        std::vector<std::pair<int64_t, int64_t> > result;

        for (int n = 1;  n <= order + 1;  ++n) {
            if (n % 2 == 1) {
                result.emplace_back(0, 1);
                continue;
            }

            auto [bnum, bden] = bernoulli(n);
            int64_t num = pow64(-1, n/2+1) * pow64(2, n) * (pow64(2, n) - 1);
            int64_t den = factorial(n);

            rational_reduce(num, den);  // reduce here to save from overflow

            num *= bnum;
            den *= bden;

            rational_reduce(num, den);  // finish then reduce again

            result.emplace_back(num, den);
        }

        return result;
    };

    auto order1 = taylor_tan_coefficients(19);
    REQUIRE(order1.size() == 20);
    
    // From Wolfram Alpha, query "taylor series expansion of tan x"
    CHECK(order1[0].first == 0);
    CHECK(order1[0].second == 1);
    CHECK(order1[1].first == 1);
    CHECK(order1[1].second == 1);
    CHECK(order1[2].first == 0);
    CHECK(order1[2].second == 1);
    CHECK(order1[3].first == 1);
    CHECK(order1[3].second == 3);
    CHECK(order1[4].first == 0);
    CHECK(order1[4].second == 1);
    CHECK(order1[5].first == 2);
    CHECK(order1[5].second == 15);
    CHECK(order1[6].first == 0);
    CHECK(order1[6].second == 1);
    CHECK(order1[7].first == 17);
    CHECK(order1[7].second == 315);
    CHECK(order1[8].first == 0);
    CHECK(order1[8].second == 1);
    CHECK(order1[9].first == 62);
    CHECK(order1[9].second == 2835);
    CHECK(order1[10].first == 0);
    CHECK(order1[10].second == 1);
    CHECK(order1[11].first == 1382);
    CHECK(order1[11].second == 155925);
    CHECK(order1[12].first == 0);
    CHECK(order1[12].second == 1);
    CHECK(order1[13].first == 21844);
    CHECK(order1[13].second == 6081075);
    CHECK(order1[14].first == 0);
    CHECK(order1[14].second == 1);
    CHECK(order1[15].first == 929569);
    CHECK(order1[15].second == 638512875);
    CHECK(order1[16].first == 0);
    CHECK(order1[16].second == 1);
    CHECK(order1[17].first == 6404582);
    CHECK(order1[17].second == 10854718875ULL);
    CHECK(order1[18].first == 0);
    CHECK(order1[18].second == 1);
    CHECK(order1[19].first == 443861162);
    CHECK(order1[19].second == 1856156927625ULL);
}

TEST_CASE("sum_of_powers")
{
    CHECK(sum_of_powers(0, 0) == 1);
    CHECK(sum_of_powers(10, 0) == 11);
    CHECK(sum_of_powers(0, 100) == 0);
    CHECK(sum_of_powers(10, 1) == 55);

    CHECK(sum_of_powers(0, 1) == 0);
    CHECK(sum_of_powers(1, 1) == 1);
    CHECK(sum_of_powers(2, 1) == 3);
    CHECK(sum_of_powers(3, 1) == 6);
    CHECK(sum_of_powers(4, 1) == 10);
    CHECK(sum_of_powers(0, 2) == 0);
    CHECK(sum_of_powers(1, 2) == 1);
    CHECK(sum_of_powers(2, 2) == 5);
    CHECK(sum_of_powers(3, 2) == 14);
    CHECK(sum_of_powers(4, 2) == 30);
    CHECK(sum_of_powers(0, 3) == 0);
    CHECK(sum_of_powers(1, 3) == 1);
    CHECK(sum_of_powers(2, 3) == 9);
    CHECK(sum_of_powers(3, 3) == 36);
    CHECK(sum_of_powers(4, 3) == 100);
    CHECK(sum_of_powers(0, 4) == 0);
    CHECK(sum_of_powers(1, 4) == 1);
    CHECK(sum_of_powers(2, 4) == 17);
    CHECK(sum_of_powers(3, 4) == 98);
    CHECK(sum_of_powers(4, 4) == 354);
    CHECK(sum_of_powers(0, 5) == 0);
    CHECK(sum_of_powers(1, 5) == 1);
    CHECK(sum_of_powers(2, 5) == 33);
    CHECK(sum_of_powers(3, 5) == 276);
    CHECK(sum_of_powers(4, 5) == 1300);

#if 1
    // test sum(i=0...n-1) i^y
    auto doTest = [] (uint32_t n, int32_t y)
    {
        int64_t naive = 0;
        for (uint32_t i = 0;  i <= n;  ++i) {
            naive += pow(i, y);
        }

        if (sum_of_powers(n, y) != naive) {
            cerr << "n = " << n << " y = " << y << endl;
        }
        CHECK(sum_of_powers(n, y) == naive);
    };

    doTest(0, 0);
    doTest(1, 0);
    doTest(0, 1);
    doTest(100, 1);
    doTest(10, 0);
    doTest(10, 1);
    doTest(10, 2);
    doTest(10, 3);
    doTest(10, 4);
    doTest(10, 5);
    doTest(10, 6);
    doTest(10, 7);
    doTest(10, 8);
    doTest(10, 9);
    doTest(10, 10);
#endif

#if 0
    for (size_t i = 0;  i < 100;  ++i) {
        for (size_t j = 0;  j < 10;  ++j) {
            doTest(i, j);
        }
    }
#endif
}

TEST_CASE("fit accumulator basics")
{
    FitAccumulator accum;

    SECTION("Fit no points (all underdetermined)") {
        CHECK(accum.fit(0) == vector<float>{0.0f}); // underdetermined
        CHECK(accum.fit(1) == vector<float>{0.0f, 0.0f}); 
        CHECK(accum.fit(2) == vector<float>{0.0f, 0.0f, 0.0f});
    }

    accum.addPoint(0, 0);
    SECTION("Fit one point (>1 underdetermined)") {
        CHECK(accum.fit(0) == vector<float>{0.0f}); // underdetermined
        CHECK(accum.fit(1) == vector<float>{0.0f, 0.0f}); 
        CHECK(accum.fit(2) == vector<float>{0.0f, 0.0f, 0.0f});
    }

    accum.addPoint(1, 1);
    SECTION("Fit 2 points (>2 underdetermined)") {
        CHECK(accum.fit(0) == vector<float>{0.5f});
        CHECK(accum.fit(1) == vector<float>{0.0f, 1.0f});
        auto f2 = accum.fit(2);  // underdetermined
        CHECK(abs(f2[0]) < 1e-5);
        CHECK(abs(f2[1] - 0.5) < 1e-5);
        CHECK(abs(f2[2] - 0.5) < 1e-5);
    }

    accum.addPoint(2, 2);
    SECTION("Fit 3 points (>3 underdetermined)") {
        CHECK(accum.fit(0) == vector<float>{1.0f});
        CHECK(accum.fit(1) == vector<float>{0.0f, 1.0f});
        auto f2 = accum.fit(2);
        CHECK(abs(f2[0]) < 1e-5);
        CHECK(abs(f2[1] - 1.0) < 1e-5);
        CHECK(abs(f2[2]) < 1e-5);
    }
}

