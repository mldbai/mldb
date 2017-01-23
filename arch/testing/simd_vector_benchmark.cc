// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* simd_vector_benchmark.cc
   Jeremy Barnes, 21 February 2007
   Copyright (c) 2007 Jeremy Barnes.  All rights reserved.

   Benchmark of SIMD vector operations.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "mldb/arch/arch.h"
#include "mldb/arch/simd_vector.h"
#include "mldb/arch/demangle.h"
#include "mldb/arch/tick_counter.h"

#include <boost/test/unit_test.hpp>
#include <boost/test/floating_point_comparison.hpp>
#include <vector>
#include <set>
#include <iostream>
#include <cmath>


using namespace MLDB;
using namespace std;

using boost::unit_test::test_suite;

#if MLDB_INTEL_ISA
namespace MLDB {
namespace SIMD {
namespace Generic {
double vec_dotprod_sse2(const double * x, const double * y, size_t n);
} // namespace Generic

namespace Avx {
double vec_dotprod(const double * x, const double * y, size_t n);
} // namespace Avx
} // namespace SIMD
} // namespace MLDB
#endif // MLDB_INTEL_ISA

double vec_dotprod_generic(const double * x, const double * y, size_t n)
{
    double result = 0.0;
    for (unsigned i = 0;  i < n;  ++i)
        result += x[i] * y[i];
    return result;
}

BOOST_AUTO_TEST_CASE( benchmark )
{
    for (int nvals: { 1, 16, 64, 256, 1024, 4096, 16386, 65536, 1000000 }) {
        cerr << "nvals = " << nvals << endl;
        double min1 = INFINITY, min2 = INFINITY, min3 = INFINITY;

        // Initialize the test case
        vector<double> x(nvals), y(nvals);

        for (unsigned i = 0; i < nvals;  ++i) {
            x[i] = rand() / 16384.0;
            y[i] = rand() / 16384.0;
        }
    
        for (unsigned i = 0;  i < 100;  ++i) {

#if MLDB_INTEL_ISA
            uint64_t t0 = ticks();
            SIMD::Generic::vec_dotprod_sse2(&x[0], &y[0], nvals);
            uint64_t t1 = ticks();
            SIMD::Avx::vec_dotprod(&x[0], &y[0], nvals);
#else  // MLDB_INTEL_ISA
            uint64_t t0 = ticks(), t1 = t0;
#endif // MLDB_INTEL_ISA
            uint64_t t2 = ticks();
            vec_dotprod_generic(&x[0], &y[0], nvals);
            uint64_t t3 = ticks();

            double res_sse2 = t1 - t0;
            double res_avx  = t2 - t1;
            double res_generic = t3 - t2;

            min1 = std::min(min1, res_sse2);
            min2 = std::min(min2, res_avx);
            min3 = std::min(min3, res_generic);
        }

        cerr << "Minimum: sse2 " << min1 << " avx " << min2 << " gen " << min3 << endl;
        cerr << "cycles/op: sse2 " << min1 / nvals << " avx " << min2 / nvals
             << " generic " << min3 / nvals
             << endl;
    }
}
