/* fit_accumulator.h                                              -*- C++ -*-
   Jeremy Barnes, 6 November 2015
   Copyright (c) 2015 mldb.ai inc.  All rights reserved.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#pragma once
#include "mldb/utils/pow.h"
#include "int_table.h"
#include "mldb/plugins/jml/algebra/least_squares.h"
#include "mldb/plugins/jml/algebra/lapack.h"
#include "mldb/utils/safe_clamp.h"
#include <cctype>
#include <iostream>


namespace MLDB {

template<typename T>
constexpr T sqr(T x)
{
    return x * x;
}

static constexpr std::pair<int32_t, int32_t> bernoulli(uint32_t n)
{
    if (n == 1)
        return { -1, 2 };  // n=1 is the only odd number that is not zero
    if (n % 2 == 1)
        return {0, 1};  // zero
    
    constexpr std::pair<int32_t, int32_t> even_table[11] = {
        { 1, 1 },
        { 1, 6 },
        {-1, 30 },
        { 1, 42 },
        {-1, 30 },
        { 5, 66 },
        { -691, 2730 },
        { 7, 6 },
        { -3617, 510 },
        { 43867, 798 },
        { -174611, 330 }};

    if (n/2 < 11)
        return even_table[n/2];
    return {0,0};  // exception; too large (divided by zero)
}

inline uint64_t factorial(uint64_t n)
{
    static constexpr uint64_t table[21] = {
        1ULL,
        1ULL,
        2ULL,
        6ULL,
        24ULL,
        120ULL,
        720ULL,
        5040ULL,
        40320ULL,
        362880ULL,
        3628800ULL,
        39916800ULL,
        479001600ULL,
        6227020800ULL,
        87178291200ULL,
        1307674368000ULL,
        20922789888000ULL,
        355687428096000ULL,
        6402373705728000ULL,
        121645100408832000ULL,
        2432902008176640000ULL
    };
    
    if (n >= 21)
        MLDB_THROW_RANGE_ERROR("factorial too large");

    return table[n];

    size_t result = 1;
    for (size_t i = 1;  i <= n;  ++i) {
        result *= i;
    }
    return result;
}

template<typename Int>
Int maybe_abs(Int val, typename std::enable_if_t<std::is_signed_v<Int>> * = 0)
{
    return std::abs(val);
}

template<typename Int>
Int maybe_abs(Int val, typename std::enable_if_t<std::is_unsigned_v<Int>> * = 0)
{
    return val;
}

template<typename Int>
inline bool rational_reduce(Int & num, Int & den)
{
    bool result = false;

    if (den < 0) {
        num *= -1;
        den *= -1;
    }

    while (num % 2 == 0 && den % 2 == 0) {
        result = true;
        num /= 2;
        den /= 2;
    }

    if (num % den == 0) {
        result = result || (den != 1);
        num /= den;
        den = 1;
    }

    if (den % num == 0) {
        result = result || (num != 1);
        den /= num;
        num = 1;
    }

    if (num == 0) {
        result = result || (den != 1);
        den = 1;
    }

    Int max = sqrt(std::max(maybe_abs(num), maybe_abs(den)));

    for (Int i = 3;  i <= maybe_abs(num) && i <= maybe_abs(den) && i < max;  i += 2) {
        //cerr << "i = " << i << endl;
        while (num % i == 0 && den % i == 0) {
            result = true;
            num /= i;
            den /= i;
            max = sqrt(std::max(maybe_abs(num), maybe_abs(den)));
        }
    }

    return result;
}

// { numerator, denominator } in a reduced form
static constexpr std::pair<uint64_t, uint64_t> choose(uint32_t n, uint32_t k)
{
    if (k > n) return { 0, 1 };
    uint64_t num = factorial(n);
    uint64_t den = factorial(k) * factorial(n - k);
    uint64_t common_factor = factorial(std::max(k, n - k));
    num /= common_factor;
    den /= common_factor;

    rational_reduce(num, den);

    return {num, den};
}

// Returns sum of all values of x^p for x = 0..n, using Faulhaber's formula
inline int64_t sum_of_powers(uint32_t n, uint32_t p)
{
    using namespace std;

    if (p == 0)
        return n + 1;
    if (n == 0)
        return pow64_signed(n, p);

#if 1
    if (p == 1)
        return n * (n + 1) / 2;
    else if (p == 2)
        return n * (n + 1) * (2*n + 1) / 6;
    else if (p == 3)
        return sqr(n) * sqr(n + 1) / 4;
#endif

    int64_t result_num = 0;
    int64_t result_den = 1;
    int64_t sign = 1;

    //cerr << endl << "sum(j=1..." << n << ") x^" << p << endl;

    for (size_t j = 0;  j <= p;  ++j, sign *= -1) {
        auto [bnum, bden] = bernoulli(j);
        if (bden == 0)
            MLDB_THROW_RUNTIME_ERROR("bernoulli number order too high");

        if (bnum == 0)
            continue;
        auto [cnum, cden] = choose(p + 1, j);

        //cerr << "  j = " << j << " result = " << result_num << "/" << result_den << " bernoulli = " << bnum << "/" << bden << " choice = " << cnum << "/" << cden;
        //cerr << "  sign " << sign << " pow " << pow64_signed(n, p + 1 - j) << endl;
        //cerr << "  " << sign << " * " << pow64(n, p + 1 - j) << " * " << cnum << " * " << bnum << " = "
         //   << sign * pow64(n, p + 1 - j) * cnum * bnum << " / (" << cden << " * " << bden << " = " << cden * bden << ")" << endl;

        int64_t term_num = int64_t(sign) * pow64_signed(n, p + 1 - j) * int64_t(cnum) * int64_t(bnum);
        int64_t term_den = cden * bden;

        if (term_num % term_den == 0) {
            term_num /= term_den;
            term_den = 1;
        }

        // The following loop is equivalent to this, except that it will detect
        // and throw on overflow.
        //result_num = result_num * term_den + term_num * result_den;
        //result_den *= term_den;

        for (;;) {
            // actually, only one retry
            int64_t left, right, total;
            if (__builtin_mul_overflow(result_num, term_den, &left)
                || __builtin_mul_overflow(term_num, result_den, &right)
                || __builtin_add_overflow(left, right, &total)) {
                    bool reducedLeft = rational_reduce(result_num, result_den);
                    bool reducedRight = rational_reduce(term_num, term_den);
                    if (!reducedLeft && !reducedRight)
                        MLDB_THROW_RUNTIME_ERROR("sum_of_powers: result too big for 64 bit integer");
                    continue;
                };
            result_num = total;
            result_den *= term_den;
            break;
        }

        if (result_num % result_den == 0) {
            result_num /= result_den;
            result_den = 1;
        }

        //cerr << "  term = " << term_num << "/" << term_den << " result after = " << result_num << "/" << result_den << endl;
    }

    if (result_num % (result_den * (p + 1)) != 0) {
        MLDB_THROW_LOGIC_ERROR("sum_of_powers: rational result isn't integral");
    }

    //cerr << "result_num " << result_num << " result_den " << result_den << endl;
    //cerr << "result = " << result_num / (result_den * (p + 1)) << endl;

    return result_num / (result_den * (p + 1));
}

struct ResidualStats {
    IntTableStats<uint32_t> xStats;
    IntTableStats<int64_t> rStats;
    std::vector<double> residuals;
    int64_t minIntResidual = MAX_LIMIT;
    int64_t maxIntResidual = MIN_LIMIT;
    std::vector<uint32_t> maxResidualPoints;
    std::vector<uint32_t> minResidualPoints;
    std::vector<uint32_t> notablePoints;
    double sumRSquared = 0;
    double minResidual = -INFINITY, maxResidual = INFINITY;
    double residualRange() const
    {
        return maxResidual - minResidual;
    }

    void add(uint32_t x, double residual)
    {
        rStats.add(residual);
        xStats.add(x);

        int64_t r = safely_clamped(residual);
        if (r < minResidual) {
            minResidual = r;
            minResidualPoints.clear();
        }
        if (r > maxResidual) {
            maxResidual = r;
            maxResidualPoints.clear();
        }
        if (r == minResidual)
            minResidualPoints.push_back(r);
        if (r == maxResidual)
            maxResidualPoints.push_back(r);
    }
};

struct FitAccumulator {
    FitAccumulator(size_t order = 2)
        : order(order), sumX(order + 1), sumXYCov(order + 1)
    {

    }

    size_t order = 0;

    IntTableStats<uint32_t> xStats, yStats;
    std::vector<uint64_t> sumX;
    std::vector<uint64_t> sumXYCov;
    boost::multi_array<double, 2> cov;
    uint64_t sumY = 0;
    uint64_t sumYVar = 0;
    size_t numPoints = 0;
    uint32_t lastX = MAX_LIMIT;
    std::vector<uint32_t> xs, ys;

    void addPoint(uint32_t x, uint32_t y)
    {
        if (x != lastX + 1) {
            MLDB_THROW_RUNTIME_ERROR("attempt to accumulate values in the wrong order");
        }
        numPoints += 1;
        lastX = x;
        //xStats.add(x);
        //yStats.add(y);

        // Basis function
        std::vector<uint64_t> basis;
        for (size_t i = 0;  i <= order;  ++i) {
            basis.emplace_back(pow64(x, i));
        }

        sumY += y;
        sumYVar += y * y;

        // Covariance between the y and each basis function
        for (size_t i = 0;  i <= order;  ++i) {
            sumXYCov[i] += y * basis[i];
        }

        // Covariance between each x is simply a higher power of x; we only materialize
        // that if the x's aren't contiguous as otherwise we can calculate it as a'
        // closed form with no loss of precision.

        xs.push_back(x);
        ys.push_back(y);
    }

    void removePoint(uint32_t x, uint32_t y);
    //void split(size_t index);

    std::vector<double> xMeans(size_t order) const
    {
        std::vector<double> result(order + 1);
        for (size_t i = 0;  i <= order && numPoints != 0;  ++i) {
            result[i] = 1.0 * sum_of_powers(numPoints - 1, i) / numPoints;
        }
        return result;
    }

    template<typename Float>
    boost::multi_array<Float, 2> covariance1(size_t order) const
    {
        using namespace std;

        ExcAssert(numPoints == xs.size());

        boost::multi_array<Float, 2> result(boost::extents[order + 1][order + 1]);
        for (size_t i = 0;  i <= order;  ++i) {
            for (size_t j = 0;  j <= order;  ++j) {
                // cov (i,j) = sum_{x=0...n} (x^i - ximean) (x^j - xjmean)
                //            = sum_{x=0...n} x^{i+j} - sum_{x=0...n} x^i xjmean - sum_{x=0...n} x^j ximean + sum_{x=0...n} ximean xjmean
                //            = sum_{x=0...n} x^{i+j}
                //              - 1/numPoints sum_{x=0...n} x^i x^j
                //              - 1/numPoints sum_{x=0...n} x^j x^i
                //              + 1/numPoints^2 sum_{x=0...n} x_i x_j
                //            = sum_{x=0...n} x^{i + j} (1 - 1/numPoints)^2 
                result[i][j] = sqr(1.0 - 1.0 / numPoints) * sum_of_powers(numPoints - 1, i + j);
            }
        }

        return result;
    }

    template<typename Float>
    boost::multi_array<Float, 2> covariance2(size_t order) const
    {
        using namespace std;

        ExcAssert(numPoints == xs.size());

        std::vector<double> sumX(order + 1);

        for (auto x: xs) {
            for (size_t i = 0;  i <= order;  ++i) {
                sumX[i] += pow(x, i);
            }
        }

        //cerr << "sumX = " << sumX << endl;

        std::vector<double> xMeans(order + 1);
        for (size_t i = 0;  i <= order && numPoints != 0;  ++i) {
            xMeans[i] = sumX[i] / numPoints;
        }

        //cerr << "xs = " << xs << endl;
        //cerr << "this->xMeans[order] = " << this->xMeans(order) << endl;
        //cerr << "xMeans              = " << xMeans << endl;
        ExcAssert(this->xMeans(order) == xMeans);

#if 0
        double sumY = 0;
        for (auto y: ys) {
            sumY += y;
        }
        double meanY = sumY / numPoints;
#endif

        boost::multi_array<Float, 2> result(boost::extents[order + 1][order + 1]);
        for (size_t i = 0;  i <= order;  ++i) {
            for (size_t j = 0;  j <= order;  ++j) {
                if (i == 0 && j == 0) {
                    result[i][j] = 1.0 / numPoints;
                }
                else {
                    double accum = 0.0;
                    for (auto x: xs) {
                        //cerr << "  i = " << i << " j = " << j << " x = " << x << " pow1 = " << pow(x, i) << " pow2 = " << pow(x, j) << endl;
                        accum += (pow(x, i) - xMeans[i]) * (pow(x, j) - xMeans[j]); 
                    }
                    //cerr << "i = " << i << " j = " << j << " accum = " << accum << endl;
                    result[i][j] = accum / numPoints;
                }
            }
        }

        return result;
    }

    template<typename Float>
    boost::multi_array<Float, 2> covariance3(size_t order) const
    {
        using namespace std;

        ExcAssert(numPoints == xs.size());

        boost::multi_array<Float, 2> result(boost::extents[order + 1][order + 1]);

        for (size_t i = 0;  i <= order;  ++i) {
            for (size_t j = 0;  j <= order;  ++j) {
                //double accum = 0.0;
                //for (auto x: xs) {
                //    accum += pow(x, i) * pow(x, j);
                //}
                //result[i][j] = accum;
                result[i][j] = sum_of_powers(numPoints - 1, i + j);
            }
        }

        cerr << "before inverse: " << result << endl;

        return pseudo_inverse(result);
    }

    // Nothrow version of inverse, returns non zero in second result if not successful
    static std::tuple<boost::multi_array<double, 2>, int, const char *>
    try_inverse(const boost::multi_array<double, 2> & x)
    {
        boost::multi_array<double, 2> A = x;

        if (x.shape()[0] != x.shape()[1])
            return { A, -1000, "nonsquare" };

        auto n = x.shape()[0];


        if (n == 1) {
            A[0][0] = 1 / x[0][0];
            return { A, 0, "ok" };
        }
        else if (n == 2) {
            double deti = 1.0 / (A[0][0] * A[1][1] - A[1][0] * A[0][1]);
            A[0][0] = deti * A[1][1];
            A[0][1] = -deti * A[1][0];
            A[1][0] = -deti * A[0][1];
            A[1][1] = deti * A[0][0];
            return { A, !std::isfinite(deti), std::isfinite(deti) ? "ok" : "non-invertible" };
        }
        else if (n == 3) {
            // ...
        }

        using namespace std;

        int ipiv[n];
        if (auto res = ML::LAPack::getrf(n, n, A.data(), A.strides()[0], ipiv) != 0) {
            return { A, res, "getrf" };
        }
        if (auto res = ML::LAPack::getri(n, A.data(), A.strides()[0], ipiv) != 0) {
            return { A, res, "getri" };
        }
        return { A, 0, "ok" };
    }

    static boost::multi_array<double, 2> inverse(const boost::multi_array<double, 2> & x)
    {
        auto [A, code, msg] = try_inverse(x);
        if (code == 0)
            return A;
        else
            MLDB_THROW_RUNTIME_ERROR(msg);
    }

    static boost::multi_array<double, 2> pseudo_inverse(const boost::multi_array<double, 2> & x, double eps = 1e-9)
    {
        // Try direct inverse first
        auto [res, code, msg] = try_inverse(x);
        if (code == 0) {
            return res;
        }

        // If not...
        auto minmn = std::min(x.shape()[0], x.shape()[1]);
        auto [VT, U, svalues] = ML::svd_square(x);
        svalues.resize(minmn);

        // Invert or zero singular values
        for (size_t i = 0;  i < svalues.size();  ++i) {
            auto & v = svalues[i];
            if (abs(v) > eps)
                v = 1.0 / v;
            else v = 0;
        }
        return ML::diag_mult(U, svalues, VT, true /* parallel */);
    }

    std::vector<float> fit(size_t order) const
    {
        using namespace std;

        std::vector<float> result(order + 1);
        if (numPoints == 0)
            return result;

        auto cov1 = covariance1<double>(order);
        cerr << "cov1 = " << endl << cov1 << endl;
        auto cov2 = covariance2<double>(order);
        cerr << "cov2 = " << endl << cov2 << endl;
        auto cov3 = covariance3<double>(order);
        cerr << "cov3 = " << endl << cov3 << endl;


        using namespace std;
        //cerr << "cov = " << cov << endl;

        //auto inv = inverse(cov);
        for (size_t i = 0;  i <= order;  ++i) {
            double val = 0.0;
            for (size_t j = 0;  j <= order;  ++j) {
                val += cov3[i][j] * sumXYCov[j]; 
            }
            result[i] = val;
        }

        return result;
    }
};

} // namespace MLDB