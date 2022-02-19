/** cluster_test.cc
    Jeremy Barnes, 10 April 2016
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
*/

#include <iostream>
#include "catch2/catch_all.hpp"

#include "mldb/plugins/tabular/cluster.h"
#include "mldb/utils/ostream_array.h"

#include <vector>

using namespace std;
using namespace MLDB;


std::ostream & operator << (std::ostream & stream, const Cluster & cluster)
{
    return stream << cluster.print(0);
}

TEST_CASE( "cluster remove outliers" )
{
    auto doTest = [] (const std::vector<uint32_t> & points)
    {
        Cluster cluster;
        cluster.label = "test";
        for (size_t x = 0;  x < points.size();  ++x) {
            cluster.addPoint(x, points[x]);
        }
        cluster.fit();

        return cluster.removeOutliers(5.0, true /* trace */);
    };

    SECTION("too small to split 0") {
        auto res = doTest({});
        CHECK(res.empty());
    }

    SECTION("too small to split 1") {
        auto res = doTest({1});
        CHECK(res.empty());
    }

    SECTION("too small to split 2") {
        auto res = doTest({1, 2});
        CHECK(res.empty());
    }

    SECTION("too small to split 3") {
        auto res = doTest({1, 2, 3});
        CHECK(res.empty());
    }

    SECTION("simple outlier two-sided") {
        std::vector<uint32_t> vals;

        // Two outliers: zero and 100000
        vals.push_back(0);
        vals.resize(1000, 1000);
        vals.push_back(2000);

        auto res = doTest(vals);

        REQUIRE(res.size() == 3);
        CHECK(res[0].points.size() == 1);
        CHECK(res[1].points.size() == 999);
        CHECK(res[2].points.size() == 1);
    }

    SECTION("simple outlier bottom") {
        std::vector<uint32_t> vals;

        // Two outliers: zero and 100000
        vals.push_back(0);
        vals.resize(1000, 1000);

        auto res = doTest(vals);

        REQUIRE(res.size() == 2);
        CHECK(res[0].points.size() == 999);
        CHECK(res[1].points.size() == 1);
    }

    SECTION("simple outlier top") {
        std::vector<uint32_t> vals;

        // Two outliers: zero and 100000
        vals.push_back(2000);
        vals.resize(1000, 1000);

        auto res = doTest(vals);

        REQUIRE(res.size() == 2);
        CHECK(res[0].points.size() == 1);
        CHECK(res[1].points.size() == 999);
    }
}

TEST_CASE("cluster fit")
{
    auto doTest = [] (const std::vector<uint32_t> & points)
    {
        Cluster cluster;
        cluster.label = "test";
        for (size_t x = 0;  x < points.size();  ++x) {
            cluster.addPoint(x, points[x]);
        }
        cluster.fit();
        return cluster;
    };

    SECTION("empty") {
        Cluster c = doTest({});
        CHECK(c.size() == 0);
        for (auto & p: c.params)
            CHECK(p == 0);
    }

    SECTION("one point") {
        Cluster c = doTest({1});
        CHECK(c.size() == 1);
        CHECK(c.predict(0) == 1);
        for (size_t i = 1;  i < c.params.size();  ++i)
            CHECK(c.params[i] == 0);
    }

    SECTION("one point too big for float") {
        Cluster c = doTest({MAX_LIMIT});
        CHECK(c.size() == 1);
        CHECK(c.predict(0) == (uint32_t)MAX_LIMIT);
        for (size_t i = 1;  i < c.params.size();  ++i)
            CHECK(c.params[i] == 0);
    }

    SECTION("one point not representable as too big for float") {
        Cluster c = doTest({(uint32_t)MAX_LIMIT - 1});
        CHECK(c.size() == 1);
        CHECK(c.predict(0) == (uint32_t)MAX_LIMIT - 1);
        for (size_t i = 1;  i < c.params.size();  ++i)
            CHECK(c.params[i] == 0);
    }

   SECTION("enormous gradient") {
        Cluster c = doTest({0, (uint32_t)MAX_LIMIT});
        CHECK(c.size() == 2);
        CHECK(c.predict(0) == 0);
        //CHECK(c.predict(1) == (uint32_t)MAX_LIMIT);  // can't quite get there due to float's precision
        CHECK(c.predict((uint32_t)MAX_LIMIT) != INFINITY);
    }

   SECTION("enormous negative gradient") {
        Cluster c = doTest({(uint32_t)MAX_LIMIT, 0});
        CHECK(c.size() == 2);
        CHECK(c.predict(1) == 0);
        //CHECK(c.predict(0) == (uint32_t)MAX_LIMIT);  // can't quite get there due to float's precision
        CHECK(c.predict((uint32_t)MAX_LIMIT) != INFINITY);
    }

}

TEST_CASE("cluster shrink to fit")
{
        auto doTest = [] (const std::vector<uint32_t> & points)
    {
        Cluster cluster;
        cluster.label = "test";
        for (size_t x = 0;  x < points.size();  ++x) {
            cluster.addPoint(x, points[x]);
        }
        cluster.fit();
        cluster.shrinkToFit();

        if (!points.empty()) {
            CHECK(cluster.fitsInCluster(cluster.residualStats.minValue));
            CHECK(cluster.fitsInCluster(cluster.residualStats.maxValue));
            CHECK(cluster.couldFitInCluster(cluster.residualStats.minValue));
            CHECK(cluster.couldFitInCluster(cluster.residualStats.maxValue));
            CHECK(!cluster.fitsInCluster(cluster.residualStats.minValue-1));
            CHECK(!cluster.fitsInCluster(cluster.residualStats.maxValue+1));
        }
        return cluster;
    };

    SECTION("empty") {
        Cluster c = doTest({});
        CHECK(c.size() == 0);
        for (auto & p: c.params)
            CHECK(p == 0);
        CHECK(c.desiredResidualRange() == MAX_LIMIT);
    }

    SECTION("one point") {
        Cluster c = doTest({1});
        CHECK(c.size() == 1);
        CHECK(c.desiredResidualRange() == 0);
    }

    SECTION("big residuals") {
        Cluster c = doTest({0, (uint32_t)MAX_LIMIT, 0, (uint32_t)MAX_LIMIT, 0, (uint32_t)MAX_LIMIT, 0, (uint32_t)MAX_LIMIT});
        CHECK(c.size() == 8);
        CHECK(c.desiredResidualRange() == (uint32_t)MAX_LIMIT);
    }

    SECTION("zero residuals") {
        Cluster c = doTest({0, 0, 0, 0, 0, 0, 0, 0});
        CHECK(c.size() == 8);
        CHECK(c.desiredResidualRange() == 0);
        CHECK(c.couldFitInCluster(0));
        CHECK(!c.couldFitInCluster(1));
    }

    SECTION("small residuals") {
        Cluster c = doTest({0, 1, 0, 1, 0, 1, 0, 1});
        cerr << c.print(0) << endl;
        CHECK(c.size() == 8);
        CHECK(c.desiredResidualRange() == 1);
        CHECK(c.couldFitInCluster(0));
        CHECK(c.couldFitInCluster(1) ^ c.couldFitInCluster(-1));
        CHECK(!c.couldFitInCluster(2));
    }
}

TEST_CASE("cluster fit piecewise & optimized")
{
    auto doTest = [] (const std::vector<uint32_t> & points, uint32_t targetResidual = 0)
    {
        Cluster cluster;
        cluster.label = "test";
        for (size_t x = 0;  x < points.size();  ++x) {
            cluster.addPoint(x, points[x]);
        }

        bool trace = true;
        auto pieces = cluster.fitPiecewise(targetResidual, Cluster::scoreByMemoryUsage, trace);

        if (!pieces.empty()) {
            size_t totalPoints = 0;
            for (auto & p: pieces)
                totalPoints += p.size();
            REQUIRE(totalPoints == cluster.size());
        }

        Cluster::printSplit(cerr, cluster, pieces);

        return std::make_pair(cluster, pieces);
    };

    SECTION("empty") {
        auto [c, pieces] = doTest({});
        CHECK(pieces.size() == 0);
    }

    SECTION("one point") {
        auto [c, pieces] = doTest({1});
        CHECK(pieces.size() == 0);
    }

    // A curve like this should be fitted into two pieces, perfectly
    SECTION("^ shaped") {
        std::vector<uint32_t> points;
        for (size_t i = 0;  i <= 500;  ++i)
            points.push_back(i);
        for (size_t i = 0;  i < 500;  ++i)
            points.push_back(499-i);

        auto [c, pieces] = doTest(points);

        CHECK(c.residualRange() == 500);
        REQUIRE(pieces.size() == 2);
        CHECK(pieces[0].residualRange() == 0);
        CHECK(pieces[0].size() >= 500);
        CHECK(pieces[0].size() <= 501);
        CHECK(pieces[1].residualRange() == 0);
        CHECK(pieces[1].size() >= 500);
        CHECK(pieces[1].size() <= 501);
    }

    // Boundary condition: at the flat top, there are two maximal values,
    // each of which should be in a different cluster for optimal fit.
    SECTION("^ shaped flat top") {
        std::vector<uint32_t> points;
        for (size_t i = 0;  i < 500;  ++i)
            points.push_back(i);
        for (size_t i = 0;  i < 500;  ++i)
            points.push_back(499-i);

        auto [c, pieces] = doTest(points);

        CHECK(c.residualRange() == 499);
        REQUIRE(pieces.size() == 2);
        CHECK(pieces[0].residualRange() == 0);
        CHECK(pieces[0].size() == 500);
        CHECK(pieces[1].residualRange() == 0);
        CHECK(pieces[1].size() == 500);
    }

    SECTION("^ shaped long flat top") {
        std::vector<uint32_t> points;
        for (size_t i = 0;  i < 500;  ++i)
            points.push_back(i);
        for (size_t i = 0;  i < 500;  ++i)
            points.push_back(500);
        for (size_t i = 0;  i < 500;  ++i)
            points.push_back(499-i);

        auto [c, pieces] = doTest(points);

        CHECK(c.residualRange() == 500);
        REQUIRE(pieces.size() == 3);
        CHECK(pieces[0].residualRange() == 0);
        CHECK(pieces[0].size() >= 499);
        CHECK(pieces[1].residualRange() == 0);
        CHECK(pieces[1].size() >= 498);
        CHECK(pieces[2].residualRange() == 0);
        CHECK(pieces[2].size() >= 499);
    }

    SECTION("// shaped") {
        std::vector<uint32_t> points;
        for (size_t i = 0;  i < 500;  ++i)
            points.push_back(i);
        for (size_t i = 0;  i < 500;  ++i)
            points.push_back(i);

        auto [c, pieces] = doTest(points);

        CHECK(c.residualRange() == 499);
        REQUIRE(pieces.size() == 2);
        CHECK(pieces[0].residualRange() == 0);
        CHECK(pieces[0].size() == 500);
        CHECK(pieces[1].residualRange() == 0);
        CHECK(pieces[1].size() == 500);
    }

    SECTION("-_ shaped") {
        std::vector<uint32_t> points;
        for (size_t i = 0;  i < 500;  ++i)
            points.push_back(100);
        for (size_t i = 0;  i < 500;  ++i)
            points.push_back(0);

        auto [c, pieces] = doTest(points);

        CHECK(c.residualRange() == 100);
        REQUIRE(pieces.size() == 2);
        CHECK(pieces[0].residualRange() == 0);
        CHECK(pieces[0].size() == 500);
        CHECK(pieces[1].residualRange() == 0);
        CHECK(pieces[1].size() == 500);
    }

    #if 0  // later
    SECTION("= shaped") {  // TODO... later...
        std::vector<uint32_t> points;
        for (size_t i = 0;  i < 1000;  ++i)
            points.push_back(i%2*1000);

        auto [c, pieces] = doTest(points);

        CHECK(c.residualRange() == 1000);
        REQUIRE(pieces.size() == 2);
        CHECK(pieces[0].residualRange() == 0);
        CHECK(pieces[0].size() == 500);
        CHECK(pieces[1].residualRange() == 0);
        CHECK(pieces[1].size() == 500);
    }
    #endif

    SECTION("v shaped") {
        std::vector<uint32_t> points;
        for (size_t i = 0;  i <= 500;  ++i)
            points.push_back(500-i);
        for (size_t i = 0;  i < 500;  ++i)
            points.push_back(i + 1);

        auto [c, pieces] = doTest(points);

        CHECK(c.residualRange() == 500);
        REQUIRE(pieces.size() == 2);
        CHECK(pieces[0].residualRange() == 0);
        CHECK(pieces[0].size() == 500);
        CHECK(pieces[1].residualRange() == 0);
        CHECK(pieces[1].size() == 501);
    }

    SECTION("v shaped flat bottom") {
        std::vector<uint32_t> points;
        for (size_t i = 0;  i <= 500;  ++i)
            points.push_back(500-i);
        for (size_t i = 0;  i <= 500;  ++i)
            points.push_back(i);

        auto [c, pieces] = doTest(points);

        CHECK(c.residualRange() == 500);
        REQUIRE(pieces.size() == 2);
        CHECK(pieces[0].residualRange() == 0);
        CHECK(pieces[0].size() == 501);
        CHECK(pieces[1].residualRange() == 0);
        CHECK(pieces[1].size() == 501);
    }
}

TEST_CASE("fits in cluster")
{
    Cluster cluster;
    cluster.slope() = 1e32;

    cluster.addPoint(0, std::numeric_limits<int32_t>::min());  // residual is highly negative

    CHECK(cluster.residualStats.minValue == std::numeric_limits<int32_t>::min());
    CHECK(cluster.residualStats.maxValue == std::numeric_limits<int32_t>::min());
    
    // Could fit now, as there is only one point
    CHECK(cluster.fitsInCluster(std::numeric_limits<int32_t>::max()));
}

// With too many degrees of freedom, numerical errors in fit can cause wild residual ranges.
TEST_CASE("fit degrees of freedom")
{
    Cluster cluster;
    std::vector<Point> points = {{0,3321886976},{1,16384},{2,0},{3,2586418761},{4,16538},{5,0},{6,2586418761}};
    cluster.addPoints(points);
    cluster.fit();
    
    cerr << "cluster " << cluster.params << " residuals " << cluster.residualStats << endl;

    CHECK(cluster.residualStats.range() < std::numeric_limits<uint32_t>::max());
}

TEST_CASE("fit degrees of freedom 2")
{
    Cluster cluster;
    std::vector<Point> points = {{0,572662306},{1,1802201963},{2,1802201963},{3,1428319083},{4,1431651328},{5,1431655748},{6,1431655765},{7,1431655765},{8,50397261},{9,0},{10,4096},{11,8},{12,89459199},{13,4278190080},{14,1769471},{15,0},{16,4294923605},{17,13276158}};
    cluster.addPoints(points);
    cluster.fit();
    
    cerr << "cluster " << cluster.params << " residuals " << cluster.residualStats << endl;

    CHECK(cluster.residualStats.range() < std::numeric_limits<uint32_t>::max());
}

TEST_CASE("fit degrees of freedom 3")
{
    //==========|==== mem ====|========== x ==========|======= y =======|========= residuals =========|====== params ======|= cluster ==============
    //i   points|  bits sizekB|     min      max   dns|     min      max|   min-max   (range)  desired| x^0 x^1 x^2 x^3    |iter label              
    //000      8|  72.00   0.07|       0        7 1.000|       0 -16651264|-7639791104:519494878812834739892    -1|4.27832e+09 -1.45156|03 reseed 6            
    //[ 4.27832e+09 -1.45156e+10 5.19503e+09 -4.58384e+08 ]
    //current {{0,4278316032},{1,851748},{2,0},{3,6553600},{4,0},{5,4278193152},{6,65356},{7,3168},}
    //fit {{0,4278316032},{4,0},{5,4278193152},{7,3168},}
    //cl params [ 4.27832e+09 -1.45156e+10 5.19503e+09 -4.58384e+08 ] pr params [ 4.27832e+09 -1.45156e+10 5.19503e+09 -4.58384e+08 ]
    //x 0 y 4278316032 predicted 11918107136 residual 7639791104
    //cluster predicted 4278316032
    //offset 0 -7639791104
    //residual > INT_MAX 7639791104 > 4294967295

    Cluster cluster;
    std::vector<Point> points = {{0,4278316032},{4,0},{5,4278193152},{7,3168}};
    cluster.addPoints(points);
    cluster.fit();
    auto rangeBefore = cluster.residualRange();
    cerr << "rangeBefore = " << rangeBefore << endl;

    std::vector<Point> newPoints = {{0,4278316032},{1,851748},{2,0},{3,6553600},{4,0},{5,4278193152},{6,65356},{7,3168}};
    cluster.reset();
    cluster.addPoints(newPoints);
    auto rangeAfter = cluster.residualRange();
    cerr << "rangeAfter = " << rangeAfter << endl;

    cerr << "cluster " << cluster.params << " residuals " << cluster.residualStats << endl;

    CHECK(cluster.residualStats.range() >= std::numeric_limits<uint32_t>::max());
}


//[ 8.33938e+07 -4.12457e+06 0 0 ]
//{{0,572662306},{1,1802201963},{2,1802201963},{3,1428319083},{4,1431651328},{5,1431655748},{6,1431655765},{7,1431655765},{8,50397261},{9,0},{10,4096},{11,8},{12,89459199},{13,4278190080},{14,1769471},{15,0},{16,4294923605},{17,13276158},}
//x 9 y 0 predicted 4323795573 residual 4323795573
//cluster predicted 46272694
//offset 2 -4277522877
//residual > INT_MAX 4323795573 > 4294967295



//003      3| 106.67   0.04|       1        7 0.429|       0 -83886081|-40348598336:8192   40348606528    -1|1.6651e+09 1.60064e+|03 reseed 3            
//[ 1.6651e+09 1.60064e+09 1.27836e+09 -3.33019e+08 ]
//{{1,4211081215},{5,0},{7,1632436288},}
//x 1 y 4211081215 predicted 44559681088 residual 40348599873
//cluster predicted 4211082752
//offset 0 -40348598336
//residual > INT_MAX 40348599873 > 4294967295

