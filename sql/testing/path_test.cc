/** coord_test.cc
    Jeremy Barnes, 10 April 2016
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.

    Test of coordinate classes.
*/

#include "mldb/sql/path.h"
#include "mldb/arch/exception_handler.h"
#include "mldb/types/value_description.h"
#include "mldb/http/http_exception.h"

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include <tuple>
#include <iostream>

using namespace std;
using namespace Datacratic;
using namespace Datacratic::MLDB;

BOOST_AUTO_TEST_CASE(test_element_compare)
{
    PathElement el;
    PathElement el0("0");
    PathElement el00("00");
    PathElement el1("1");
    PathElement el10("10");
    PathElement el010("010");
    PathElement el0010("0010");
    PathElement elx("x");

    BOOST_CHECK_EQUAL(el0.compare(el0), 0);
    BOOST_CHECK_EQUAL(el0.compare(el00), 1);
    BOOST_CHECK_EQUAL(el00.compare(el0), -1);
    BOOST_CHECK_EQUAL(el1.compare(el0), 1);
    BOOST_CHECK_EQUAL(el1.compare(el00), 1);
    BOOST_CHECK_EQUAL(el1.compare(el10), -1);
    BOOST_CHECK_EQUAL(el1.compare(el010), -1);
    BOOST_CHECK_EQUAL(el1.compare(el0010), -1);
    BOOST_CHECK_EQUAL(el.compare(el0), -1);
    BOOST_CHECK_LE(el0.compare(elx), -1);

    // Longer prefixes should be smaller, as then it allows for numbers
    // like 0.01 to be larger than 0.001
    BOOST_CHECK_EQUAL(el0010.compare(el010), -1);
}

BOOST_AUTO_TEST_CASE(test_coord_constructor)
{
    PathElement coord1;
    BOOST_CHECK(coord1.empty());
 
    BOOST_CHECK_EQUAL(coord1.toUtf8String(), "");
   
    Path coords1;
    BOOST_CHECK(coords1.empty());

    BOOST_CHECK_EQUAL(coords1.toUtf8String(), "");

    BOOST_CHECK_EQUAL((coords1 + coord1).toUtf8String(), "\"\"");
    BOOST_CHECK_EQUAL(Path(coord1).toUtf8String(), "");

    vector<PathElement> coords2;
    coords2.push_back(coord1);
}

BOOST_AUTO_TEST_CASE(test_coord_printing)
{
    PathElement coord1("x");
    Path coords1(coord1);
    BOOST_CHECK_EQUAL(coord1.toUtf8String(), "x");
    BOOST_CHECK_EQUAL(coord1.toEscapedUtf8String(), "x");
    BOOST_CHECK_EQUAL(coords1.toUtf8String(), "x");

    PathElement coord2("x.y");
    Path coords2(coord2);
    BOOST_CHECK_EQUAL(coord2.toUtf8String(), "x.y");
    BOOST_CHECK_EQUAL(coord2.toEscapedUtf8String(), "\"x.y\"");
    BOOST_CHECK_EQUAL(coords2.toUtf8String(), "\"x.y\"");

    PathElement coord3("x\"y");
    Path coords3(coord3);
    BOOST_CHECK_EQUAL(coord3.toUtf8String(), "x\"y");
    BOOST_CHECK_EQUAL(coord3.toEscapedUtf8String(), "\"x\"\"y\"");
    BOOST_CHECK_EQUAL(coords3.toUtf8String(), "\"x\"\"y\"");

    Path coords4 = coord1 + coord2 + coord3;
    BOOST_CHECK_EQUAL(coords4.toUtf8String(), "x.\"x.y\".\"x\"\"y\"");
}

BOOST_AUTO_TEST_CASE(test_coord_parsing)
{
    {
        PathElement coord = PathElement::parse("x");
        BOOST_CHECK_EQUAL(coord.toUtf8String(), "x");
    }

    for (auto c: { "x", "x.y", "\"", "\"\"", "\"x.y\"", ".", "..", "...", "\".\"",  "[\"d5\",1]" }) {
        //cerr << "doing " << c << endl;
        PathElement coord(c);
        //cerr << "c = " << coord.toEscapedUtf8String() << endl;
        PathElement coord2 = PathElement::parse(coord.toEscapedUtf8String());
        //cerr << "coord2 = " << coord2 << endl;
        BOOST_CHECK_EQUAL(coord2.toUtf8String(), c);
    }

    {
        JML_TRACE_EXCEPTIONS(false);
        BOOST_CHECK_THROW(PathElement::parse("."), ML::Exception);
        BOOST_CHECK_THROW(PathElement::parse("\n"), ML::Exception);
        BOOST_CHECK_THROW(PathElement::parse("\""), ML::Exception);
        BOOST_CHECK_THROW(PathElement::parse(".."), ML::Exception);
        BOOST_CHECK_THROW(PathElement::parse("\"x."), ML::Exception);
        BOOST_CHECK_THROW(PathElement::parse("\"x."), ML::Exception);
        BOOST_CHECK_THROW(PathElement::parse("x\"\""), ML::Exception);
        BOOST_CHECK_THROW(PathElement::parse("x.y"), ML::Exception);
        BOOST_CHECK_THROW(PathElement::parse("\"x\",y"), ML::Exception);
    }
}

BOOST_AUTO_TEST_CASE(test_coords_parsing)
{
    {
        Path coords1 = Path::parse("x");
        BOOST_CHECK_EQUAL(coords1.toUtf8String(), "x");
    }

    {
        Path coords1 = Path::parse("x.y");
        BOOST_CHECK_EQUAL(coords1.toUtf8String(), "x.y");
    }

    {
        Path coords1 = Path::parse("\"x.y\"");
        BOOST_CHECK_EQUAL(coords1.toUtf8String(), "\"x.y\"");
    }

    {
        Path coords1 = Path::parse("\"x.y\".z");
        BOOST_CHECK_EQUAL(coords1.toUtf8String(), "\"x.y\".z");
    }

    {
        Path coords1 = Path::parse("\"x\".y");
        BOOST_CHECK_EQUAL(coords1.toUtf8String(), "x.y");
    }

    {
        Path coords1 = Path::parse("é");
        BOOST_CHECK_EQUAL(coords1.toUtf8String(), "é");
    }

    {
        Path coords1 = Path::parse("..");
        BOOST_CHECK_EQUAL(coords1.size(), 3);
        BOOST_CHECK_EQUAL(coords1.toUtf8String(), "\"\".\"\".\"\"");
    }

    {
        Path coords1 = Path::parse("\"\".\"\".\"\"");
        BOOST_CHECK_EQUAL(coords1.size(), 3);
        BOOST_CHECK_EQUAL(coords1.toUtf8String(), "\"\".\"\".\"\"");
    }

    {
        Path coords1 = Path::parse(".");
        BOOST_CHECK_EQUAL(coords1.size(), 2);
        BOOST_CHECK_EQUAL(coords1.toUtf8String(), "\"\".\"\"");
    }

    {
        Path coords1 = Path::parse("\"\".\"\"");
        BOOST_CHECK_EQUAL(coords1.size(), 2);
        BOOST_CHECK_EQUAL(coords1.toUtf8String(), "\"\".\"\"");
    }

    // MLDB-1721
    {
        Path coords1 = Path::parse("\"\n\"");
        BOOST_CHECK_EQUAL(coords1.size(), 1);
        BOOST_CHECK_EQUAL(coords1.toUtf8String(), "\"\n\"");
    }

    {
        JML_TRACE_EXCEPTIONS(false);
        BOOST_CHECK_THROW(Path::parse("\n"), ML::Exception);
        BOOST_CHECK_THROW(Path::parse("\0", 1), ML::Exception);
        BOOST_CHECK_THROW(Path::parse("\"\0\"", 3), ML::Exception);
        BOOST_CHECK_THROW(Path::parse("\""), ML::Exception);
        BOOST_CHECK_THROW(Path::parse("\"x."), ML::Exception);
        BOOST_CHECK_THROW(Path::parse("\"x."), ML::Exception);
        BOOST_CHECK_THROW(Path::parse("x\"\""), ML::Exception);
        BOOST_CHECK_THROW(Path::parse("\"x\",y"), ML::Exception);
    }
}

BOOST_AUTO_TEST_CASE(test_wildcards)
{
    Path empty;
    Path x("x");
    Path y("y");
    Path svd("svd");
    Path xy = x + y;

    BOOST_CHECK(x.matchWildcard(Path()));
    BOOST_CHECK(x.matchWildcard(x));
    BOOST_CHECK(!x.matchWildcard(y));

    BOOST_CHECK_EQUAL(x.replaceWildcard(empty, empty).toUtf8String(),
                      "x");
    BOOST_CHECK_EQUAL(x.replaceWildcard(empty, x).toUtf8String(),
                      "x.x");
    BOOST_CHECK_EQUAL(x.replaceWildcard(empty, y).toUtf8String(),
                      "y.x");
    BOOST_CHECK_EQUAL(x.replaceWildcard(x, y).toUtf8String(),
                      "y");
    BOOST_CHECK_EQUAL(x.replaceWildcard(x, xy).toUtf8String(),
                      "x.y");
    BOOST_CHECK_EQUAL(x.replaceWildcard(empty, xy).toUtf8String(),
                      "x.y.x");
    BOOST_CHECK_EQUAL(svd.replaceWildcard(PathElement("s"), xy).toUtf8String(),
                      "x.yvd");
}

BOOST_AUTO_TEST_CASE(test_indexes)
{
    BOOST_CHECK_EQUAL(PathElement(0).toIndex(), 0);
    BOOST_CHECK_EQUAL(PathElement("0").toIndex(), 0);
    BOOST_CHECK_EQUAL(PathElement("00").toIndex(), -1);
    BOOST_CHECK_EQUAL(PathElement(123456789).toIndex(), 123456789);
    BOOST_CHECK_EQUAL(PathElement("123456789").toIndex(), 123456789);
    BOOST_CHECK_EQUAL(PathElement("0123456789").toIndex(), -1);
    BOOST_CHECK_EQUAL(PathElement(-1).toIndex(), -1);
    BOOST_CHECK_EQUAL(PathElement(-1000).toIndex(), -1);
}

BOOST_AUTO_TEST_CASE(test_remove_prefix)
{
    Path test = PathElement("test1") + PathElement("x");
    Path none;

    BOOST_CHECK(test.startsWith(none));
    BOOST_CHECK_EQUAL(test.removePrefix(none), test);
}

void test_self_compare(const Path & p)
{
    BOOST_CHECK_EQUAL(p, p);
    BOOST_CHECK(!(p != p));
    BOOST_CHECK_LE(p, p);
    BOOST_CHECK_GE(p, p);
    BOOST_CHECK(!(p < p));
    BOOST_CHECK(!(p > p));
    BOOST_CHECK_EQUAL(p.compare(p), 0);
}

void test_compare_eq(const Path & p1, const Path & p2)
{
    BOOST_CHECK_EQUAL(p1, p2);
    BOOST_CHECK_EQUAL(p1 < p2, false);
    BOOST_CHECK_EQUAL(p1 > p2, false);
    BOOST_CHECK_EQUAL(p1 <= p2, true);
    BOOST_CHECK_EQUAL(p1 >= p2, true);
    BOOST_CHECK_EQUAL(p1.compare(p2), 0);
    BOOST_CHECK_EQUAL(p1.toUtf8String(), p2.toUtf8String());
}

void test_compare_ordered(const Path & p1, const Path & p2)
{
    BOOST_CHECK_NE(p1, p2);

    BOOST_CHECK_EQUAL(p1 < p2, true);
    BOOST_CHECK_EQUAL(p1 <= p2, true);
    BOOST_CHECK_EQUAL(p1 > p2, false);
    BOOST_CHECK_EQUAL(p1 >= p2, false);
    BOOST_CHECK_LE(p1.compare(p2), -1);

    BOOST_CHECK_EQUAL(p2 < p1, false);
    BOOST_CHECK_EQUAL(p2 <= p1, false);
    BOOST_CHECK_EQUAL(p2 > p1, true);
    BOOST_CHECK_EQUAL(p2 >= p1, true);
    BOOST_CHECK_GE(p2.compare(p1), 1);
}

BOOST_AUTO_TEST_CASE(test_compare)
{
    Path p0 {};
    Path p1a { 1 };
    Path p1b { 2 };
    Path p2a { 1, 2 };
    Path p2b { 1, 3 };
    Path p2c { 2, 3 };
    Path p3a { 1, 2, 3 };

    vector<Path> ordered = { p0, p1a, p2a, p3a, p1b, p2c };

    for (size_t i = 0;  i < ordered.size();  ++i) {

        test_compare_eq(ordered[i], ordered[i]);
        test_self_compare(ordered[i]);

        for (size_t j = 0;  j < i;  ++j) {
            test_compare_ordered(ordered[j], ordered[i]);
        }

        for (size_t j = i + 1;  j < ordered.size();  ++j) {
            test_compare_ordered(ordered[i], ordered[j]);
        }
    }
}

BOOST_AUTO_TEST_CASE(test_append)
{
    Path p0 {};
    Path p1 { 1 };

    BOOST_CHECK_EQUAL(p0 + p0, p0);
    BOOST_CHECK_EQUAL(p0 + p1, p1);
    BOOST_CHECK_EQUAL(p1 + p0, p1);
}
