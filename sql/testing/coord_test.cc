/** coord_test.cc
    Jeremy Barnes, 10 April 2016
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2016 Datacratic. All rights reserved.

    Test of coordinate classes.
*/

#include "mldb/sql/coord.h"
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

BOOST_AUTO_TEST_CASE(test_coord_constructor)
{
    Coord coord1;
    BOOST_CHECK(coord1.empty());
 
    BOOST_CHECK_EQUAL(coord1.toUtf8String(), "");
   
    Coords coords1;
    BOOST_CHECK(coords1.empty());

    BOOST_CHECK_EQUAL(coords1.toUtf8String(), "");

    // Make sure we can't add an empty element to a Coords
    {
        JML_TRACE_EXCEPTIONS(false);
        BOOST_CHECK_THROW(coords1 + coord1, HttpReturnException);
    }

    // Make sure we can't create a coords from an empty coord
    {
        JML_TRACE_EXCEPTIONS(false);
        
        BOOST_CHECK_THROW(Coords val(coord1), HttpReturnException);
    }
}

BOOST_AUTO_TEST_CASE(test_coord_printing)
{
    Coord coord1("x");
    Coords coords1(coord1);
    BOOST_CHECK_EQUAL(coord1.toUtf8String(), "x");
    BOOST_CHECK_EQUAL(coord1.toEscapedUtf8String(), "x");
    BOOST_CHECK_EQUAL(coords1.toUtf8String(), "x");

    Coord coord2("x.y");
    Coords coords2(coord2);
    BOOST_CHECK_EQUAL(coord2.toUtf8String(), "x.y");
    BOOST_CHECK_EQUAL(coord2.toEscapedUtf8String(), "\"x.y\"");
    BOOST_CHECK_EQUAL(coords2.toUtf8String(), "\"x.y\"");

    Coord coord3("x\"y");
    Coords coords3(coord3);
    BOOST_CHECK_EQUAL(coord3.toUtf8String(), "x\"y");
    BOOST_CHECK_EQUAL(coord3.toEscapedUtf8String(), "\"x\"\"y\"");
    BOOST_CHECK_EQUAL(coords3.toUtf8String(), "\"x\"\"y\"");

    Coords coords4 = coord1 + coord2 + coord3;
    BOOST_CHECK_EQUAL(coords4.toUtf8String(), "x.\"x.y\".\"x\"\"y\"");
}

BOOST_AUTO_TEST_CASE(test_coord_parsing)
{
    {
        Coord coord = Coord::parse("x");
        BOOST_CHECK_EQUAL(coord.toUtf8String(), "x");
    }

    for (auto c: { "x", "x.y", "\"", "\"\"", "\"x.y\"", ".", "..", "...", "\".\"" }) {
        //cerr << "doing " << c << endl;
        Coord coord(c);
        //cerr << "c = " << coord.toEscapedUtf8String() << endl;
        Coord coord2 = Coord::parse(coord.toEscapedUtf8String());
        //cerr << "coord2 = " << coord2 << endl;
        BOOST_CHECK_EQUAL(coord2.toUtf8String(), c);
    }

    {
        JML_TRACE_EXCEPTIONS(false);
        BOOST_CHECK_THROW(Coord::parse(""), ML::Exception);
        BOOST_CHECK_THROW(Coord::parse("."), ML::Exception);
        BOOST_CHECK_THROW(Coord::parse("\n"), ML::Exception);
        BOOST_CHECK_THROW(Coord::parse("\""), ML::Exception);
        BOOST_CHECK_THROW(Coord::parse("\"\""), ML::Exception);
        BOOST_CHECK_THROW(Coord::parse(".."), ML::Exception);
        BOOST_CHECK_THROW(Coord::parse("\"x."), ML::Exception);
        BOOST_CHECK_THROW(Coord::parse("\"x."), ML::Exception);
        BOOST_CHECK_THROW(Coord::parse("x\"\""), ML::Exception);
        BOOST_CHECK_THROW(Coord::parse("x.y"), ML::Exception);
        BOOST_CHECK_THROW(Coord::parse("\"x\",y"), ML::Exception);
    }
}

BOOST_AUTO_TEST_CASE(test_coords_parsing)
{
    {
        Coords coords1 = Coords::parse("x");
        BOOST_CHECK_EQUAL(coords1.toUtf8String(), "x");
    }

    {
        Coords coords1 = Coords::parse("x.y");
        BOOST_CHECK_EQUAL(coords1.toUtf8String(), "x.y");
    }

    {
        Coords coords1 = Coords::parse("\"x.y\"");
        BOOST_CHECK_EQUAL(coords1.toUtf8String(), "\"x.y\"");
    }

    {
        Coords coords1 = Coords::parse("\"x.y\".z");
        BOOST_CHECK_EQUAL(coords1.toUtf8String(), "\"x.y\".z");
    }

    {
        Coords coords1 = Coords::parse("\"x\".y");
        BOOST_CHECK_EQUAL(coords1.toUtf8String(), "x.y");
    }

    {
        Coords coords1 = Coords::parse("é");
        BOOST_CHECK_EQUAL(coords1.toUtf8String(), "é");
    }

    {
        JML_TRACE_EXCEPTIONS(false);
        BOOST_CHECK_THROW(Coords::parse(""), ML::Exception);
        BOOST_CHECK_THROW(Coords::parse("."), ML::Exception);
        BOOST_CHECK_THROW(Coords::parse("\n"), ML::Exception);
        BOOST_CHECK_THROW(Coords::parse("\""), ML::Exception);
        BOOST_CHECK_THROW(Coords::parse("\"\""), ML::Exception);
        BOOST_CHECK_THROW(Coords::parse(".."), ML::Exception);
        BOOST_CHECK_THROW(Coords::parse("\"x."), ML::Exception);
        BOOST_CHECK_THROW(Coords::parse("\"x."), ML::Exception);
        BOOST_CHECK_THROW(Coords::parse("x\"\""), ML::Exception);
        BOOST_CHECK_THROW(Coords::parse("\"x\",y"), ML::Exception);
    }
}

BOOST_AUTO_TEST_CASE(test_wildcards)
{
    Coords empty;
    Coords x("x");
    Coords y("y");
    Coords svd("svd");
    Coords xy = x + y;

    BOOST_CHECK(x.matchWildcard(Coords()));
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
    BOOST_CHECK_EQUAL(svd.replaceWildcard(Coord("s"), xy).toUtf8String(),
                      "x.yvd");
}

BOOST_AUTO_TEST_CASE(test_indexes)
{
    BOOST_CHECK_EQUAL(Coord(0).toIndex(), 0);
    BOOST_CHECK_EQUAL(Coord("0").toIndex(), 0);
    BOOST_CHECK_EQUAL(Coord("00").toIndex(), 0);
    BOOST_CHECK_EQUAL(Coord(123456789).toIndex(), 123456789);
    BOOST_CHECK_EQUAL(Coord("123456789").toIndex(), 123456789);
    BOOST_CHECK_EQUAL(Coord("0123456789").toIndex(), 123456789);
    BOOST_CHECK_EQUAL(Coord(-1).toIndex(), -1);
    BOOST_CHECK_EQUAL(Coord(-1000).toIndex(), -1);
}

BOOST_AUTO_TEST_CASE(test_remove_prefix)
{
    Coords test = Coord("test1") + Coord("x");
    Coords none;

    BOOST_CHECK(test.startsWith(none));
    BOOST_CHECK_EQUAL(test.removePrefix(none), test);
}
