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
