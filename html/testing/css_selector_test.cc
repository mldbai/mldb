/** mldb_plugin_test.cc
    Jeremy Barnes, 13 December 2014
    Copyright (c) 2014 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
*/

#include "mldb/html/css.h"

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>


using namespace std;

using namespace MLDB;

BOOST_AUTO_TEST_CASE( test_css_selector_parsing )
{
    using namespace Css;
    BOOST_CHECK_EQUAL(Selector::parse("")->toString(), "*");
    BOOST_CHECK_EQUAL(Selector::parse("*")->toString(), "*");
    BOOST_CHECK_EQUAL(Selector::parse("h1")->toString(), "h1");
    BOOST_CHECK_EQUAL(Selector::parse("h1 a")->toString(), "h1 a");


    auto sel2 = Selector::parse("h1 h2");
    auto sel3 = Selector::parse("h1>h2");

    auto p1 = Path::parse("h1 h2");
    auto p2 = Path::parse("h1");
    auto p3 = Path::parse("h1 h1");
    auto p4 = Path::parse("h2 h1");
    auto p5 = Path::parse("");
    auto p6 = Path::parse("h1 h2 h3");

    auto sel = Selector::parse("h1");
    BOOST_CHECK_EQUAL(sel->match(Path::parse("h1")), true);
    BOOST_CHECK_EQUAL(sel->match(Path::parse("h1 h2")), false);
    BOOST_CHECK_EQUAL(sel->match(Path::parse("h1 h1")), true);
    BOOST_CHECK_EQUAL(sel->match(Path::parse("h2 h1")), true);
    BOOST_CHECK_EQUAL(sel->match(Path::parse("")), false);

    sel = Selector::parse("h1 h2");
    BOOST_CHECK_EQUAL(sel->match(Path::parse("h1")), false);
    BOOST_CHECK_EQUAL(sel->match(Path::parse("h1 h2")), true);
    BOOST_CHECK_EQUAL(sel->match(Path::parse("h3 h1 h2")), true);
    BOOST_CHECK_EQUAL(sel->match(Path::parse("h1 h1")), false);
    BOOST_CHECK_EQUAL(sel->match(Path::parse("h2 h1")), false);
    BOOST_CHECK_EQUAL(sel->match(Path::parse("")), false);

    sel = Selector::parse("h1 *");
    BOOST_CHECK_EQUAL(sel->match(Path::parse("h1")), false);
    BOOST_CHECK_EQUAL(sel->match(Path::parse("h1 h2")), true);
    BOOST_CHECK_EQUAL(sel->match(Path::parse("h3 h1 h2")), true);
    BOOST_CHECK_EQUAL(sel->match(Path::parse("h1 h1")), true);
    BOOST_CHECK_EQUAL(sel->match(Path::parse("h2 h1")), false);
    BOOST_CHECK_EQUAL(sel->match(Path::parse("h1 h1")), true);
    BOOST_CHECK_EQUAL(sel->match(Path::parse("h1 h1 h1")), true);
    BOOST_CHECK_EQUAL(sel->match(Path::parse("h1 h3 h1")), true);
    BOOST_CHECK_EQUAL(sel->match(Path::parse("")), false);

    sel = Selector::parse("h1>*");
    BOOST_CHECK_EQUAL(sel->match(Path::parse("h1")), false);
    BOOST_CHECK_EQUAL(sel->match(Path::parse("h1 h2")), true);
    BOOST_CHECK_EQUAL(sel->match(Path::parse("h3 h1 h2")), true);
    BOOST_CHECK_EQUAL(sel->match(Path::parse("h1 h1")), true);
    BOOST_CHECK_EQUAL(sel->match(Path::parse("h2 h1")), false);
    BOOST_CHECK_EQUAL(sel->match(Path::parse("h1 h1")), true);
    BOOST_CHECK_EQUAL(sel->match(Path::parse("h1 h1 h1")), true);
    BOOST_CHECK_EQUAL(sel->match(Path::parse("h1 h3 h1")), false);
    BOOST_CHECK_EQUAL(sel->match(Path::parse("")), false);
}
