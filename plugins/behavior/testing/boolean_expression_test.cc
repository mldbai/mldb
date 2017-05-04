// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* boolean_expression_test.cc
   Jeremy Barnes, 20 August 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

   Test our ability to parse boolean expressions.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>

#include "mldb/arch/format.h"
#include "mldb/arch/exception.h"
#include "mldb/arch/exception_handler.h"
#include "mldb/jml/utils/vector_utils.h"
#include "mldb/plugins/behavior/boolean_expression.h"
#include "mldb/plugins/behavior/mutable_behavior_domain.h"
#include <iostream>

using namespace std;
using namespace MLDB;


void testParse(const std::string & expr)
{
    auto result = BooleanExpression::parse(expr);
    BOOST_CHECK_EQUAL(result->print(), expr);
}

void testParse(const std::string & expr, const std::string & expected)
{
    auto result = BooleanExpression::parse(expr);
    BOOST_CHECK_EQUAL(result->print(), expected);
}

BOOST_AUTO_TEST_CASE( test_parsing )
{
    testParse("123");
    testParse("\"123\"", "123");
    testParse("(123 OR 456)");
    testParse("123");
    testParse("NOT 123");
    testParse("(123 AND NOT 456)");
    testParse("(123 AND NOT 456 AND (789 OR 1011))");
    testParse("(123 AND NOT 456 AND (789 OR \"hello\"))");
}

BOOST_AUTO_TEST_CASE( test_error_handling )
{
    ML::Set_Trace_Exceptions guard(false);

    BOOST_CHECK_THROW(testParse("bonus"), MLDB::Exception);
    BOOST_CHECK_THROW(testParse("123 FOR 456"), MLDB::Exception);

    // Can't be parsed because we mix operators
    BOOST_CHECK_THROW(testParse("(123 OR 456 AND 789)"), MLDB::Exception);
}

BOOST_AUTO_TEST_CASE( test_matching )
{
    // Create some behaviors to test on
    MutableBehaviorDomain behs;
    behs.recordId(Id(1), Id(123), Date(), 3);
    behs.recordId(Id(1), Id(456), Date(), 1);
    behs.recordId(Id(1), Id(130), Date(), 1);

    behs.recordId(Id(2), Id(123), Date(), 1);
    behs.recordId(Id(2), Id(789), Date(), 1);

    behs.recordId(Id(3), Id(123), Date(), 5);
    behs.recordId(Id(3), Id(130), Date(), 1);

    behs.recordId(Id(4), Id(678), Date(), 1);
    behs.recordId(Id(4), Id(678), Date(), 1);

    behs.recordId(Id(5), Id(678), Date(), 1);
    behs.recordId(Id(5), Id(678), Date().plusSeconds(1), 1);

    behs.finish();

    // Function to test that matching the given expression on the
    // behaviors gives us the expected behavior
    auto testMatch = [&] (const std::string & expr,
                          const std::vector<int> & expected)
        {
            auto parsed = BooleanExpression::parse(expr);

            cerr << "matching " << parsed->print() << endl;

            vector<pair<SH, Date> > matched
               = parsed->generate(behs, SH::max());
            vector<int> res;
            for (auto m: matched)
                res.push_back(behs.getSubjectId(m.first).val1);
            std::sort(res.begin(), res.end());
            BOOST_CHECK_EQUAL(res, expected);
        };
    
    testMatch("123", { 1, 2, 3 });
    testMatch("(123 AND 456)", { 1 });
    testMatch("(123 AND 456 AND 234)", {});
    testMatch("(123 AND 456 AND NOT 789)", { 1 });
    testMatch("(123 AND NOT 456)", { 2, 3 });
    testMatch("(456 OR 789)", { 1, 2 });
    testMatch("(456 OR 789 OR 234)", { 1, 2 });

    testMatch("NOT 456", {2,3,4,5});
    testMatch("123 OR NOT 123", {1,2,3,4,5});
    testMatch("456 OR NOT 130", {1,2,4,5});
    testMatch("(NOT 130) OR 456", {1,2,4,5});
    testMatch("NOT (130 OR 456)", {2,4,5});
    testMatch("(NOT 130) AND (NOT 456)", {2,4,5});

    testMatch("TIMES(123,1)", {1,2,3});
    testMatch("TIMES(678,2)", {5});
}

// Not a very convincing test but I think right now
// all the boolean expression returns unique SHs anyway.
BOOST_AUTO_TEST_CASE( test_matching_unique ) {

    MutableBehaviorDomain behs;
    behs.recordId(Id(1), Id(123), Date::fromSecondsSinceEpoch(1), 1);
    behs.recordId(Id(1), Id(456), Date::fromSecondsSinceEpoch(2), 1);
    behs.recordId(Id(1), Id(130), Date::fromSecondsSinceEpoch(3), 1);
    behs.recordId(Id(2), Id(130), Date::fromSecondsSinceEpoch(4), 1);


    auto parsed = BooleanExpression::parse("123 OR 456 OR 130");
    vector<pair<SH, Date> > unique
       = parsed->generateUnique(behs, SH::max());

    // This is a quick and dirty test : I checked the hash to know
    // which one comes first after sorting
    BOOST_CHECK_EQUAL(unique.size(), 2);
    BOOST_CHECK_EQUAL(unique[0].first, SH(Id(1)));
    BOOST_CHECK_EQUAL(unique[1].first, SH(Id(2)));
    BOOST_CHECK_EQUAL(unique[0].second, Date::fromSecondsSinceEpoch(1));
    BOOST_CHECK_EQUAL(unique[1].second, Date::fromSecondsSinceEpoch(4));
}

BOOST_AUTO_TEST_CASE( test_print_sql )
{
    auto parsed = BooleanExpression::parse("\"P\" AND NOT \"C\"");
    BOOST_REQUIRE_EQUAL(parsed->printSql(), "(\"P\" AND \"C\" IS NULL)");

    parsed = BooleanExpression::parse("\"P\" AND NOT (\"C\" AND \"D\")");
    BOOST_REQUIRE_EQUAL(parsed->printSql(),
                        "(\"P\" AND (\"C\" AND \"D\") IS NULL)");

    parsed = BooleanExpression::parse("1");
    BOOST_REQUIRE_EQUAL(parsed->printSql(), "\"1\"");
}
