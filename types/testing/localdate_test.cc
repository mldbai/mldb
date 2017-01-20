// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* date_test.cc
   Copyright (c) 2010 mldb.ai inc.  All rights reserved.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>
#include "mldb/arch/exception.h"
#include "mldb/arch/exception_handler.h"

#include "mldb/types/localdate.h"

using namespace std;
using namespace MLDB;


BOOST_AUTO_TEST_CASE( test_constructor )
{
    Set_Trace_Exceptions trace(false);

    /* default constructor */
    LocalDate d;
    BOOST_CHECK_EQUAL(d.secondsSinceEpoch(), 0.0);
    BOOST_CHECK_EQUAL(d.tzOffset(), 0);
    BOOST_CHECK_EQUAL(d.timezone(), "UTC");

    /* valid and invalid timezone names */
    BOOST_CHECK_THROW(d = LocalDate(0.0, "NoWhereIsNamedLikeThis"),
                      MLDB::Exception);
    BOOST_CHECK_NO_THROW(d = LocalDate(0.0, "America/Montreal"));

    /* Jan 1, 1970 00:00 UTC = Dec 31, 1969 19:00 EST */
    d = LocalDate(0.0, "America/Montreal");
    BOOST_CHECK_EQUAL(d.tzOffset(), (-1 * 5 * 3600));
    BOOST_CHECK_EQUAL(d.timezone(), "America/Montreal");
}

BOOST_AUTO_TEST_CASE( test_time_getters )
{
    /* UTC, 2012-12-10 17:24:14 = 1355160254  */
    LocalDate d(1355160254);
    BOOST_CHECK_EQUAL(d.hour(), 17);
    BOOST_CHECK_EQUAL(d.dayOfMonth(), 10);
    BOOST_CHECK_EQUAL(d.year(), 2012);

    /* America/Montreal, 2012-12-10 12:23:30 = 1355160210 */
    d = LocalDate(1355160210, "America/Montreal");
    BOOST_CHECK_EQUAL(d.hour(), 12);
    BOOST_CHECK_EQUAL(d.dayOfMonth(), 10);
    BOOST_CHECK_EQUAL(d.year(), 2012);
}
