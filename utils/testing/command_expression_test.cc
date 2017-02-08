// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* command_expression_test.cc
   Jeremy Barnes, 2 July 2012
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.

   Test of command expressions.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>

#include "mldb/jml/utils/smart_ptr_utils.h"
#include "mldb/jml/utils/vector_utils.h"
#include "mldb/jml/utils/pair_utils.h"
#include "mldb/jml/utils/string_functions.h"
#include "mldb/jml/utils/file_functions.h"
#include "mldb/arch/exception_handler.h"
#include "mldb/arch/timers.h"
#include "mldb/types/date.h"
#include "mldb/utils/command_expression.h"
#include "mldb/types/periodic_utils.h"
#include "mldb/utils/command_expression_impl.h"

#include <sstream>
#include <iostream>
#include <fstream>


using namespace std;
using namespace ML;

using namespace MLDB;
using namespace MLDB::PluginCommand;


BOOST_AUTO_TEST_CASE( test_command_expression )
{
    CommandTemplate tmpl("hello %{world}");
    
    BOOST_CHECK_EQUAL(tmpl({ {"world", "bob"} }).renderShell(), "hello bob");
    BOOST_CHECK_EQUAL(tmpl({ {"world", "tim"} }).renderShell(), "hello tim");
}

BOOST_AUTO_TEST_CASE( test_complex_command_line )
{
    vector<string> testTemplate = {
        "./build/x86_64/bin/behaviour_merge_and_distribute",
        "--outputUriBase", "%{outputUri}/%{phase}/%{dateFormat('%Y/%m/%d/%Y-%m-%d-%H:%M:%S',periodStart)}-%{tranches}-%{type}",
        "--start-date",    "%{dateFormatIso8601(periodStart)}",
        "--end-date",      "%{dateFormatIso8601(periodEnd)}",
        "--tranche-spec",  "%{tranches}",
        "--type",          "%{type}",
        "--phase",         "%{phase}",
        "--input-phase",   "%{inputPhase}",
        "--distribute-on", "owner",
        "--equalize-on",   "size",
        "--arg-with-spaces", " has spaces "};

    CommandTemplate tmpl(testTemplate);
    
    Date periodStart = Date::now();
    Date periodEnd = periodStart.plusDays(1);
    string tranches = "0_31";
    string type = "beh";
    string phase = "merged";
    string inputPhase = "input";

    CommandExpressionContext context;
    context.addGlobal("outputUri", "s3://this.is.a.bucket/and/this/is/a/path with a space");
    context.addGlobal("periodStart", periodStart);
    context.addGlobal("periodEnd", periodEnd);
    context.addGlobal("tranches", tranches);
    context.addGlobal("type", type);
    context.addGlobal("phase", phase);
    context.addGlobal("inputPhase", inputPhase);
    
    string command = tmpl(context).renderShell();
    
    cerr << "command = " << command << endl;

    BOOST_REQUIRE_NE(command.find("' has spaces '"), string::npos);
}

BOOST_AUTO_TEST_CASE( test_expressions )
{
    BOOST_CHECK_EQUAL(CommandTemplate("%{3}")({}).renderShell(), "3");
    BOOST_CHECK_EQUAL(CommandTemplate("%{3+5}")({}).renderShell(), "8");
    BOOST_CHECK_EQUAL(CommandTemplate("%{3+5+1}")({}).renderShell(), "9");
    BOOST_CHECK_EQUAL(CommandTemplate("%{3 + 5 + 1}")({}).renderShell(), "9");
    BOOST_CHECK_EQUAL(CommandTemplate("%{3 + 5}")({}).renderShell(), "8");
    BOOST_CHECK_EQUAL(CommandTemplate("%{3.0 + 5}")({}).renderShell(), "8.000000");
    CommandExpressionContext context;
    context.addGlobal("x", 3);
    BOOST_CHECK_EQUAL(CommandTemplate("%{3 + x}")(context).renderShell(), "6");
    BOOST_CHECK_EQUAL(CommandTemplate("%{'hello ' + 'world'}")({}).renderShell(), "'hello world'");
    BOOST_CHECK_EQUAL(StringTemplate("%{[1,2,3] + [4,5,6]}")({}),
                      "1 2 3 4 5 6");

    BOOST_CHECK_EQUAL(StringTemplate("%{map val: [1,2,3] -> val + 1}")({}),
                      "2 3 4");
    BOOST_CHECK_EQUAL(StringTemplate("%{map x: [0,1], y: [0,1] -> x + y}")({}),
                      "0 1 1 2");
    BOOST_CHECK_EQUAL(CommandTemplate("%")({}).renderShell(), "%");
    BOOST_CHECK_EQUAL(CommandTemplate("%%")({}).renderShell(), "%");
    BOOST_CHECK_EQUAL(CommandTemplate("3 % 2")({}).renderShell(), "3 % 2");
    BOOST_CHECK_EQUAL(CommandTemplate("%%coco")({}).renderShell(), "%coco");

    string sql = "SELECT * FROM ds WHERE rowName() % 2 = 0";
    vector<string> cmd = {sql};
    BOOST_CHECK_EQUAL(CommandTemplate(cmd)({}).renderShell(), "'" + sql + "'");
    {
        Json::Value val;
        val["sql"] = sql;
        cmd = {val.toString()};
        BOOST_CHECK_EQUAL(CommandTemplate(cmd)({}).renderShell(),
                          "'{\"sql\":\"" + sql + "\"}\n'");
    }

    {
        Json::Value val;
        val["hello"]["this"][0]["is"]["a"] = "path";
        
        auto expr = CommandExpression::parse("val"/*".hello.this[0].is"*/);
        CommandExpressionContext context;
        context.addGlobal("%{val}", val);
        auto out = expr->apply(context);
        cerr << "out = " << out << endl;
    }
}
