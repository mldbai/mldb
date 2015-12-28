// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** mldb_plugin_delete_test.cc
  Francois Maillet, 18 mars 2015  
  Copyright (c) 2015 Datacratic Inc.  All rights reserved.

*/

#include "mldb/server/mldb_server.h"
#include "mldb/core/function.h"
#include "mldb/http/http_rest_proxy.h"
#include "mldb/types/vector_description.h"
#include "mldb/types/tuple_description.h"

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include <boost/test/unit_test.hpp>


using namespace std;
using namespace Datacratic;
using namespace Datacratic::MLDB;

BOOST_AUTO_TEST_CASE( test_function_pins )
{
    MldbServer server;
    
    server.init();
    string httpBoundAddress = server.bindTcp(PortRange(17000,18000), "127.0.0.1");
    cerr << "http listening on " << httpBoundAddress << endl;
    server.start();
    HttpRestProxy proxy(httpBoundAddress);
    
    PolyConfig config;
    config.type = "testfunction";

    FunctionInfo info;
    info.input.addRowValue("row");

    FunctionContext context;

    ColumnName hello("hello");

    Date now = Date::now();

    RowValue row;
    row.emplace_back(hello, "world", now);

    context.set("row", row);

    CellValue val = context.get<CellValue>("row.hello");

    BOOST_CHECK_EQUAL(val, "world");

    Json::Value jval;
    jval["bob"] = "is your uncle";
    jval["jill"] = "and jack 4 eva";
    jval["jack"]["and"]["jill"] = "went down the hill";

    ExpressionValue expr(jval, Date());

    cerr << jsonEncode(expr);

    RowValue row2;
    expr.appendToRow(hello, row2);

    cerr << jsonEncode(row2);

    context.set("people", expr);

    cerr << jsonEncode(context);

    BOOST_CHECK_EQUAL(context.get<CellValue>("people.bob"), "is your uncle");
    //BOOST_CHECK_EQUAL(context.get<CellValue>("people.jack.and.jill"), "went down the hill");

}
