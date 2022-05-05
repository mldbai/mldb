/* json_diff_test.cc
   Jeremy Barnes, 2 November 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Test of JSON streams.
*/

#include "catch2/catch_all.hpp"
#include "mldb/utils/json_stream.h"
#include "mldb/arch/demangle.h"
#include "mldb/types/value_description.h"
#include "mldb/utils/grammar.h"
#include <iostream>
#include <iomanip>
#include <fstream>

using namespace std;
using namespace MLDB;
using namespace MLDB::Grammar;


TEST_CASE("test-grammar-parsing", "[none]")
{
    Lisp::Context lcontext;

    std::string filename = "utils/jq.grammar";
    ParseContext pcontext(filename);

    auto grammar = parseGrammar(lcontext, pcontext);

    skipLispWhitespace(pcontext);
    pcontext.expect_eof("extra junk at end of grammar");

    auto ccontext = std::make_shared<CompilationContext>(lcontext);
    auto parser = grammar.compile(ccontext);

    ParseContext context2("test", "true", 4);
    auto res = parser.parse(lcontext, context2);
    ExcAssert(res);

    CHECK(res->print() == "true");
}

