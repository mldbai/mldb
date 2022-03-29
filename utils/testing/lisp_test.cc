/* lisp_test.cc
   Jeremy Barnes, 2 November 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Test of base Lisp functionality (at the base of the compilation and unification engines).
*/

#include "catch2/catch_all.hpp"
#include "mldb/types/value_description.h"
#include "mldb/utils/lisp.h"
#include "mldb/utils/lisp_predicate.h"
#include <iostream>
#include <iomanip>
#include <fstream>

using namespace std;
using namespace MLDB;
using namespace Lisp;

void testParsingPrinting(const Value & val)
{
    SECTION("parsing/printing " + val.print().rawString()) {
        EnterContext enter(val.getContext());
        auto jsonEncoded = jsonEncodeStr(val);
        auto jsonParsed = jsonDecodeStr<Value>(jsonEncoded);

        CHECK(jsonParsed == val);

        auto printed = jsonParsed.print();

        CHECK(printed == val.print());

        auto parsed = Value::parse(val.getContext(), printed);
        
        CHECK(parsed == val);
    }
}

TEST_CASE("test-lisp-parsing", "[none]")
{
    auto runTest = [] (const std::string & expr, std::string expectedStr)
    {
        SECTION(expr) {
            Context lcontext;
            ParseContext pcontext(expr, expr.data(), expr.length());
            auto val = Value::parse(lcontext, pcontext);
            pcontext.skip_whitespace();
            pcontext.expect_eof();

            auto jsonEncoded = jsonEncodeStr(val);

            CHECK(jsonEncoded == expectedStr);
            testParsingPrinting(val);
        }
    };

    runTest("true", "true");
    runTest("false", "false");
    runTest("null", "null");
    runTest("()", "[]");
    runTest("(true)", "[true]");
    runTest("(false)", "[false]");
    runTest("(null)", "[null]");
    runTest("x", "{\"var\":\"x\"}");
    runTest("`x", "{\"sym\":\"x\"}");
    runTest("_", "{\"wc\":\"_\"}");
    runTest("\"hello\"", "\"hello\"");
}

TEST_CASE("test-lisp-compilation", "[none]")
{
    auto runTest = [] (const std::string & expr, const std::string & input, const std::string & expectedOut)
    {
        SECTION(expr + " " + input + " --> " + expectedOut) {
            Context lcontext;
            auto e = Value::parse(lcontext, expr);
            auto i = Value::parse(lcontext, input);
            auto x = Value::parse(lcontext, expectedOut);

            CompilationScope cscope(lcontext);
            auto [executor, createXScope] = cscope.compile(e);

            auto outer = std::make_shared<ExecutionScope>(lcontext);
            std::shared_ptr<ExecutionScope> xscope = createXScope ? createXScope(*outer) : outer;
            auto out = executor(*xscope);

            CHECK(out == x);

            testParsingPrinting(e);
            testParsingPrinting(i);
            testParsingPrinting(x);
        }
    };

    runTest("(+ 1 2)", "()", "3");
    runTest("(+ 1 2 -1 -2)", "()", "0");
    //runTest("(+ \"hello\" \" \" \"world!\")", "()", "\"hello world\"");
}

TEST_CASE("test-lisp-predicates", "[none]")
{
    auto runTest = [] (const std::string & pred, const std::string & input, const std::string & expectedOut)
    {
        SECTION(pred + " " + input + " --> " + expectedOut) {
            Context lcontext;
            ParseContext pcontext(pred, pred.data(), pred.length());
            auto p = Predicate::parse(lcontext, pred);
            auto i = Value::parse(lcontext, input);
            auto x = Value::parse(lcontext, expectedOut);
            auto out = p.match(i);
            CHECK(out == x);
            testParsingPrinting(p.toLisp());
            testParsingPrinting(i);
            testParsingPrinting(x);
        }
    };

    runTest("(+ x)", "(+ 1)", "(matched x 1)");
    runTest("(+ x)", "(- 1)", "()");
    runTest("(+ x x)", "(+ 1 1)", "(matched x 1)");
    runTest("(+ 0 x)", "(+ 0 1)", "(matched x 1)");
    runTest("(* (- x y) (+ x y))", "(* (- 1 3) (+ 1 3))", "(matched x 1 y 3)");
    runTest("(* (- x y) (+ x y))", "(* (- 1 3) (+ 1 2))", "()");
    runTest("(* _ _)", "(* (- 1 3) (+ 1 2))", "(matched)");
}

TEST_CASE("test-lisp-substitutions", "[none]")
{
    auto runTest = [] (const std::string & pred, const std::string & subst, const std::string & input, const std::string & expectedOut)
    {
        SECTION(pred + " " + subst + " " + input + " --> " + expectedOut) {
            Context lcontext;
            ParseContext pcontext(pred, pred.data(), pred.length());
            auto p = Predicate::parse(lcontext, pred);
            auto i = Value::parse(lcontext, input);
            auto s = Substitution::parse(lcontext, subst);
            auto x = Value::parse(lcontext, expectedOut);
            auto matched = p.match(i);
            CHECK(!matched.is<Null>());
            auto out = s.subst(matched);
            CHECK(out == x);
            testParsingPrinting(p.toLisp());
            testParsingPrinting(s.toLisp());
            testParsingPrinting(matched);
            testParsingPrinting(i);
            testParsingPrinting(x);
        }
    };

    runTest("(+ x)", "x", "(+ 1)", "1");
    runTest("(+ x x)", "(* 2 x)", "(+ 1 1)", "(* 2 1)");
}
