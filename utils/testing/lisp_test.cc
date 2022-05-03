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
#include "mldb/base/parse_context.h"
#include "mldb/types/any_impl.h"
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

        ParseContext pcontext(printed.rawString(), printed.rawData(), printed.rawLength());
        auto matched = Value::match(val.getContext(), pcontext);

        REQUIRE(matched);
        CHECK(matched == val);
        CHECK(pcontext.eof());
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

    SECTION("empty") {
        CHECK(jsonEncodeStr(Value()) == "{\"uninitialized\":null}");
    };

    runTest("true", "{\"atom\":true}");
    runTest("false", "{\"atom\":false}");
    runTest("nil", "{\"atom\":null}");
    runTest("()", "{\"list\":[]}");
    runTest("(true)", "{\"list\":[{\"atom\":true}]}");
    runTest("(false)", "{\"list\":[{\"atom\":false}]}");
    runTest("(nil)", "{\"list\":[{\"atom\":null}]}");
    runTest("x", "{\"sym\":\"x\"}");
    runTest("'x", "{\"sym\":\"x\",\"q\":1}");
    runTest("''x", "{\"sym\":\"x\",\"q\":2}");
    runTest("_", "{\"wc\":\"_\"}");
    runTest("...", "{\"wc\":\"...\"}");
    runTest("\"hello\"", "{\"atom\":\"hello\"}");
}

TEST_CASE("test-lisp-compilation", "[none]")
{
    auto runTest = [] (const std::string & expr, const std::string & expectedOut)
    {
        SECTION(expr + " --> " + expectedOut) {
            Context lcontext;
            auto e = Value::parse(lcontext, expr);
            auto x = Value::parse(lcontext, expectedOut);

            CompilationScope cscope(lcontext);
            auto [name, executor, createXScope, xcontext, info] = cscope.compile(e);

            auto outer = std::make_shared<ExecutionScope>(lcontext);
            std::shared_ptr<ExecutionScope> xscope
                = createXScope ? createXScope(outer, List()) : outer;
            auto out = executor(*xscope);

            CHECK(out == x);

            testParsingPrinting(e);
            testParsingPrinting(x);
        }
    };

    runTest("(+ 1 2)", "3");
    runTest("'(+ 1 2)", "(+ 1 2)");
    runTest("(+ 1 -1)", "0");
    runTest("(+ 1 2 -1 -2)", "0");
    runTest("(+ \"hello\" \" \" \"world!\")", "\"hello world!\"");
}

TEST_CASE("test-lisp-evaluation", "[none]")
{
    auto runTest = [] (const std::string & expr, const std::string & input, const std::string & expectedOut)
    {
        SECTION(expr + " " + input + " --> " + expectedOut) {
            cerr << "running test " << expr << endl;
            Context lcontext;
            ParseContext pcontext(expr, expr.data(), expr.size());
            CompilationScope cscope(lcontext);
            while (auto v = Value::match(lcontext, pcontext)) {
                testParsingPrinting(*v);
                cscope.consult(std::move(*v));
            }

            auto i = Value::parse(lcontext, input);
            auto x = Value::parse(lcontext, expectedOut);

            cerr << "compiling " << i << endl;

            auto [name, executor, createXScope, xcontext, info] = cscope.compile(i);

            cerr << "name = " << name << " info = " << info << endl;

            auto outer = std::make_shared<ExecutionScope>(lcontext);
            cerr << "creating scope; createXScope = " << (bool)createXScope << endl;
            std::shared_ptr<ExecutionScope> xscope = createXScope ? createXScope(outer, List()) : outer;
            cerr << "executing" << endl;
            auto out = executor(*xscope);

            CHECK(out == x);

            testParsingPrinting(i);
            testParsingPrinting(x);
        }
    };

    runTest("", "(+ 1 1)", "2");
    runTest("", "(+ 1 1.5)", "2.5");
    runTest("", "(+)", "0");
    runTest("", "(+ 1)", "1");
    runTest("", "(+ \"hello\")", "\"hello\"");
    runTest("", "(+ \"hello\" \" \" \"world\")", "\"hello world\"");
    runTest("", "(-)", "0");
    runTest("", "(- 1)", "-1");
    runTest("", "(- 10 1 2 3 4)", "0");
    runTest("", "(*)", "1");
    runTest("", "(* 1)", "1");
    runTest("", "(* 1.5 1.5)", "2.25");
    runTest("", "(* \"x\" 5 2)", "\"xxxxxxxxxx\"");
    runTest("", "(/)", "1");
    runTest("", "(/ 10)", "10");
    runTest("", "(/ 10 5)", "2");
    runTest("", "(/ 10 5 2)", "1");
    runTest("", "(/ 10 5 4.0)", "0.5");

    runTest("", "(let ((a 3)) (+ a a a))", "9");
    runTest("(defun dbl (x) (+ x x))", "(dbl 2)", "4");
    runTest("(set 'z 1)", "z", "1");
    runTest("(set 'z 1)(defun next () (setq z (+ z 1)) z)", "((next) (next) (next))", "(2 3 4)");

    // p151
    runTest("", "3.1416", "3.1416");
    runTest("", "100", "100");
    runTest("", "'hyphenated-name", "hyphenated-name");
    runTest("", "'*some-global*", "*some-global*");
    runTest("", "'nil", "()");

    runTest("", "'(a (b c) (d (e f)))", "(a (b c) (d (e f)))");
    runTest("", "'(1 2 3 4)", "(1 2 3 4)");
    runTest("", "'(george kate james joyce)", "(george kate james joyce)");

    // p152
    runTest("","'(on block-1 table)", "(on block-1 table)");
    runTest("","'(likes bill X)", "(likes bill X)");
    runTest("","'(and (likes george kate) (likes bill merry))", "(and (likes george kate) (likes bill merry))");
    runTest("","'((2467 (lovelace ada) programmer)"
               " (3592 (babbage charles) computer-designer))",
               "((2467 (lovelace ada) programmer)"
               " (3592 (babbage charles) computer-designer))");
    runTest("","'((key-1 value-1) (key-2 value-2) (key-3 value-3))", "((key-1 value-1) (key-2 value-2) (key-3 value-3))");
    runTest("", "(* 7 9)", "63");
    runTest("", "(- (+ 3 4) 7)", "0");
    runTest("", "(+ 14 5)", "19");
    runTest("", "(+ 1 2 3 4)", "10");
    runTest("", "(* (+ 2 5) (- 7 (/ 21 7)))", "28");
    runTest("", "(= (+ 2 3) 5)", "t");
    runTest("", "(> (* 5 6) (+ 4 5))", "t");
    //CHECK_THROWS(runTest("", "(a b c)", ""));

    // p154
    runTest("", "(list 1 2 3 4 5)", "(1 2 3 4 5)");
    runTest("", "(nth 0 '(a b c d))", "a");
    runTest("", "(nth 2 (list 1 2 3 4 5))", "3");
    runTest("", "(nth 2 '((a 1) (b 2) (c 3) (d 4)))", "(c 3)");
    runTest("", "(length '(a b c d))", "4");
    //runTest("", "(member 7 '(1 2 3 4 5))", "nil");
    runTest("", "(null ( ))", "t");

    // p155
    runTest("", "(quote (a b c))", "(a b c)");
    runTest("", "(quote (+ 1 3))", "(+ 1 3)");
    runTest("", "'(a b c)", "(a b c)");
    runTest("", "'(+ 1 3)", "(+ 1 3)");
    runTest("", "(list (+ 1 2) (+ 3 4))", "(3 7)");
    runTest("", "(list '(+ 1 2) '(+ 3 4))", "((+ 1 2) (+ 3 4))");
    runTest("", "(quote (+ 2 3))", "(+ 2 3)");
    runTest("", "(eval (quote (+ 2 3)))", "5");
    runTest("", "(list '* 2 5)", "(* 2 5)");
    runTest("", "(eval (list '* 2 5))", "10");

    // p156

}

TEST_CASE("test-lisp-predicates-parsing", "[none]")
{
    auto runTest = [] (const std::string & pred)
    {
        SECTION(pred) {
            Context lcontext;
            ParseContext pcontext(pred, pred.data(), pred.length());
            auto p = Predicate::parse(lcontext, pred);
            CHECK(p.toLisp().print() == pred);
            testParsingPrinting(p.toLisp());
        }
    };

    runTest("(+ $x:i64)");
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

    runTest("(+ $x)", "(+ 1)", "(matched $x 1)");
    runTest("(+ $x)", "(- 1)", "()");
    runTest("(+ '$x)", "(+ 1)", "()");
    runTest("(+ '$x)", "(+ $x)", "(matched)");
    runTest("(+ $x $x)", "(+ 1 1)", "(matched $x 1)");
    runTest("(+ 0 $x)", "(+ 0 1)", "(matched $x 1)");
    runTest("(* (- $x $y) (+ $x $y))", "(* (- 1 3) (+ 1 3))", "(matched $x 1 $y 3)");
    runTest("(* (- $x $y) (+ $x $y))", "(* (- 1 3) (+ 1 2))", "()");
    runTest("(* _ _)", "(* (- 1 3) (+ 1 2))", "(matched)");
    runTest("(+ $x $rest...)", "(+ 1)", "(matched $rest () $x 1)");
    runTest("(+ $x $rest...)", "(+ 1 2)", "(matched $rest (2) $x 1)");
    runTest("(+ $x $rest...)", "(+ 1 (* 2 3))", "(matched $rest ((* 2 3)) $x 1)");
    runTest("(+ (- $x $y)...)", "(+)", "(matched $x () $y ())");
    runTest("(+ (- $x $y)...)", "(+ (- 1 2))", "(matched $x (1) $y (2))");
    runTest("(+ (- $x $y)...)", "(+ (- 1 2) (- 3 4)))", "(matched $x (1 3) $y (2 4))");
    // TODO
    //runTest("(($x...)...)", "((0 1 2) (3 4 5) (6 7) (8) ())", "(matched $x ((0 1 2) (3 4 5) (6 7) (8) ()))");
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

    runTest("(+ $x)", "$x", "(+ 1)", "1");
    runTest("(+ $x)", "'$x", "(+ 1)", "$x");
    runTest("(+ $x $x)", "(* 2 $x)", "(+ 1 1)", "(* 2 1)");
    runTest("(+ $x $y $rest...)", "(+ (+ $x $y) $rest...)", "(+ 1 2 3)", "(+ (+ 1 2) 3)");
    runTest("(+ $x $y $rest...)", "(+ (+ $x $y) $rest...)", "(+ 1 2 3 (4 5))", "(+ (+ 1 2) 3 (4 5))");
    runTest("(+ $x $y $rest...)", "(+ (+ $x $y) $rest...)", "(+ 1 2)", "(+ (+ 1 2))");
    runTest("(+ (- $x $y)...)", "(- (+ $x...) (+ $y...))", "(+ (- 1 2) (- 3 4) (- 5 6))", "(- (+ 1 3 5) (+ 2 4 6))");
}