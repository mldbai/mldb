/* lisp_test.cc
   Jeremy Barnes, 2 November 2013
   Copyright (c) 2013 mldb.ai inc.  All rights reserved.
   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Test of base Lisp functionality (at the base of the compilation and unification engines).
*/

#include "catch2/catch_all.hpp"
#include "mldb/types/value_description.h"
#include "mldb/utils/lisp.h"
#include "mldb/utils/lisp_parsing.h"
#include "mldb/utils/lisp_predicate.h"
#include "mldb/base/parse_context.h"
#include "mldb/types/any_impl.h"
#include <iostream>
#include <iomanip>
#include <fstream>

using namespace std;
using namespace MLDB;
using namespace Lisp;

#define RUN_TEST(...) runTest(__FILE__, __LINE__, __builtin_COLUMN(), __VA_ARGS__)

std::string testLoc(const char * file, int line, int column)
{
    return string(file) + ":" + std::to_string(line) + ":" + std::to_string(column);
}

#define TEST_LOC(...) testLoc(file, line, column) + std::string(" ") + (__VA_ARGS__)

void testParsingPrinting(const Value & val)
{
    SECTION("parsing/printing " + val.print().rawString()) {
        EnterContext enter(val.getContext());
        auto jsonEncoded = jsonEncodeStr(val);
        auto jsonParsed = jsonDecodeStr<Value>(jsonEncoded);
        CHECK(jsonParsed == val);

        auto printed = jsonParsed.print();

        CHECK(printed == val.print());

        SourceLocation loc = LISP_CREATE_SOURCE_LOCATION(val);

        auto parsed = Value::parse(val.getContext(), printed, loc);
        
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
    auto runTest = [] (const char * file, int line, int column, const std::string & expr, std::string expectedStr)
    {
        SECTION(TEST_LOC(expr)) {
            Context lcontext;
            ParseContext pcontext(expr, expr.data(), expr.length());
            auto val = Value::parse(lcontext, pcontext);
            val.clearMetadataRecursive();
            skipLispWhitespace(pcontext);
            pcontext.expect_eof();

            auto jsonEncoded = jsonEncodeStr(val);

            CHECK(jsonEncoded == expectedStr);
            testParsingPrinting(val);
        }
    };

    SECTION("empty") {
        CHECK(jsonEncodeStr(Value()) == "{\"uninitialized\":null}");
    };

    RUN_TEST("true", "{\"atom\":true}");
    RUN_TEST("false", "{\"atom\":false}");
    RUN_TEST("nil", "{\"atom\":null}");
    RUN_TEST("()", "{\"list\":[]}");
    RUN_TEST("(true)", "{\"list\":[{\"atom\":true}]}");
    RUN_TEST("(false)", "{\"list\":[{\"atom\":false}]}");
    RUN_TEST("(nil)", "{\"list\":[{\"atom\":null}]}");
    RUN_TEST("x", "{\"sym\":\"x\"}");
    RUN_TEST("'x", "{\"sym\":\"x\",\"q\":1}");
    RUN_TEST("''x", "{\"sym\":\"x\",\"q\":2}");
    RUN_TEST("_", "{\"wc\":\"_\"}");
    RUN_TEST("...", "{\"wc\":\"...\"}");
    RUN_TEST("\"hello\"", "{\"atom\":\"hello\"}");
}

TEST_CASE("test-lisp-compilation", "[none]")
{
    auto runTest = [] (const char * file, int line, int column, const std::string & expr, const std::string & expectedOut)
    {
        SECTION(TEST_LOC(expr + " --> " + expectedOut)) {
            Context lcontext;
            SourceLocation loc = getRawSourceLocation(file, line, column);

            auto e = Value::parse(lcontext, expr, loc);
            auto x = Value::parse(lcontext, expectedOut, loc);

            SourceLocation compileLoc = LISP_CREATE_SOURCE_LOCATION(e);

            CompilationScope cscope(lcontext, compileLoc, "testCase");
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

    RUN_TEST("(+ 1 2)", "3");
    RUN_TEST("'(+ 1 2)", "(+ 1 2)");
    RUN_TEST("(+ 1 -1)", "0");
    RUN_TEST("(+ 1 2 -1 -2)", "0");
    RUN_TEST("(+ \"hello\" \" \" \"world!\")", "\"hello world!\"");
}

TEST_CASE("test-lisp-evaluation", "[none]")
{
    auto runTest = [] (const char * file, int line, int column, const std::string & expr, const std::string & input, const std::string & expectedOut)
    {
        SECTION(TEST_LOC(expr + " " + input + " --> " + expectedOut)) {
            //cerr << "running test " << expr << endl;
            Context lcontext;
            ParseContext pcontext(file, expr.data(), expr.size(), line, 1000 /* column */);
            SourceLocation loc = getRawSourceLocation(file, line, column);
            CompilationScope cscope(lcontext, loc, "testCase");
            while (auto v = Value::match(lcontext, pcontext)) {
                testParsingPrinting(*v);
                cscope.consult(std::move(*v));
            }

            auto i = Value::parse(lcontext, input, loc);
            auto x = Value::parse(lcontext, expectedOut, loc);

            //cerr << "compiling " << i << endl;

            auto [name, executor, createXScope, xcontext, info] = cscope.compile(i);

            //cerr << "name = " << name << " info = " << info << endl;

            auto outer = std::make_shared<ExecutionScope>(lcontext);
            //cerr << "creating scope; createXScope = " << (bool)createXScope << endl;
            std::shared_ptr<ExecutionScope> xscope = createXScope ? createXScope(outer, List()) : outer;
            //cerr << "executing" << endl;
            auto out = executor(*xscope);

            CHECK(out == x);

            testParsingPrinting(i);
            testParsingPrinting(x);
        }
    };

    RUN_TEST("", "(+ 1 1)", "2");
    RUN_TEST("", "(+ 1 1.5)", "2.5");
    RUN_TEST("", "(+)", "0");
    RUN_TEST("", "(+ 1)", "1");
    RUN_TEST("", "(+ \"hello\")", "\"hello\"");
    RUN_TEST("", "(+ \"hello\" \" \" \"world\")", "\"hello world\"");
    RUN_TEST("", "(-)", "0");
    RUN_TEST("", "(- 1)", "-1");
    RUN_TEST("", "(- 10 1 2 3 4)", "0");
    RUN_TEST("", "(*)", "1");
    RUN_TEST("", "(* 1)", "1");
    RUN_TEST("", "(* 1.5 1.5)", "2.25");
    RUN_TEST("", "(* \"x\" 5 2)", "\"xxxxxxxxxx\"");
    RUN_TEST("", "(/)", "1");
    RUN_TEST("", "(/ 10)", "10");
    RUN_TEST("", "(/ 10 5)", "2");
    RUN_TEST("", "(/ 10 5 2)", "1");
    RUN_TEST("", "(/ 10 5 4.0)", "0.5");
    RUN_TEST("", "(let ((a 3)) (+ a a a))", "9");
    RUN_TEST("(defun dbl (x) (+ x x))", "(dbl 2)", "4");
    RUN_TEST("(set 'z 1)", "z", "1");
    RUN_TEST("(set 'z 1)(defun next () (setq z (+ z 1)) z)", "((next) (next) (next))", "(2 3 4)");

    // p151
    RUN_TEST("", "3.1416", "3.1416");
    RUN_TEST("", "100", "100");
    RUN_TEST("", "'hyphenated-name", "hyphenated-name");
    RUN_TEST("", "'*some-global*", "*some-global*");
    RUN_TEST("", "'nil", "nil");

    RUN_TEST("", "'(a (b c) (d (e f)))", "(a (b c) (d (e f)))");
    RUN_TEST("", "'(1 2 3 4)", "(1 2 3 4)");
    RUN_TEST("", "'(george kate james joyce)", "(george kate james joyce)");

    // p152
    RUN_TEST("","'(on block-1 table)", "(on block-1 table)");
    RUN_TEST("","'(likes bill X)", "(likes bill X)");
    RUN_TEST("","'(and (likes george kate) (likes bill merry))", "(and (likes george kate) (likes bill merry))");
    RUN_TEST("","'((2467 (lovelace ada) programmer)"
               " (3592 (babbage charles) computer-designer))",
               "((2467 (lovelace ada) programmer)"
               " (3592 (babbage charles) computer-designer))");
    RUN_TEST("","'((key-1 value-1) (key-2 value-2) (key-3 value-3))", "((key-1 value-1) (key-2 value-2) (key-3 value-3))");
    RUN_TEST("", "(* 7 9)", "63");
    RUN_TEST("", "(- (+ 3 4) 7)", "0");
    RUN_TEST("", "(+ 14 5)", "19");
    RUN_TEST("", "(+ 1 2 3 4)", "10");
    RUN_TEST("", "(* (+ 2 5) (- 7 (/ 21 7)))", "28");
    RUN_TEST("", "(= (+ 2 3) 5)", "t");
    RUN_TEST("", "(> (* 5 6) (+ 4 5))", "t");
    //CHECK_THROWS(RUN_TEST("", "(a b c)", ""));

    // p154
    RUN_TEST("", "(list 1 2 3 4 5)", "(1 2 3 4 5)");
    RUN_TEST("", "(nth 0 '(a b c d))", "a");
    RUN_TEST("", "(nth 2 (list 1 2 3 4 5))", "3");
    RUN_TEST("", "(nth 2 '((a 1) (b 2) (c 3) (d 4)))", "(c 3)");
    RUN_TEST("", "(length '(a b c d))", "4");
    RUN_TEST("", "(member 7 '(1 2 3 4 5))", "nil");
    RUN_TEST("", "(null ( ))", "t");

    // p155
    RUN_TEST("", "(quote (a b c))", "(a b c)");
    RUN_TEST("", "(quote (+ 1 3))", "(+ 1 3)");
    RUN_TEST("", "'(a b c)", "(a b c)");
    RUN_TEST("", "'(+ 1 3)", "(+ 1 3)");
    RUN_TEST("", "(list (+ 1 2) (+ 3 4))", "(3 7)");
    RUN_TEST("", "(list '(+ 1 2) '(+ 3 4))", "((+ 1 2) (+ 3 4))");
    RUN_TEST("", "(quote (+ 2 3))", "(+ 2 3)");
    RUN_TEST("", "(eval (quote (+ 2 3)))", "5");
    RUN_TEST("", "(list '* 2 5)", "(* 2 5)");
    RUN_TEST("", "(eval (list '* 2 5))", "10");

    // p156
    RUN_TEST("(defun square (x) (* x x))", "(square 5)", "25");
    RUN_TEST("(defun square (a) (* a a)) (defun hypotenuse (x y) (sqrt (+ (square x) (square y))))", "(hypotenuse 3 4)", "5");
    RUN_TEST("(defun square (x) (* x x)) (defun hypotenuse (x y) (sqrt (+ (square x) (square y))))", "(hypotenuse 3 4)", "5");

    // p157
    RUN_TEST("", "(< -1 0)", "t");
    RUN_TEST("(defun neg (x) (- x))", "(neg -1)", "1");

    RUN_TEST(R"((defun absolute-value (x)
                (cond((< x 0) (- x))     ;if x<0, return -x
                     ((>= x 0) x)))      ;else return x)",
            "((absolute-value -1) (absolute-value 1))",
            "(1 1)");

    // p158
    RUN_TEST(R"((defun absolute-value (x)
                (cond((< x 0) (- x))     ;if x<0, return -x
                     (t x)))             ;else return x)",
            "((absolute-value -1) (absolute-value 1))",
            "(1 1)");

    RUN_TEST("", "(= 9 (+ 4 5))",  "t");
    RUN_TEST("", "(>= 17 4)",      "t");
    RUN_TEST("", "(< 8 (+ 4 2))",  "nil");
    RUN_TEST("", "(oddp 3)",       "t");
    RUN_TEST("", "(minusp 6)",     "nil");
    RUN_TEST("", "(numberp 17)",   "t");
    RUN_TEST("", "(numberp nil)",  "nil");
    RUN_TEST("", "(zerop 0)",      "t");
    RUN_TEST("", "(plusp 10)",     "t");
    RUN_TEST("", "(plusp -2)",     "nil");

    // p159
    RUN_TEST("", "(member 3 '(1 2 3 4 5))", "(3 4 5)");

    RUN_TEST("(defun absolute-value (x) (if (< x 0) (- x) x))", "(absolute-value -42)", "42");

    RUN_TEST("", "(and)", "t");
    RUN_TEST("", "(or)", "nil");
    RUN_TEST("", "(and t)", "t");
    RUN_TEST("", "(and nil)", "nil");
    RUN_TEST("", "(and 3)", "3");
    RUN_TEST("", "(or 3)", "3");
    RUN_TEST("", "(and t t t)", "t");
    RUN_TEST("", "(and t t 3)", "3");
    RUN_TEST("", "(and nil t t 3)", "nil");
    RUN_TEST("", "(and t t 3 nil)", "nil");
    RUN_TEST("", "(oddp 2)", "nil");
    RUN_TEST("", "(and (oddp 2) (print \"eval second statement\"))", "nil");
    RUN_TEST("", "(and (oddp 3) (print \"eval second statement\"))", "nil");
    RUN_TEST("", "(or (oddp 3) (print \"eval second statement\"))", "t");
    RUN_TEST("", "(or (oddp 2) (print \"eval second statement\"))", "nil");

    // p161
    RUN_TEST("", "'((Ada Lovelace) 45000.00 38519)", "((Ada Lovelace) 45000.00 38519)");
    RUN_TEST("(defun name-field (record) (nth 0 record))", "(name-field '((Ada Lovelace) 45000.00 38519))", "(Ada Lovelace)");

    // p162
    RUN_TEST("(defun name-field (record) (nth 0 record))\n(defun first-name (name) (nth 0 name))", "(first-name (name-field '((Ada Lovelace) 45000.00 38519)))", "Ada");
    RUN_TEST("", "(list 1 2 3 4)", "(1 2 3 4)");
    RUN_TEST("", "(list '(Ada Lovelace) 45000.00 338519)", "((Ada Lovelace) 45000.00 338519)");
    RUN_TEST("(defun build-record (name salary emp-number) (list name salary emp-number))", "(build-record '(Alan Turing) 50000.00 135772)", "((Alan Turing) 50000.00 135772)");
    RUN_TEST(R"(
            (defun build-record (name salary emp-number) (list name salary emp-number))
            (defun name-field (record) (nth 0 record))
            (defun salary-field (record) (nth 1 record))
            (defun number-field (record) (nth 2 record))
            (defun replace-salary-field (record new-salary) (build-record (name-field record) new-salary (number-field record)))
        )",
        R"(
            (replace-salary-field '((Ada Lovelace) 45000.00 338519) 50000.00)
        )",
        "((Ada Lovelace) 50000.00 338519)");
    
    // p163
    RUN_TEST("", "(car '(a b c))", "a");
    RUN_TEST("", "(cdr '(a b c))", "(b c)");
    RUN_TEST("", "(car '((a b) (c d)))", "(a b)");
    RUN_TEST("", "(cdr '((a b) (c d)))", "((c d))");
    RUN_TEST("", "(car (cdr '(a b c d)))", "b");

    // p164
    RUN_TEST(R"(
                (defun my-member (element my-list) (cond ((null my-list) nil)
                ((equal element (car my-list)) my-list)
                (t (my-member element (cdr my-list)))))
            )",
            "((my-member 4 '(1 2 3 4 5 6)) (my-member 5 '(a b c d)))",
            "((4 5 6) nil)");
}   

TEST_CASE("test-lisp-predicates-parsing", "[none]")
{
    auto runTest = [] (const char * file, int line, int column, const std::string & pred)
    {
        SECTION(TEST_LOC(pred)) {
            Context lcontext;
            ParseContext pcontext(file, pred.data(), pred.length(), line, 1000 /* column */);
            auto p = Predicate::parse(lcontext, pcontext);
            CHECK(p.toLisp().print({MetadataType::TYPE_INFO}) == pred);
            testParsingPrinting(p.toLisp());
        }
    };

    RUN_TEST("(+ $x:(type i64))");
}

TEST_CASE("test-lisp-predicates", "[none]")
{
    auto runTest = [] (const char * file, int line, int column, const std::string & pred, const std::string & input, const std::string & expectedOut)
    {
        SECTION(TEST_LOC(pred + " " + input + " --> " + expectedOut)) {
            Context lcontext;
            SourceLocation loc = getRawSourceLocation(file, line, column);
            auto p = Predicate::parse(lcontext, pred, loc);
            auto i = Value::parse(lcontext, input, loc);
            auto x = Value::parse(lcontext, expectedOut, loc);
            auto out = p.match(i);
            CHECK(out == x);
            testParsingPrinting(p.toLisp());
            testParsingPrinting(i);
            testParsingPrinting(x);
        }
    };

    RUN_TEST("(+ $x)", "(+ 1)", "(matched $x 1)");
    RUN_TEST("(+ $x)", "(- 1)", "nil");
    RUN_TEST("(+ '$x)", "(+ 1)", "nil");
    RUN_TEST("(+ '$x)", "(+ $x)", "(matched)");
    RUN_TEST("(+ $x $x)", "(+ 1 1)", "(matched $x 1)");
    RUN_TEST("(+ 0 $x)", "(+ 0 1)", "(matched $x 1)");
    RUN_TEST("(* (- $x $y) (+ $x $y))", "(* (- 1 3) (+ 1 3))", "(matched $x 1 $y 3)");
    RUN_TEST("(* (- $x $y) (+ $x $y))", "(* (- 1 3) (+ 1 2))", "nil");
    RUN_TEST("(* _ _)", "(* (- 1 3) (+ 1 2))", "(matched)");
    RUN_TEST("(+ $x $rest...)", "(+ 1)", "(matched $rest () $x 1)");
    RUN_TEST("(+ $x $rest...)", "(+ 1 2)", "(matched $rest (2) $x 1)");
    RUN_TEST("(+ $x $rest...)", "(+ 1 (* 2 3))", "(matched $rest ((* 2 3)) $x 1)");
    RUN_TEST("(+ (- $x $y)...)", "(+)", "(matched $x () $y ())");
    RUN_TEST("(+ (- $x $y)...)", "(+ (- 1 2))", "(matched $x (1) $y (2))");
    RUN_TEST("(+ (- $x $y)...)", "(+ (- 1 2) (- 3 4))", "(matched $x (1 3) $y (2 4))");
    // TODO
    //RUN_TEST("(($x...)...)", "((0 1 2) (3 4 5) (6 7) (8) ())", "(matched $x ((0 1 2) (3 4 5) (6 7) (8) ()))");
}

TEST_CASE("test-lisp-substitutions", "[none]")
{
    auto runTest = [] (const char * file, int line, int column, const std::string & pred, const std::string & subst, const std::string & input, const std::string & expectedOut)
    {
        SECTION(TEST_LOC(pred + " " + subst + " " + input + " --> " + expectedOut)) {
            Context lcontext;
            ParseContext pcontext(file, pred.data(), pred.length(), line);
            SourceLocation loc = getRawSourceLocation(file, line, column);
            auto p = Predicate::parse(lcontext, pred, loc);
            auto i = Value::parse(lcontext, input, loc);
            auto s = Substitution::parse(lcontext, subst, loc);
            auto x = Value::parse(lcontext, expectedOut, loc);
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

    RUN_TEST("(+ $x)", "$x", "(+ 1)", "1");
    RUN_TEST("(+ $x)", "'$x", "(+ 1)", "$x");
    RUN_TEST("(+ $x $x)", "(* 2 $x)", "(+ 1 1)", "(* 2 1)");
    RUN_TEST("(+ $x $y $rest...)", "(+ (+ $x $y) $rest...)", "(+ 1 2 3)", "(+ (+ 1 2) 3)");
    RUN_TEST("(+ $x $y $rest...)", "(+ (+ $x $y) $rest...)", "(+ 1 2 3 (4 5))", "(+ (+ 1 2) 3 (4 5))");
    RUN_TEST("(+ $x $y $rest...)", "(+ (+ $x $y) $rest...)", "(+ 1 2)", "(+ (+ 1 2))");
    RUN_TEST("(+ (- $x $y)...)", "(- (+ $x...) (+ $y...))", "(+ (- 1 2) (- 3 4) (- 5 6))", "(- (+ 1 3 5) (+ 2 4 6))");
}