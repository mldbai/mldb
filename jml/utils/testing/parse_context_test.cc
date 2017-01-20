/* parse_context_test.cc                                           -*- C++ -*-
   Jeremy Barnes, 16 February 2007
   Copyright (c) 2007 Jeremy Barnes.

   This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

   Test of tick counter functionality.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK

#include "mldb/base/parse_context.h"
#include "mldb/jml/utils/file_functions.h"
#include "mldb/jml/utils/guard.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/jml/utils/vector_utils.h"
#include "mldb/arch/exception_handler.h"
#include "mldb/jml/utils/environment.h"
#include "mldb/jml/utils/csv.h"
#include "mldb/arch/format.h"
#include "mldb/ext/utf8cpp/source/utf8.h"
#include <boost/test/unit_test.hpp>
#include <boost/filesystem.hpp>
#include <sstream>
#include <iomanip>
#include <fstream>
#include <stdlib.h>
#include <locale.h>

using namespace ML;
using namespace MLDB;
using namespace std;

using boost::unit_test::test_suite;

#if 0

BOOST_AUTO_TEST_CASE( test_float_parsing )
{
    for (unsigned i = 0;  i < 1000;  ++i) {
        float f = random() / 100000000.0;
        string s = format("%.6f", f);
        ParseContext pc(s, s.c_str(), s.c_str() + s.length());
        float f2 = pc.expect_float();
        string s2 = format("%.6f", f2);
        
        BOOST_CHECK_EQUAL(s, s2);
    }
}

BOOST_AUTO_TEST_CASE( test_float_parsing2 )
{
    for (unsigned i = 0;  i < 1000;  ++i) {
        float f = random() + random() / 100000000.0;
        string s = format("%.6f", f);
        ParseContext pc(s, s.c_str(), s.c_str() + s.length());
        float f2 = pc.expect_float();
        string s2 = format("%.6f", f2);
        
        BOOST_CHECK_EQUAL(s, s2);
    }
}

BOOST_AUTO_TEST_CASE( test_double_parsing )
{
    for (unsigned i = 0;  i < 1000;  ++i) {
        double f = random() / 100000000.0;
        string s = format("%.6f", f);
        ParseContext pc(s, s.c_str(), s.c_str() + s.length());
        double f2 = pc.expect_double();
        string s2 = format("%.6f", f2);
        
        BOOST_CHECK_EQUAL(s, s2);
    }
}

#endif

BOOST_AUTO_TEST_CASE( test_double_parsing2 )
{
    for (unsigned digits = 0;  digits < 20;  ++digits) {

        for (unsigned i = 0;  i < 100;  ++i) {

            string fmt = MLDB::format("%%.%df", digits);

            double f = random() + random() / 100000000.0;
            string s = format(fmt.c_str(), f);
            char * end = (char *)(s.c_str() + s.length());
            double f3 = strtod(s.c_str(), &end);

            if (digits < 18)
                f = f3;

            ParseContext pc(s, s.c_str(), s.c_str() + s.length());
            double f2 = pc.expect_double();
            string s2 = format(fmt.c_str(), f2);
            string s3 = format(fmt.c_str(), f3);

#if 0
            auto to_i = [] (double d)
                {
                    union {
                        double d;
                        uint64_t i;
                    } u;
                    u.d = d;
                    return u.i;
                };

            cerr << endl;

            uint64_t u1 = to_i(f);
            uint64_t u2 = to_i(f2);
            uint64_t u3 = to_i(f3);
        
            cerr << "f = " << f << " s = " << s << " f2 = "
                 << f2 << " f3 = " << f3 << endl;

            cerr << "u1 = " << MLDB::format("%016llx\n", u1)
                 << "u2 = " << MLDB::format("%016llx\n", u2)
                 << "u3 = " << MLDB::format("%016llx\n", u3);
#endif

            // Make sure that strtod can parse it back to the same number
            BOOST_REQUIRE_EQUAL(f, f3);

            BOOST_CHECK_EQUAL(f, f2);
        
            // NOTE: even using strtod, we get differences here
            // It's just a double range thing...
            BOOST_CHECK_EQUAL(s, s2);
            BOOST_CHECK_EQUAL(f2, f3);
        }
    }
}

BOOST_AUTO_TEST_CASE( test_double_parsing3 )
{
    double f = 1877212.719993526;
    string s = "1877212.719993526";
    ParseContext pc(s, s.c_str(), s.c_str() + s.length());
    double f2 = pc.expect_double();
    char * end = (char *)(s.c_str() + s.length());
    double f3 = strtod(s.c_str(), &end);
    string s2 = format("%.9f", f2);
    string s3 = format("%.9f", f3);

    BOOST_CHECK_EQUAL(f, f2);
    BOOST_CHECK_EQUAL(f, f3);
    BOOST_CHECK_EQUAL(s, s2);
    BOOST_CHECK_EQUAL(s, s3);
}

BOOST_AUTO_TEST_CASE( test_large_double_parsing )
{
    double f = 9.5637941572227e+307;
    string s = "9.5637941572227e+307";
    const char* sp = s.c_str();
    ParseContext pcs(s, sp, sp + s.length());
    double f2 = pcs.expect_double();
    char * end = (char *)(s.c_str() + s.length());
    double f3 = strtod(s.c_str(), &end);

    BOOST_CHECK_EQUAL(f, f2);
    BOOST_CHECK_EQUAL(f, f3);
}

BOOST_AUTO_TEST_CASE( test_difficult_double_parsing )
{
    long long ints_representable_as_double[] = {
        (1LL << 53) - 1,
        (1LL << 53),
        -(1LL << 53),
        -(1LL << 53) + 1,
        (1LL << 62)
    };

   for (long long i: ints_representable_as_double) {
        string s = format("%lld", i);
        const char* sp = s.c_str();
        ParseContext pcs(s, sp, sp + s.length());
        {
            ParseContext::Revert_Token tok(pcs);
            double f2 = pcs.expect_double();
            std::cout << i << " " << (long long) f2 << std::endl;
            BOOST_CHECK_EQUAL(i, (long long) f2);
        }
        double dummy;
        BOOST_CHECK(!pcs.match_double(dummy, -INFINITY, INFINITY, false));
    }

   // integer with mantissa too large to fit in a double
   long long ints_not_representable_as_double[] = {
       (1LL << 53) + 1,
       -(1LL << 53) - 1,
       (1LL << 62) + 1
   };

   for (long long i: ints_not_representable_as_double) {
       string s = format("%lld", i);
       const char* sp = s.c_str();
       ParseContext pcs(s, sp, sp + s.length());
       {
           ParseContext::Revert_Token tok(pcs);
           double f2 = pcs.expect_double();
           std::cout << std::setprecision(18) << strtod(sp, nullptr) << " " << f2 << std::endl;
           BOOST_CHECK_EQUAL(strtod(sp, nullptr), f2);
       }
       double dummy;
       BOOST_CHECK(!pcs.match_double(dummy, -INFINITY, INFINITY, false));
   }

}

void test_long_long(long long value)
{
    string s = format("%lld", value);
    ParseContext pc(s, s.c_str(), s.c_str() + s.length());
    long long value2 = pc.expect_long_long(value);
    BOOST_CHECK_EQUAL(value, value2);
}

BOOST_AUTO_TEST_CASE( test_long_long_parsing )
{
    test_long_long(0);
    test_long_long(1);
    test_long_long(-1);
    test_long_long(-9219216340478909303LL);
    test_long_long(LONG_LONG_MIN);
    test_long_long(LONG_LONG_MAX);
}

static const char * testUnicode_str = "0026\n026";

void run_test_unicode(ParseContext & context)
{
    int j = -1 ;
    BOOST_CHECK(context.match_hex4(j));
    BOOST_CHECK_EQUAL(j, 38);
    BOOST_CHECK(context.match_eol());
    BOOST_CHECK_EQUAL(context.get_line(), 2);
    BOOST_CHECK_EQUAL(context.get_col(), 1);
    j = -1 ;
    BOOST_CHECK(!context.match_hex4(j));
}

BOOST_AUTO_TEST_CASE( test_Unicode )
{
    {
        ParseContext context("test unicode",
                              testUnicode_str, testUnicode_str + strlen(testUnicode_str));
        run_test_unicode(context);
    }
}
static const char * test1_str = "Here \t is a\tparse context\nwith two\ni mean 3 lines";

void run_test1(ParseContext & context)
{
    BOOST_CHECK(!context.eof());
    BOOST_CHECK_EQUAL(context.get_offset(), 0);
    BOOST_REQUIRE(!context.eof());
    BOOST_CHECK(context.match_literal('H'));
    BOOST_REQUIRE(!context.eof());
    BOOST_CHECK(context.match_literal("ere"));
    BOOST_CHECK(context.match_whitespace());
    string word;
    BOOST_CHECK(context.match_text(word, ' '));
    BOOST_CHECK_EQUAL(word, "is");
    BOOST_CHECK(context.match_whitespace());
    BOOST_CHECK(!context.match_literal('A'));
    BOOST_CHECK(context.match_text(word, " \t"));
    BOOST_CHECK_EQUAL(word, "a");
    BOOST_CHECK_EQUAL(std::string(test1_str).find('p') - 1, context.get_offset());
    BOOST_CHECK(context.match_text(word, " \t"));
    BOOST_CHECK_EQUAL(word, "");
    BOOST_CHECK(context.match_text(word, " "));
    BOOST_CHECK_EQUAL(word, "\tparse");
    int col_before = context.get_col(), line_before = context.get_line(),
        ofs_before = context.get_offset();
    BOOST_CHECK(!context.match_literal("context"));
    BOOST_CHECK_EQUAL(*context, ' ');
    BOOST_CHECK_EQUAL(context.get_col(), col_before);
    BOOST_CHECK_EQUAL(context.get_line(), line_before);
    BOOST_CHECK_EQUAL(context.get_offset(), ofs_before);
    BOOST_CHECK(context.match_whitespace());
    BOOST_CHECK(context.match_literal("context"));
    BOOST_CHECK_EQUAL(context.get_line(), 1);
    BOOST_CHECK_EQUAL(context.get_col(), 26);
    BOOST_CHECK(context.match_eol());
    BOOST_CHECK_EQUAL(context.get_line(), 2);
    BOOST_CHECK_EQUAL(context.get_col(), 1);
    BOOST_CHECK(context.match_line(word));
    BOOST_CHECK(context.match_literal("i mean "));
    BOOST_CHECK_EQUAL(context.get_line(), 3);
    BOOST_CHECK_EQUAL(context.get_col(), 8);
    int i = -1;
    BOOST_CHECK(context.match_int(i));
    BOOST_CHECK_EQUAL(i, 3);
    BOOST_CHECK(!context.match_int(i));
    BOOST_CHECK(context.match_whitespace());
    BOOST_CHECK(context.match_text(word, "abcdfghjkmopqrstuvwxyz"));
    BOOST_CHECK_EQUAL(word, "line");
    BOOST_CHECK(context.match_literal('s'));
    BOOST_CHECK(context.match_eol());
    BOOST_CHECK(context.eof());
    {
        MLDB_TRACE_EXCEPTIONS(false);
        BOOST_CHECK_THROW(*context, MLDB::Exception);
    }
    BOOST_CHECK_EQUAL(context.get_offset(), strlen(test1_str));
}

BOOST_AUTO_TEST_CASE( test1 )
{
    cerr << "starting test1" << endl;

    {
        ParseContext context("test",
                              test1_str, test1_str + strlen(test1_str));
        run_test1(context);
    }

    size_t sizes[] = { 1, 65530, 1, 2, 3, 5, 128, 0 };
    
    for (unsigned i = 0;  sizes[i];  ++i) {
        cerr << "starting test1 with size " << sizes[i] << endl;
        istringstream stream(test1_str);
        ParseContext context("test", stream, 1, 1, sizes[i]);
        run_test1(context);
        cerr << "done test" << endl;
   }
}

/* Test opening a file */
BOOST_AUTO_TEST_CASE( test2 )
{
    string tmp_filename = "tmp/parse_context_test_file";
    boost::filesystem::create_directory("tmp");
    Call_Guard guard;
    {
        ofstream stream(tmp_filename.c_str());
        guard.set(std::bind(&delete_file, tmp_filename));
        stream << test1_str;
    }

    ParseContext context(tmp_filename);

    run_test1(context);
}

std::string expect_feature_name(ParseContext & c)
{
    std::string result;
    /* We have a backslash escaped name. */
    bool after_backslash = false;
    
    ParseContext::Revert_Token tok(c);
    
    int len = 0;
    while (c && *c != '\n') {
        if (!after_backslash && (isspace(*c) || *c == ':' || *c == ','))
            break;
        if (*c == '\\') after_backslash = true;
        else { ++len;  after_backslash = false; }
        ++c;
    }
    
    result.reserve(len);
    tok.apply();
    
    if (after_backslash) c.exception("Invalid backslash escaping");
    after_backslash = false;
    while (c && *c != '\n') {
        if (!after_backslash && (isspace(*c) || *c == ':'
                                 || *c == ',')) break;
        if (*c == '\\') after_backslash = true;
        else { result += *c;  after_backslash = false; }
        ++c;
    }
    
    if (result.empty())
        c.exception("expect_feature_name(): no feature name found");
    
    return result;
}

/* Test parse bug */
BOOST_AUTO_TEST_CASE( test3 )
{
    string header = "LABEL X Y";
    ParseContext context("test file", header.c_str(),
                          header.c_str() + header.size());

    BOOST_CHECK(context);
    context.skip_whitespace();
    BOOST_CHECK_EQUAL(expect_feature_name(context), "LABEL");
    context.skip_whitespace();
    BOOST_CHECK_EQUAL(expect_feature_name(context), "X");
    context.skip_whitespace();
    BOOST_CHECK(context);
    BOOST_CHECK_EQUAL(expect_feature_name(context), "Y");
    BOOST_CHECK(!context);
}

BOOST_AUTO_TEST_CASE( test4 )
{
    string file = "LABEL X Y\n1 0 0\n0 1 0\n0 0 1\n1 1 1\n";
    ParseContext context("test file", file.c_str(),
                          file.c_str() + file.size());
    
    BOOST_CHECK(context);
    context.skip_line();
    BOOST_CHECK_EQUAL(context.get_offset(), 10);
    BOOST_CHECK_EQUAL(context.expect_text(" \n\t"), "1");
    BOOST_CHECK_EQUAL(context.get_offset(), 11);
    BOOST_CHECK(context.match_whitespace());
    BOOST_CHECK_EQUAL(context.get_offset(), 12);
    BOOST_CHECK_EQUAL(context.expect_text(" \n\t"), "0");
    BOOST_CHECK_EQUAL(context.get_offset(), 13);
    BOOST_CHECK(context.match_whitespace());
    BOOST_CHECK_EQUAL(context.get_offset(), 14);
    BOOST_CHECK_EQUAL(context.expect_text(" \n\t"), "0");
    BOOST_CHECK_EQUAL(context.get_offset(), 15);
    BOOST_CHECK_EQUAL((int)*context, (int)'\n');
    BOOST_CHECK(!context.match_whitespace());
    BOOST_CHECK(context.match_eol());
    BOOST_CHECK_EQUAL(context.get_offset(), 16);
    BOOST_CHECK_EQUAL(context.expect_text(" \n\t"), "0");
}

BOOST_AUTO_TEST_CASE( test5 )
{
    string file = "1.234e-05";
    ParseContext context("test file", file.c_str(),
                          file.c_str() + file.size());

    float f = context.expect_float();
    float f2 = 1.234e-05;

    BOOST_CHECK_EQUAL(f, f2);
}

BOOST_AUTO_TEST_CASE( test6 )
{
    string file = "33 nan nan 1 nan nan 2 -nan +nan";
    ParseContext context("test file", file.c_str(),
                          file.c_str() + file.size());

    float f = context.expect_float();
    float f2 = 33;
    context.expect_whitespace();
    
    BOOST_CHECK_EQUAL(f, f2);

    f = context.expect_float();
    cerr << "f = " << f << endl;
    BOOST_CHECK(isnanf(f));
    context.expect_whitespace();

    f = context.expect_float();
    BOOST_CHECK(isnanf(f));
    context.expect_whitespace();

    f = context.expect_float();
    BOOST_CHECK_EQUAL(f, 1.0f);
    context.expect_whitespace();

    f = context.expect_float();
    BOOST_CHECK(isnanf(f));
    context.expect_whitespace();

    f = context.expect_float();
    BOOST_CHECK(isnanf(f));
    context.expect_whitespace();

    f = context.expect_float();
    BOOST_CHECK_EQUAL(f, 2.0f);
}

BOOST_AUTO_TEST_CASE( test_big_chunk_size )
{
    size_t NCHARS = 1024 * 1024 * 16;

    string s(NCHARS, 0);

    BOOST_CHECK_EQUAL(s.size(), NCHARS);

    for (unsigned i = 0;  i < NCHARS;  ++i)
        s[i] = i % 256;
    
    istringstream stream(s);

    ParseContext context("test file", stream);
    
    // 1G, can't possibly fit on stack
    context.set_chunk_size(1024 * 1024 * 1024);

    int n = 0;
    while (context) {
        if (s[n] != *context++) {
            cerr << "error at position " << n << endl;
            break;
        }
        ++n;
    }

    BOOST_CHECK_EQUAL(n, NCHARS);

    BOOST_CHECK_EQUAL(context.get_offset(), s.size());
}

BOOST_AUTO_TEST_CASE( test_chunking_stream1 )
{
    // Make some random records in a string
    string s = "33 nan nan 1 nan nan 2 -nan +nan";

    int chunk_sizes[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
                          31, 61, 63, 1024 * 1024 * 16 };
    
    int nchunk_sizes = sizeof(chunk_sizes) / sizeof(chunk_sizes[0]);

    for (unsigned i = 0;  i < nchunk_sizes;  ++i) {
        istringstream stream(s);

        ParseContext context("test file", stream, 1, 1, 1);
        
        context.set_chunk_size(chunk_sizes[i]);

        float f = context.expect_float();
        float f2 = 33;
        context.expect_whitespace();
        
        BOOST_CHECK_EQUAL(f, f2);
        
        f = context.expect_float();
        BOOST_CHECK(isnanf(f));
        context.expect_whitespace();
        
        f = context.expect_float();
        BOOST_CHECK(isnanf(f));
        context.expect_whitespace();
        
        f = context.expect_float();
        BOOST_CHECK_EQUAL(f, 1.0f);
        context.expect_whitespace();
        
        f = context.expect_float();
        BOOST_CHECK(isnanf(f));
        context.expect_whitespace();
        
        f = context.expect_float();
        BOOST_CHECK(isnanf(f));
        context.expect_whitespace();
        
        f = context.expect_float();
        BOOST_CHECK_EQUAL(f, 2.0f);
        context.expect_whitespace();

        f = context.expect_float();
        BOOST_CHECK(isnanf(f));
        context.expect_whitespace();
        
        f = context.expect_float();
        BOOST_CHECK(isnanf(f));
        
        BOOST_CHECK(context.eof());
    }
}

void test_csv_data_size(int chunk_size, vector<vector<string> > & reference)
{
    string input_file = "mldb/jml/utils/testing/parse_context_test_data.csv.gz";

    cerr << "input_file = " << input_file << endl;

    MLDB::filter_istream stream(input_file);
    ParseContext context(input_file, stream);

    context.set_chunk_size(chunk_size);

    bool is_reference = reference.empty();

    try {
        for (int i = 0;  context;  ++i) {
            vector<string> row = expect_csv_row(context);
            if (is_reference)
                reference.push_back(row);
            else {
                if (reference.at(i) != row) {
                    cerr << "error on row " << i << endl;
                    BOOST_CHECK_EQUAL(reference.at(i), row);
                }
            }
        }
    } catch (...) {
        if (is_reference) reference.clear();
        throw;
    }

    cerr << "succeeded in reading " << reference.size()
         << " rows with chunk_size " << chunk_size << endl;

    BOOST_CHECK_EQUAL(reference.size(), 10000);
}

BOOST_AUTO_TEST_CASE( test_csv_data )
{
    int chunk_sizes[] = { 1024 * 1024 * 16,
                          1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
                          31, 61, 63, 65531 };
    
    int nchunk_sizes = sizeof(chunk_sizes) / sizeof(chunk_sizes[0]);
    
    vector<vector<string> > reference;

    for (unsigned i = 0;  i < nchunk_sizes;  ++i) {
        BOOST_CHECK_NO_THROW(test_csv_data_size(chunk_sizes[i], reference));
    }
}

BOOST_AUTO_TEST_CASE( test_token )
{
    string s = "aaabac";
    istringstream stream(s);
    ParseContext context("test", stream, 1, 1, 1 /* chunk size */);
    
    BOOST_CHECK_EQUAL(context.readahead_available(), 1);
    BOOST_CHECK_EQUAL(context.total_buffered(), 1);
    BOOST_CHECK_EQUAL(*context, 'a');
    BOOST_CHECK_EQUAL(context.get_offset(), 0);

    {
        ParseContext::Revert_Token token(context);

        BOOST_CHECK_EQUAL(*context, 'a');
        BOOST_CHECK_EQUAL(context.total_buffered(), 1);
        BOOST_CHECK_EQUAL(context.get_offset(), 0);
        ++context;
        BOOST_CHECK_EQUAL(*context, 'a');
        BOOST_CHECK_EQUAL(context.total_buffered(), 2);
        BOOST_CHECK_EQUAL(context.get_offset(), 1);
        ++context;
        BOOST_CHECK_EQUAL(*context, 'a');
        BOOST_CHECK_EQUAL(context.total_buffered(), 3);
        BOOST_CHECK_EQUAL(context.get_offset(), 2);
        ++context;
        BOOST_CHECK_EQUAL(*context, 'b');
        BOOST_CHECK_EQUAL(context.total_buffered(), 4);
        BOOST_CHECK_EQUAL(context.get_offset(), 3);
        ++context;
        BOOST_CHECK_EQUAL(*context, 'a');
        BOOST_CHECK_EQUAL(context.total_buffered(), 5);
        BOOST_CHECK_EQUAL(context.get_offset(), 4);
        ++context;
        BOOST_CHECK_EQUAL(*context, 'c');
        BOOST_CHECK_EQUAL(context.total_buffered(), 6);
        BOOST_CHECK_EQUAL(context.get_offset(), 5);
        ++context;
        ParseContext::Revert_Token token2(context);
    }

    BOOST_CHECK_EQUAL(context.get_offset(), 0);
    BOOST_CHECK_EQUAL(*context, 'a');
    BOOST_CHECK_EQUAL(context.readahead_available(), 6);
    BOOST_CHECK_EQUAL(context.total_buffered(), 6);
}

BOOST_AUTO_TEST_CASE(test_dodgy_float_parsing1)
{
    string s = "3eabd3c2-825c-11e0-a4a8-0026b937c890";
    ParseContext c1(s, s.c_str(), s.c_str() + s.length());
    double d = -1.0;
    BOOST_CHECK(c1.match_double(d));
    BOOST_CHECK_EQUAL(d, 3.0);
    BOOST_CHECK_EQUAL(*c1, 'e');
}

BOOST_AUTO_TEST_CASE(test_dodgy_float_parsing2)
{
    string s = "Englewood";
    ParseContext c1(s, s.c_str(), s.c_str() + s.length());
    double d = -1.0;
    BOOST_CHECK(!c1.match_double(d));
    BOOST_CHECK_EQUAL(d, -1.0);
    BOOST_CHECK_EQUAL(*c1, 'E');
}

BOOST_AUTO_TEST_CASE(test_dodgy_float_parsing3)
{
    string s = "";
    ParseContext c1(s, s.c_str(), s.c_str() + s.length());
    double d = -1.0;
    BOOST_CHECK(!c1.match_double(d));
    BOOST_CHECK_EQUAL(d, -1.0);
}

BOOST_AUTO_TEST_CASE(test_dodgy_float_parsing4)
{
    string s = "-";
    ParseContext c1(s, s.c_str(), s.c_str() + s.length());
    double d = -1.0;
    BOOST_CHECK(!c1.match_double(d));
    BOOST_CHECK_EQUAL(d, -1.0);
}

BOOST_AUTO_TEST_CASE(test_dodgy_float_parsing5)
{
    string s = "+";
    ParseContext c1(s, s.c_str(), s.c_str() + s.length());
    double d = -1.0;
    BOOST_CHECK(!c1.match_double(d));
    BOOST_CHECK_EQUAL(d, -1.0);
}

BOOST_AUTO_TEST_CASE(test_match_literal_at_eof)
{
    string s = "hello";
    ParseContext c1(s, s.c_str(), s.c_str() + s.length());
    BOOST_CHECK(c1.match_literal("hello"));
    BOOST_CHECK(!c1.match_literal("one"));
}

BOOST_AUTO_TEST_CASE(test_revert_token_at_eof)
{
    string s = "a";
    ParseContext c1(s, s.c_str(), s.c_str() + s.length());
    ++c1;
    ParseContext::Hold_Token token1(c1);
    try {
        ParseContext::Revert_Token token(c1);
        throw MLDB::Exception("hello");
    } catch (const std::exception & exc) {
        cerr << "got exception " << exc.what() << endl;
    }
    cerr << "finished" << endl;
}


