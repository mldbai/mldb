/* string_algorithm_test.cc
 * Jeremy Barnes, 22 April 2018
 * This file is part of MLDB.  See the accompanying file LICENSE for licensing details.
 */

#include "catch2/catch_all.hpp"
#include "mldb/utils/split.h"
#include "mldb/utils/to_lower.h"
#include "mldb/utils/trim.h"
#include "mldb/utils/starts_with.h"
#include "mldb/utils/ostream_vector.h"
#include "mldb/utils/replace_all.h"
#include "mldb/types/string.h"
#include "mldb/arch/exception.h"

using namespace std;
using namespace MLDB;

// Test against boost string algorithm if available
#if __has_include(<boost/algorithm/string.hpp>)
#include <boost/algorithm/string.hpp>
#define BOOST_ALGORITHM_STRING_INCLUDED 1

template<typename Pred>
Pred boost_split_arg(Pred pred)
{
    return std::move(pred);
}

inline auto boost_split_arg(char c) -> decltype(boost::is_any_of(std::declval<std::string>()))
{
    return boost::is_any_of(std::string(1, c));
}
#endif

template<typename ResultString, typename String, typename Match>
void test_split_case(const String & str, Match&& match, const vector<ResultString> & expected)
{
    vector<ResultString> result;
    split(result, str, match);
    CHECK(result == expected);

#if BOOST_ALGORITHM_STRING_INCLUDED
    vector<ResultString> result2;
    boost::split(result2, str, boost_split_arg(match));
    CHECK(result2 == expected);
#endif
}

TEST_CASE("split")
{
    SECTION("split")
    {
        test_split_case<std::string>("a b c", ' ', { "a", "b", "c" });
    }

    SECTION("split with lambda")
    {
        test_split_case<std::string>("a b c", [] (char c) { return c == ' '; }, { "a", "b", "c" });
    }

    SECTION("split in middle")
    {
        test_split_case<std::string>("a b c", 'b', { "a ", " c" });
    }

    SECTION("split at beginning")
    {
        test_split_case<std::string>("a b c", 'a', { "", " b c" });
    }

    SECTION("split at end")
    {
        test_split_case<std::string>("a b c", [](char c) { return c == 'c'; }, { "a b ", "" });
    }

    SECTION("split empty string")
    {
        test_split_case<std::string>("", ' ', { "" });
    }

    SECTION("split single separator")
    {
        test_split_case<std::string>(" ", ' ', { "", "" });
    }

    SECTION("split no separator")
    {
        test_split_case<std::string>("a b c", 'd', { "a b c" });
    }

    SECTION("split just a separator")
    {
        test_split_case<std::string>("a", 'a', { "", "" });
    }

    SECTION("split multiple separators")
    {
        test_split_case<std::string>("a   b", ' ', { "a", "", "", "b" });
        test_split_case<std::string>("   ", ' ', { "", "", "", "" });
    }

    SECTION("split with isupper")
    {
        test_split_case<std::string>("aXbYc", [] (char c) { return std::isupper(c); }, { "a", "b", "c" });
    }
}

TEST_CASE("split UTF-8")
{
    SECTION("utf8 output")
    {
        Utf8String str("a b c");
        vector<Utf8String> result;
        split(result, "a b c", ' ');
        CHECK(result == vector<Utf8String>{ "a", "b", "c" });
    }

    SECTION("utf8 in and out")
    {
        vector<Utf8String> result;
        split(result, Utf8String("a b c"), ' ');
        CHECK(result == vector<Utf8String>{ "a", "b", "c" });
    }

    SECTION("utf8 non-ascii")
    {
        vector<Utf8String> result;
        split(result, Utf8String(U"a é c"), [] (char32_t c) { return (c & 127) != c; });
        CHECK(result == vector<Utf8String>{ "a ", " c" });
    }
}

template<typename String>
String & to_lower_result(String & str)
{
    to_lower(str);
    return str;
}

TEST_CASE("to_lower")
{
    SECTION("ascii")
    {
        std::string str;
        CHECK(to_lower_result(str = "") == "");
        CHECK(to_lower_result(str = "ABC") == "abc");
        CHECK(to_lower_result(str = "abc") == "abc");
        CHECK(to_lower_result(str = "aBc") == "abc");
    }

    SECTION("utf8")
    {
        Utf8String str;
        CHECK(to_lower_result(str = "") == "");
        CHECK(to_lower_result(str = "ABC") == Utf8String("abc"));
        CHECK(to_lower_result(str = "abc") == Utf8String("abc"));
        CHECK(to_lower_result(str = "aBc") == Utf8String("abc"));
        CHECK(to_lower_result(str = "AÉC") == Utf8String("aéc"));
    }
}

TEST_CASE("trim")
{
    SECTION("ltrimmed")
    {
        CHECK(ltrimmed(string("  a")) == "a");
        CHECK(ltrimmed(string("a")) == "a");
        CHECK(ltrimmed(string("a ")) == "a ");
        CHECK(ltrimmed(string("  ")) == "");
        CHECK(ltrimmed(string("")) == "");
    }
 
    SECTION("rtrimmed")
    {
        CHECK(rtrimmed(string("a  ")) == "a");
        CHECK(rtrimmed(string("a")) == "a");
        CHECK(rtrimmed(string("  a")) == "  a");
        CHECK(rtrimmed(string("  ")) == "");
        CHECK(rtrimmed(string("")) == "");
    }
 
    SECTION("trim")
    {
        CHECK(trimmed(string("  a  ")) == "a");
        CHECK(trimmed(string("a  ")) == "a");
        CHECK(trimmed(string("  a")) == "a");
        CHECK(trimmed(string("a")) == "a");
        CHECK(trimmed(string("  ")) == "");
        CHECK(trimmed(string("")) == "");
    }

    SECTION("trim utf8")
    {
        CHECK(trimmed(Utf8String("  a  ")) == Utf8String("a"));
        CHECK(trimmed(Utf8String("a  ")) == Utf8String("a"));
        CHECK(trimmed(Utf8String("  a")) == Utf8String("a"));
        CHECK(trimmed(Utf8String("a")) == Utf8String("a"));
        CHECK(trimmed(Utf8String("  ")) == Utf8String(""));
        CHECK(trimmed(Utf8String("")) == Utf8String(""));
    }
}

template<typename Str1, typename Str2>
bool test_starts_with(const Str1 & str1, const Str2 & str2)
{
    bool res = starts_with(str1, str2);
#if BOOST_ALGORITHM_STRING_INCLUDED
    if (res != boost::starts_with(str1, str2)) {
        std::cerr << "starts_with(" << str1 << ", " << str2 << ") MLDB = " << res
                  << " != " << boost::starts_with(str1, str2) << " boost" << std::endl;
    }
    CHECK(res == boost::starts_with(str1, str2));
#endif
    return res;
}

template<typename Str1, typename Str2>
bool test_ends_with(const Str1 & str1, const Str2 & str2)
{
    bool res = ends_with(str1, str2);
#if BOOST_ALGORITHM_STRING_INCLUDED
    CHECK(res == boost::ends_with(str1, str2));
#endif
    return res;
}

TEST_CASE("starts_with")
{
    SECTION("ascii")
    {
        CHECK(test_starts_with("abc", "a"));
        CHECK(test_starts_with("abc", "ab"));
        CHECK(test_starts_with("abc", "abc"));
        CHECK(!test_starts_with("abc", "abcd"));
        CHECK(!test_starts_with("abc", "b"));
        CHECK(!test_starts_with("abc", "bc"));
        CHECK(!test_starts_with("abc", "c"));
        CHECK(!test_starts_with("abc", "ac"));
        CHECK(!test_starts_with("abc", "abcde"));
        CHECK(test_starts_with("abc", ""));
        CHECK(     starts_with("abc", 'a'));
        CHECK(!    starts_with("abc", 'c'));
        CHECK(     starts_with('a', 'a'));
        CHECK(!    starts_with('c', 'a'));
    }

    SECTION("utf8")
    {
        CHECK(test_starts_with(Utf8String("abc"), "a"));
        CHECK(test_starts_with(Utf8String("abc"), "ab"));
        CHECK(test_starts_with(Utf8String("abc"), "abc"));
        CHECK(!test_starts_with(Utf8String("abc"), "abcd"));
        CHECK(!test_starts_with(Utf8String("abc"), "b"));
        CHECK(!test_starts_with(Utf8String("abc"), "bc"));
        CHECK(!test_starts_with(Utf8String("abc"), "c"));
        CHECK(!test_starts_with(Utf8String("abc"), "ac"));
        CHECK(!test_starts_with(Utf8String("abc"), "abcde"));
        CHECK(test_starts_with(Utf8String("abc"), ""));
    }

    SECTION("utf8 non-ascii")
    {
        CHECK( test_starts_with(Utf8String("aéc"), Utf8String("a")));
        CHECK( test_starts_with(Utf8String("aéc"), Utf8String("aé")));
        CHECK( test_starts_with(Utf8String("aéc"), Utf8String("aéc")));
        CHECK(!test_starts_with(Utf8String("aéc"), Utf8String("aécd")));
        CHECK(!test_starts_with(Utf8String("aéc"), Utf8String("é")));
        CHECK(!test_starts_with(Utf8String("aéc"), Utf8String("éc")));
        CHECK(!test_starts_with(Utf8String("aéc"), Utf8String("c")));
        CHECK(!test_starts_with(Utf8String("aéc"), Utf8String("ac")));
        CHECK(!test_starts_with(Utf8String("aéc"), Utf8String("aécdé")));
        CHECK( test_starts_with(Utf8String("aéc"), Utf8String("")));
    }

    SECTION("starts_with with different types")
    {
        CHECK(test_starts_with("abc", Utf8String("a")));
        CHECK(test_starts_with(Utf8String("abc"), "a"));
        CHECK(test_starts_with(Utf8String("abc"), Utf8String("a")));
    }

    SECTION("must_remove_prefix")
    {
        CHECK(must_remove_prefix<std::string>("abc", "a") == "bc");
        CHECK(must_remove_prefix<std::string>("abc", "ab") == "c");
        CHECK(must_remove_prefix<std::string>("abc", "abc") == "");
        CHECK(must_remove_prefix<std::string>("abc", "") == "abc");

        MLDB_TRACE_EXCEPTIONS(false);
        CHECK_THROWS(must_remove_prefix<std::string>("abc", "abcd"));
    }

    SECTION("must_remove_prefix UTF-8")
    {
        CHECK(must_remove_prefix(Utf8String("aéc"), "a") == Utf8String("éc"));
        CHECK(must_remove_prefix(Utf8String("aéc"), Utf8String("aé")) == Utf8String("c"));
        CHECK(must_remove_prefix(Utf8String("aéc"), Utf8String("aéc")) == Utf8String(""));
        CHECK(must_remove_prefix(Utf8String("aéc"), "") == Utf8String("aéc"));

        MLDB_TRACE_EXCEPTIONS(false);
        CHECK_THROWS(must_remove_prefix(Utf8String("aéc"), Utf8String("aécd")));
    }
}

TEST_CASE("test_ends_with")
{
    SECTION("ascii")
    {
        CHECK(test_ends_with("abc", "c"));
        CHECK(test_ends_with("abc", "bc"));
        CHECK(test_ends_with("abc", "abc"));
        CHECK(!test_ends_with("abc", "abcd"));
        CHECK(!test_ends_with("abc", "b"));
        CHECK(!test_ends_with("abc", "ab"));
        CHECK(!test_ends_with("abc", "a"));
        CHECK(!test_ends_with("abc", "ac"));
        CHECK(!test_ends_with("abc", "abcde"));
        CHECK(test_ends_with("abc", ""));
    }

    SECTION("utf8")
    {
        CHECK(test_ends_with(Utf8String("abc"), "c"));
        CHECK(test_ends_with(Utf8String("abc"), "bc"));
        CHECK(test_ends_with(Utf8String("abc"), "abc"));
        CHECK(!test_ends_with(Utf8String("abc"), "abcd"));
        CHECK(!test_ends_with(Utf8String("abc"), "b"));
        CHECK(!test_ends_with(Utf8String("abc"), "ab"));
        CHECK(!test_ends_with(Utf8String("abc"), "a"));
        CHECK(!test_ends_with(Utf8String("abc"), "ac"));
        CHECK(!test_ends_with(Utf8String("abc"), "abcde"));
        CHECK(test_ends_with(Utf8String("abc"), ""));
    }

    SECTION("utf8 non-ascii")
    {
        CHECK( test_ends_with(Utf8String("aéc"), Utf8String("c")));
        CHECK( test_ends_with(Utf8String("aéc"), Utf8String("éc")));
        CHECK( test_ends_with(Utf8String("aéc"), Utf8String("aéc")));
        CHECK(!test_ends_with(Utf8String("aéc"), Utf8String("écd")));
        CHECK(!test_ends_with(Utf8String("aéc"), Utf8String("é")));
        CHECK(!test_ends_with(Utf8String("aéc"), Utf8String("aé")));
        CHECK(!test_ends_with(Utf8String("aéc"), Utf8String("a")));
        CHECK(!test_ends_with(Utf8String("aéc"), Utf8String("ac")));
        CHECK(!test_ends_with(Utf8String("aéc"), Utf8String("aécdé")));
        CHECK( test_ends_with(Utf8String("aéc"), Utf8String("")));
    }

    SECTION("ends_with with different types")
    {
        CHECK(test_ends_with("abc", Utf8String("c")));
        CHECK(test_ends_with(Utf8String("abc"), "c"));
        CHECK(test_ends_with(Utf8String("abc"), Utf8String("c")));
    }

    SECTION("must_remove_suffix")
    {
        CHECK(must_remove_suffix<std::string>("abc", "c") == "ab");
        CHECK(must_remove_suffix<std::string>("abc", "bc") == "a");
        CHECK(must_remove_suffix<std::string>("abc", "abc") == "");
        CHECK(must_remove_suffix<std::string>("abc", "") == "abc");

        MLDB_TRACE_EXCEPTIONS(false);
        CHECK_THROWS(must_remove_suffix<std::string>("abc", "abcd"));
    }

    SECTION("must_remove_suffix utf-8")
    {
        CHECK(must_remove_suffix(Utf8String("aéc"), "c") == Utf8String("aé"));
        CHECK(must_remove_suffix(Utf8String("aéc"), Utf8String("éc")) == Utf8String("a"));
        CHECK(must_remove_suffix(Utf8String("aéc"), Utf8String("aéc")) == Utf8String(""));
        CHECK(must_remove_suffix(Utf8String("aéc"), "") == Utf8String("aéc"));

        MLDB_TRACE_EXCEPTIONS(false);
        CHECK_THROWS(must_remove_suffix(Utf8String("aéc"), "écd"));
    }
}

template<typename Haystack, typename Needle, typename Replacement>
Haystack test_replace_all_copy(Haystack haystack, Needle&& needle, Replacement&& replacement)
{
    //cerr << "haystack = " << haystack << ", needle = " << needle << ", replacement = " << replacement << endl;
    Haystack res = replace_all_copy(haystack, std::forward<Needle>(needle), std::forward<Replacement>(replacement));
#if BOOST_ALGORITHM_STRING_INCLUDED
    CHECK(res == boost::replace_all_copy(haystack, needle, replacement));
#endif
    return res;
}

TEST_CASE("replace_all_copy")
{
    SECTION("ascii")
    {
        // Boundary conditions
        CHECK(test_replace_all_copy<std::string>("", "", "") == "");
        CHECK(test_replace_all_copy<std::string>("", "a", "") == "");
        CHECK(test_replace_all_copy<std::string>("", "", "a") == "");
        CHECK(test_replace_all_copy<std::string>("a", "", "") == "a");
        CHECK(test_replace_all_copy<std::string>("a", "a", "") == "");

        CHECK(test_replace_all_copy<std::string>("a", "a", "b") == "b");
        CHECK(test_replace_all_copy<std::string>("a", "b", "c") == "a");
        CHECK(test_replace_all_copy<std::string>("a", "a", "aa") == "aa");
        CHECK(test_replace_all_copy<std::string>("a", "a", "aaa") == "aaa");
        CHECK(test_replace_all_copy<std::string>("abc", "b", "x") == "axc");
        CHECK(test_replace_all_copy<std::string>("abc", "b", "x") == "axc");
        CHECK(test_replace_all_copy<std::string>("abc", "bc", "x") == "ax");
        CHECK(test_replace_all_copy<std::string>("abc", "abc", "x") == "x");
        CHECK(test_replace_all_copy<std::string>("abc", "abcd", "x") == "abc");
        CHECK(test_replace_all_copy<std::string>("abc", "b", "") == "ac");
        CHECK(test_replace_all_copy<std::string>("abc", "bc", "") == "a");
        CHECK(test_replace_all_copy<std::string>("abc", "abc", "") == "");
        CHECK(test_replace_all_copy<std::string>("abc", "abcd", "") == "abc");
        CHECK(test_replace_all_copy<std::string>("abc", "b", "xyz") == "axyzc");
        CHECK(test_replace_all_copy<std::string>("abc", "bc", "xyz") == "axyz");
        CHECK(test_replace_all_copy<std::string>("abc", "abc", "xyz") == "xyz");
        CHECK(test_replace_all_copy<std::string>("abc", "abcd", "xyz") == "abc");
        CHECK(test_replace_all_copy<std::string>("abc", "b", "xyz") == "axyzc");
        CHECK(test_replace_all_copy<std::string>("abc", "bc", "xyz") == "axyz");
        CHECK(test_replace_all_copy<std::string>("abc", "abc", "xyz") == "xyz");
        CHECK(test_replace_all_copy<std::string>("abc", "abcd", "xyz") == "abc");
        CHECK(test_replace_all_copy<std::string>("abc", "b", "xyz") == "axyzc");
        CHECK(test_replace_all_copy<std::string>("abc", "bc", "xyz") == "axyz");
        CHECK(test_replace_all_copy<std::string>("abc", "abc", "xyz") == "xyz");
        CHECK(test_replace_all_copy<std::string>("abc", "abcd", "xyz") == "abc");
        CHECK(test_replace_all_copy<std::string>("abc", "b", "xyz") == "axyzc");
        CHECK(test_replace_all_copy<std::string>("abc", "bc", "xyz") == "axyz");
        CHECK(test_replace_all_copy<std::string>("abc", "abc", "xyz") == "xyz");
        CHECK(test_replace_all_copy<std::string>("abc", "abcd", "xyz") == "abc");
        CHECK(test_replace_all_copy<std::string>("abc", "b", "xyz") == "axyzc");
        CHECK(test_replace_all_copy<std::string>("abc", "bc", "xyz") == "axyz");
    }

    SECTION("utf8")
    {
        // Boundary conditions
        CHECK(test_replace_all_copy<Utf8String>("", "", "") == "");
        CHECK(test_replace_all_copy<Utf8String>("", "a", "") == "");
        CHECK(test_replace_all_copy<Utf8String>("", "", "a") == "");
        CHECK(test_replace_all_copy<Utf8String>("a", "", "") == "a");
        CHECK(test_replace_all_copy<Utf8String>("a", "a", "") == "");

        // With Unicode characters
        CHECK(test_replace_all_copy<Utf8String>(u"é", Utf8String("é"), Utf8String("b")) == "b");
        CHECK(test_replace_all_copy<Utf8String>(u"é", Utf8String("b"), Utf8String("c")) == "é");
        CHECK(test_replace_all_copy<Utf8String>(u"é", Utf8String("é"), Utf8String("aa")) == "aa");
        CHECK(test_replace_all_copy<Utf8String>(u"é", Utf8String("é"), Utf8String("ààà")) == "ààà");

        // Multi-byte Unicode characters
        CHECK(test_replace_all_copy<Utf8String>(u"aéb",  Utf8String("é"), Utf8String("x")) == "axb");
        CHECK(test_replace_all_copy<Utf8String>(u"a❤️b❤️", Utf8String("❤️"), Utf8String("")) == "ab");
        CHECK(test_replace_all_copy<Utf8String>(u"a❤️b❤️", Utf8String("❤️"), Utf8String("x")) == "axbx");
        CHECK(test_replace_all_copy<Utf8String>(u"a❤️b❤️", Utf8String("❤️"), Utf8String("xyz")) == "axyzbxyz");
        CHECK(test_replace_all_copy<Utf8String>(u"a❤️b❤️", Utf8String("a"), Utf8String("❤️")) == "❤️❤️b❤️");
    }
}

