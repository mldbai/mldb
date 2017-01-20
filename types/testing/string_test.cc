// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

/* string_test.cc
   Copyright (c) 2012 mldb.ai inc.  All rights reserved.
*/

#define BOOST_TEST_MAIN
#define BOOST_TEST_DYN_LINK
#include "mldb/types/string.h"
#include <boost/test/unit_test.hpp>
#include <boost/regex/icu.hpp>
#include <boost/regex.hpp>
#include "mldb/ext/jsoncpp/json.h"
#include "mldb/arch/format.h"

using namespace std;
using namespace MLDB;


BOOST_AUTO_TEST_CASE( test_print_format )
{
   	std::string raw = "saint-jérôme";
   	unsigned numAccentedChars = 0;

   	Utf8String utf8(raw);
   	// Now iterate through the utf8 string
   	for (Utf8String::const_iterator it = utf8.begin(); it != utf8.end(); ++it)
   	{
   		if (*it ==  L'é' || *it ==  L'ô')
   			numAccentedChars++;
   	}
   	BOOST_CHECK_EQUAL(numAccentedChars, 2);
   	// Now add another string to it
  	std::string raw2 = "saint-jérôme2";
  	utf8+=raw2;
  	numAccentedChars=0;
  	// Now iterate through the utf8 string
   	for (Utf8String::const_iterator it = utf8.begin(); it != utf8.end(); ++it)
   	{
   		if (*it ==  L'é' || *it ==  L'ô')
   			numAccentedChars++;
   	}
   	BOOST_CHECK_EQUAL(numAccentedChars, 4);
   	string theString(utf8.rawData(), utf8.rawLength());
   	size_t found = raw.find(L'é') ;
   	BOOST_CHECK_EQUAL(found, string::npos);
   	// We do a normal regex first
   	boost::regex reg("é");
   	std::string raw4 = "saint-jérôme";
   	BOOST_CHECK_EQUAL( boost::regex_search(raw4, reg), true);
   	// Please see Saint-j\xC3A9r\xC3B4me for UTF-8 character table
   	boost::u32regex withHex = boost::make_u32regex("saint-j\xc3\xa9r\xc3\xb4me");
   	boost::u32regex withoutHex = boost::make_u32regex(L"[a-z]*-jérôme");
    boost::match_results<std::string::const_iterator> matches;
    BOOST_CHECK_EQUAL(boost::u32regex_search(raw, matches, withoutHex), true);
    if (boost::u32regex_search(raw, matches, withoutHex))
    {
    	for (boost::match_results< std::string::const_iterator >::const_iterator i = matches.begin(); i != matches.end(); ++i)
    	{
    	        if (i->matched) std::cout << "matches :       [" << i->str() << "]\n";
    	        else            std::cout << "doesn't match : [" << i->str() << "]\n";
    	}
    }
    else
    {
    	cerr << "did not get a match without hex" << endl;
    }
    BOOST_CHECK_EQUAL(boost::u32regex_search(raw, matches, withHex), true);
}


template<typename Str>
static size_t count_chars(const Str &str, std::initializer_list<wchar_t> chars) {
    size_t count = std::count_if(begin(str), end(str),
            [&](typename std::iterator_traits<typename Str::iterator>::value_type c) {
            return std::find(begin(chars), end(chars), c) != end(chars);
    });

    return count;
}

BOOST_AUTO_TEST_CASE( test_u32_string )
{
    const std::string str1 { "daß auf dïch" };

    auto nonAscii = count_chars(str1, { L'ß', L'ï' });

    BOOST_CHECK_EQUAL(nonAscii, 0);

    const Utf32String u32str1 { str1 };
    nonAscii = count_chars(u32str1, { L'ß', L'ï' });

    BOOST_CHECK_EQUAL(nonAscii, 2);

    Utf32String u32str2 { "daß" };

    Utf32String u32str3 { "für" };

    auto u32str4 = u32str2 + u32str3;
    nonAscii = count_chars(u32str4, { L'ß', L'ü' });
    BOOST_CHECK_EQUAL(nonAscii, 2);

    u32str4 += "Ô Mélodie!";

    nonAscii = count_chars(u32str4, { L'ß', L'ü', L'Ô', L'é' });
    BOOST_CHECK_EQUAL(nonAscii, 4);

    std::string ascii = u32str1.extractAscii();
    BOOST_CHECK_EQUAL(ascii, "da? auf d?ch");

    Utf32String plainAscii { "Plain Ascii" };
    BOOST_CHECK_EQUAL(plainAscii.extractAscii(), "Plain Ascii");

}

BOOST_AUTO_TEST_CASE( test_latin1 )
{
    std::string latin1 { 'N', 'i', (char)0xe7, 'o', 'l', (char)0xe2, (char)0xdf };
    std::string utf8 = "Niçolâß";

    Utf8String l = Utf8String::fromLatin1(latin1);
    Utf8String l2 = Utf8String::fromLatin1(std::move(latin1));
    Utf8String u(utf8);

    BOOST_CHECK_EQUAL(l, u);
    BOOST_CHECK_EQUAL(l2, u);
}

BOOST_AUTO_TEST_CASE( test_rfind )
{
    Utf8String s1("hello");
    BOOST_CHECK_EQUAL(std::distance(s1.begin(), s1.find('l')), 2);
    BOOST_CHECK_EQUAL(std::distance(s1.begin(), s1.find('h')), 0);
    BOOST_CHECK_EQUAL(std::distance(s1.begin(), s1.rfind('l')), 3);
    BOOST_CHECK_EQUAL(std::distance(s1.begin(), s1.rfind('h')), 0);
    BOOST_CHECK_EQUAL(std::distance(s1.begin(), s1.rfind("h")), 0);
    BOOST_CHECK_EQUAL(std::distance(s1.begin(), s1.rfind("l")), 3);
    BOOST_CHECK_EQUAL(std::distance(s1.begin(), s1.rfind("ll")), 2);
    BOOST_CHECK_EQUAL(std::distance(s1.begin(), s1.rfind("x")), 5);
}
