/* url_test.cc
 * Jeremy Barnes, 22 April 2018
 * This file is part of MLDB.  See the accompanying file LICENSE for licensing details.
 */

#include "catch2/catch_all.hpp"
#include "mldb/types/url.h"

using namespace std;
using namespace MLDB;

// Test against boost lexical cast if available
//#if __has_include(<boost/lexical_cast.hpp>)
//#include <boost/lexical_cast.hpp>
//#define MLDB_BOOST_LEXICAL_CAST_INCLUDED 1
//#endif


TEST_CASE("url parsing")
{
    SECTION("base")
    {
        Url url("http://www.google.com:80/search?q=hello#world");
        CHECK(url.scheme() == "http");
        CHECK(url.host() == "www.google.com");
        CHECK(url.port() == 80);
        CHECK(url.path() == "/search");
        CHECK(url.query() == "q=hello");
        CHECK(url.fragment() == "world");
    }

#if 0
    SECTION("utf-8 components")
    {
        // Create a URL with utf-8 components
        Url url(Utf8String("https://www.etemadonline.com/بخش-سیاسی-9/669562-گزارش-فارس-جزئیات-ترور-اسماعیل-هنیه"));
        CHECK(url.scheme() == "https");
        CHECK(url.host() == "www.etemadonline.com");
        CHECK(url.path() == Utf8String("/بخش-سیاسی-9/669562-گزارش-فارس-جزئیات-ترور-اسماعیل-هنیه"));
    }

    SECTION("utf-8 tricky domain name")
    {
        Url url("http://müsic.example/motörhead");
        CHECK(url.scheme() == "http");
        CHECK(url.host() == Utf8String("müsic.example"));
        CHECK(url.path() == "/motörhead");
    }
#endif

    SECTION("file")
    {
        Url url("file:///etc/passwd");
        CHECK(url.scheme() == "file");
        CHECK(url.path() == "/etc/passwd");
    }

    SECTION("empty")
    {
        Url url;
        CHECK(url.empty());
        CHECK(!url.valid());
    }

    SECTION("invalid")
    {
        SECTION("no path")
        {
            Url url("http://www.google.com:80");
            CHECK(url.valid());
            CHECK(url.path() == "/");
        }

        SECTION("no scheme")
        {
            CHECK_THROWS(Url("www.google.com:80"));
        }

        SECTION("no host")
        {
            Url url("http://:80");
            CHECK(!url.valid());
        }

        SECTION("no port")
        {
            Url url("http://www.google.com:");
            CHECK(url.valid());
            CHECK(url.port() == 80);
        }

        SECTION("no port https")
        {
            Url url("https://www.google.com:");
            CHECK(url.valid());
            CHECK(url.port() == 443);
        }

        SECTION("no port or host")
        {
            Url url("http://");
            CHECK(!url.valid());
        }
    }
}
