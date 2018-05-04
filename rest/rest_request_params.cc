/** rest_request_params.cc
    Jeremy Barnes, 18 April 2015
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/


#include "mldb/ext/googleurl/src/url_util.h"
#include "mldb/rest/rest_request_params.h"
#include "mldb/types/annotated_exception.h"
#include <cstdlib>

namespace MLDB {

template<typename Int, typename Res>
static Int doParseInt(const Utf8String & str,
                      Res (&fn) (const std::string &, std::size_t *, int))
{
    size_t n;
    Res res = fn(str.rawString(), &n, 10 /* base */);
    if (n != str.rawLength()) {
        throw std::invalid_argument("string does not contain an integer");
    }
    Int res2 = res;
    if ((Res)res2 != res) {
        throw std::out_of_range("integer is out of range");
    }
    return res2;
}

unsigned char restDecode(const Utf8String & str, unsigned char *)
{
    return doParseInt<unsigned char>(str, std::stoi);
}

signed char restDecode(const Utf8String & str, signed char *)
{
    return doParseInt<signed char>(str, std::stoi);
}

char restDecode(const Utf8String & str, char *)
{
    return doParseInt<char>(str, std::stoi);
}

unsigned short restDecode(const Utf8String & str, unsigned short *)
{
    return doParseInt<unsigned short>(str, std::stoul);
}

signed short restDecode(const Utf8String & str, signed short *)
{
    return doParseInt<signed short>(str, std::stoi);
}

unsigned int restDecode(const Utf8String & str, unsigned int *)
{
    return doParseInt<unsigned int>(str, std::stoul);
}

signed int restDecode(const Utf8String & str, signed int *)
{
    return doParseInt<signed int>(str, std::stoi);
}

unsigned long restDecode(const Utf8String & str, unsigned long *)
{
    return doParseInt<unsigned long>(str, std::stoul);
}

signed long restDecode(const Utf8String & str, signed long *)
{
    return doParseInt<signed long>(str, std::stol);
}

unsigned long long restDecode(const Utf8String & str, unsigned long long *)
{
    return doParseInt<unsigned long long>(str, std::stoull);
}

signed long long restDecode(const Utf8String & str, signed long long *)
{
    return doParseInt<signed long long>(str, std::stoll);
}

Utf8String restEncode(const Utf8String & str)
{
    return str;
}

Utf8String restEncode(const std::string & str)
{
    return str;
}

Utf8String restDecode(std::string str, Utf8String *)
{
    return std::move(str);
}

std::string restDecode(std::string str, std::string *)
{
    return str;
}

Utf8String restDecode(Utf8String str, Utf8String *)
{
    return str;
}

std::string restDecode(Utf8String str, std::string *)
{
    return str.rawData();
}

bool restDecode(const std::string & str, bool *)
{
    if (str == "1"
        || (str.length() == 4
            && tolower(str[0]) == 't'
            && tolower(str[1]) == 'r'
            && tolower(str[2]) == 'u'
            && tolower(str[3]) == 'e')) {
        return true;
    }

    if (str == "0"
        || (str.length() == 5
            && tolower(str[0]) == 'f'
            && tolower(str[1]) == 'a'
            && tolower(str[2]) == 'l'
            && tolower(str[3]) == 's'
            && tolower(str[4]) == 'e')) {
        return false;
    }

    throw AnnotatedException(400, "Attempting to interpret REST parameter value '"
                              + Utf8String(str) + "' as a boolean.  Acceptable "
                              "values are 'true', '1' and 'false', '0' in any "
                              "case.");
}

bool restDecode(const Utf8String & str, bool *)
{
    return restDecode(str.rawString(), (bool *)nullptr);
}

Utf8String restEncode(bool b)
{
    return std::to_string(b);
}

Utf8String encodeUriComponent(const Utf8String & in)
{
    return encodeUriComponent(in.rawString());
}

std::string encodeUriComponent(const std::string & in)
{
    using namespace std;
    url_canon::RawCanonOutputT<char> buffer;
    url_util::EncodeURIComponent(in.c_str(), in.length(), &buffer);
    return std::string(buffer.data(), buffer.length());
}

} // namespace MLDB
