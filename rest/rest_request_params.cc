/** rest_request_params.cc
    Jeremy Barnes, 18 April 2015
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/


#include "mldb/ext/googleurl/src/url_util.h"
#include "mldb/rest/rest_request_params.h"
#include "mldb/http/http_exception.h"

namespace MLDB {

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

    throw HttpReturnException(400, "Attempting to interpret REST parameter value '"
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
