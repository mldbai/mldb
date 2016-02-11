/** rest_request_params.cc
    Jeremy Barnes, 18 April 2015
    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
*/


#include "mldb/ext/googleurl/src/url_util.h"
#include "mldb/rest/rest_request_params.h"


namespace Datacratic {

Utf8String restEncode(const Utf8String & str)
{
    return str;
}

Utf8String restEncode(const std::string & str)
{
    return str;
    url_canon::RawCanonOutputT<char> buffer;
    url_util::EncodeURIComponent(str.c_str(), str.length(), &buffer);
    return std::string(buffer.data(), buffer.length());
}

Utf8String restDecode(std::string str, Utf8String *)
{
    return std::move(str);
}

std::string restDecode(std::string str, std::string *)
{
    return std::move(str);
}

Utf8String restDecode(Utf8String str, Utf8String *)
{
    return std::move(str);
}

std::string restDecode(Utf8String str, std::string *)
{
    return str.rawData();
}

bool restDecode(const std::string & str, bool *)
{
    if (str == "true")
        return true;
    else if (str == "false")
        return false;
    else return boost::lexical_cast<bool>(str);
}

bool restDecode(const Utf8String & str, bool *)
{
    if (str == "true")
        return true;
    else if (str == "false")
        return false;
    else return boost::lexical_cast<bool>(str.rawData());
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


} // namespace Datacratic
