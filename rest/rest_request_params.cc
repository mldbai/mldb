// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** rest_request_params.cc
    Jeremy Barnes, 18 April 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.
*/


#include <curl/curl.h>
#include "mldb/rest/rest_request_params.h"


namespace Datacratic {


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
    return std::move(str);
}

Utf8String restDecode(Utf8String str, Utf8String *)
{
    CURL *curl = curl_easy_init();
    int outlength;
    std::unique_ptr<char, void (*)(char *)> cres(
        curl_easy_unescape(curl, str.rawData(), str.length(), &outlength),
        [](char *p) { curl_free(p); });
    Utf8String utf8(cres.get(), cres.get() + outlength);
    curl_easy_cleanup(curl);
    return std::move(utf8);
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



} // namespace Datacratic
