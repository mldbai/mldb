/** http_exception.h                                               -*- C++ -*-
    Jeremy Barnes, 13 January 2015
    Copyright (c) 2015 mldb.ai inc.  All rights.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
    
    Exception class to use to return HTTP exceptions.
*/

#pragma once

#include "mldb/arch/exception.h"
#include "mldb/types/any.h"
#include "mldb/types/string.h"
#include "mldb/ext/jsoncpp/value.h"

namespace MLDB {

/// Indicates on a rethrow that we should keep the HTTP code that was in
/// the outer exception.
constexpr int KEEP_HTTP_CODE = -1;

struct HttpReturnException: public MLDB::Exception {
    HttpReturnException(int httpCode, const Utf8String & message, Any details = Any())
        : MLDB::Exception(message.rawData()), message(message), httpCode(httpCode), details(details)
    {
    }

#if 0
    HttpReturnException(int httpCode, const std::string & message, Any details = Any())
        : MLDB::Exception(message), message(message), httpCode(httpCode),details(details)
    {
    }
#endif

    template<typename T>
    static void addToDetails(Json::Value & details, const std::string & key, T && val)
    {
        details[key] = jsonEncode(val);
    }

    template<typename T>
    static void addToDetails(Json::Value & details, const char * key, T && val)
    {
        details[key] = jsonEncode(val);
    }

    template<typename T>
    static void addToDetails(Json::Value & details, const Utf8String & key, T && val)
    {
        details[key.rawString()] = jsonEncode(val);
    }

    static void addListToDetails(Json::Value & details)
    {
    }

    template<typename Str, typename Val, typename... OtherKeyValuePairs>
    static void addListToDetails(Json::Value & details, const Str & str, Val && val,
                          OtherKeyValuePairs&&... otherKeyValuePairs)
    {
        addToDetails(details, str, std::forward<Val>(val));
        addListToDetails(details, std::forward<OtherKeyValuePairs>(otherKeyValuePairs)...);
    }

#if 0
    template<typename Str, typename Val, typename... OtherKeyValuePairs>
    HttpReturnException(int httpCode, const std::string & message,
                        const Str & key, Val&& val,
                        OtherKeyValuePairs&&... otherKeyValuePairs)
        : MLDB::Exception(message), message(message), httpCode(httpCode)
    {
        Json::Value ourDetails;
        addListToDetails(ourDetails, key, std::forward<Val>(val),
                         std::forward<OtherKeyValuePairs>(otherKeyValuePairs)...);
        this->details = std::move(ourDetails);
    }
#endif    

    template<typename Str, typename Val, typename... OtherKeyValuePairs>
    HttpReturnException(int httpCode, const Utf8String & message,
                        const Str & key, Val&& val,
                        OtherKeyValuePairs&&... otherKeyValuePairs)
        : MLDB::Exception(message.rawData()), message(message), httpCode(httpCode)
    {
        Json::Value ourDetails;
        addListToDetails(ourDetails, key, std::forward<Val>(val),
                         std::forward<OtherKeyValuePairs>(otherKeyValuePairs)...);
        this->details = std::move(ourDetails);
    }

    ~HttpReturnException() throw ()
    {
    }

    Utf8String message;
    int httpCode;
    Any details;
};


/** Rethrow an exception, adding some extra context to it.  The exception is
    obtained from std::current_exception().
*/
void rethrowHttpException(int httpCode, const Utf8String & message, Any details = Any()) MLDB_NORETURN;
void rethrowHttpException(int httpCode, const std::string & message, Any details = Any()) MLDB_NORETURN;
void rethrowHttpException(int httpCode, const char * message, Any details = Any()) MLDB_NORETURN;

template<typename Key, typename Val, typename... OtherKeyValuePairs>
void rethrowHttpException(int httpCode, const Utf8String & message,
                          Key && key, Val && val, OtherKeyValuePairs&&... details) MLDB_NORETURN;

template<typename Key, typename Val, typename... OtherKeyValuePairs>
void rethrowHttpException(int httpCode, const std::string & message,
                          Key && key, Val && val, OtherKeyValuePairs&&... details) MLDB_NORETURN;

template<typename Key, typename Val, typename... OtherKeyValuePairs>
void rethrowHttpException(int httpCode, const Utf8String & message,
                          Key && key, Val && val, OtherKeyValuePairs&&... otherDetails)
{
    Json::Value details;
    HttpReturnException::addListToDetails(details, std::forward<Key>(key), std::forward<Val>(val),
                                          std::forward<OtherKeyValuePairs>(otherDetails)...);
    rethrowHttpException(httpCode, message, details);
}

template<typename Key, typename Val, typename... OtherKeyValuePairs>
void rethrowHttpException(int httpCode, const std::string & message,
                          Key && key, Val && val, OtherKeyValuePairs&&... otherDetails)
{
    Json::Value details;
    HttpReturnException::addListToDetails(details, std::forward<Key>(key), std::forward<Val>(val),
                                          std::forward<OtherKeyValuePairs>(otherDetails)...);
    rethrowHttpException(httpCode, message, details);
}

template<typename Key, typename Val, typename... OtherKeyValuePairs>
void rethrowHttpException(int httpCode, const char * message,
                          Key && key, Val && val, OtherKeyValuePairs&&... otherDetails)
{
    Json::Value details;
    HttpReturnException::addListToDetails(details, std::forward<Key>(key), std::forward<Val>(val),
                                          std::forward<OtherKeyValuePairs>(otherDetails)...);
    rethrowHttpException(httpCode, message, details);
}


} // namespace MLDB
