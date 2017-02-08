/** rest_request_params.h                                          -*- C++ -*-
    Jeremy Barnes, 24 January 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

*/

#pragma once

#include <boost/lexical_cast.hpp>
#include "json_codec.h"
#include "mldb/types/value_description_fwd.h"

namespace MLDB {

/*****************************************************************************/
/* REST CODEC                                                                */
/*****************************************************************************/

/** Default parameter encoder / decoder that uses a lexical cast to convert
    to the required type.
*/
template<typename T>
decltype(boost::lexical_cast<T>(std::declval<std::string>()))
restDecode(const std::string & str, T * = 0)
{
    return boost::lexical_cast<T>(str);
}

template<typename T>
decltype(boost::lexical_cast<T>(std::declval<std::string>()))
restDecode(const Utf8String & str, T * = 0)
{
    return boost::lexical_cast<T>(str.rawString());
}

template<typename T>
Utf8String restEncode(const T & val,
                      decltype(boost::lexical_cast<std::string>(std::declval<T>())) * = 0)
{
    // TODO: don't go through std::string
    return Utf8String(boost::lexical_cast<std::string>(val));
}

Utf8String restEncode(const Utf8String & str);
Utf8String restEncode(const std::string & str);
Utf8String restDecode(std::string str, Utf8String *);
std::string restDecode(std::string str, std::string *);
Utf8String restDecode(Utf8String str, Utf8String *);
std::string restDecode(Utf8String str, std::string *);
bool restDecode(const std::string & str, bool *);
bool restDecode(const Utf8String & str, bool *);
Utf8String restEncode(bool b);
Utf8String encodeUriComponent(const Utf8String & in);
std::string encodeUriComponent(const std::string & in);

template<typename T, typename Enable = void>
struct RestCodec {
    static T decode(const std::string & str)
    {
        return restDecode(str, (T *)0);
    }

    static T decode(const Utf8String & str)
    {
        return restDecode(str, (T *)0);
    }

    static Utf8String encode(const T & val)
    {
        return restEncode(val);
    }
};

/*****************************************************************************/
/* JSON STR CODEC                                                            */
/*****************************************************************************/

/** Parameter encoder / decoder that takes parameters encoded in JSON as a
    string and converts them into the required type.
*/

template<typename T>
struct JsonStrCodec {
    JsonStrCodec(std::shared_ptr<const ValueDescriptionT<T> > desc
                 = getDefaultDescriptionSharedT<T>())
        : desc(std::move(desc))
    {
    }

    T decode(const std::string & str) const
    {
        T result;

        StreamingJsonParsingContext context(str, str.c_str(), str.length());
        desc->parseJson(&result, context);
        return result;
    }

    T decode(const Utf8String & str) const
    {
        T result;

        StreamingJsonParsingContext context(str.rawData(), str.rawData(), str.rawLength());
        desc->parseJson(&result, context);
        return result;
    }

    Utf8String encode(const T & obj) const
    {
        std::ostringstream stream;
        StreamJsonPrintingContext context(stream);
        desc->printJson(&obj, context);
        return stream.str();
    }

    std::shared_ptr<const ValueDescriptionT<T> > desc;
};


/*****************************************************************************/
/* REST PARAMETER SELECTORS                                                  */
/*****************************************************************************/

/** This indicates that we get a parameter from the query string.  If the
    parameter is not present, we throw an exception.
*/
template<typename T, typename Codec = RestCodec<T> >
struct RestParam {
    RestParam()
    {
    }

    RestParam(Utf8String name, Utf8String description,
              Codec codec = Codec())
        : name(std::move(name)), description(std::move(description)),
          codec(std::move(codec))
    {
        //std::cerr << "created RestParam with " << name << " at "
        //          << this << std::endl;
    }
    
    RestParam(const RestParam & other)
        : name(other.name), description(other.description), codec(other.codec)
    {
        //std::cerr << "copied RestParam with " << name << " to "
        //          << this << std::endl;
    }

    Utf8String name;
    Utf8String description;
    Codec codec;

private:
    void operator = (const RestParam & other);
};


/** This indicates that we get a parameter from the query string.  If the
    parameter is not present, we use a default value.
*/
template<typename T, typename Codec = RestCodec<T> >
struct RestParamDefault {
    RestParamDefault()
    {
    }

    RestParamDefault(const Utf8String & name,
                     const Utf8String & description,
                     T defaultValue,
                     const Utf8String & defaultValueStr,
                     Codec codec = Codec())
        : name(name), description(description), defaultValue(defaultValue),
          defaultValueStr(defaultValueStr),
          codec(std::move(codec))
    {
        //std::cerr << "created RestParam with " << name << " at "
        //          << this << std::endl;
    }

    RestParamDefault(const Utf8String & name,
                     const Utf8String & description,
                     T defaultValue = T(),
                     Codec codec = Codec())
        : name(name), description(description), defaultValue(defaultValue),
          defaultValueStr(codec.encode(defaultValue)),
          codec(std::move(codec))
    {
        //std::cerr << "created RestParam with " << name << " at "
        //          << this << std::endl;
    }
    
    RestParamDefault(const RestParamDefault & other)
        : name(other.name), description(other.description),
          defaultValue(other.defaultValue),
          defaultValueStr(other.defaultValueStr),
          codec(other.codec)
    {
        //std::cerr << "copied RestParam with " << name << " to "
        //          << this << std::endl;
    }

    Utf8String name;
    Utf8String description;
    T defaultValue;
    Utf8String defaultValueStr;
    Codec codec;

private:
    void operator = (const RestParamDefault & other);
};


/** This indicates that we get a parameter from the query string and decode
    it from JSON.  If the parameter is not present, we throw an exception.
*/
template<typename T, typename Codec = JsonStrCodec<T> >
struct RestParamJson : public RestParam<T, Codec> {
    RestParamJson()
    {
    }

    RestParamJson(const Utf8String & name, const Utf8String & description,
                  Codec codec = Codec())
        : RestParam<T, Codec>(name, description, std::move(codec))
    {
    }
    
private:
    void operator = (const RestParamJson & other);
};


/** This indicates that we get a parameter from the query string.  If the
    parameter is not present, we use a default value.  Encoding/decoding
    is done through JSON.
*/
template<typename T, typename Codec = JsonStrCodec<T> >
struct RestParamJsonDefault : public RestParamDefault<T, Codec> {
    RestParamJsonDefault()
    {
    }

    RestParamJsonDefault(const Utf8String & name,
                         const Utf8String & description,
                         T defaultValue = T(),
                         const Utf8String & defaultValueStr = "",
                         Codec codec = Codec())
        : RestParamDefault<T, Codec>(name, description, defaultValue,
                                     defaultValueStr, std::move(codec))
    {
    }
    
private:
    void operator = (const RestParamJsonDefault & other);
};


/** This indicates that we get a parameter from the JSON payload.
    If name is empty, then we get the whole payload.  Otherwise we
    get the named value of the payload.
*/
template<typename T>
struct JsonParam {
    JsonParam()
    {
    }

    JsonParam(const Utf8String & name, const Utf8String & description)
        : name(name), description(description)
    {
    }
    
    JsonParam(const JsonParam & other)
        : name(other.name), description(other.description)
    {
    }
    
    Utf8String name;
    Utf8String description;
};

/** This indicates that we get a parameter from the JSON payload.
    If name is empty, then we get the whole payload.  Otherwise we
    get the named value of the payload.
*/
template<typename T, typename Codec = JsonStrCodec<T> >
struct JsonParamDefault {
    JsonParamDefault()
    {
    }

    JsonParamDefault(const Utf8String & name,
                     const Utf8String & description,
                     T defaultValue = T(),
                     const Utf8String & defaultValueStr = "",
                     Codec codec = Codec())
        : name(name), description(description), defaultValue(defaultValue),
          defaultValueStr(codec.encode(defaultValue)),
          codec(std::move(codec))
    {
    }
    
    JsonParamDefault(const JsonParamDefault & other)
        : name(other.name), description(other.description),
          defaultValue(other.defaultValue),
          defaultValueStr(other.defaultValueStr), codec(other.codec)
    {
    }
    
    Utf8String name;
    Utf8String description;
    T defaultValue;
    Utf8String defaultValueStr;
    Codec codec;
};


/** This indicates that we get a parameter from the path of the request. 
    For example, GET /v1/object/3/value, this would be able to bind "3" to
    a parameter of the request.
*/
template<typename T, class Codec = RestCodec<T> >
struct RequestParam {
    RequestParam()
    {
    }

    RequestParam(int index, const Utf8String & name, const Utf8String & description)
        : index(index), name(name), description(description)
    {
    }

    RequestParam(const RequestParam & other)
        : index(other.index),
          name(other.name),
          description(other.description)
    {
    }

    int index;
    Utf8String name;
    Utf8String description;
};

/** This indicates that we get a parameter from either the query string (json)
    or the JSON payload. If the query string and the payload are used
    simultaneously an error is returned.
*/
template<typename T, typename Codec = JsonStrCodec<T> >
struct HybridParamJsonDefault {
    HybridParamJsonDefault()
    {
    }

    HybridParamJsonDefault(const Utf8String & name,
                           const Utf8String & description,
                           T defaultValue = T(),
                           const Utf8String & defaultValueStr = "",
                           Codec codec = Codec())
        : name(name), description(description), defaultValue(defaultValue),
          defaultValueStr(codec.encode(defaultValue)), codec(std::move(codec))
    {
    }

    HybridParamJsonDefault(const HybridParamJsonDefault & other)
        : name(other.name), description(other.description),
          defaultValue(other.defaultValue),
          defaultValueStr(other.defaultValueStr), codec(other.codec)
    {
    }

    Utf8String name;
    Utf8String description;
    T defaultValue;
    Utf8String defaultValueStr;
    Codec codec;
};

/** This indicates that we get a parameter from either the query string
    (non json) or the JSON payload. If the query string and the payload are
    used simultaneously an error is returned.
*/
template<typename T, typename RestCodec = RestCodec<T>, typename JsonCodec = JsonStrCodec<T>>
struct HybridParamDefault {
    HybridParamDefault()
    {
    }

    HybridParamDefault(const Utf8String & name,
                       const Utf8String & description,
                       T defaultValue = T(),
                       const Utf8String & defaultValueStr = "",
                       RestCodec restCodec = RestCodec(),
                       JsonCodec jsonCodec = JsonCodec())
        : name(name), description(description), defaultValue(defaultValue),
          defaultValueStr(restCodec.encode(defaultValue)),
          restCodec(std::move(restCodec)), jsonCodec(std::move(jsonCodec))
    {
    }

    HybridParamDefault(const HybridParamDefault & other)
        : name(other.name), description(other.description),
          defaultValue(other.defaultValue),
          defaultValueStr(other.defaultValueStr), restCodec(other.restCodec),
          jsonCodec(other.jsonCodec)
    {
    }

    Utf8String name;
    Utf8String description;
    T defaultValue;
    Utf8String defaultValueStr;
    RestCodec restCodec;
    JsonCodec jsonCodec;
};



/** Free function to take the payload and pass it as a string. */
struct StringPayload {
    StringPayload(const Utf8String & description)
        : description(description)
    {
    }

    Utf8String description;
};

struct PassConnectionId {
};

struct PassParsingContext {
};

struct PassRequest {
};

/** This means to extract an object of the given type from the given
    resource of the context, and return a reference to it.
*/
template<typename Value, int Index = -1>
struct ObjectExtractor {
};
        


} // namespace MLDB
