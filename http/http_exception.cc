// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** http_exception.cc
    Jeremy Barnes, 2 April 2015

*/

#include "http_exception.h"
#include "mldb/types/basic_value_descriptions.h"


using namespace std;


namespace Datacratic {

void rethrowHttpException(int httpCode, const Utf8String & message, Any details)
{
    JML_TRACE_EXCEPTIONS(false);
    try {
        std::rethrow_exception(std::current_exception());
    } catch (const HttpReturnException & http) {
        Json::Value details2 = jsonEncode(details);
        details2["context"]["details"] = jsonEncode(http.details);
        details2["context"]["error"] = http.message;
        throw HttpReturnException(httpCode == -1 ? http.httpCode : httpCode, message, details2);
    } catch (const std::bad_alloc & exc) {
        Json::Value details2 = jsonEncode(details);
        details2["context"]["error"]
            = "Out of memory.  A memory allocation failed when performing "
            "the operation.  Consider retrying with a smaller amount of data "
            "or running on a machine with more memory.  "
            "(std::bad_alloc)";
        throw HttpReturnException(httpCode == -1 ? 400 : httpCode, message, details2);
    } catch (const std::exception & exc) {
        Json::Value details2 = jsonEncode(details);
        details2["context"]["error"] = exc.what();
        throw HttpReturnException(httpCode == -1 ? 400 : httpCode, message, details2);
    }

    throw HttpReturnException(httpCode == -1 ? 400 : httpCode, message, details);
}

void rethrowHttpException(int httpCode, const std::string & message, Any details)
{
    rethrowHttpException(httpCode, Utf8String(message), details);
}

void rethrowHttpException(int httpCode, const char * message, Any details)
{
    rethrowHttpException(httpCode, Utf8String(message), details);
}

} // namespace Datacratic
