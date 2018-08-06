/** annotated_exception.cc
    Jeremy Barnes, 2 April 2015

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "annotated_exception.h"
#include "mldb/types/basic_value_descriptions.h"


using namespace std;


namespace MLDB {

void rethrowException(int httpCode, const Utf8String & message, Any details)
{
    MLDB_TRACE_EXCEPTIONS(false);
    try {
        std::rethrow_exception(std::current_exception());
    } catch (const AnnotatedException & http) {
        Json::Value details2 = jsonEncode(details);
        details2["context"]["details"] = jsonEncode(http.details);
        details2["context"]["error"] = http.message;
        throw AnnotatedException(httpCode == KEEP_HTTP_CODE
                                  ? http.httpCode : httpCode, message, details2);
    } catch (const std::bad_alloc & exc) {
        Json::Value details2 = jsonEncode(details);
        details2["context"]["error"]
            = "Out of memory.  A memory allocation failed when performing "
            "the operation.  Consider retrying with a smaller amount of data "
            "or running on a machine with more memory.  "
            "(std::bad_alloc)";
        throw AnnotatedException(httpCode == KEEP_HTTP_CODE
                                  ? 400 : httpCode, message, details2);
    } catch (const std::exception & exc) {
        Json::Value details2 = jsonEncode(details);
        details2["context"]["error"] = exc.what();
        throw AnnotatedException(httpCode == KEEP_HTTP_CODE
                                  ? 400 : httpCode, message, details2);
    }

    throw AnnotatedException(httpCode == KEEP_HTTP_CODE
                              ? 400 : httpCode, message, details);
}

void rethrowException(int httpCode, const std::string & message, Any details)
{
    rethrowException(httpCode, Utf8String(message), details);
}

void rethrowException(int httpCode, const char * message, Any details)
{
    rethrowException(httpCode, Utf8String(message), details);
}

} // namespace MLDB
