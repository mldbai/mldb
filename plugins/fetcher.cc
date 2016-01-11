/** fetcher.cc                                                      -*- C++ -*-
    Jeremy Barnes, 6 January 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
    
    Functions to fetch a URLs.
*/

#include "mldb/core/function.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/types/value_description.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/any_impl.h"

namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* FETCHER FUNCTION                                                          */
/*****************************************************************************/

/** Function that fetches a row from a url. */

struct FetcherFunctionConfig {
    FetcherFunctionConfig()
    {
    }
};

DECLARE_STRUCTURE_DESCRIPTION(FetcherFunctionConfig);

DEFINE_STRUCTURE_DESCRIPTION(FetcherFunctionConfig);

FetcherFunctionConfigDescription::
FetcherFunctionConfigDescription()
{
    nullAccepted = true;
}

struct FetcherFunction: public Function {
    FetcherFunction(MldbServer * owner,
                    PolyConfig config,
                    const std::function<bool (const Json::Value &)> & onProgress)
    : Function(owner)
    {
        functionConfig = config.params.convert<FetcherFunctionConfig>();
    }
    
    virtual Any getStatus() const
    {
        Json::Value result;
        return result;
    }
    
    virtual FunctionOutput apply(const FunctionApplier & applier,
                                 const FunctionContext & context) const
    {
        // Get the URL as a string
        FunctionOutput result;
        Utf8String url = context.get<CellValue>("url").toUtf8String();
        try {
            ML::filter_istream stream(url.rawString(), { { "mapped", "true" } });

            FsObjectInfo info = stream.info();
            
            const char * mappedAddr;
            size_t mappedSize;
            std::tie(mappedAddr, mappedSize) = stream.mapped();

            CellValue blob;
            if (mappedAddr) {
                blob = CellValue::blob(mappedAddr, mappedSize);
            }
            else {
                std::ostringstream streamo;
                streamo << stream.rdbuf();
                blob = CellValue::blob(std::move(streamo.str()));
            }

            result.set("content", ExpressionValue(std::move(blob), info.lastModified));
            result.set("error", ExpressionValue::null(Date::notADate()));
            return result;
        }
        JML_CATCH_ALL {
            result.set("content", ExpressionValue::null(Date::notADate()));
            result.set("error", ExpressionValue(ML::getExceptionString(), Date::now()));
        }
        return result;
    }

    virtual FunctionInfo getFunctionInfo() const
    {
        FunctionInfo result;
        result.input.addStringValue("url");
        result.output.addBlobValue("content");
        //result.output.addRowValue("metadata");
        //result.output.addTimestampValue("lastModified");
        //result.output.addStringValue("etag");
        result.output.addRowValue("error");
        return result;
    }

    FetcherFunctionConfig functionConfig;
};

static RegisterFunctionType<FetcherFunction, FetcherFunctionConfig>
regFetcherFunction(builtinPackage(),
                   "fetcher",
                   "Fetches the contents of a URL each time it's invoked",
                   "functions/Fetcher.md.html");

} // namespace MLDB
} // namespace Datacratic

