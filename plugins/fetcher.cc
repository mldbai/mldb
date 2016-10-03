/** fetcher.cc                                                      -*- C++ -*-
    Jeremy Barnes, 6 January 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
    
    Functions to fetch a URLs.
*/

#include "mldb/core/value_function.h"
#include "mldb/sql/fetch_function.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/types/value_description.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/any_impl.h"


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

struct FetcherArgs {
    Utf8String url;
};

DECLARE_STRUCTURE_DESCRIPTION(FetcherArgs);

DEFINE_STRUCTURE_DESCRIPTION(FetcherArgs)
FetcherArgsDescription::
FetcherArgsDescription()
{
    addField("url", &FetcherArgs::url,
             "URL to fetch the blob from.  All URI schemes are accepted. "
             "Note that URIs requiring credentials will need those "
             "credentials to be pre-loaded into MLDB before the call is "
             "made.");
}

struct FetcherOutput {
    ExpressionValue content;
    ExpressionValue error;
};

DECLARE_STRUCTURE_DESCRIPTION(FetcherOutput);

DEFINE_STRUCTURE_DESCRIPTION(FetcherOutput);

FetcherOutputDescription::FetcherOutputDescription()
{
    addFieldDesc("content", &FetcherOutput::content,
                 "Binary blob containing the contents of the fetched version of "
                 "the URL.  Will be null if there was an error.",
                 makeExpressionValueDescription(std::make_shared<BlobValueInfo>()));
    addField("error", &FetcherOutput::error,
             "Row containing the structured error message obtained when "
             "attempting to read the URL.  Will be null if the fetch was "
             "successful.");
}

struct FetcherFunction: public ValueFunctionT<FetcherArgs, FetcherOutput> {
    FetcherFunction(MldbServer * owner,
                    PolyConfig config,
                    const std::function<bool (const Json::Value &)> & onProgress)
        : BaseT(owner)
    {
        functionConfig = config.params.convert<FetcherFunctionConfig>();
    }
    
    virtual FetcherOutput applyT(const ApplierT & applier,
                                 FetcherArgs args) const
    {
        FetcherOutput result;
        Utf8String url = args.url;
        auto fetchStructVal = FetchFunction::fetch(url.rawString());
        for (const auto & col: fetchStructVal) {
            auto colName = std::get<0>(col).toUtf8String();
            if (colName == "content") {
                result.content = std::move(std::get<1>(col));
            }
            else {
                ExcAssert(colName == "error");
                result.error = std::move(std::get<1>(col));
            }
        }
        return result;
    }

    FetcherFunctionConfig functionConfig;
};

static RegisterFunctionType<FetcherFunction, FetcherFunctionConfig>
regFetcherFunction(builtinPackage(),
                   "fetcher",
                   "Fetches the contents of a URL each time it's invoked",
                   "functions/Fetcher.md.html",
                   nullptr, //static route
                   { MldbEntity::INTERNAL_ENTITY });

} // namespace MLDB


