/** fetcher.cc                                                      -*- C++ -*-
    Jeremy Barnes, 6 January 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
    
    Functions to fetch a URLs.
*/

#include "mldb/core/value_function.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/types/value_description.h"
#include "mldb/types/structure_description.h"
#include "mldb/types/any_impl.h"

#include <condition_variable>
#include <mutex>
#include <memory>

using namespace std;

namespace MLDB {


/*****************************************************************************/
/* FETCHER FUNCTION                                                          */
/*****************************************************************************/

/** Function that fetches a row from a url. */

struct FetcherFunctionConfig {
    int maxConcurrentFetch = -1;
};

DECLARE_STRUCTURE_DESCRIPTION(FetcherFunctionConfig);

DEFINE_STRUCTURE_DESCRIPTION(FetcherFunctionConfig);

FetcherFunctionConfigDescription::
FetcherFunctionConfigDescription()
{
    nullAccepted = true;
    addField("maxConcurrentFetch", &FetcherFunctionConfig::maxConcurrentFetch,
             "The maximum number of concurrent fetching operations to allow."
             "-1 leaves the control to MLDB.", -1);

    onPostValidate = [&] (FetcherFunctionConfig * cfg,
                          JsonParsingContext & context)
    {
        if (cfg->maxConcurrentFetch < 1 && cfg->maxConcurrentFetch != -1) {
            throw MLDB::Exception("maxConcurrentFetch accepts values equal or "
                                  "greater to 1 or equal to -1");
        }
    };
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
        : BaseT(owner, config)
    {
        functionConfig = config.params.convert<FetcherFunctionConfig>();
        maxConcurrency = functionConfig.maxConcurrentFetch;
        cv = make_shared<std::condition_variable>();
        mtx = make_shared<std::mutex>();
    }

    struct ConcurrencyHandler {
        ConcurrencyHandler(int & maxConcurrency,
                           condition_variable & cv, mutex & mtx) :
            maxConcurrency(maxConcurrency), doIt(maxConcurrency != -1),
            cv(cv), mtx(mtx)
        {
            if (doIt) {
                unique_lock<mutex> lck(mtx);
                while (maxConcurrency == 0) {
                    cv.wait(lck);
                }
                --maxConcurrency;
            }
        }

        ~ConcurrencyHandler() {
            if (doIt) {
                unique_lock<mutex> lck(mtx);
                ++maxConcurrency;
                cv.notify_one();
            }
        }

        private:
            int & maxConcurrency;
            bool doIt;
            condition_variable & cv;
            mutex & mtx;
    };
    
    virtual FetcherOutput applyT(const ApplierT & applier,
                                 FetcherArgs args) const
    {
        ConcurrencyHandler concurrencyHandler(maxConcurrency,
                                              *cv.get(), *mtx.get());
        FetcherOutput result;
        Utf8String url = args.url;
        try {
                filter_istream stream(url.rawString(),
                                      { { "mapped", "true" },
                                        { "httpArbitraryTooSlowAbort", "1"} });

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
                blob = CellValue::blob(streamo.str());
            }

            result.content = ExpressionValue(std::move(blob), info.lastModified);
            result.error = ExpressionValue::null(Date::notADate());
            return result;
        }
        MLDB_CATCH_ALL {
            result.content = ExpressionValue::null(Date::notADate());
            result.error = ExpressionValue(getExceptionString(), Date::now());
        }
        return result;
    }

    FetcherFunctionConfig functionConfig;
    mutable int maxConcurrency;
    mutable shared_ptr<condition_variable> cv;
    mutable shared_ptr<mutex> mtx;
};

static RegisterFunctionType<FetcherFunction, FetcherFunctionConfig>
regFetcherFunction(builtinPackage(),
                   "fetcher",
                   "Fetches the contents of a URL each time it's invoked",
                   "functions/Fetcher.md.html",
                   nullptr, //static route
                   { MldbEntity::INTERNAL_ENTITY });

} // namespace MLDB


