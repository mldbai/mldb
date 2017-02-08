// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.


#include "sql_expression.h"
#include "mldb/http/http_exception.h"
#include "mldb/builtin/transposed_dataset.h"
#include "mldb/builtin/sub_dataset.h"
#include "mldb/types/value_description.h"
#include <functional>

using namespace std;
using namespace std::placeholders;


namespace MLDB {

typedef std::function<BoundTableExpression (const std::vector<BoundTableExpression> &) > BoundDatasetFunction;

// Overridden by libmldb.so when it loads up to break circular link dependency
// and allow expression parsing to be in a separate library
std::shared_ptr<Dataset> (*createTransposedDatasetFn) (MldbServer *, std::shared_ptr<Dataset> dataset);
std::shared_ptr<Dataset> (*createTransposedTableFn) (MldbServer *, const TableOperations& table);
std::shared_ptr<Dataset> (*createMergedDatasetFn) (MldbServer *, std::vector<std::shared_ptr<Dataset> >);
std::shared_ptr<Dataset> (*createSampledDatasetFn) (MldbServer *,
                                                    std::shared_ptr<Dataset> dataset,
                                                    const ExpressionValue & options);
std::shared_ptr<Dataset> (*createSubDatasetFromRowsFn) (MldbServer *, const std::vector<NamedRowValue> &);

// defined in table_expression_operations.cc
BoundTableExpression
bindDataset(std::shared_ptr<Dataset> dataset, Utf8String asName);

namespace Builtins {

typedef BoundTableExpression (*BuiltinDatasetFunction) (const SqlBindingScope & context,
                                                        const std::vector<BoundTableExpression> &,
                                                        const ExpressionValue & options,
                                                        const Utf8String& alias,
                                                        const ProgressFunc & onProgress);

struct RegisterBuiltin {
    template<typename... Names>
    RegisterBuiltin(const BuiltinDatasetFunction & function, Names&&... names)
    {
        doRegister(function, std::forward<Names>(names)...);
    }

    void doRegister(const BuiltinDatasetFunction & function)
    {
    }

    template<typename... Names>
    void doRegister(const BuiltinDatasetFunction & function, std::string name,
                    Names&&... names)
    {
        auto fn = [=] (const Utf8String & str,
                       const std::vector<BoundTableExpression> & args,
                       const ExpressionValue & options,
                       const SqlBindingScope & context,
                       const Utf8String& alias,
                       const ProgressFunc & onProgress)
            -> BoundTableExpression
            {
                try {
                    return function(context, args, options, alias, onProgress);
                } MLDB_CATCH_ALL {
                    rethrowHttpException(-1, "Binding builtin Dataset function "
                                         + str + ": " + getExceptionString(),
                                         "functionName", str);
                }
            };
        handles.push_back(registerDatasetFunction(Utf8String(name), fn));
        doRegister(function, std::forward<Names>(names)...);
    }

    std::vector<std::shared_ptr<void> > handles;
};


/*****************************************************************************/
/* TRANSPOSED DATASET                                                        */
/*****************************************************************************/

BoundTableExpression transpose(const SqlBindingScope & context,
                               const std::vector<BoundTableExpression> & args,
                               const ExpressionValue & options,
                               const Utf8String& alias,
                               const ProgressFunc & onProgress)
{
    if (args.size() != 1)
        throw HttpReturnException(500, "transpose() takes a single argument");
     if(!options.empty())
         throw HttpReturnException(500, "transpose() does not take any options");

    std::shared_ptr<Dataset> ds;
    if (args[0].dataset) {
        ds = createTransposedDatasetFn(context.getMldbServer(), args[0].dataset);
    }
    else {
        ExcAssert(args[0].table);
        ds = createTransposedTableFn(context.getMldbServer(), args[0].table);
    }

    return bindDataset(ds, alias); 
}

static RegisterBuiltin registerTranspose(transpose, "transpose");


/*****************************************************************************/
/* MERGED DATASET                                                            */
/*****************************************************************************/

// Overridden by libmldb.so when it loads up to break circular link dependency
// and allow expression parsing to be in a separate library

BoundTableExpression merge(const SqlBindingScope & context,
                           const std::vector<BoundTableExpression> & args,
                           const ExpressionValue & options,
                           const Utf8String& alias,
                           const ProgressFunc & onProgress)
{
    if (args.size() < 1)
        throw HttpReturnException(500, "merge() needs at least 1 argument");
    if(!options.empty())
        throw HttpReturnException(500, "merge() does not take any options");

    std::vector<std::shared_ptr<Dataset> > datasets;
    datasets.reserve(args.size());

    size_t steps = args.size();
    ProgressState joinState(100);
    auto stepProgress = [&](uint step, const ProgressState & state) {
        joinState = (100 / steps * state.count / *state.total) + (100 / steps * step);
        return onProgress(joinState);
    };
 
    for (uint i = 0; i < steps; ++i) {
        auto & arg = args[i];
        auto & combinedProgress = onProgress ? std::bind(stepProgress, i, _1) : onProgress;
    
        if (arg.dataset) {
            datasets.push_back(arg.dataset);
            if (combinedProgress) {
                ProgressState progress(100);
                progress = 100; // there is nothing to perform here
                combinedProgress(progress);
            }
        }
        else if (!!arg.table) {
            SqlBindingScope dummyScope;
            auto generator = arg.table.runQuery(dummyScope,
                                                SelectExpression::STAR,
                                                WhenExpression::TRUE,
                                                *SqlExpression::TRUE,
                                                OrderByExpression(),
                                                0, -1, combinedProgress);
            SqlRowScope fakeRowScope;
            // Generate all outputs of the query
            std::vector<NamedRowValue> rows
                = generator(-1, fakeRowScope);
            auto subDataset = createSubDatasetFromRowsFn(context.getMldbServer(), rows);

            datasets.push_back(subDataset);
        }
    }

    auto ds = createMergedDatasetFn(context.getMldbServer(), datasets);

    return bindDataset(ds, alias);
}

static RegisterBuiltin registerMerge(merge, "merge");



/*****************************************************************************/
/* SAMPLED DATASET                                                           */
/*****************************************************************************/

BoundTableExpression sample(const SqlBindingScope & context,
                            const std::vector<BoundTableExpression> & args,
                            const ExpressionValue & options,
                            const Utf8String& alias,
                            const ProgressFunc & onProgress)
{
    if (args.size() != 1)
        throw HttpReturnException(400, "The 'sample' function takes 1 dataset as input, "
                                  "followed by a row expression of optional parameters. "
                                  "See the documentation of the 'From Expressions' for "
                                  "more details.",
                                  "options", options,
                                  "alias", alias);

    if(!options.empty() && !options.isRow()) {
        throw HttpReturnException(400,
                "The parameters provided to the 'sample' function "
                "should be a row expression, or not be provided to use the "
                "sampled dataset's defaults. Value provided: " + 
                jsonEncodeStr(options) + ". See the documentation for the "
                "dataset of type 'sampled' for the supported paramters, or "
                "of the 'From Expressions' for more details on using "
                "the 'sample' function");
    }

    auto ds = createSampledDatasetFn(context.getMldbServer(),
                                     args[0].dataset,
                                     options);

    return bindDataset(ds, alias); 
}

static RegisterBuiltin registerSample(sample, "sample");



}
}

