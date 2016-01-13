// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.


#include "sql_expression.h"
#include "mldb/http/http_exception.h"
#include "mldb/builtin/transposed_dataset.h"

using namespace std;

namespace Datacratic {
namespace MLDB {

typedef std::function<BoundTableExpression (const std::vector<BoundTableExpression> &) > BoundDatasetFunction;

// Overridden by libmldb.so when it loads up to break circular link dependency
// and allow expression parsing to be in a separate library
std::shared_ptr<Dataset> (*createTransposedDatasetFn) (MldbServer *, std::shared_ptr<Dataset> dataset);
std::shared_ptr<Dataset> (*createMergedDatasetFn) (MldbServer *, std::vector<std::shared_ptr<Dataset> >);
std::shared_ptr<Dataset> (*createSampledDatasetFn) (MldbServer *,
                                                    std::shared_ptr<Dataset> dataset,
                                                    const ExpressionValue & options);

// defined in table_expression_operations.cc
BoundTableExpression
bindDataset(std::shared_ptr<Dataset> dataset, Utf8String asName);

namespace Builtins {

typedef BoundTableExpression (*BuiltinDatasetFunction) (const SqlBindingScope & context,
                                                        const std::vector<BoundTableExpression> &,
                                                        const ExpressionValue & options,
                                                        const Utf8String& alias);

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
                       const Utf8String& alias)
            -> BoundTableExpression
            {
                try {
                    return std::move(function(context, args, options, alias));
                } JML_CATCH_ALL {
                    rethrowHttpException(-1, "Binding builtin Dataset function "
                                         + str + ": " + ML::getExceptionString(),
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
                               const Utf8String& alias)
{
    if (args.size() != 1)
        throw HttpReturnException(500, "transpose() takes a single argument");
     if(!options.empty())
         throw HttpReturnException(500, "transpose() does not take any options");


    auto ds = createTransposedDatasetFn(context.getMldbServer(), args[0].dataset);

    return bindDataset(ds, alias); 
}

static RegisterBuiltin registerTranspose(transpose, "transpose");


/*****************************************************************************/
/* MERGED DATASET                                                            */
/*****************************************************************************/

BoundTableExpression merge(const SqlBindingScope & context,
                           const std::vector<BoundTableExpression> & args,
                           const ExpressionValue & options,
                           const Utf8String& alias)
{
    if (args.size() < 2)
        throw HttpReturnException(500, "merge() needs at least 2 arguments");
    if(!options.empty())
        throw HttpReturnException(500, "merge() does not take any options");

    std::vector<std::shared_ptr<Dataset> > datasets;
    datasets.reserve(args.size());
    for (auto arg : args)
    {
        if (arg.dataset)
            datasets.push_back(arg.dataset);
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
                            const Utf8String& alias)
{
    if (args.size() != 1)
        throw HttpReturnException(500, "sample() takes 1 dataset as input, "
                "followed by a options");

    if(!options.isRow())
        throw ML::Exception("options should be a row");

    auto ds = createSampledDatasetFn(context.getMldbServer(),
                                     args[0].dataset,
                                     options); //config);

    return bindDataset(ds, alias); 
}

static RegisterBuiltin registerSample(sample, "sample");



}
}
}
