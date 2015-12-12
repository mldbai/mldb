/* caffe_predict.cc
   Jeremy Barnes, 8 December 2015
   Copyright (c) 2015 Datacratic Inc.  All rights reserved.
   
   Function that loads a Caffe model and applies it for prediction.
*/

#include "mldb/core/function.h"
#include "mldb/types/value_description.h"
#include "mldb/types/structure_description.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/types/any_impl.h"
#include "mldb/http/http_exception.h"
#include "caffe/caffe.hpp"
#include <google/protobuf/text_format.h>


using namespace std;


namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* CAFFE PREDICTER                                                           */
/*****************************************************************************/

struct CaffePredicterConfig {
    Url modelFileUrl;
};

DECLARE_STRUCTURE_DESCRIPTION(CaffePredicterConfig);

DEFINE_STRUCTURE_DESCRIPTION(CaffePredicterConfig);

CaffePredicterConfigDescription::
CaffePredicterConfigDescription()
{
    //addField("modelFileUrl", &CaffePredicterConfig::modelFileUrl,
    //         "URL that contains the Protobuf file that tells Caffe "
    //         "how to train the model.");
}

struct CaffePredicter: public Function {

    CaffePredicter(MldbServer * owner,
                   PolyConfig config_,
                   const std::function<bool (const Json::Value &)> & onProgress)
        : Function(owner)
    {
#if 0
        config = config_.params.convert<CaffePredicterConfig>();

        std::ostringstream configStr;

        {
            ML::filter_istream stream(config.solverUrl.toString());
            configStr << stream.rdbuf();
        }

        std::string solverFilename;

        using namespace caffe;

        SolverParameter solver_param;

        bool success = google::protobuf::TextFormat::ParseFromString(configStr.str(), &solver_param);
        if (!success)
            throw HttpReturnException(400, "Unable to read Caffe solver file",
                                      "solverUrl", config.solverUrl);

        // ...
#endif
    }
    
    CaffePredicterConfig config;

    Any getStatus() const
    {
        Json::Value result;
        return result;
    }
    
    Any getDetails() const
    {
        Json::Value result;
        return result;
    }
    
    struct Applier: public FunctionApplier {
        Applier(const Function * owner)
            : FunctionApplier(owner)
        {
            info = owner->getFunctionInfo();
        }
    };

    virtual std::unique_ptr<FunctionApplier>
    bind(SqlBindingScope & outerContext,
         const FunctionValues & input) const
    {
        std::unique_ptr<Applier> result
            (new Applier(this));
        return std::move(result);
    }

    virtual FunctionInfo
    getFunctionInfo() const
    {
        FunctionInfo result;
        return result;
    }

    virtual FunctionOutput
    apply(const FunctionApplier & applier_,
          const FunctionContext & context) const
    {
        FunctionOutput result;
        return result;
    }

    CaffePredicterConfig procConfig;
};

static RegisterFunctionType<CaffePredicter, CaffePredicterConfig>
regCaffePredict(builtinPackage(),
                "neural.caffe.predict",
                "Apply a trained Caffe deep learning model",
                "procedures/CaffePredicter.md.html");


} // namespace MLDB
} // namespace Datacratic
