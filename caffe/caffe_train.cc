/* caffe_trainer.cc
   Jeremy Barnes, 8 December 2015
   Copyright (c) 2015 Datacratic Inc.  All rights reserved.
   
   Trainer for deep networks using Caffe.
*/

#include "mldb/core/procedure.h"
#include "mldb/core/dataset.h"
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
/* CAFFE TRAINER                                                             */
/*****************************************************************************/

struct CaffeTrainerConfig {
    Url solverUrl;
};

DECLARE_STRUCTURE_DESCRIPTION(CaffeTrainerConfig);

DEFINE_STRUCTURE_DESCRIPTION(CaffeTrainerConfig);

CaffeTrainerConfigDescription::
CaffeTrainerConfigDescription()
{
    addField("solverUrl", &CaffeTrainerConfig::solverUrl,
             "URL that contains the Protobuf file that tells Caffe "
             "how to train the model.");
}

struct CaffeTrainer: public Procedure {

    CaffeTrainer(MldbServer * owner,
                 PolyConfig config_,
                 const std::function<bool (const Json::Value &)> & onProgress)
        : Procedure(owner)
    {
        config = config_.params.convert<CaffeTrainerConfig>();
    }
    
    CaffeTrainerConfig config;

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const
    {
        std::ostringstream configStr;

        {
            ML::filter_istream stream(config.solverUrl.toString());
            configStr << stream.rdbuf();
        }

        RunOutput result;

        std::string solverFilename;

        using namespace caffe;

        SolverParameter solver_param;

        bool success = google::protobuf::TextFormat::ParseFromString(configStr.str(), &solver_param);
        if (!success)
            throw HttpReturnException(400, "Unable to read Caffe solver file",
                                      "solverUrl", config.solverUrl);

        Caffe::set_mode(Caffe::CPU);

        std::shared_ptr<Solver<float> >
            solver(SolverRegistry<float>::CreateSolver(solver_param));

#if 0
        SignalHandler signal_handler(
                                            GetRequestedAction(FLAGS_sigint_effect),
                                            GetRequestedAction(FLAGS_sighup_effect));

        solver->SetActionFunction(signal_handler.GetActionFunction());
#endif

        cerr << "Starting Optimization";
        solver->Solve();
        cerr << "Optimization Done.";

        return result;
    }

    virtual Any getStatus() const
    {
        return Any();
    }
    
    CaffeTrainerConfig procConfig;
};

static RegisterProcedureType<CaffeTrainer, CaffeTrainerConfig>
regCaffeTrain(builtinPackage(),
              "neural.caffe.train",
              "Train a Caffe deep learning model",
              "procedures/CaffeTrainer.md.html");


} // namespace MLDB
} // namespace Datacratic
