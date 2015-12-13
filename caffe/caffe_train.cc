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
#include <glog/logging.h>


using namespace std;


namespace Datacratic {
namespace MLDB {


/*****************************************************************************/
/* CAFFE TRAINER                                                             */
/*****************************************************************************/

struct CaffeTrainerConfig {
    Url networkUrl;
};

DECLARE_STRUCTURE_DESCRIPTION(CaffeTrainerConfig);

DEFINE_STRUCTURE_DESCRIPTION(CaffeTrainerConfig);

CaffeTrainerConfigDescription::
CaffeTrainerConfigDescription()
{
    addField("networkUrl", &CaffeTrainerConfig::networkUrl,
             "URL that contains the Protobuf file that tells Caffe "
             "how to construct its network.");
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
            ML::filter_istream stream(config.networkUrl.toString());
            configStr << stream.rdbuf();
        }

        RunOutput result;

        std::string networkFilename;

        using namespace caffe;

        NetParameter network_param;

        bool success = google::protobuf::TextFormat::ParseFromString(configStr.str(), &network_param);
        if (!success)
            throw HttpReturnException(400, "Unable to read Caffe network file",
                                      "networkUrl", config.networkUrl);

        for (auto & l: *network_param.mutable_layer()) {
            cerr << "layer of type " << l.type() << endl;
        }

#if 0
        Caffe::set_mode(Caffe::CPU);

        Caffe::Net net;
        


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
#endif

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

namespace {

static void glogFatal() JML_NORETURN;

static void glogFatal()
{
    throw HttpReturnException(400, "Caffe fatal error");
}

static struct AtInit {

    AtInit()
    {
        // Throw an exception, rather than abort, on a fatal error
        google::InstallFailureFunction(glogFatal);
    }

} atInit;

} // file scope

} // namespace MLDB
} // namespace Datacratic
