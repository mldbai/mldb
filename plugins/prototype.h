/*


*/

#pragma once

#include "mldb/core/dataset.h"
#include "mldb/core/procedure.h"
#include "mldb/core/function.h"
#include "matrix.h"
#include "mldb/types/value_description_fwd.h"
#include "mldb/ml/jml/feature_info.h"

namespace Datacratic {
namespace MLDB {


struct PrototypeConfig : public ProcedureConfig {
    PrototypeConfig()
    {
    }

    /// Query to select the training data
    InputQuery trainingData;

    /// Where to save the classifier to
   // Url modelFileUrl;

    /// Configuration of the algorithm.  If empty, the configurationFile
    /// will be used instead.
    Json::Value configuration;    
};

DECLARE_STRUCTURE_DESCRIPTION(PrototypeConfig);


/*****************************************************************************/
/* CLASSIFIER PROCEDURE                                                       */
/*****************************************************************************/

struct PrototypeProcedure: public Procedure {

    PrototypeProcedure(MldbServer * owner,
                PolyConfig config,
                const std::function<bool (const Json::Value &)> & onProgress);

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const;

    virtual Any getStatus() const;

    PrototypeConfig procedureConfig;
};


}
}