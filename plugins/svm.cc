// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** svm.cc
    Mathieu Marquis Bolduc, October 28th, 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

   Support Vector Machine Procedure and Function
*/

#include "svm.h"

#include <boost/filesystem.hpp>

#include "mldb/server/mldb_server.h"
#include "mldb/core/dataset.h"
#include "mldb/server/analytics.h"

#include "types/structure_description.h"
#include "mldb/types/any_impl.h"
#include "mldb/jml/db/persistent.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/vfs/filter_streams.h"

#include "mldb/ext/svm/svm.h"

using namespace std;

namespace fs = boost::filesystem;

namespace Datacratic {
namespace MLDB {

DEFINE_ENUM_DESCRIPTION(SVMType);

SVMTypeDescription::
SVMTypeDescription()
{
    addValue("classification", SVM_CLASSIFICATION, "Use regular SVM classification");
    addValue("nu-classification", SVM_CLASSIFICATION_NU, "Use SVM classification with nu metric");
    addValue("one class", SVM_ONE_CLASS, "Use one-class SVM classification");
    addValue("regression", SVM_REGRESSION_EPSILON, "Use SVM for regression with epsilon metric");
    addValue("nu-regression", SVM_REGRESSION_NU, "Use SVM for regression with nu metric");
}

enum SVMKernelType {
    SVM_KERNEL_LINEAR,
    SVM_KERNEL_POLY,
    SVM_KERNEL_RBF,
    SVM_KERNEL_SIGMOID,
//    SVM_KERNEL_PRECOMPUTED
};

DECLARE_ENUM_DESCRIPTION(SVMKernelType);
DEFINE_ENUM_DESCRIPTION(SVMKernelType);

SVMKernelTypeDescription::
SVMKernelTypeDescription()
{
    addValue("linear", SVM_KERNEL_LINEAR, "Linear kernel");
    addValue("poly", SVM_KERNEL_POLY, "Polynomial kernel");
    addValue("rbf", SVM_KERNEL_RBF, "RBF kernel");
    addValue("sigmoid", SVM_KERNEL_SIGMOID, "Sigmoid kernel");
   // addValue("precomputed", SVM_KERNEL_PRECOMPUTED, "Precomputed kernel");
}

struct SVMParameterWrapper
{
    SVMParameterWrapper()
    {
        param.svm_type = C_SVC;
        kernel = SVM_KERNEL_RBF;
        degree = 3;
        gamma = 0;    // 1/num_features
        coef0 = 0;
        nu = 0.5;
        param.cache_size = 100;
        C = 1;
        eps = 1e-3;
        p = 0.1;
        shrinking = 1;
        probability = 0;
        param.nr_weight = 0;
        param.weight_label = NULL;
        param.weight = NULL;
    }

    void apply()
    {
        param.kernel_type = kernel;
        param.degree = degree;
        param.gamma = gamma;    // 1/num_features
        param.coef0 = coef0;
        param.nu = nu;
    //    param.cache_size = cache_size;
        param.C = C;
        param.eps = eps;
        param.p = p;
        param.shrinking = shrinking;
        param.probability = probability;
     //   param.nr_weight = nr_weight;
     //   param.weight_label = NULL;
     //   param.weight = NULL;
    }

 //   int svm_type;
    SVMKernelType kernel;
    int degree; /* for poly */
    double gamma;   /* for poly/rbf/sigmoid */
    double coef0;   /* for poly/sigmoid */

    /* these are for training only */
  //  double cache_size; /* in MB */
    double eps; /* stopping criteria */
    double C;   /* for C_SVC, EPSILON_SVR and NU_SVR */
//    int nr_weight;      /* for C_SVC */
//    int *weight_label;  /* for C_SVC */
//    double* weight;     /* for C_SVC */
    double nu;  /* for NU_SVC, ONE_CLASS, and NU_SVR */
    double p;   /* for EPSILON_SVR */
    int shrinking;  /* use the shrinking heuristics */
    int probability; /* do probability estimates */

    svm_parameter param;
};

DECLARE_STRUCTURE_DESCRIPTION(SVMParameterWrapper);

DEFINE_STRUCTURE_DESCRIPTION(SVMParameterWrapper);

SVMParameterWrapperDescription::
SVMParameterWrapperDescription()
{
    addField("kernel", &SVMParameterWrapper::kernel,
             "Kernel Type for support vector machine training", SVM_KERNEL_RBF);
    addField("degree", &SVMParameterWrapper::degree,
             "degree of polynome for polynomial kernel", 3);
    addField("coef0", &SVMParameterWrapper::coef0,
             "coefficient for polynomial for sigmoid kernel");
    addField("eps", &SVMParameterWrapper::eps,
             "stopping criteria for SVM training", 1e-3);
    addField("C", &SVMParameterWrapper::C,
             "C parameter for NU and one class");
    addField("gamma", &SVMParameterWrapper::gamma,
             "gamma parameter for NU and one class");
    addField("nu", &SVMParameterWrapper::nu,
             "nu parameter for NU and one class");
    addField("p", &SVMParameterWrapper::p,
             "p parameter for support vector machine regression");
    addField("shrinking", &SVMParameterWrapper::shrinking,
             "Use the shrinking heuristics", 1);
    addField("probability", &SVMParameterWrapper::probability,
             "Do probability estimated", 0);
}

DEFINE_STRUCTURE_DESCRIPTION(SVMConfig);

SVMConfigDescription::
SVMConfigDescription()
{
    addField("trainingData", &SVMConfig::trainingData,
             "Specification of the data for input to the SVM Procedure.  This should be "
             "organized as an embedding, with each selected row containing the same "
             "set of columns with numeric values to be used as coordinates.  The select statement "
             "does not support groupby and having clauses.");
    addField("modelFileUrl", &SVMConfig::modelFileUrl,
             "URL where the model file (with extension '.svm') should be saved. "
             "This file can be loaded by a function of type 'svm'.");
    addField("configuration", &SVMConfig::configuration,
             "Configuration object to use for the SVM Procedure.  Each one has "
             "its own parameters.  If none is passed, then the configuration "
             "will be loaded from the ConfigurationFile parameter",
             Json::Value());
    addField("functionName", &SVMConfig::functionName,
             "If specified, a SVM function of this name will be created using "
             "the trained SVM");
    //SVM-specific parameters
    addField("svmType", &SVMConfig::svmType,
             "If specified, a SVM function of this name will be created using "
             "the trained SVM.", SVM_CLASSIFICATION);
    addParent<ProcedureConfig>();

    onPostValidate = validate<SVMConfig, InputQuery, NoGroupByHaving>(&SVMConfig::trainingData, "svm");
}

/*****************************************************************************/
/* Wrappers                                                                  */
/*****************************************************************************/

struct MLDBSVM_Problem : svm_problem
{
    MLDBSVM_Problem(int numRows, int maxNodeNumber)
    {
        l = 0;
        y = new double[numRows];
        x = new svm_node *[numRows];
        x_space = new svm_node[maxNodeNumber];
    }

    ~MLDBSVM_Problem()
    {
        delete [] y;
        delete [] x; 
        delete [] x_space;
    }

    svm_node* x_space;
};

/*****************************************************************************/
/* SVM PROCEDURE                                                             */
/*****************************************************************************/
SVMProcedure::
SVMProcedure(MldbServer * owner,
            PolyConfig config,
            const std::function<bool (const Json::Value &)> & onProgress)
    : Procedure(owner)
{
    procedureConfig = config.params.convert<SVMConfig>();   
}

Any
SVMProcedure::
getStatus() const
{
    return Any();
}

RunOutput
SVMProcedure::
run(const ProcedureRunConfig & run,
      const std::function<bool (const Json::Value &)> & onProgress) const
{
    auto runProcConf = applyRunConfOverProcConf(procedureConfig, run);

    auto onProgress2 = [&] (const Json::Value & progress)
    {
            Json::Value value;
            value["dataset"] = progress;
            return onProgress(value);
    };

    SqlExpressionMldbContext context(server);

    auto embeddingOutput = getEmbedding(*runProcConf.trainingData.stm, context, -1, onProgress2);

    std::vector<std::tuple<RowHash, RowName, std::vector<double>,
                           std::vector<ExpressionValue> > > & rows
        = embeddingOutput.first;

    std::vector<KnownColumn> & vars = embeddingOutput.second;

    size_t num_features = vars.size();
    size_t sizeY = rows.size();
    size_t labelIndex = 0;

    for (size_t i = 0; i < num_features; ++i) {
        if (vars[i].columnName.toUtf8String() == "label")
        {
            labelIndex = i;
            break;
        }
    }

    size_t maxNodeNumber = (num_features+1)*sizeY;

    MLDBSVM_Problem prob(sizeY, maxNodeNumber);
    size_t element = 0;

    for (auto& r : rows)
    {
        prob.x[prob.l] = &prob.x_space[element];
        const std::vector<double>& row = std::get<2>(r);

        for (size_t i = 0; i < row.size(); ++i)
        {            
            if (i == labelIndex)
            {
                prob.y[prob.l] = row[i];
            }
            else
            {
                double val = row[i];
                if (val != 0)
                {
                    prob.x_space[element].index = i < labelIndex ? i : i - 1;
                    prob.x_space[element].value = val;
                    element++;
                }                
            }
        }

         prob.x_space[element++].index = -1;

        prob.l++;        
    }

   

    SVMParameterWrapper paramWrapper;

    if (!runProcConf.configuration.isNull()) {
        cerr << "Has Configuration" << endl;
        paramWrapper = jsonDecode<SVMParameterWrapper>(runProcConf.configuration);
    }

    paramWrapper.apply();
    paramWrapper.param.svm_type = (int)runProcConf.svmType;

    if(paramWrapper.param.gamma == 0 && num_features > 1)
        paramWrapper.param.gamma = 1.0/(num_features-1);

    rows.resize(0);
    vars.resize(0);

    svm_model * model = svm_train(&prob,&paramWrapper.param);
    if(!model) {
        throw HttpReturnException(500, "Could not train support vector machine");
    }

    auto plugin_working_dir = fs::temp_directory_path() / fs::unique_path();
    auto model_tmp_name = plugin_working_dir.string() + std::string("svmmodeltemp_a.svm");
    try {
        if (svm_save_model(model_tmp_name.c_str(),model))          
            throw ML::Exception("");

        Datacratic::makeUriDirectory(runProcConf.modelFileUrl.toString());
        ML::filter_istream in(model_tmp_name);
        ML::filter_ostream out(runProcConf.modelFileUrl.toString());
        out << in.rdbuf();
    }
    catch (const std::exception & exc) {
        svm_free_and_destroy_model(&model);

        throw HttpReturnException(500, "Could not save support vector machine model file", runProcConf.modelFileUrl.toString());
    }

    svm_free_and_destroy_model(&model);   

    return RunOutput();
}

/*****************************************************************************/
/*SVM FUNCTION                                                               */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(SVMFunctionConfig);

SVMFunctionConfigDescription::
SVMFunctionConfigDescription()
{
    addField("modelFileUrl", &SVMFunctionConfig::modelFileUrl,
             "URL of the model file (with extension '.svm') to load. "
             "This file is created by a procedure of type 'svm.train'.");
}

struct SVMFunction::Itl {
    svm_model * model;
};

SVMFunction::
SVMFunction(MldbServer * owner,
            PolyConfig config,
            const std::function<bool (const Json::Value &)> & onProgress)
    : Function(owner)
{
    auto functionConfig = config.params.convert<SVMFunctionConfig>();

    itl.reset(new Itl());

    itl->model = nullptr;

    auto plugin_working_dir = fs::temp_directory_path() / fs::unique_path();
    auto model_tmp_name = plugin_working_dir.string() + std::string("svmmodeltemp_b.svm");
    try {
        ML::filter_istream in(functionConfig.modelFileUrl.toString());
        ML::filter_ostream out(model_tmp_name);
        out << in.rdbuf();
        in.close();
        out.close();
        itl->model = svm_load_model(model_tmp_name.c_str());

        if (!itl->model)
          throw;
    }
    catch (const std::exception & exc) {
        throw HttpReturnException(500, "Could not load support vector machine model file", functionConfig.modelFileUrl.toString());
    }
}

SVMFunction::
~SVMFunction()
{
    svm_free_and_destroy_model(&itl->model);
}

Any
SVMFunction::
getStatus() const
{
    return Any();
}

struct SVMFunctionApplier: public FunctionApplier {
    SVMFunctionApplier(const Function * owner)
        : FunctionApplier(owner)
    {
       info = owner->getFunctionInfo();
    }

  
};

std::unique_ptr<FunctionApplier>
SVMFunction::
bind(SqlBindingScope & outerContext,
     const FunctionValues & input) const
{

    std::unique_ptr<SVMFunctionApplier> result
        (new SVMFunctionApplier(this));
 
    return std::move(result);
}

FunctionOutput
SVMFunction::
apply(const FunctionApplier & applier_,
      const FunctionContext & context) const
{
    FunctionOutput result;

    ExpressionValue storage;
    const ExpressionValue & inputVal = context.get("embedding", storage);
    ML::distribution<float> input = inputVal.getEmbedding();
    Date ts = inputVal.getEffectiveTimestamp();

    svm_node * x = new svm_node[input.size()+1];

    int nbSparse = 0;
    for (size_t i = 0; i < input.size(); ++i) {
        if (input[i] != 0) {
             x[nbSparse].index = i;
             x[nbSparse].value = input[i];
             nbSparse++;
        }     
    }

    x[nbSparse].index = -1;

    double predict_label = svm_predict(itl->model,x);

    result.set("output", ExpressionValue(predict_label, ts));

    delete x;

    return std::move(result);
}

FunctionInfo
SVMFunction::
getFunctionInfo() const
{    
    FunctionInfo result;

    result.input.addEmbeddingValue("embedding", 2);
    result.output.addAtomValue("output");

    return std::move(result);
}

namespace {

RegisterProcedureType<SVMProcedure, SVMConfig>
regClassifier(builtinPackage(),
              "svm.train",
              "Train a supervised Support Vector Machine",
              "procedures/Svm.md.html");

RegisterFunctionType<SVMFunction, SVMFunctionConfig>
regClassifyFunction(builtinPackage(),
                    "svm",
                    "Apply a trained Support Vector Machine to new data",
                    "functions/SVMApply.md.html");

} // filescope
} // MLDB
} // Datacratic
