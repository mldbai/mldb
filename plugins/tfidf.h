/** tfidf.h                                                          -*- C++ -*-
    Mathieu Marquis Bolduc, November 27th 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    TF-IDF algorithm for a dataset.
*/

#pragma once

#include "mldb/core/dataset.h"
#include "mldb/core/procedure.h"
#include "mldb/core/function.h"
#include "matrix.h"
#include "mldb/ml/value_descriptions.h"
#include "mldb/types/optional.h"
#include "metric_space.h"


namespace MLDB {

enum TFType {
    TF_raw,
    TF_log,
    TF_augmented
};

enum IDFType {
    IDF_unary,
    IDF_inverse,
    IDF_inverseSmooth,
    IDF_inverseMax,
    IDF_probabilistic_inverse
};

DECLARE_ENUM_DESCRIPTION(TFType);
DECLARE_ENUM_DESCRIPTION(IDFType);

struct TfidfConfig : public ProcedureConfig {
    static constexpr const char * name = "tfidf.train";

    InputQuery trainingData;
    Url modelFileUrl;
    Optional<PolyConfigT<Dataset> > output;
    static constexpr char const * defaultOutputDatasetType = "sparse.mutable";

    Utf8String functionName;
};

DECLARE_STRUCTURE_DESCRIPTION(TfidfConfig);



/*****************************************************************************/
/* TFIDF PROCEDURE                                                           */
/*****************************************************************************/

struct TfidfProcedure: public Procedure {

    TfidfProcedure(MldbServer * owner,
                   PolyConfig config,
                   const std::function<bool (const Json::Value &)> & onProgress);

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const;

    virtual Any getStatus() const;

    TfidfConfig tfidfconfig;
};


/*****************************************************************************/
/* TFIDF FUNCTION                                                             */
/*****************************************************************************/

struct TfidfFunctionConfig {
    TfidfFunctionConfig()
        : tf_type(TF_raw),
          idf_type(IDF_inverseSmooth)
    {
    }

    Url modelFileUrl;
    TFType tf_type;
    IDFType idf_type;
};

DECLARE_STRUCTURE_DESCRIPTION(TfidfFunctionConfig);

struct TfidfFunction: public Function {
    TfidfFunction(MldbServer * owner,
                PolyConfig config,
                const std::function<bool (const Json::Value &)> & onProgress);

    virtual Any getStatus() const;
    
    virtual ExpressionValue apply(const FunctionApplier & applier,
                              const ExpressionValue & context) const;
    
    /** Describe what the input and output is for this function. */
    virtual FunctionInfo getFunctionInfo() const;

    TfidfFunctionConfig functionConfig;
    // document frequencies for terms
    std::unordered_map<Utf8String, uint64_t> dfs;
    uint64_t corpusSize;
};

} // namespace MLDB

