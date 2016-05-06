/** svd.h                                                          -*- C++ -*-
    Jeremy Barnes, 16 December 2014
    Copyright (c) 2014 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

    SVD algorithm for a dataset.
*/

#pragma once

#include "mldb/core/dataset.h"
#include "mldb/core/procedure.h"
#include "mldb/core/value_function.h"
#include "matrix.h"
#include "mldb/types/value_description.h"
#include "mldb/types/optional.h"

namespace Datacratic {
namespace MLDB {


struct SelectExpression;
struct SqlExpression;

struct SvdConfig : ProcedureConfig {
    static constexpr char const * name = "svd.train";

    SvdConfig()
        : outputColumn("svd"),
          numSingularValues(100),
          numDenseBasisVectors(1000)
    {
    }

    InputQuery trainingData;
    Optional<PolyConfigT<Dataset> > columnOutput;      ///< Embedding per column of input dataset
    Optional<PolyConfigT<Dataset> > rowOutput;   ///< Embedding per row of input dataset
    static constexpr char const * defaultOutputDatasetType = "embedding";

    Url modelFileUrl;
    std::string outputColumn;
    int numSingularValues;
    int numDenseBasisVectors;
    Utf8String functionName;
};

DECLARE_STRUCTURE_DESCRIPTION(SvdConfig);

struct SvdColumnEntry: public ColumnSpec {

    SvdColumnEntry & operator = (const ColumnSpec & column)
    {
        ColumnSpec::operator = (column);
        return *this;
    }

    ML::distribution<float> singularVector;
};

DECLARE_STRUCTURE_DESCRIPTION(SvdColumnEntry);

struct SvdColumnIndexEntry {
    ColumnName columnName;
    std::map<CellValue, int> values;
};

DECLARE_STRUCTURE_DESCRIPTION(SvdColumnIndexEntry);

struct SvdBasis {
    std::vector<SvdColumnEntry> columns;
    ML::distribution<float> singularValues;
    std::map<ColumnHash, SvdColumnIndexEntry> columnIndex;
    Date modelTs;   ///< Timestamp up to which model incorporates data from

    size_t numSingularValues() const { return singularValues.size(); }

    /** Given the other column, project it onto the basis. */
    ML::distribution<float>
    rightSingularVector(const ColumnIndexEntries & basisColumns,
                        const ColumnIndexEntry & column) const;

    /** Given a particular column and its value, calculate the right
        singular value for that column.
    */
    ML::distribution<float>
    rightSingularVectorForColumn(ColumnHash col, const CellValue & value,
                                 int maxValues,
                                 bool acceptUnknownValues) const;

    /** Given the row, calculate its embedding. */
    std::pair<ML::distribution<float>, Date>
    leftSingularVector(const std::vector<std::tuple<ColumnHash, CellValue, Date> > & row,
                       int maxValues,
                       bool acceptUnknownValues) const;

    std::pair<ML::distribution<float>, Date>
    leftSingularVector(const std::vector<std::tuple<ColumnName, CellValue, Date> > & row,
                       int maxValues,
                       bool acceptUnknownValues) const;

    template<typename Tuple>
    std::pair<ML::distribution<float>, Date>
    doLeftSingularVector(const std::vector<Tuple> & row,
                         int maxValues,
                         bool acceptUnknownValues) const;

    /** Check the validity of the data structure after loading. */
    void validate();
};

DECLARE_STRUCTURE_DESCRIPTION(SvdBasis);


/*****************************************************************************/
/* SVD PROCEDURE                                                              */
/*****************************************************************************/

// Input: a dataset, training parameters
// Output: a version, which has an artifact (SVD file), a configuration, ...
// the important thing is that it can be deployed as a function, both internally
// or externally
struct SvdProcedure: public Procedure {

    SvdProcedure(MldbServer * owner,
                PolyConfig config,
                const std::function<bool (const Json::Value &)> & onProgress);

    virtual RunOutput run(const ProcedureRunConfig & run,
                          const std::function<bool (const Json::Value &)> & onProgress) const;

    virtual Any getStatus() const;

    SvdConfig svdConfig;
};

/* The output of an SVD is:
   - A set of n singular values
   - A set of singular vectors of length n, corresponding to the most important
     columns and column values in the dataset.
   - An algorithm that, given a dataset, the singular values and the singular
     vectors, can generate a singular vector for any column, column value or
     derivation.  In other words, an algorithm to re-embed a column from a dataset
   - An algorithm that, given a dataset row, the singular values and the
     singular vectors, can generate a singular vector for the row.  In other
     words, an algorithm to embed a row.
   - What about x nearest neighbours, etc?
*/

struct SvdEmbedConfig {
    SvdEmbedConfig(const Url & modelFileUrl = Url())
        : modelFileUrl(modelFileUrl), maxSingularValues(-1),
          acceptUnknownValues(false)
    {
    }

    Url modelFileUrl;
    int maxSingularValues;
    bool acceptUnknownValues;
};

DECLARE_STRUCTURE_DESCRIPTION(SvdEmbedConfig);


/*****************************************************************************/
/* SVD EMBED ROW                                                             */
/*****************************************************************************/

struct SvdInput {
    ExpressionValue row;  // is a row valued object
};

DECLARE_STRUCTURE_DESCRIPTION(SvdInput);

struct SvdOutput {
    ExpressionValue embedding;  // is an embedding object
};

DECLARE_STRUCTURE_DESCRIPTION(SvdOutput);

struct SvdEmbedRow: public ValueFunctionT<SvdInput, SvdOutput> {
    SvdEmbedRow(MldbServer * owner,
                PolyConfig config,
                const std::function<bool (const Json::Value &)> & onProgress);
    
    virtual SvdOutput call(SvdInput input) const;
    
    SvdBasis svd;
    SvdEmbedConfig functionConfig;

    /// Number of singular vectors actually produced
    int nsv;
};


} // namespace MLDB
} // namespace Datacratic
