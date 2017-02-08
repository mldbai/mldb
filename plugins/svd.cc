/** svd.cc
    Jeremy Barnes, 16 December 2014
    Copyright (c) 2014 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Implementation of an SVD algorithm for embedding of a dataset.
*/

#include "svd.h"
#include "matrix.h"
#include "mldb/server/mldb_server.h"
#include "mldb/core/dataset.h"
#include "mldb/jml/stats/distribution.h"
#include <boost/multi_array.hpp>
#include "mldb/jml/utils/guard.h"
#include "mldb/base/parallel.h"
#include "mldb/jml/utils/pair_utils.h"
#include "mldb/arch/timers.h"
#include "mldb/arch/simd_vector.h"
#include "mldb/ml/svd_utils.h"
#include "mldb/jml/utils/vector_utils.h"
#include "mldb/ext/svdlibc/svdlib.h"
#include "mldb/types/basic_value_descriptions.h"
#include "mldb/types/distribution_description.h"
#include "mldb/types/optional_description.h"
#include "mldb/ml/value_descriptions.h"
#include "mldb/sql/sql_expression.h"
#include "mldb/plugins/sql_config_validator.h"
#include "mldb/vfs/fs_utils.h"
#include "mldb/types/map_description.h"
#include "mldb/types/any_impl.h"
#include "mldb/http/http_exception.h"
#include "mldb/types/hash_wrapper_description.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/utils/progress.h"
#include "mldb/utils/log.h"
#include <sstream>

using namespace std;

namespace MLDB {

// jsonDecode implementation for any type which:
// 1) has a default description;
template<typename T>
T jsonDecodeFile(const std::string & filename, T * = 0)
{
    T result;

    MLDB::filter_istream stream(filename);

    static auto desc = MLDB::getDefaultDescriptionSharedT<T>();
    MLDB::StreamingJsonParsingContext context(filename, stream);
    desc->parseJson(&result, context);
    return result;
}

DEFINE_STRUCTURE_DESCRIPTION(SvdConfig);

SvdConfigDescription::
SvdConfigDescription()
{
    Optional<PolyConfigT<Dataset> > optionalOutputDataset;
    optionalOutputDataset.emplace(PolyConfigT<Dataset>().
                                  withType(SvdConfig::defaultOutputDatasetType));

    addField("trainingData", &SvdConfig::trainingData,
             "Specification of the data for input to the SVD Procedure.  This should be "
             "organized as an embedding, with each selected row containing the same "
             "set of columns with numeric values to be used as coordinates.  The select statement "
             "does not support groupby and having clauses.");
    addField("columnOutputDataset", &SvdConfig::columnOutput,
             "Output dataset for embedding (column singular vectors go here)",
             optionalOutputDataset);
    addField("rowOutputDataset", &SvdConfig::rowOutput,
             "Output dataset for embedding (row singular vectors go here)",
             optionalOutputDataset);
    addField("numSingularValues", &SvdConfig::numSingularValues,
             "Maximum number of singular values to work with.  "
             "If there are not enough "
             "degrees of freedom in the dataset (it is rank-deficient), then "
             "less than this number may be used", 100);
    addField("numDenseBasisVectors", &SvdConfig::numDenseBasisVectors,
             "Maximum number of dense basis vectors to use for the SVD.  "
             "This parameter gives the number of dimensions into which the "
             "projection is made.  Higher values may allow the SVD to model "
             "slightly more diverse behaviour.  "
             "The runtime goes up with the square of this parameter, "
             "in other words 10 times as many is 100 times as long to run.",
             2000);
    addField("outputColumn", &SvdConfig::outputColumn,
             "Base name of the column that will be written by the SVD.  "
             "It will be an embedding with numSingularValues elements.",
             PathElement("embedding"));
    addField("modelFileUrl", &SvdConfig::modelFileUrl,
             "URL where the model file (with extension '.svd') should be saved. "
             "This file can be loaded by the ![](%%doclink svd.embedRow function). "
             "This parameter is optional unless the `functionName` parameter is used.");
    addField("functionName", &SvdConfig::functionName,
             "If specified, an instance of the ![](%%doclink svd.embedRow function) of this name will be created using "
             "the trained model. Note that to use this parameter, the `modelFileUrl` must "
             "also be provided.");
    addParent<ProcedureConfig>();

    onPostValidate = chain(validateQuery(&SvdConfig::trainingData,
                                         NoGroupByHaving(),
                                         MustContainFrom()),
                           validateFunction<SvdConfig>());
}

DEFINE_STRUCTURE_DESCRIPTION(SimpleSvdColumnEntry);

SimpleSvdColumnEntryDescription::
SimpleSvdColumnEntryDescription()
{
    addParent<ColumnSpec>();
    addField("singularVector", &SimpleSvdColumnEntry::singularVector,
             "Singular vector for this column");
}

DEFINE_STRUCTURE_DESCRIPTION(SvdColumnIndexEntry);

SvdColumnIndexEntryDescription::
SvdColumnIndexEntryDescription()
{
    addField("columnName", &SvdColumnIndexEntry::columnName,
             "Name of the column");
    addField("values", &SvdColumnIndexEntry::values,
             "Values of fields for this column");
}

/** Given the other column, project it onto the basis. */
distribution<float>
SvdBasis::
rightSingularVector(const ColumnIndexEntries & basisColumns,
                    const ColumnIndexEntry & column,
                    shared_ptr<spdlog::logger> logger) const
{
    // For each basis vector, calculate the overlap
    distribution<float> result(singularValues.size());

    for (unsigned i = 0;  i < columns.size();  ++i) {
        double overlap = column.correlation(basisColumns.at(i));
        if (overlap == 0.0)
            continue;
        result += overlap * columns[i].singularVector;
    }

    result /= singularValues * singularValues;

    if (logger->should_log(spdlog::level::trace) && (result == 0.0).all()) {
        logger->trace() <<  "all zero projection"
                        << "column " << column.columnName;

        for (unsigned i = 0;  i < columns.size();  ++i) {
            double overlap = column.correlation(basisColumns.at(i));
            logger->trace() << "overlap with " << columns[i].columnName
                            << " is " << overlap;
        }
        
    }
    
    return result;
}

distribution<float>
SvdBasis::
rightSingularVectorForColumn(ColumnHash col, const CellValue & value,
                             int maxValues,
                             bool acceptUnknownValues,
                             shared_ptr<spdlog::logger> logger) const
{
    if (maxValues < 0 || maxValues > singularValues.size())
        maxValues = singularValues.size();

    // 1.  Find the columns involved with the index
    auto it = columnIndex.find(col);
    if (it == columnIndex.end()) {
        TRACE_MSG(logger) << "column " << col << " not found in "
                          << columnIndex.size() << " entries";
        return distribution<float>();
    }

    // 2.  Look up the value of the cell
    const SvdColumnIndexEntry & columnEntry = it->second;

    auto it2 = columnEntry.values.find(value);
    if (it2 == columnEntry.values.end()) {
        // Extract the cell value...
        if (value.isNumeric()) {
            double d = value.toDouble();

            TRACE_MSG(logger) << "looking into " << columnEntry.values.size() << " values"
                              << " for column " << it->second.columnName;

            for (auto & e: columnEntry.values) {
                if (logger->should_log(spdlog::level::trace)) {
                    logger->trace() << "e.first = " << jsonEncodeStr(e.first);
                    logger->trace() << "e.first.cellType() = " << jsonEncodeStr(e.first.cellType());
                    logger->trace() << "e.second = " << e.second;
                }

                if (!e.first.empty() && e.first != value)
                    continue;
                int columnNum = e.second;
                auto & col = columns[columnNum];
                distribution<float> result = col.singularVector;
                result.resize(maxValues);

                double oldd = d;

                d += col.offset;
                d *= col.scale;

                TRACE_MSG(logger) << "value " << oldd << " transformed to " << d;

                result *= d;
                return result;
            }

            if (columnEntry.values.size() == 1) {
                // MLDB-687
                // We saw only a single value in training, which makes it
                // essentially a "has value" or "doesn't have value"
                // feature.  Take the output for the one and only value
                // seen in training.

                auto & col = columns[columnEntry.values.begin()->second];

                distribution<float> result = col.singularVector;
                result.resize(maxValues);

                double oldd = d;

                d += col.offset;
                d *= col.scale;

                TRACE_MSG(logger) << "value " << oldd << " transformed to " << d;

                result *= d;
                return result;
            }

        }

        if (acceptUnknownValues) {
            DEBUG_MSG(logger) << "Numeric value not found";
            return distribution<float>();
        }

        Json::Value details;
        details["columnName"] = jsonEncode(it->second.columnName);
        details["columnValueNotFound"] = jsonEncode(value);
        details["columnValueNotFoundType"] = jsonEncode(value.cellType());

        auto & vals = details["firstTenknownColumnValues"];

        bool expectedNumber = false;
        bool onlyNumber = true;
        for (auto & v: it->second.values) {
            if (vals.size() >= 10)
                break;

            auto & col = columns.at(v.second);

            Json::Value thisVal;
            if (col.op == COL_VALUE) {
                thisVal = "<<any number>>";
                expectedNumber = true;
            }
            else {
                thisVal = jsonEncode(v.first);
                onlyNumber = false;
            }

            vals.append(thisVal);
        }

        Utf8String message;
        if (value.isNumeric()) {
            message = "Column '" + it->second.columnName.toUtf8String()
                + "' was a string in training but has numeric value "
                + jsonEncodeUtf8(value) + " when passed to SVD";
        }
        else if (expectedNumber && onlyNumber) {
            message = "Column '" + it->second.columnName.toUtf8String()
                + "' passed as a string value " + jsonEncodeUtf8(value)
                + " but only numbers were seen in training";
        }
        else {
            message = "Column '" + it->second.columnName.toUtf8String()
                + "' passed a value " + jsonEncodeUtf8(value)
                + " that was never seen in training when passed to SVD";
        }

        DEBUG_MSG(logger) << details;

        throw HttpReturnException(400, message, details);
    }

    distribution<float> result = columns[it2->second].singularVector;
    result.resize(maxValues);
    return result;
}

std::pair<distribution<float>, Date>
SvdBasis::
leftSingularVector(const std::vector<std::tuple<ColumnPath, CellValue, Date> > & row,
                   int maxValues,
                   bool acceptUnknownValues,
                   shared_ptr<spdlog::logger> logger) const
{
    return doLeftSingularVector(row, maxValues, acceptUnknownValues, logger);
}

std::pair<distribution<float>, Date>
SvdBasis::
leftSingularVector(const std::vector<std::tuple<ColumnHash, CellValue, Date> > & row,
                   int maxValues,
                   bool acceptUnknownValues,
                   shared_ptr<spdlog::logger> logger) const
{
    return doLeftSingularVector(row, maxValues, acceptUnknownValues, logger);
}

template<typename Tuple>
std::pair<distribution<float>, Date>
SvdBasis::
doLeftSingularVector(const std::vector<Tuple> & row,
                     int maxValues,
                     bool acceptUnknownValues,
                     shared_ptr<spdlog::logger> logger) const
{
    if (maxValues < 0 || maxValues > singularValues.size())
        maxValues = singularValues.size();

    Date ts = modelTs;
    distribution<float> result(maxValues);

    for (auto & v: row) {
        ColumnHash column;
        CellValue value;
        Date columnTs;

        std::tie(column, value, columnTs) = v;

        const distribution<float> & rsv
            = rightSingularVectorForColumn(column, value, maxValues, acceptUnknownValues, logger);

        // If it was excluded, it will have an empty vector calculated
        if (rsv.empty())
            continue;
        result += rsv;
        ts.setMax(columnTs);
    }

    for (unsigned i = 0;  i < result.size();  ++i)
        result[i] /= singularValues[i];

    return make_pair(std::move(result), ts);
}

void
SvdBasis::
validate()
{
    for (unsigned i = 0;  i < columns.size();  ++i) {
        auto & c = columns[i];
        ExcAssert(columnIndex.count(c.columnName));
        ExcAssert(columnIndex[c.columnName].values.count(c.cellValue));
        ExcAssertEqual(columnIndex[c.columnName].columnName, c.columnName);
        ExcAssertEqual(columnIndex[c.columnName].values[c.cellValue], i);
        if (c.op == COL_VALUE)
            ExcAssertEqual(c.cellValue, CellValue());
    }
}

DEFINE_STRUCTURE_DESCRIPTION(SvdBasis);

SvdBasisDescription::
SvdBasisDescription()
{
    addField("columns", &SvdBasis::columns, "Columns of SVD");
    addField("singularValues", &SvdBasis::singularValues,
             "Singular values of SVD");
    addField("columnIndex", &SvdBasis::columnIndex, "Index of columns");
    addField("modelTs", &SvdBasis::modelTs, "Timestamp of latest information incorporated into model");
}

struct SvdTrainer {
    static SvdBasis calcSvdBasis(const ColumnCorrelations & correlations,
                                 int numSingularValues,
                                 shared_ptr<spdlog::logger> logger);

    static SvdBasis calcRightSingular(const ClassifiedColumns & columns,
                                      const ColumnIndexEntries & columnIndex,
                                      const SvdBasis & svd,
                                      shared_ptr<spdlog::logger> logger);
};

SvdBasis
SvdTrainer::
calcSvdBasis(const ColumnCorrelations & correlations,
             int numSingularValues,
             shared_ptr<spdlog::logger> logger)
{
#if 0
    static int n = 0;
    {
        INFO_MSG(logger) << "saving correlations " << n;
        filter_ostream stream(MLDB::format("correlations-%d.json", n++));
        stream << jsonEncode(correlations.columns);
        for (unsigned i = 0;  i < correlations.correlations.shape()[0];  ++i) {
            for (unsigned j = 0;  j < correlations.correlations.shape()[1];  ++j) {
                stream << i << " " << j << " " << correlations.correlations[i][j]
                       << endl;
            }
        }
        INFO_MSG(logger) << "done saving correlations ";
    }
#endif

    int ndims = correlations.columnCount();

    Timer timer;

    if (logger->should_log(spdlog::level::trace)) {
        for (unsigned i = 0;  i < ndims;  ++i) {
            logger->trace() << "correlation between " << 0 << " and "
                            << i << " is " << correlations.correlations[0][i];
        }
    }

    /**************************************************************
     * multiplication of matrix B by vector x, where B = A'A,     *
     * and A is nrow by ncol (nrow >> ncol). Hence, B is of order *
     * n = ncol (y stores product vector).		              *
     **************************************************************/

    auto opb_fn = [&] (const double * x, double * y)
    {
        for (unsigned i = 0; i != ndims; i++) {
            y[i] = ML::SIMD::vec_dotprod_dp(&correlations.correlations[i][0], x, ndims);
        }
    };

    SVDParams params;
    params.opb = opb_fn;
    params.ierr = 0;
    params.nrows = ndims;
    params.ncols = ndims;
    params.nvals = 0;
    params.doU = false;
    params.calcPrecision(params.ncols);

    svdrec * svdResult = svdLAS2A(numSingularValues, params);
    ML::Call_Guard cleanUp( [&](){ svdFreeSVDRec(svdResult); });

    INFO_MSG(logger) << "done SVD " << timer.elapsed();

    // It doesn't clean up the ones that didn't converge properly... do it ourselves
    // We go until we get a NaN or one with too small a ratio.
    // Eg, seen in the wild:
    // svalues = { 3.06081 2.01797 1.91045 1.39165 1.20556 1.0859 1.01295 0.973041 0.96686 0.795663 0.787847 0.753074 0.663018 0.58732 0.566861 0.53674 0.507972 0.481893 0.476135 0.451054 0.434212 0.428739 0.406749 0.396502 0.388368 0.383147 0.381553 0.34724 0.322744 0.311273 0.297784 0.285271 0.275972 0.272025 0.271609 0.265779 0.254749 0.244108 0.234286 0.229235 0.21586 0.208849 0.207129 0.194427 0.186311 0.184302 0.18284 0.170876 0.1612 0.153722 0.145908 0.145039 0.139881 0.136478 0.134853 0.131319 0.124427 0.112027 0.0839514 0.0766772 0.0687135 0.0484199 0.0354719 0.034498 9.62614e-05 7.98612e-05 7.48308e-05 6.6479e-05 5.5881e-05 5.00391e-05 4.59796e-05 4.33525e-05 3.0214e-05 2.67698e-05 2.66379e-05 1.749e-05 1.64916e-05 1.20429e-05 5.02268e-08 -nan -nan -nan -nan 2.46486e-09 -nan -nan -nan -nan -nan -nan -nan -nan -nan -nan -nan 1.61711e-08 }

    unsigned realD = 0;
    while (realD < svdResult->d
           && isfinite(svdResult->S[realD])
           && svdResult->S[realD] / svdResult->S[0] > 1e-9)
        ++realD;

    INFO_MSG(logger) << "skipped " << svdResult->d - realD << " bad singular values";
    ExcAssertLessEqual(realD, svdResult->d);
    ExcAssertLessEqual(realD, numSingularValues);
    svdResult->d = realD;

    INFO_MSG(logger) << "got " << svdResult->d << " singular values";

    numSingularValues = svdResult->d;

    TRACE_MSG(logger) << "Vt rows " << svdResult->Vt->rows;
    TRACE_MSG(logger) << "Vt cols " << svdResult->Vt->cols;

    SvdBasis result;
    result.modelTs = correlations.modelTs;
    result.singularValues.resize(numSingularValues);
    std::copy(svdResult->S, svdResult->S + numSingularValues,
              result.singularValues.begin());

    INFO_MSG(logger) << "svalues = " << result.singularValues;

    result.columns.resize(ndims);
    std::copy(correlations.columns.begin(), correlations.columns.end(),
              result.columns.begin());

#if 1
    // Extract the singular vectors for the dense behaviours
    for (unsigned i = 0;  i < ndims;  ++i) {

        distribution<float> & d = result.columns[i].singularVector;
        d.resize(numSingularValues);
        for (unsigned j = 0;  j < numSingularValues;  ++j)
            d[j] = svdResult->Vt->value[j][i];

        ColumnPath columnName = result.columns[i].columnName;
        CellValue cellValue = result.columns[i].cellValue;

        result.columnIndex[columnName].values[cellValue] = i;
        result.columnIndex[columnName].columnName = columnName;


        //std::copy(svdResult->Vt->value[i],
        //          svdResult->Vt->value[i] + numSingularValues,
        //          d.begin());
    }
#endif

    TRACE_MSG(logger) << "result.columnIndex.size() = " << result.columnIndex.size();
    TRACE_MSG(logger) << "ndims = " << ndims;

    for (auto & i: result.columnIndex)
        ExcAssertNotEqual(i.second.columnName, ColumnPath());

    if (logger->should_log(spdlog::level::trace)) {
        logger->trace() << "Testing the orthonormal-ness of the singular vectors";

        for (unsigned i = 0;  i < ndims;  ++i) {
            const auto & v = result.columns[i].singularVector;
              logger->trace() << "dim " << i << ": two_norm = " << v.two_norm() << " min " << v.min()
                              << " max " << v.max() << " one_norm " << v.total();
        }

        for (unsigned i = 0;  i < ndims;  ++i) {
              logger->trace() << "dim 0 with dim " << i << ": two_norm = "
                              << result.columns[i].singularVector.dotprod(result.columns[0].singularVector);
        }
    }

    return result;
}

SvdBasis
SvdTrainer::
calcRightSingular(const ClassifiedColumns & columns,
                  const ColumnIndexEntries & columnIndex,
                  const SvdBasis & svd,
                  shared_ptr<spdlog::logger> logger)

{
    DEBUG_MSG(logger) << "project extra " << columns.continuousColumns.size() - svd.columns.size()
                      << " continuous columns onto basis";
    DEBUG_MSG(logger) << "projecting " << columns.sparseColumns.size()
                      << " discrete columns onto basis";

    Timer timer;

    std::atomic<int> numDone(0);
    double lastSeconds = 0.0;

    size_t totalColumns = columns.continuousColumns.size() + columns.sparseColumns.size();

    SvdBasis result(svd);
    result.modelTs = svd.modelTs;
    result.columns.resize(totalColumns);

    auto outputFirstRightSingular = [&svd, logger](const ColumnIndexEntries & columnIndex) {
        stringstream output;

        // Calc the right singular of the first continuous column
        auto vec0 = svd.rightSingularVector(columnIndex, columnIndex[0], logger);
        auto svec = svd.columns[0].singularVector;

        vec0.resize(10);
        svec.resize(10);

        output << "vec0 = " << vec0 << " svec = " << svec;
        return output.str();
    };

    if (logger->should_log(spdlog::level::debug) && svd.columns.size() > 0)
        logger->debug() << outputFirstRightSingular(columnIndex);

    auto calcRightSingular = [&] (int i)
        {
            int done = numDone.fetch_add(1);

            // If we've already done it, then skip
            if (!result.columns[i].singularVector.empty())
                return;

            auto vec = svd.rightSingularVector(columnIndex, columnIndex[i], logger);

            SimpleSvdColumnEntry column;
            column = columnIndex[i];
            column.singularVector = std::move(vec);

            result.columns[i] = std::move(column);

            if (done % 1000 == 0) {
                double seconds = timer.elapsed_wall();
                INFO_MSG(logger) << "done " << done << " of " << totalColumns
                     << " in " << timer.elapsed();
                INFO_MSG(logger) << "Average " << done / seconds << " per second; inst "
                     << 1000 / (seconds - lastSeconds) << " per second";
                lastSeconds = seconds;
            }
        };

    parallelMap(0, totalColumns, calcRightSingular);

    for (unsigned i = 0;  i < totalColumns;  ++i) {
        ColumnPath columnName = result.columns[i].columnName;
        CellValue cellValue = result.columns[i].cellValue;
        if (result.columns[i].op == COL_VALUE)
            ExcAssertEqual(cellValue, CellValue());

        TRACE_MSG(logger) << "column " << i << " name " << columnName
                          << " value " << jsonEncodeStr(cellValue) << " op "
                          << result.columns[i].op;

        result.columnIndex[columnName].values[cellValue] = i;
        result.columnIndex[columnName].columnName = columnName;
    }

    for (auto & i: result.columnIndex) {
        ExcAssertNotEqual(i.second.columnName, ColumnPath());
        for (auto & v: i.second.values) {
            auto col = result.columns.at(v.second);
            auto val = v.first;

            ExcAssertEqual(val, col.cellValue);
            ExcAssertEqual(i.second.columnName, col.columnName);

            if (col.op == COL_VALUE)
                ExcAssertEqual(val, CellValue());
        }
    }

    ExcAssertLessEqual(result.columnIndex.size(), result.columns.size());

    for (auto & c: result.columns) {
        ExcAssertNotEqual(c.columnName, ColumnPath());
    }

    return result;
}


/*****************************************************************************/
/* SVD PROCEDURE                                                              */
/*****************************************************************************/

SvdProcedure::
SvdProcedure(MldbServer * owner,
            PolyConfig config,
            const std::function<bool (const Json::Value &)> & onProgress)
    : Procedure(owner)
{
    this->svdConfig = config.params.convert<SvdConfig>();
}

Any
SvdProcedure::
getStatus() const
{
    return Any();
}

RunOutput
SvdProcedure::
run(const ProcedureRunConfig & run,
      const std::function<bool (const Json::Value &)> & onProgress) const
{
    auto runProcConf = applyRunConfOverProcConf(svdConfig, run);

    if (runProcConf.outputColumn.null()) {
        throw HttpReturnException
            (400, "SVD training procedure requires a non-empty output column name",
             "config", runProcConf);
    }

    if (!runProcConf.modelFileUrl.empty()) {
        checkWritability(runProcConf.modelFileUrl.toDecodedString(), "modelFileUrl");
    }

    int numBasisVectors = runProcConf.numDenseBasisVectors;
    
    SqlExpressionMldbScope context(server);
    
    ConvertProgressToJson convertProgressToJson(onProgress);
    auto dataset = runProcConf.trainingData.stm->from->bind(context, convertProgressToJson).dataset;

    Progress svdProgress;
    std::shared_ptr<Step> classificationStep = svdProgress.steps({
            make_pair("classifying columns", "percentile"), 
            make_pair("extracting features", "percentile"),
            make_pair("inverting features", "percentile"),
            make_pair("calculating correlations", "percentile")
            });


    mutex progressMutex;

    ClassifiedColumns columns = classifyColumns(runProcConf.trainingData.stm->select,
                                                *dataset,
                                                runProcConf.trainingData.stm->when,
                                                *runProcConf.trainingData.stm->where,
                                                runProcConf.trainingData.stm->orderBy,
                                                runProcConf.trainingData.stm->offset,
                                                runProcConf.trainingData.stm->limit,
                                                logger,
                                                convertProgressToJson);

    auto outputColumns = [](const ClassifiedColumns & columns) {
        stringstream output;
        output << "continuous columns are:" << endl;
        for (auto & c: columns.continuousColumns) {
            output << "name " << c.columnName << " val " << c.cellValue << " op " << c.op
                   << " rowCount " << c.rowCount << endl;;
        }
        output << "sparse columns are:" << endl;
        for (auto & c: columns.sparseColumns) {
            output << "name " << c.columnName << " val " << c.cellValue << " op " << c.op
                   << " rowCount " << c.rowCount << endl;
        }
        return output.str();
    };

    DEBUG_MSG(logger) <<  "classified columns: " << columns.continuousColumns.size()
                      << " continuous columns, " << columns.sparseColumns.size()
                      << " sparse columns";
    DEBUG_MSG(logger) << outputColumns(columns);

    auto extractionStep = classificationStep->nextStep(1);

    FeatureBuckets extractedFeatures = extractFeaturesFromRows(runProcConf.trainingData.stm->select,
                                                               *dataset,
                                                               runProcConf.trainingData.stm->when,
                                                               runProcConf.trainingData.stm->where,
                                                               runProcConf.trainingData.stm->orderBy,
                                                               runProcConf.trainingData.stm->offset,
                                                               runProcConf.trainingData.stm->limit,
                                                               columns,
                                                               logger,
                                                               convertProgressToJson);

    auto inversionStep = extractionStep->nextStep(1);

    ColumnIndexEntries columnIndex = invertFeatures(columns, extractedFeatures, logger, convertProgressToJson);

    ColumnCorrelations correlations = calculateCorrelations(columnIndex, numBasisVectors, logger);
    SvdBasis svd = SvdTrainer::calcSvdBasis(correlations,
                                            runProcConf.numSingularValues,
                                            logger);

    auto outputSvdColumns = [](const SvdBasis & basis) {
        stringstream output;
         for (auto & c: basis.columns) {
             output << "name " << c.columnName << " val " << c.cellValue << " op " << c.op << endl;
         }
        return output.str();
    };

    DEBUG_MSG(logger) << "----------- SVD columns";
    DEBUG_MSG(logger) << outputSvdColumns(svd);
   
    SvdBasis allSvd = SvdTrainer::calcRightSingular(columns, columnIndex, svd, logger);

    DEBUG_MSG(logger) << "----------- ALL SVD columns";
    DEBUG_MSG(logger) << outputSvdColumns(allSvd);

    if (!runProcConf.modelFileUrl.empty()) {
        makeUriDirectory(runProcConf.modelFileUrl.toDecodedString());
        filter_ostream stream(runProcConf.modelFileUrl);
        jsonEncodeToStream(allSvd, stream);
    }

    Date ts;  // TODO: actually fill this in...

    // Save the column embedding to a dataset if we ask for it
    if (runProcConf.columnOutput) {

        PolyConfigT<Dataset> columnOutput = *runProcConf.columnOutput;
        if (columnOutput.type.empty())
            columnOutput.type = SvdConfig::defaultOutputDatasetType;

        auto onProgress2 = [&] (const Json::Value & progress) {
            Json::Value value;
            value["dataset"] = progress;
            return onProgress(value);
        };

        auto output = createDataset(server, columnOutput, onProgress2, true /*overwrite*/);

        auto doColumn = [&] (size_t i)
            {
                auto & col = allSvd.columns[i];

                if (i % 10000 == 0)
                    INFO_MSG(logger) << "saving column " << i << " of " << allSvd.columns.size();

                ColumnPath outputName = col.columnName;

                if (col.op == COL_EQUAL) {
                    if (col.cellValue.empty()) {
                        outputName = outputName + PathElement("isNull");
                    }
                    else if (col.cellValue.isNumber()) {
                        outputName = outputName + PathElement("numberEquals")
                            + col.cellValue.toUtf8String();
                    }
                    else if (col.cellValue.isString()) {
                        if (col.cellValue.toUtf8String().empty()) {
                            outputName = outputName + PathElement("stringEmpty");
                        }
                        else {
                            outputName = outputName + PathElement("stringEquals")
                                + col.cellValue.toUtf8String();
                        }
                    }
#if 0
                    else if (col.cellValue.isTimestamp()) {
                    }
                    else if (col.cellValue.isTimeinterval()) {
                    }
                    else if (col.cellValue.isBlob()) {
                    }
#endif
                    else {
                        throw HttpReturnException
                            (400,"Can't apply an SVD to a column that's not "
                             "numeric or categorical (string)",
                             "columnValue", col.cellValue,
                             "columnName", col.columnName);
                    }
                }
                else if (col.op == COL_VALUE) {
                    outputName = outputName + PathElement("numericValue");
                }
                try {
                    StructValue cols;
                    cols.emplace_back(runProcConf.outputColumn,
                                      ExpressionValue(col.singularVector, ts));
                    output->recordRowExpr(outputName, std::move(cols));
                } catch (const std::exception & exc) {
                    rethrowHttpException(-1, "Error adding SVD column '" + outputName.toUtf8String() + "' to output: "
                                         + exc.what(),
                                         "columnName", outputName);
                }
            };

        parallelMap(0, allSvd.columns.size(), doColumn);

        output->commit();
    }

    // Save the row embedding to a dataset.  This is optional (it's not actually
    // needed to apply the SVD).

    if (runProcConf.rowOutput) {

        PolyConfigT<Dataset> rowOutput = *runProcConf.rowOutput;
        if (rowOutput.type.empty())
            rowOutput.type = SvdConfig::defaultOutputDatasetType;

        auto onProgress2 = [&] (const Json::Value & progress) {
            Json::Value value;
            value["dataset"] = progress;
            return onProgress(value);
        };
        auto output = createDataset(server, rowOutput, onProgress2, true /*overwrite*/);

        // getRowPaths can return row names in an arbitrary order as long as it is deterministic.
        auto rows = dataset->getMatrixView()->getRowPaths(0, -1);

        DEBUG_MSG(logger) << "writing embeddings for " << rows.size() << " rows to dataset "
                          << runProcConf.rowOutput->id;

        int numSingularValues = allSvd.numSingularValues();

        auto doRow = [&] (int rowNum)
            {
                if (rowNum % 10000 == 0)
                    INFO_MSG(logger) << "saving row " << rowNum << " of " << rows.size();

                auto row = dataset->getMatrixView()->getRow(rows[rowNum]);

                distribution<float> embedding;
                Date ts;

                std::tie(embedding, ts)
                = allSvd.leftSingularVector(row.columns, numSingularValues,
                                            false /* acceptUnknownValues*/,
                                            logger);

                StructValue cols;
                cols.emplace_back(runProcConf.outputColumn,
                                  ExpressionValue(std::move(embedding), ts));
                output->recordRowExpr(row.rowName, std::move(cols));
            };

        parallelMap(0, rows.size(), doRow);

        output->commit();
    }

    if(!runProcConf.functionName.empty()) {
        PolyConfig svdFuncPC;
        svdFuncPC.type = "svd.embedRow";
        svdFuncPC.id = runProcConf.functionName;
        svdFuncPC.params = SvdEmbedConfig(runProcConf.modelFileUrl);

        createFunction(server, svdFuncPC, onProgress, true);
    }

    return Any();
}


/*****************************************************************************/
/* SVD EMBED ROW                                                             */
/*****************************************************************************/

DEFINE_STRUCTURE_DESCRIPTION(SvdEmbedConfig);

SvdEmbedConfigDescription::
SvdEmbedConfigDescription()
{
    addField("modelFileUrl", &SvdEmbedConfig::modelFileUrl,
             "URL of the model file (with extension '.svd') to load. "
             "This file is created by the ![](%%doclink svd.train procedure).");
    addField("maxSingularValues", &SvdEmbedConfig::maxSingularValues,
             "Maximum number of singular values to use (-1 = all)", -1);
    addField("acceptUnknownValues", &SvdEmbedConfig::acceptUnknownValues,
             "This parameter (which defaults to false) tells us whether or "
             "not unknown values should be accepted by the SVD.  An unknown "
             "value occurs when a column that was always a number in training "
             "is presented with a string value, or vice versa, or when a "
             "string valued column is presented with a value unknown in "
             "training.  If its value is true, an unknown value will be "
             "silently ignored.  If its value is false, an unknown value "
             "will return an error when the function is applied.",
             false);
}

DEFINE_STRUCTURE_DESCRIPTION(SvdInput);

SvdInputDescription::
SvdInputDescription()
{
    addField("row", &SvdInput::row,
             "Row to apply SVD to.  The embedding will be the average of "
             "the SVD of each of the keys of the row.");
}

DEFINE_STRUCTURE_DESCRIPTION(SvdOutput);

SvdOutputDescription::
SvdOutputDescription()
{
    addField("embedding", &SvdOutput::embedding,
             "Embedding of the row into the vector space defined by the "
             "SVD.  There will be a number of coordinates equal to the "
             "`maxSingluarValues` value of the configuration, or if not "
             "set, the number of SVD coordinates available in the SVD model "
             "file.  PathElementinates will have simple numerical names.");
}


SvdEmbedRow::
SvdEmbedRow(MldbServer * owner,
            PolyConfig config,
            const std::function<bool (const Json::Value &)> & onProgress)
    : BaseT(owner, config)
{
    functionConfig = config.params.convert<SvdEmbedConfig>();
    svd = jsonDecodeFile<SvdBasis>(functionConfig.modelFileUrl.toDecodedString());

    std::map<ColumnHash, SvdColumnIndexEntry> columnIndex2;

    // Deal with the older version of the file
    for (auto & c: svd.columnIndex) {
        columnIndex2[c.second.columnName] = std::move(c.second);
    }

    svd.columnIndex = std::move(columnIndex2);

    svd.validate();

    nsv = functionConfig.maxSingularValues;
    if (nsv < 0 || nsv > svd.numSingularValues())
        nsv = svd.numSingularValues();
}

SvdOutput
SvdEmbedRow::
call(SvdInput input) const
{
    RowValue row;
    input.row.mergeToRowDestructive(row);
    
    distribution<float> embedding;
    Date ts;

    std::tie(embedding, ts)
        = svd.leftSingularVector(row, nsv,
                                 functionConfig.acceptUnknownValues,
                                 logger);

    DEBUG_MSG(logger) << "nsv = " << nsv;
    DEBUG_MSG(logger) << "embedding = " << embedding;

    // TODO: what to do when no embedding is returned?
    if (embedding.empty())
        embedding.resize(nsv);

    SvdOutput result;
    result.embedding = ExpressionValue(std::move((vector<float> &)embedding), ts);
    
    return result;
}

namespace {

RegisterProcedureType<SvdProcedure, SvdConfig>
regSvd(builtinPackage(),
       "Train a SVD to convert rows or columns to embedding coordinates",
       "procedures/Svd.md.html");

RegisterFunctionType<SvdEmbedRow, SvdEmbedConfig>
regSvdEmbedRow(builtinPackage(),
               "svd.embedRow",
               "Apply a trained SVD to embed a row into a coordinate space",
               "functions/SvdEmbedRow.md.html");

} // file scope

} // namespace MLDB

