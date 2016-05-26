/** svd.cc
    Jeremy Barnes, 16 December 2014
    Copyright (c) 2014 Datacratic Inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

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
#include "mldb/plugins/progress.h"

using namespace std;


namespace Datacratic {

// jsonDecode implementation for any type which:
// 1) has a default description;
template<typename T>
T jsonDecodeFile(const std::string & filename, T * = 0)
{
    T result;

    filter_istream stream(filename);

    static auto desc = getDefaultDescriptionSharedT<T>();
    StreamingJsonParsingContext context(filename, stream);
    desc->parseJson(&result, context);
    return result;
}

namespace MLDB {

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
             "does not support groupby and having clauses. "
             "Only plain column names may be used; it is not possible to select on "
             "an expression (like x + 1)");
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
                                         PlainColumnSelect(),
                                         MustContainFrom()),
                           validateFunction<SvdConfig>());
}

DEFINE_STRUCTURE_DESCRIPTION(SvdColumnEntry);

SvdColumnEntryDescription::
SvdColumnEntryDescription()
{
    addParent<ColumnSpec>();
    addField("singularVector", &SvdColumnEntry::singularVector,
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
ML::distribution<float>
SvdBasis::
rightSingularVector(const ColumnIndexEntries & basisColumns,
                    const ColumnIndexEntry & column) const
{
    // For each basis vector, calculate the overlap
    ML::distribution<float> result(singularValues.size());

    for (unsigned i = 0;  i < columns.size();  ++i) {
        double overlap = column.correlation(basisColumns.at(i));
        if (overlap == 0.0)
            continue;
        result += overlap * columns[i].singularVector;
    }

    result /= singularValues * singularValues;

    if ((result == 0.0).all()) {
        //cerr << "all zero projection" << endl;
        //cerr << "column " << column.columnName << endl;

        for (unsigned i = 0;  i < columns.size();  ++i) {
            //double overlap = column.correlation(basisColumns.at(i));
            //cerr << "overlap with " << columns[i].columnName
            //     << " is " << overlap << endl;
        }

    }

    return result;
}

ML::distribution<float>
SvdBasis::
rightSingularVectorForColumn(ColumnHash col, const CellValue & value,
                             int maxValues,
                             bool acceptUnknownValues) const
{
    if (maxValues < 0 || maxValues > singularValues.size())
        maxValues = singularValues.size();

    // 1.  Find the columns involved with the index
    auto it = columnIndex.find(col);
    if (it == columnIndex.end()) {
        //cerr << "column not found in " << columnIndex.size() << " entries" << endl;
        return ML::distribution<float>();
    }

    // 2.  Look up the value of the cell
    const SvdColumnIndexEntry & columnEntry = it->second;

    auto it2 = columnEntry.values.find(value);
    if (it2 == columnEntry.values.end()) {
        // Extract the cell value...
        if (value.isNumeric()) {
            double d = value.toDouble();

            //cerr << "looking into " << columnEntry.values.size() << " values"
            //     << " for column " << it->second.columnName << endl;

            for (auto & e: columnEntry.values) {
                //cerr << "e.first = " << jsonEncodeStr(e.first) << endl;
                //cerr << "e.first.cellType() = " << jsonEncodeStr(e.first.cellType()) << endl;
                //cerr << "e.second = " << e.second << endl;
                if (!e.first.empty() && e.first != value)
                    continue;
                int columnNum = e.second;
                auto & col = columns[columnNum];
                ML::distribution<float> result = col.singularVector;
                result.resize(maxValues);

                //double oldd = d;

                d += col.offset;
                d *= col.scale;

                //cerr << "value " << oldd << " transformed to " << d << endl;

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

                ML::distribution<float> result = col.singularVector;
                result.resize(maxValues);

                //double oldd = d;

                d += col.offset;
                d *= col.scale;

                //cerr << "value " << oldd << " transformed to " << d << endl;

                result *= d;
                return result;
            }

#if 0
            // It's a value we haven't seen.  We can't really do anything but return
            // an empty vector
            if (acceptUnknownValues)
                return ML::distribution<float>();

            throw HttpReturnException(400);

            cerr << "column " << columnEntry.columnName << endl;
            cerr << "value = " << value << endl;
            cerr << columnEntry.values.size() << " known values" << endl;
            for (auto & e: columnEntry.values) {
                cerr << "known value " << e.first << endl;
            }

            throw ML::Exception("Numeric value not found");
#endif
        }

        if (acceptUnknownValues)
            return ML::distribution<float>();

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
            //cerr << "col.op = " << col.op << endl;

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

        //cerr << details << endl;

        throw HttpReturnException(400, message, details);
    }

    ML::distribution<float> result = columns[it2->second].singularVector;
    result.resize(maxValues);
    return result;
}

std::pair<ML::distribution<float>, Date>
SvdBasis::
leftSingularVector(const std::vector<std::tuple<ColumnName, CellValue, Date> > & row,
                   int maxValues,
                   bool acceptUnknownValues) const
{
    return doLeftSingularVector(row, maxValues, acceptUnknownValues);
}

std::pair<ML::distribution<float>, Date>
SvdBasis::
leftSingularVector(const std::vector<std::tuple<ColumnHash, CellValue, Date> > & row,
                   int maxValues,
                   bool acceptUnknownValues) const
{
    return doLeftSingularVector(row, maxValues, acceptUnknownValues);
}

template<typename Tuple>
std::pair<ML::distribution<float>, Date>
SvdBasis::
doLeftSingularVector(const std::vector<Tuple> & row,
                     int maxValues,
                     bool acceptUnknownValues) const
{
    if (maxValues < 0 || maxValues > singularValues.size())
        maxValues = singularValues.size();

    Date ts = modelTs;
    ML::distribution<float> result(maxValues);

    for (auto & v: row) {
        ColumnHash column;
        CellValue value;
        Date columnTs;

        std::tie(column, value, columnTs) = v;

        const ML::distribution<float> & rsv
            = rightSingularVectorForColumn(column, value, maxValues, acceptUnknownValues);

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
                                 int numSingularValues);

    static SvdBasis calcRightSingular(const ClassifiedColumns & columns,
                                      const ColumnIndexEntries & columnIndex,
                                      const SvdBasis & svd);
};

SvdBasis
SvdTrainer::
calcSvdBasis(const ColumnCorrelations & correlations,
             int numSingularValues)
{
#if 0
    static int n = 0;
    {
        cerr << "saving correlations " << n << endl;
        filter_ostream stream(ML::format("correlations-%d.json", n++));
        stream << jsonEncode(correlations.columns);
        for (unsigned i = 0;  i < correlations.correlations.shape()[0];  ++i) {
            for (unsigned j = 0;  j < correlations.correlations.shape()[1];  ++j) {
                stream << i << " " << j << " " << correlations.correlations[i][j]
                       << endl;
            }
        }
        cerr << "done saving correlations " << endl;
    }
#endif

    int ndims = correlations.columnCount();

    ML::Timer timer;

    //for (unsigned i = 0;  i < ndims;  ++i) {
    //    cerr << "correlation between " << 0 << " and "
    //         << i << " is " << correlations[0][i]
    //         << endl;
    //}

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

    cerr << "done SVD " << timer.elapsed() << endl;

    // It doesn't clean up the ones that didn't converge properly... do it ourselves
    // We go until we get a NaN or one with too small a ratio.
    // Eg, seen in the wild:
    // svalues = { 3.06081 2.01797 1.91045 1.39165 1.20556 1.0859 1.01295 0.973041 0.96686 0.795663 0.787847 0.753074 0.663018 0.58732 0.566861 0.53674 0.507972 0.481893 0.476135 0.451054 0.434212 0.428739 0.406749 0.396502 0.388368 0.383147 0.381553 0.34724 0.322744 0.311273 0.297784 0.285271 0.275972 0.272025 0.271609 0.265779 0.254749 0.244108 0.234286 0.229235 0.21586 0.208849 0.207129 0.194427 0.186311 0.184302 0.18284 0.170876 0.1612 0.153722 0.145908 0.145039 0.139881 0.136478 0.134853 0.131319 0.124427 0.112027 0.0839514 0.0766772 0.0687135 0.0484199 0.0354719 0.034498 9.62614e-05 7.98612e-05 7.48308e-05 6.6479e-05 5.5881e-05 5.00391e-05 4.59796e-05 4.33525e-05 3.0214e-05 2.67698e-05 2.66379e-05 1.749e-05 1.64916e-05 1.20429e-05 5.02268e-08 -nan -nan -nan -nan 2.46486e-09 -nan -nan -nan -nan -nan -nan -nan -nan -nan -nan -nan 1.61711e-08 }

    unsigned realD = 0;
    while (realD < svdResult->d
           && isfinite(svdResult->S[realD])
           && svdResult->S[realD] / svdResult->S[0] > 1e-9)
        ++realD;

    cerr << "skipped " << svdResult->d - realD << " bad singular values" << endl;
    ExcAssertLessEqual(realD, svdResult->d);
    ExcAssertLessEqual(realD, numSingularValues);
    svdResult->d = realD;

    cerr << "got " << svdResult->d << " singular values" << endl;

    numSingularValues = svdResult->d;


#if 0
    cerr    << "Vt rows " << svdResult->Vt->rows << endl
            << "Vt cols " << svdResult->Vt->cols << endl;
#endif

    SvdBasis result;
    result.modelTs = correlations.modelTs;
    result.singularValues.resize(numSingularValues);
    std::copy(svdResult->S, svdResult->S + numSingularValues,
              result.singularValues.begin());

    cerr << "svalues = " << result.singularValues << endl;

    //cerr << "svdResult->Vt->value = " << svdResult->Vt->value << endl;

    result.columns.resize(ndims);
    std::copy(correlations.columns.begin(), correlations.columns.end(),
              result.columns.begin());

#if 1
    // Extract the singular vectors for the dense behaviours
    for (unsigned i = 0;  i < ndims;  ++i) {
        //cerr << "i = " << i << "svdResult->Vt->value[i] = "
        //     << svdResult->Vt->value[i] << endl;
        ML::distribution<float> & d = result.columns[i].singularVector;
        d.resize(numSingularValues);
        for (unsigned j = 0;  j < numSingularValues;  ++j)
            d[j] = svdResult->Vt->value[j][i];

        ColumnName columnName = result.columns[i].columnName;
        CellValue cellValue = result.columns[i].cellValue;

        result.columnIndex[columnName].values[cellValue] = i;
        result.columnIndex[columnName].columnName = columnName;


        //std::copy(svdResult->Vt->value[i],
        //          svdResult->Vt->value[i] + numSingularValues,
        //          d.begin());
    }
#endif

    //cerr << "result.columnIndex.size() = " << result.columnIndex.size() << endl;
    //cerr << "ndims = " << ndims << endl;

    for (auto & i: result.columnIndex)
        ExcAssertNotEqual(i.second.columnName, ColumnName());

#if 0
    // Test the orthonormal-ness of the singular vectors
    for (unsigned i = 0;  i < ndims;  ++i) {
        const auto & v = result.columns[i].singularVector;
        cerr << "dim " << i << ": two_norm = " << v.two_norm() << " min " << v.min()
             << " max " << v.max() << " one_norm " << v.total() << endl;
    }

    for (unsigned i = 0;  i < ndims;  ++i) {
        cerr << "dim 0 with dim " << i << ": two_norm = "
             << result.columns[i].singularVector.dotprod(result.columns[0].singularVector)
             << endl;
    }
#endif

    return result;
}

SvdBasis
SvdTrainer::
calcRightSingular(const ClassifiedColumns & columns,
                  const ColumnIndexEntries & columnIndex,
                  const SvdBasis & svd)
{
    //cerr << "project extra " << columns.continuousColumns.size() - svd.columns.size()
    //     << " continuous columns onto basis" << endl;
    //cerr << "projecting " << columns.sparseColumns.size()
    //     << " discrete columns onto basis" << endl;

    ML::Timer timer;

    std::atomic<int> numDone(0);
    std::mutex doneMutex;
    double lastSeconds = 0.0;

    size_t totalColumns = columns.continuousColumns.size() + columns.sparseColumns.size();

    SvdBasis result(svd);
    result.modelTs = svd.modelTs;
    result.columns.resize(totalColumns);

    if (false && svd.columns.size() > 0) {
        // Calc the right singular of the first continuous column
        auto vec0 = svd.rightSingularVector(columnIndex, columnIndex[0]);
        auto svec = svd.columns[0].singularVector;

        vec0.resize(10);
        svec.resize(10);

        cerr << "vec0 = " << vec0 << " svec = " << svec << endl;
    }

    auto calcRightSingular = [&] (int i)
        {
            int done = numDone.fetch_add(1);

            // If we've already done it, then skip
            if (!result.columns[i].singularVector.empty())
                return;

            auto vec = svd.rightSingularVector(columnIndex, columnIndex[i]);

            SvdColumnEntry column;
            column = columnIndex[i];
            column.singularVector = std::move(vec);

            result.columns[i] = std::move(column);

            if (done % 1000 == 0) {
                std::unique_lock<std::mutex> guard(doneMutex);
                double seconds = timer.elapsed_wall();
                cerr << "done " << done << " of " << totalColumns
                     << " in " << timer.elapsed() << endl;
                cerr << "Average " << done / seconds << " per second; inst "
                     << 1000 / (seconds - lastSeconds) << " per second" << endl;
                lastSeconds = seconds;
            }
        };

    parallelMap(0, totalColumns, calcRightSingular);

    for (unsigned i = 0;  i < totalColumns;  ++i) {
        ColumnName columnName = result.columns[i].columnName;
        CellValue cellValue = result.columns[i].cellValue;
        if (result.columns[i].op == COL_VALUE)
            ExcAssertEqual(cellValue, CellValue());

        //cerr << "column " << i << " name " << columnName
        //     << " value " << jsonEncodeStr(cellValue) << " op "
        //     << result.columns[i].op << endl;

        result.columnIndex[columnName].values[cellValue] = i;
        result.columnIndex[columnName].columnName = columnName;
    }

    for (auto & i: result.columnIndex) {
        ExcAssertNotEqual(i.second.columnName, ColumnName());
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
        ExcAssertNotEqual(c.columnName, ColumnName());
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
    //this->config.reset(new ProcedureConfig(std::move(config)));
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
      const std::function<bool (const Step &)> & onProgress) const
{
    auto runProcConf = applyRunConfOverProcConf(svdConfig, run);

    if (runProcConf.outputColumn.empty()) {
        throw HttpReturnException
            (400, "SVD training procedure requires a non-empty output column name",
             "config", runProcConf);
    }

    auto onProgress2 = [&] (const Json::Value & progress)
        {
            Json::Value value;
            value["dataset"] = progress;
            return onProgress(value);
        };

    if (!runProcConf.modelFileUrl.empty()) {
        checkWritability(runProcConf.modelFileUrl.toString(), "modelFileUrl");
    }

    int numBasisVectors = runProcConf.numDenseBasisVectors;
    
    SqlExpressionMldbScope context(server);

    auto dataset = runProcConf.trainingData.stm->from->bind(context).dataset;

    ClassifiedColumns columns = classifyColumns(*dataset, runProcConf.trainingData.stm->select);

#if 0
    cerr << "columns: " << columns.continuousColumns.size()
         << " continuous, " << columns.sparseColumns.size()
         << " sparse" << endl;
    for (auto & c: columns.continuousColumns) {
        cerr << "name " << c.columnName << " val " << c.cellValue << " op " << c.op
             << " rowCount " << c.rowCount << endl;
    }
    cerr << "sparse" << endl;
    for (auto & c: columns.sparseColumns) {
        cerr << "name " << c.columnName << " val " << c.cellValue << " op " << c.op
             << " rowCount " << c.rowCount << endl;
    }
#endif

    FeatureBuckets extractedFeatures = extractFeaturesFromRows(*dataset,
                                                               runProcConf.trainingData.stm->when,
                                                               runProcConf.trainingData.stm->where,
                                                               runProcConf.trainingData.stm->orderBy,
                                                               runProcConf.trainingData.stm->offset,
                                                               runProcConf.trainingData.stm->limit,
                                                               columns);
    ColumnIndexEntries columnIndex = invertFeatures(columns, extractedFeatures);
    ColumnCorrelations correlations = calculateCorrelations(columnIndex, numBasisVectors);
    SvdBasis svd = SvdTrainer::calcSvdBasis(correlations,
                                            runProcConf.numSingularValues);

#if 0
    cerr << "----------- SVD columns" << endl;
    for (auto & c: svd.columns) {
        cerr << "name " << c.columnName << " val " << c.cellValue << " op " << c.op << endl;
    }
#endif

    SvdBasis allSvd = SvdTrainer::calcRightSingular(columns, columnIndex, svd);

#if 0
    cerr << "----------- ALL SVD columns" << endl;
    for (auto & c: allSvd.columns) {
        cerr << "name " << c.columnName << " val " << c.cellValue << " op " << c.op << endl;
    }
#endif

    if (!runProcConf.modelFileUrl.empty()) {
        Datacratic::makeUriDirectory(runProcConf.modelFileUrl.toString());
        filter_ostream stream(runProcConf.modelFileUrl.toString());
        jsonEncodeToStream(allSvd, stream);
    }

    Date ts;  // TODO: actually fill this in...

    // Save the column embedding to a dataset if we ask for it
    if (runProcConf.columnOutput) {

        PolyConfigT<Dataset> columnOutput = *runProcConf.columnOutput;
        if (columnOutput.type.empty())
            columnOutput.type = SvdConfig::defaultOutputDatasetType;

        auto output = createDataset(server, columnOutput, onProgress2, true /*overwrite*/);

        auto doColumn = [&] (size_t i)
            {
                auto & col = allSvd.columns[i];

                if (i % 10000 == 0)
                    cerr << "saving column " << i << " of " << allSvd.columns.size()
                         << endl;

                ColumnName outputName = col.columnName;

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

        auto output = createDataset(server, rowOutput, onProgress2, true /*overwrite*/);

        // getRowNames can return row names in an arbitrary order as long as it is deterministic.
        auto rows = dataset->getMatrixView()->getRowNames(0, -1);

        //cerr << "writing embeddings for " << rows.size() << " rows to dataset "
        //     << runProcConf.rowOutput.id << endl;

        int numSingularValues = allSvd.numSingularValues();

        auto doRow = [&] (int rowNum)
            {
                if (rowNum % 10000 == 0)
                    cerr << "saving row " << rowNum << " of " << rows.size()
                         << endl;

                auto row = dataset->getMatrixView()->getRow(rows[rowNum]);

                ML::distribution<float> embedding;
                Date ts;

                std::tie(embedding, ts)
                = allSvd.leftSingularVector(row.columns, numSingularValues,
                                                false /* acceptUnknownValues*/);

#if 0
                cerr << "row = " << jsonEncodeStr(row) << endl;
                cerr << "embedding = " << embedding << endl;

                // Now go back to the original representation
                for (auto & column: row.columns) {

                    auto index = allSvd.columnIndex.find(std::get<0>(column));
                    if (index == allSvd.columnIndex.end())
                        continue;

                    float val = embedding.dotprod(allSvd.rightSingularVectorForColumn(std::get<0>(column), std::get<1>(column), 50));



                    cerr << "reconstruction: input " << std::get<1>(column) << " reconst "
                         << val << endl;
                }
#endif

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

#if 0




    // A = U^T S V
    // UA = U U^T S V = S V since S is orthonormal
    // S' U A = V
    // A = U^T S S' U A

    for (auto & s: features) {
        for (const ExtractedRow & entry: s.second) {

            // Look for out of range values of first order


            // In goes the values
            ML::distribution<double> accum(numSingularValues);

            for (unsigned i = 0;  i < entry.values.size();  ++i) {
                float v = (entry.values[i] - means[i]) / stddevs[i];
                accum += v * singularVectors[i];
            }
        }
    }

    // Try to deconstruct and reconstruct a vector
    for (auto & s: features) {
        for (const ExtractedRow & entry: s.second) {
            // In goes the values
            ML::distribution<double> accum(numSingularValues);

            for (unsigned i = 0;  i < entry.values.size();  ++i) {
                float v = (entry.values[i] - means[i]) / stddevs[i];
                accum += v * singularVectors[i];
            }

            // Plus all of the dense values
            for (BH beh: entry.dense) {
                //cerr << "dense " << behs.getBehaviourId(beh) << endl;
                auto it = discreteIndex.find(beh);
                if (it == discreteIndex.end())
                    throw ML::Exception("couldn't find dense beh index");
                accum += singularVectors[featureNum + it->second];
            }

            cerr << "accum = " << accum << endl;

            // We should also do the sparse, but we don't currently have
            // them

            //...

            // We now have an approximation to V in accum.  Take it back to
            // a feature vector

            ML::distribution<double> reconst(featureNum);

            for (unsigned i = 0;  i < featureNum;  ++i) {
                for (unsigned j = 0;  j < numSingularValues;  ++j) {
                    reconst[i] += accum[j] * singularVectors[i][j];
                }
            }

            reconst = reconst * stddevs.cast<double>() + means.cast<double>();

            cerr << "input: " << entry.values << endl;
            cerr << "reconst: " << reconst << endl;

            ML::distribution<double> reconstDense(numDenseColumns);

            for (unsigned i = 0;  i < numDenseColumns;  ++i) {
                for (unsigned j = 0;  j < numSingularValues;  ++j) {
                    reconstDense[i] += accum[j] * singularVectors[i + featureNum][j];
                }
            }

            std::vector<std::pair<BH, double> > sorted2;
            for (unsigned i = 0;  i < numDenseColumns;  ++i) {
                sorted2.emplace_back(sorted[i].first, reconstDense[i]);
            }

            ML::sort_on_second_descending(sorted2);

#if 0
            for (unsigned i = 0;  i < sorted2.size();  ++i) {
                if (i < 50 || behs.getBehaviourId(sorted2[i].first).toString().find("status=") != string::npos)
                    cerr << "  dense " << behs.getBehaviourId(sorted2[i].first) << " has score "
                         << sorted2[i].second << endl;
            }
#endif

            return;
        }
    }
}
#endif


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
    : BaseT(owner)
{
    functionConfig = config.params.convert<SvdEmbedConfig>();
    svd = std::move(jsonDecodeFile<SvdBasis>(functionConfig.modelFileUrl.toString()));

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
    
    ML::distribution<float> embedding;
    Date ts;

    std::tie(embedding, ts)
        = svd.leftSingularVector(row, nsv,
                                 functionConfig.acceptUnknownValues);

    //cerr << "nsv = " << nsv << endl;
    //cerr << "embedding = " << embedding << endl;

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
} // namespace Datacratic
