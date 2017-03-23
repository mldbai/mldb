/** dataset_feature_space.cc
    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.
*/

#include "dataset_feature_space.h"
#include "mldb/core/dataset.h"
#include "mldb/ml/jml/registry.h"
#include "mldb/base/parallel.h"
#include "mldb/ml/jml/training_index_entry.h"
#include "mldb/types/value_description.h"
#include "mldb/types/vector_description.h"
#include "mldb/types/hash_wrapper_description.h"
#include "mldb/http/http_exception.h"
#include "mldb/utils/log.h"

using namespace std;



namespace MLDB {


const ML::Feature labelFeature(0, 0, 0), weightFeature(0, 1, 0);


/*****************************************************************************/
/* DATASET FEATURE SPACE                                                     */
/*****************************************************************************/

DatasetFeatureSpace::
DatasetFeatureSpace() 
    : logger(MLDB::getMldbLog<DatasetFeatureSpace>())
{
}

DatasetFeatureSpace::
DatasetFeatureSpace(std::shared_ptr<Dataset> dataset,
                    ML::Feature_Info labelInfo,
                    const std::set<ColumnPath> & knownInputColumns,
                    bool bucketize)
    : labelInfo(labelInfo),
      logger(MLDB::getMldbLog<DatasetFeatureSpace>())
{
    auto columns = dataset->getColumnPaths();
    std::vector<ColumnPath> filteredColumns;

    for (auto & columnName: columns) {
        if (!knownInputColumns.count(columnName))
            continue;

        ColumnHash ch = columnName;
        columnInfo[ch].columnName = columnName;
        columnInfo[ch].index = filteredColumns.size();
        
        filteredColumns.push_back(columnName);
    }

    auto onColumn = [&] (int i)
        {
            const ColumnPath & columnName = filteredColumns[i];
            ColumnHash ch = columnName;
            int oldIndex = columnInfo[ch].index;
            columnInfo[ch] = getColumnInfo(dataset, columnName, bucketize);
            columnInfo[ch].index = oldIndex;
            return true;
        };

    parallelMap(0, filteredColumns.size(), onColumn);
}

//static
DatasetFeatureSpace::ColumnInfo
DatasetFeatureSpace::
getColumnInfo(std::shared_ptr<Dataset> dataset,
              const ColumnPath & columnName,
              bool bucketize)
{
    ColumnInfo result;
    result.columnName = columnName;

    if (!bucketize) {
        ColumnStats statsStorage;
        auto & stats = dataset->getColumnIndex()
            ->getColumnStats(columnName, statsStorage);

        result.distinctValues = stats.values.size();

        if (stats.isNumeric()) {
            result.info = ML::REAL;
        }
        else {
            std::map<CellValue::CellType, std::pair<size_t, size_t> > types;

            std::vector<std::string> stringValues;
            std::vector<std::string> allValues;

            for (auto & v: stats.values) {
                types[v.first.cellType()].first += 1;
                types[v.first.cellType()].second += v.second.rowCount();
                if (v.first.cellType() == CellValue::ASCII_STRING)
                    stringValues.push_back(v.first.toString());
                else if (v.first.cellType() == CellValue::UTF8_STRING)
                    stringValues.push_back(v.first.toUtf8String().rawString());

                if(v.first.cellType() != CellValue::UTF8_STRING)
                    allValues.push_back(v.first.toString());
                else
                    allValues.push_back(v.first.toUtf8String().rawString());
            }

            if (types.count(CellValue::ASCII_STRING) || 
                types.count(CellValue::UTF8_STRING)) {
                // Has string values; make it categorical
                auto categorical = std::make_shared<ML::Fixed_Categorical_Info>(allValues);
                result.info = ML::Feature_Info(categorical);
            }
            else {
                result.info = ML::REAL;
            }
        }
    }
    else {
        auto bucketsAndDescriptions
            = dataset->getColumnIndex()
            ->getColumnBuckets(columnName, 255 /* num buckets */);

        BucketList & buckets = std::get<0>(bucketsAndDescriptions);
        BucketDescriptions & descriptions = std::get<1>(bucketsAndDescriptions);

        if (descriptions.numeric.active) {
            result.info = ML::REAL;
            // TODO: if we have both numbers and strings, we probably do need
            // to support it in the long term.
            if (!descriptions.strings.buckets.empty()) {
                std::vector<Utf8String> stringValues;
                if (descriptions.strings.buckets.size() > 100) {
                    stringValues.insert(stringValues.end(),
                                        descriptions.strings.buckets.begin(),
                                        descriptions.strings.buckets.begin() + 100);
                }
                else stringValues = std::move(descriptions.strings.buckets);

                throw HttpReturnException
                    (400, "This classifier can't train on column "
                     + columnName.toUtf8String() + " which has both string "
                     + " and numeric values.  Consider using \nCAST ("
                     + columnName.toUtf8String() + " AS STRING)\nor splitting "
                     + "into two columns, one numeric-or-null and one "
                     + "string-or-null, eg\nCASE WHEN " + columnName.toUtf8String() + " IS NUMBER THEN " + columnName.toUtf8String() + " ELSE NULL END AS "
                     + columnName.toUtf8String() + "_numeric, CASE WHEN " + columnName.toUtf8String() + " IS STRING THEN " + columnName.toUtf8String() + " ELSE NULL END AS "
                     + columnName.toUtf8String() + "_string",
                     "stringValues", stringValues);
            }
            ExcAssert(descriptions.strings.buckets.empty());
        }
        else if (!descriptions.strings.buckets.empty()) {
            std::vector<std::string> categories;
            categories.reserve(descriptions.strings.buckets.size());
            for (auto & s: descriptions.strings.buckets) {
                categories.emplace_back(s.rawString());
            }
            auto categorical
                = std::make_shared<ML::Fixed_Categorical_Info>(std::move(categories));
            result.info = ML::Feature_Info(categorical);
        }

        result.distinctValues = descriptions.numBuckets();
        result.buckets = std::move(buckets);
        result.bucketDescriptions = std::move(descriptions);

    }

    return result;
}

float
DatasetFeatureSpace::
encodeFeatureValue(ColumnHash column, const CellValue & value) const
{
    if (value.empty())
        return std::numeric_limits<float>::quiet_NaN();
        
    auto it = columnInfo.find(column);
    if (it == columnInfo.end()) {
        throw MLDB::Exception("Encoding unknown column");
    }

    return encodeValue(value, it->second.columnName, it->second.info);
}

void
DatasetFeatureSpace::
encodeFeature(ColumnHash column, const CellValue & value,
              std::vector<std::pair<ML::Feature, float> > & fset) const
{
    if (value.empty())
        return;
        
    auto it = columnInfo.find(column);
    if (it == columnInfo.end()) {
        throw MLDB::Exception("Encoding unknown column");
    }

    fset.emplace_back(getFeature(column),
                      encodeValue(value, it->second.columnName,
                                  it->second.info));
}

std::pair<int, int>
DatasetFeatureSpace::
getFeatureBucket(ColumnHash column, const CellValue & value) const
{
    if (value.empty())
        throw MLDB::Exception("Encoding empty value");
        
    auto it = columnInfo.find(column);
    if (it == columnInfo.end()) {
        throw MLDB::Exception("Encoding unknown column");
    }

    if (it->second.info.type() == ML::CATEGORICAL
        || it->second.info.type() == ML::STRING) {
        std::string key;
        if (value.isUtf8String())
            key = value.toUtf8String().rawString();
        else 
            key  = value.toString();

        int val = it->second.info.categorical()->lookup(key);
        return { it->second.index, val };
    }

    return { it->second.index, it->second.bucketDescriptions.getBucket(value) };
}

ML::Label
DatasetFeatureSpace::
encodeLabel(const CellValue & value, bool isRegression) const
{
    static ColumnPath LABEL("<<LABEL>>");
    float label = encodeValue(value.isString() ? jsonEncodeStr(value) : value,
                           LABEL, labelInfo);

    if (isRegression) {
        return ML::Label(label);
    }
    else {
        return ML::Label((int)label);
    }
}

float
DatasetFeatureSpace::
encodeValue(const CellValue & value,
            const ColumnPath & columnName,
            const ML::Feature_Info & info) const
{
    if (value.empty())
        return std::numeric_limits<float>::quiet_NaN();

    if (info.type() == ML::CATEGORICAL
        || info.type() == ML::STRING) {
        // Look up the value in the categorical info

        std::string key;
        if (value.isUtf8String())
            key = value.toUtf8String().rawString();
        else 
            key  = value.toString();

        int val = info.categorical()->lookup(key);
        return val;
    }
    if (!value.isNumeric()) {
        throw MLDB::Exception("Value for column '"
                            + columnName.toUtf8String().rawString() + 
                            "' was numeric in training, but is now " +
                            jsonEncodeStr(value));
        
    }
    return value.toDouble();

}

ML::Feature_Info
DatasetFeatureSpace::
info(const ML::Feature & feature) const
{
    if (feature == labelFeature)
        return labelInfo;
    else if (feature == weightFeature)
        return ML::Feature_Info(ML::REAL, false, true, false);
    else if (feature == ML::MISSING_FEATURE)
        return ML::MISSING_FEATURE_INFO;

    // Look up the feature info we just extracted
    auto it = columnInfo.find(getHash(feature));
    if (it == columnInfo.end())
        throw MLDB::Exception("feature " + feature.print() + " not found");
    return it->second.info;
}

ColumnHash
DatasetFeatureSpace::
getHashRaw(ML::Feature feature)
{
    if (feature == ML::MISSING_FEATURE)
        return ColumnHash();
    ExcAssertEqual(feature.type(), 1);

    uint64_t high = (uint32_t)feature.arg1();
    uint64_t low  = (uint32_t)feature.arg2();

    ColumnHash result(high << 32 | low);
    return result;
}

ColumnHash
DatasetFeatureSpace::
getHash(ML::Feature feature) const
{
    if (feature == ML::MISSING_FEATURE)
        return ColumnHash();
    ExcAssertEqual(feature.type(), 1);

    if (versionTwoMapping.empty())
        return getHashRaw(feature);
    else {
        auto it = versionTwoMapping.find(feature);
        if (it == versionTwoMapping.end()) {
            throw HttpReturnException(400, "feature was not found");
        }
        return it->second;
    }
}

ML::Feature
DatasetFeatureSpace::
getFeatureRaw(ColumnHash hash)
{
    uint32_t high = hash >> 32;
    uint32_t low  = hash;

    ML::Feature result(1, high, low);
    ExcAssertEqual(getHashRaw(result), hash);

    return result;
}

ML::Feature
DatasetFeatureSpace::
getFeature(ColumnHash hash) const
{
    if (versionTwoReverseMapping.empty()) {
        return getFeatureRaw(hash);
    }

    auto it = versionTwoReverseMapping.find(hash);
    if (it == versionTwoReverseMapping.end()) {
        throw HttpReturnException(400, "hash was not found");
    }
    return it->second;
}

static CellValue getValueFromInfo(const ML::Feature_Info & info,
                                  float value)
{
    if (std::isnan(value))
        return CellValue();
    
    if (info.type() == ML::CATEGORICAL) {
        return Utf8String(info.categorical()->print(value));
    }
    
    // Non-categorical just get their face values
    return value;
}

CellValue
DatasetFeatureSpace::
getValue(const ML::Feature & feature, float value) const
{
    if (feature == labelFeature) {
        return getValueFromInfo(labelInfo, value);
    }

    // First, get the column
    ColumnHash column = getHash(feature);

    auto it = columnInfo.find(column);

    if (it == columnInfo.end())
        throw MLDB::Exception("Couldn't find column");

    return getValueFromInfo(it->second.info, value);
}

std::string
DatasetFeatureSpace::
print(const ML::Feature_Set & fs) const
{
    std::string result;

    bool first = true;
    for (auto f: fs) {
        if (!first)
            result += " ";
        first = false;
        result += print(f.first) + "=" + print(f.first, f.second);
    }

    return result;
}

std::string
DatasetFeatureSpace::
print(const ML::Feature & feature) const
{
    if (feature == labelFeature)
        return "LABEL";
    else if (feature == weightFeature)
        return "WEIGHT";
    else if (feature == ML::MISSING_FEATURE)
        return "<<<MISSING>>>";

        
    auto it = columnInfo.find(getHash(feature));
    if (it == columnInfo.end()) {
        throw MLDB::Exception("Couldn't find feature in dataset");
    }
    return it->second.columnName.toUtf8String().rawString();
}

std::string
DatasetFeatureSpace::
print(const ML::Feature & feature, float value) const
{
    if (feature == weightFeature)
        return to_string(value);
    if (feature == ML::MISSING_FEATURE) {
        return "";
    }

    auto val = getValue(feature, value);

    if (val.isAsciiString())
        return val.toString();
    else if (val.isUtf8String())
        return val.toUtf8String().rawString();
    else
        return jsonEncodeStr(getValue(feature, value));
}

void
DatasetFeatureSpace::
serialize(ML::DB::Store_Writer & store, const ML::Feature & feature) const
{
    return ML::Feature_Space::serialize(store, feature);
}

void
DatasetFeatureSpace::
reconstitute(ML::DB::Store_Reader & store, ML::Feature & feature) const
{
    return ML::Feature_Space::reconstitute(store, feature);
}

void
DatasetFeatureSpace::
serialize(ML::DB::Store_Writer & store, const ML::Feature & feature,
          float value) const
{
    auto it = columnInfo.find(getHash(feature));
    if (it == columnInfo.end())
        throw MLDB::Exception("Couldn't find feature in dataset");

    if (it->second.info.categorical()) {
        store << it->second.info.categorical()->print(value);
    }
    else store << value;
}

void
DatasetFeatureSpace::
reconstitute(ML::DB::Store_Reader & store,
             const ML::Feature & feature,
             float & value) const
{
    auto it = columnInfo.find(getHash(feature));
    if (it == columnInfo.end())
        throw MLDB::Exception("Couldn't find feature in dataset");

    if (it->second.info.categorical()) {
        string val;
        store >> val;
        value = it->second.info.categorical()->parse(val);
    }
    else store >> value;
}

std::string
DatasetFeatureSpace::
class_id() const
{
    return "MLDB::DatasetFeatureSpace";
}

ML::Feature_Space_Type
DatasetFeatureSpace::
type() const
{
    return ML::SPARSE;
}

ML::Feature_Space *
DatasetFeatureSpace::
make_copy() const
{
    return new DatasetFeatureSpace(*this);
}

void
DatasetFeatureSpace::
reconstitute(ML::DB::Store_Reader & store)
{
    char version;
    store >> version;
    if (version > 3)
        throw MLDB::Exception("unexpected version of DatasetFeatureSpace");
    ML::DB::compact_size_t numFeatures(store);

    if (version > 1)
        store >> labelInfo;
    else labelInfo = ML::BOOLEAN;
    
    decltype(columnInfo) newColumnInfo;
    decltype(versionTwoMapping) newVersionTwoMapping;
    decltype(versionTwoReverseMapping) newVersionTwoReverseMapping;

    for (unsigned i = 0;  i < numFeatures;  ++i) {
        string s, s2;
        store >> s >> s2;
        ColumnInfo info;
        ColumnHash featureHash = jsonDecodeStr<ColumnHash>(s);
        info.columnName = ColumnPath::parse(s2);
        ColumnHash hash(info.columnName);
        info.index = i;
        store >> info.info;

        if (version < 3) {
            // Version 2 used a different hash, so our feature names
            // operate on that
            ML::Feature feature = getFeatureRaw(featureHash);
            newVersionTwoMapping[feature] = hash;
            newVersionTwoReverseMapping[hash] = feature;

            TRACE_MSG(logger) << "v2 feature " << s2 << " hash " << hash << " featureHash " << featureHash
                              << " feature " << feature;
        }
        else {
            if (hash != featureHash) {
                  TRACE_MSG(logger) << "feature " << info.columnName;
            }
            ExcAssertEqual(hash, featureHash);
        }
        
        newColumnInfo[hash] = std::move(info);
    }

    columnInfo.swap(newColumnInfo);
    versionTwoMapping.swap(newVersionTwoMapping);
    versionTwoReverseMapping.swap(newVersionTwoReverseMapping);
}

void
DatasetFeatureSpace::
serialize(ML::DB::Store_Writer & store) const
{
    store << (char)3 // version
          << ML::DB::compact_size_t(columnInfo.size());

    store << labelInfo;

    DEBUG_MSG(logger) << "serializing " << columnInfo.size() << " features";
    for (auto & i: columnInfo) {
        store << jsonEncodeStr(i.first);
        const ColumnInfo& columnInfo = i.second;
        store << columnInfo.columnName.toUtf8String().rawString();
        columnInfo.info.serialize(store);
    }

    DEBUG_MSG(logger) << "done serializing feature space";
}

std::ostream & operator << (std::ostream & stream,
                            const DatasetFeatureSpace::ColumnInfo & columnInfo)
{
    stream << columnInfo.print();
    return stream;
}


static ML::Register_Factory<ML::Feature_Space, DatasetFeatureSpace>
DFS_REG("MLDB::DatasetFeatureSpace");


} // namespace MLDB


