/** dataset_feature_space.cc
    This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.
*/

#include "dataset_feature_space.h"
#include "mldb/core/dataset.h"
#include "mldb/ml/jml/registry.h"
#include "mldb/base/parallel.h"
#include "mldb/ml/jml/training_index_entry.h"
#include "mldb/types/value_description.h"
#include "mldb/types/hash_wrapper_description.h"

using namespace std;


namespace Datacratic {
namespace MLDB {


const ML::Feature labelFeature(0, 0, 0), weightFeature(0, 1, 0);


/*****************************************************************************/
/* DATASET FEATURE SPACE                                                     */
/*****************************************************************************/

DatasetFeatureSpace::
DatasetFeatureSpace()
{
}

DatasetFeatureSpace::
DatasetFeatureSpace(std::shared_ptr<Dataset> dataset,
                    ML::Feature_Info labelInfo,
                    const std::set<ColumnName> & knownInputColumns,
                    bool bucketize)
    : labelInfo(labelInfo)
{
    auto columns = dataset->getColumnNames();
    std::vector<ColumnName> filteredColumns;

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
            const ColumnName & columnName = filteredColumns[i];
            ColumnHash ch = columnName;
            int oldIndex = columnInfo[ch].index;
            columnInfo[ch] = getColumnInfo(dataset, columnName.toUtf8String(), bucketize);
            columnInfo[ch].index = oldIndex;
            return true;
        };

    parallelMap(0, filteredColumns.size(), onColumn);
}

//static
DatasetFeatureSpace::ColumnInfo
DatasetFeatureSpace::
getColumnInfo(std::shared_ptr<Dataset> dataset,
              const Utf8String & columnName,
              bool bucketize)
{
    ColumnInfo result;
    result.columnName = columnName;

    if (!bucketize) {
        //cerr << "doing column " << columnName << endl;
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

            //cerr << "feature " << columnName << " has types " << jsonEncodeStr(types)
            //     << endl;

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


#if 0
        if (descriptions.isOnlyNumeric()) {
            result.info = ML::REAL;
        }
        else {
            std::set<CellValue::CellType> types;

            std::vector<std::string> stringValues;
            std::vector<std::string> allValues;

            for (auto & v: descriptions.values) {
                types.insert(v.cellType());
                if (v.cellType() == CellValue::ASCII_STRING)
                    stringValues.push_back(v.toString());
                else if (v.cellType() == CellValue::UTF8_STRING)
                    stringValues.push_back(v.toUtf8String().rawString());

                if(v.cellType() != CellValue::UTF8_STRING)
                    allValues.push_back(v.toString());
                else
                    allValues.push_back(v.toUtf8String().rawString());
            }

            if (types.count(CellValue::ASCII_STRING) || 
                types.count(CellValue::UTF8_STRING)) {
                // Has string values; make it categorical
                auto categorical = std::make_shared<ML::Fixed_Categorical_Info>
                    (allValues);
                result.info = ML::Feature_Info(categorical);
            }
            else {
                result.info = ML::REAL;
            }
        }
#endif

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
        throw ML::Exception("Encoding unknown column");
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
        throw ML::Exception("Encoding unknown column");
    }

    ML::Feature f = getFeature(column);
    
    fset.emplace_back(getFeature(column),
                      encodeValue(value, it->second.columnName,
                                  it->second.info));
}

std::pair<int, int>
DatasetFeatureSpace::
getFeatureBucket(ColumnHash column, const CellValue & value) const
{
    if (value.empty())
        throw ML::Exception("Encoding empty value");
        
    auto it = columnInfo.find(column);
    if (it == columnInfo.end()) {
        throw ML::Exception("Encoding unknown column");
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

float
DatasetFeatureSpace::
encodeLabel(const CellValue & value) const
{
    static ColumnName LABEL("<<LABEL>>");
    return encodeValue(value.isString() ? jsonEncodeStr(value) : value,
                       LABEL, labelInfo);
}

float
DatasetFeatureSpace::
encodeValue(const CellValue & value,
            const ColumnName & columnName,
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
        throw ML::Exception("Value for column '"
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
        throw ML::Exception("feature " + feature.print() + " not found");
    return it->second.info;
}

ColumnHash
DatasetFeatureSpace::
getHash(ML::Feature feature)
{
    ExcAssertEqual(feature.type(), 1);

    uint64_t high = (uint32_t)feature.arg1();
    uint64_t low  = (uint32_t)feature.arg2();

    ColumnHash result(high << 32 | low);
    return result;
}

ML::Feature
DatasetFeatureSpace::
getFeature(ColumnHash hash) const
{
    uint32_t high = hash >> 32;
    uint32_t low  = hash;

    //uint32_t high = columnInfo.find(hash)->second.index;
    //uint32_t low  = 0;

    ML::Feature result(1, high, low);
    ExcAssertEqual(getHash(result), hash);

    return result;
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
        throw ML::Exception("Couldn't find column");

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
        cerr << "feature = " << feature << endl;
        cerr << "hash = " << getHash(feature) << endl;
        throw ML::Exception("Couldn't find feature in dataset");
    }
    return it->second.columnName.toUtf8String().rawString();
}

std::string
DatasetFeatureSpace::
print(const ML::Feature & feature, float value) const
{
    if (feature == weightFeature)
        return to_string(value);

    auto val = getValue(feature, value);

    if (val.isString())
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
        throw ML::Exception("Couldn't find feature in dataset");

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
        throw ML::Exception("Couldn't find feature in dataset");

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
    if (version > 2)
        throw ML::Exception("unexpected version of DatasetFeatureSpace");
    ML::DB::compact_size_t numFeatures(store);

    if (version > 1)
        store >> labelInfo;
    else labelInfo = ML::BOOLEAN;
    
    decltype(columnInfo) newColumnInfo;

    for (unsigned i = 0;  i < numFeatures;  ++i) {
        string s, s2;
        store >> s >> s2;
        ColumnInfo info;
        ColumnHash hash = jsonDecodeStr<ColumnHash>(s);
        info.columnName = ColumnName(s2);
        info.index = i;
        store >> info.info;

        newColumnInfo[hash] = std::move(info);
    }

    columnInfo.swap(newColumnInfo);

}

void
DatasetFeatureSpace::
serialize(ML::DB::Store_Writer & store) const
{
    store << (char)2 // version
          << ML::DB::compact_size_t(columnInfo.size());

    store << labelInfo;

    //cerr << "serializing " << columnInfo.size() << " features" << endl;
    for (auto & i: columnInfo) {
        store << jsonEncodeStr(i.first);
        store << i.second.columnName.toUtf8String().rawString();
        i.second.info.serialize(store);
    }

    //cerr << "done serializing feature space" << endl;
}


static ML::Register_Factory<ML::Feature_Space, DatasetFeatureSpace>
DFS_REG("MLDB::DatasetFeatureSpace");


} // namespace MLDB
} // namespace Datacratic

