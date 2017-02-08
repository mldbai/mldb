/** dataset_feature_space.h                                        -*- C++ -*-
    Jeremy Barnes, 13 March 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Feature space for datasets to allow training of classifiers.
*/

#pragma once

#include "mldb/ml/jml/feature_space.h"
#include "mldb/ml/jml/buckets.h"
#include "mldb/sql/dataset_types.h"
#include "mldb/core/dataset.h"
#include "mldb/server/bucket.h"
#include "mldb/ml/jml/label.h"
#include "mldb/utils/log_fwd.h"


namespace MLDB {


// For internal use
extern const ML::Feature labelFeature, weightFeature;


/*****************************************************************************/
/* DATASET FEATURE SPACE                                                     */
/*****************************************************************************/

/** A ML::Feature_Space which gets its information from a dataset.  In other
    words, the feature space is implicit in the data in the dataset.

    There is one feature per column.

    Used to hook MLDB in the the JML machine learning system.
*/

struct DatasetFeatureSpace: public ML::Feature_Space {
    DatasetFeatureSpace();

    DatasetFeatureSpace(std::shared_ptr<Dataset> dataset,
                        ML::Feature_Info labelInfo,
                        const std::set<ColumnPath> & includeColumns,
                        bool bucketize = false);

    struct ColumnInfo {
        ColumnInfo()
            : index(-1), distinctValues(-1)
        {
        }

        ColumnPath columnName;
        ML::Feature_Info info;
        int index;

        int distinctValues;

        // These are only filled in if bucketize is true on construction
        BucketList buckets;
        BucketDescriptions bucketDescriptions;

        Utf8String print() const {
            return "[Column '"+columnName.toUtf8String()
                +"'; Info: "+info.print()+
                "; distinctVals: "+std::to_string(distinctValues)+"]";
        }
    };

    static ColumnInfo getColumnInfo(std::shared_ptr<Dataset> dataset,
                                    const ColumnPath & columnName,
                                    bool bucketize);

    std::unordered_map<ColumnHash, ColumnInfo> columnInfo;

    /// Mapping from the first two 32 bit values of a feature to
    /// a column hash, for classifiers serialized with the old
    /// column hashing scheme (version 2).  This allows the old
    /// feature<->columnHash mapping to be maintained.  For new
    /// files (version 3 and above), these two are empty.
    std::map<ML::Feature, ColumnHash> versionTwoMapping;
    std::unordered_map<ColumnHash, ML::Feature> versionTwoReverseMapping;

    ML::Feature_Info labelInfo;
    std::shared_ptr<spdlog::logger> logger;
    

    /** Encode the given column value into a feature, adding to the given
        feature set.
    */
    void encodeFeature(ColumnHash column, const CellValue & value,
                       std::vector<std::pair<ML::Feature, float> > & fset) const;

    /** Bucketize a feature and return its feature number and bucket
        number.  For when bucketizeNumerics is set to true.
    */
    std::pair<int, int>
    getFeatureBucket(ColumnHash column, const CellValue & value) const;

    /** Encode the column value as a feature, ready to add to a dense
        vector.
    */
    float encodeFeatureValue(ColumnHash column, const CellValue & value) const;

    /** Encode the label into a feature, returning it. */
    ML::Label encodeLabel(const CellValue & value, bool isRegression) const;

    float encodeValue(const CellValue & value,
                      const ColumnPath & columnName,
                      const ML::Feature_Info & info) const;

    virtual ML::Feature_Info info(const ML::Feature & feature) const override;


    /*************************************************************************/
    /* FEATURES                                                              */
    /*************************************************************************/

    /** In JML, features are represented by 3 arbirary 32 bit integers.
        JML doesn't try to interpret them; if all 3 are the same, it's
        the same feature, otherwise it's not.

        The value of a feature is represented by a float.  That mapping is
        done elsewhere; for real valued features it's simple; for categorical
        features, each possible value out of the universe of possible
        values of the category must be mapped onto a separate float.

        Here we define an encoding from a ColumnHash, which is a 64 bit
        integer, to a Feature.  We do this by setting:
        - the first 32 bit integer, type(), to 0
        - the second 32 bit integer, arg1(), to the high 32 bits of the column
          hash
        - the third 32 bit integer, arg2(), to the low 32 bits of the column.
    */

    static ColumnHash getHashRaw(ML::Feature feature);

    ColumnHash getHash(ML::Feature feature) const;

    /** Undo the mapping from getHash.  This is the inverse of the getHash
        function.
    */
    ML::Feature getFeature(ColumnHash hash) const;

    static ML::Feature getFeatureRaw(ColumnHash hash);

    CellValue getValue(const ML::Feature & feature, float value) const;

    using ML::Feature_Space::print;

    virtual std::string print(const ML::Feature_Set & fs) const override;

    virtual std::string print(const ML::Feature & feature) const override;

    virtual std::string print(const ML::Feature & feature, float value) const override;

    virtual void serialize(ML::DB::Store_Writer & store, const ML::Feature & feature) const override;

    virtual void reconstitute(ML::DB::Store_Reader & store, ML::Feature & feature) const override;

    virtual void serialize(ML::DB::Store_Writer & store, const ML::Feature & feature,
                           float value) const override;

    virtual void reconstitute(ML::DB::Store_Reader & store,
                              const ML::Feature & feature,
                              float & value) const override;


    /*************************************************************************/
    /* FEATURE SPACE                                                         */
    /*************************************************************************/

    virtual std::string class_id() const override;

    virtual ML::Feature_Space_Type type() const override;

    virtual Feature_Space * make_copy() const override;

    using ML::Feature_Space::serialize;
    using ML::Feature_Space::reconstitute;

    void reconstitute(ML::DB::Store_Reader & store);
    virtual void serialize(ML::DB::Store_Writer & store) const override;
};


std::ostream & operator << (std::ostream & stream,
                            const DatasetFeatureSpace::ColumnInfo & columnInfo);


} // namespace MLDB


