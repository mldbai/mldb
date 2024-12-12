/** dataset_feature_space.h                                        -*- C++ -*-
    Jeremy Barnes, 13 March 2015
    Copyright (c) 2015 mldb.ai inc.  All rights reserved.

    This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

    Feature space for datasets to allow training of classifiers.
*/

#pragma once

#include "mldb/plugins/jml/jml/feature_space.h"
#include "mldb/utils/buckets.h"
#include "mldb/sql/dataset_types.h"
#include "mldb/core/dataset.h"
#include "mldb/core/bucket.h"
#include "mldb/plugins/jml/jml/label.h"
#include "mldb/utils/log_fwd.h"


namespace MLDB {


// For internal use
extern const MLDB::Feature labelFeature, weightFeature;


/*****************************************************************************/
/* DATASET FEATURE SPACE                                                     */
/*****************************************************************************/

/** A MLDB::Feature_Space which gets its information from a dataset.  In other
    words, the feature space is implicit in the data in the dataset.

    There is one feature per column.

    Used to hook MLDB in the the JML machine learning system.
*/

struct DatasetFeatureSpace: public MLDB::Feature_Space {
    DatasetFeatureSpace();

    DatasetFeatureSpace(std::shared_ptr<Dataset> dataset,
                        MLDB::Feature_Info labelInfo,
                        const std::set<ColumnPath> & includeColumns,
                        bool bucketize = false);

    struct ColumnInfo {
        ColumnPath columnName;
        MLDB::Feature_Info info;
        int index = -1;
        int distinctValues = -1;

        // These are only filled in if bucketize is true on construction
        BucketList buckets;
        BucketDescriptions bucketDescriptions;

        Utf8String print() const
        {
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
    std::map<MLDB::Feature, ColumnHash> versionTwoMapping;
    std::unordered_map<ColumnHash, MLDB::Feature> versionTwoReverseMapping;

    MLDB::Feature_Info labelInfo;
    std::shared_ptr<spdlog::logger> logger;
    

    /** Encode the given column value into a feature, adding to the given
        feature set.
    */
    void encodeFeature(ColumnHash column, const CellValue & value,
                       std::vector<std::pair<MLDB::Feature, float> > & fset) const;

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
    MLDB::Label encodeLabel(const CellValue & value, bool isRegression) const;

    float encodeValue(const CellValue & value,
                      const ColumnPath & columnName,
                      const MLDB::Feature_Info & info) const;

    virtual MLDB::Feature_Info info(const MLDB::Feature & feature) const override;


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

    static ColumnHash getHashRaw(MLDB::Feature feature);

    ColumnHash getHash(MLDB::Feature feature) const;

    /** Undo the mapping from getHash.  This is the inverse of the getHash
        function.
    */
    MLDB::Feature getFeature(ColumnHash hash) const;

    static MLDB::Feature getFeatureRaw(ColumnHash hash);

    CellValue getValue(const MLDB::Feature & feature, float value) const;

    using MLDB::Feature_Space::print;

    virtual std::string print(const MLDB::Feature_Set & fs) const override;

    virtual std::string print(const MLDB::Feature & feature) const override;

    virtual std::string print(const MLDB::Feature & feature, float value) const override;

    virtual void serialize(MLDB::DB::Store_Writer & store, const MLDB::Feature & feature) const override;

    virtual void reconstitute(MLDB::DB::Store_Reader & store, MLDB::Feature & feature) const override;

    virtual void serialize(MLDB::DB::Store_Writer & store, const MLDB::Feature & feature,
                           float value) const override;

    virtual void reconstitute(MLDB::DB::Store_Reader & store,
                              const MLDB::Feature & feature,
                              float & value) const override;


    /*************************************************************************/
    /* FEATURE SPACE                                                         */
    /*************************************************************************/

    virtual std::string class_id() const override;

    virtual MLDB::Feature_Space_Type type() const override;

    using MLDB::Feature_Space::serialize;
    using MLDB::Feature_Space::reconstitute;

    void reconstitute(MLDB::DB::Store_Reader & store);
    virtual void serialize(MLDB::DB::Store_Writer & store) const override;
    virtual MLDB::Feature_Space * make_copy() const override;

};


std::ostream & operator << (std::ostream & stream,
                            const DatasetFeatureSpace::ColumnInfo & columnInfo);


} // namespace MLDB


