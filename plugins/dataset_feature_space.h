// This file is part of MLDB. Copyright 2015 Datacratic. All rights reserved.

/** dataset_feature_space.h                                        -*- C++ -*-
    Jeremy Barnes, 13 March 2015
    Copyright (c) 2015 Datacratic Inc.  All rights reserved.

    Feature space for datasets to allow training of classifiers.
*/

#pragma once

#include "mldb/ml/jml/feature_space.h"
#include "mldb/sql/dataset_types.h"

namespace Datacratic {
namespace MLDB {


// For internal use
extern ML::Feature labelFeature, weightFeature;

    

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
                        const std::set<ColumnName> & includeColumns);

    struct ColumnInfo {
        ColumnName columnName;
        ML::Feature_Info info;
        int index;
    };

    std::unordered_map<ColumnHash, ColumnInfo> columnInfo;

    ML::Feature_Info labelInfo;

    /** Encode the given column value into a feature, adding to the given
        feature set.
    */
    void encodeFeature(ColumnHash column, const CellValue & value,
                       std::vector<std::pair<ML::Feature, float> > & fset) const;

    /** Encode the column value as a feature, ready to add to a dense
        vector.
    */
    float encodeFeatureValue(ColumnHash column, const CellValue & value) const;

    /** Encode the label into a feature, returning it. */
    float encodeLabel(const CellValue & value) const;

    float encodeValue(const CellValue & value,
                      const ColumnName & columnName,
                      const ML::Feature_Info & info) const;

    virtual ML::Feature_Info info(const ML::Feature & feature) const;


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

    static ColumnHash getHash(ML::Feature feature);

    /** Undo the mapping from getHash.  This is the inverse of the getHash
        function.
    */

    /*static*/ ML::Feature getFeature(ColumnHash hash) const;

    CellValue getValue(const ML::Feature & feature, float value) const;

    using ML::Feature_Space::print;

    virtual std::string print(const ML::Feature_Set & fs) const JML_OVERRIDE;

    virtual std::string print(const ML::Feature & feature) const JML_OVERRIDE;

    virtual std::string print(const ML::Feature & feature, float value) const JML_OVERRIDE;

    virtual void serialize(ML::DB::Store_Writer & store, const ML::Feature & feature) const JML_OVERRIDE;

    virtual void reconstitute(ML::DB::Store_Reader & store, ML::Feature & feature) const JML_OVERRIDE;

    virtual void serialize(ML::DB::Store_Writer & store, const ML::Feature & feature,
                           float value) const JML_OVERRIDE;

    virtual void reconstitute(ML::DB::Store_Reader & store,
                              const ML::Feature & feature,
                              float & value) const JML_OVERRIDE;


    /*************************************************************************/
    /* FEATURE SPACE                                                         */
    /*************************************************************************/

    virtual std::string class_id() const;

    virtual ML::Feature_Space_Type type() const;

    virtual Feature_Space * make_copy() const;

    using ML::Feature_Space::serialize;
    using ML::Feature_Space::reconstitute;

    void reconstitute(ML::DB::Store_Reader & store);

    void serialize(ML::DB::Store_Writer & store) const;
};


} // namespace MLDB
} // namespace Datacratic

