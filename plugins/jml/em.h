// -*- C++ -*-
// em.h
// Mathieu Marquis Bolduc - 30 Dec 2015
// Copyright (c) 2013 mldb.ai inc. All rights reserved.
// This file is part of MLDB. Copyright 2015 mldb.ai inc. All rights reserved.

#pragma once

#include "mldb/utils/distribution.h"
#include "mldb/plugins/jml/algebra/matrix.h"
#include "mldb/types/db/persistent.h"
#include "mldb/vfs/filter_streams.h"
#include "mldb/types/string.h"

namespace MLDB {

struct EstimationMaximisation
{
    struct Cluster {
        double totalWeight;
        distribution<double> centroid;
        MLDB::Matrix<double, 2> covarianceMatrix;
        MLDB::Matrix<double, 2> invertCovarianceMatrix;
        double pseudoDeterminant;
    };

    std::vector<Cluster> clusters;
    std::vector<MLDB::Utf8String> columnNames;

    void
    train(const std::vector<distribution<double>> & points,
          std::vector<int> & in_cluster,
          int nbClusters,
          int maxIterations,
          int randomSeed); 

    int
    assign(const distribution<double> & point,
           MLDB::MatrixRef<double, 2>& distanceMatrix,
           int pIndex) const;
    int
    assign(const distribution<double> & point) const;
  
    void serialize(MLDB::DB::Store_Writer & store) const;
    void reconstitute(MLDB::DB::Store_Reader & store);
    void save(const std::string & filename) const;
    void load(const std::string & filename);

};

} // namespace MLDB
